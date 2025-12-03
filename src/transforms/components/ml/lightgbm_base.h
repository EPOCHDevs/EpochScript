#pragma once
//
// LightGBM Base Utilities and RAII Wrappers for Financial ML Transforms
//
#include <LightGBM/c_api.h>
#include <memory>
#include <vector>
#include <stdexcept>
#include <string>
#include <sstream>

namespace epoch_script::transform::lightgbm_utils {

/**
 * @brief Check LightGBM return code and throw on error
 */
inline void CheckError(int ret) {
  if (ret != 0) {
    throw std::runtime_error(std::string("LightGBM error: ") + LGBM_GetLastError());
  }
}

/**
 * @brief RAII wrapper for LightGBM Dataset
 */
class DatasetWrapper {
public:
  DatasetWrapper() = default;
  ~DatasetWrapper() {
    if (handle_) {
      LGBM_DatasetFree(handle_);
    }
  }

  // Non-copyable
  DatasetWrapper(const DatasetWrapper&) = delete;
  DatasetWrapper& operator=(const DatasetWrapper&) = delete;

  // Movable
  DatasetWrapper(DatasetWrapper&& other) noexcept : handle_(other.handle_) {
    other.handle_ = nullptr;
  }
  DatasetWrapper& operator=(DatasetWrapper&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        LGBM_DatasetFree(handle_);
      }
      handle_ = other.handle_;
      other.handle_ = nullptr;
    }
    return *this;
  }

  /**
   * @brief Create dataset from dense matrix (row-major)
   * @param data Row-major data matrix (n_samples x n_features)
   * @param labels Target labels
   * @param params LightGBM parameters string
   */
  void Create(const std::vector<double>& data, int32_t nrow, int32_t ncol,
              const std::vector<float>& labels, const std::string& params) {
    CreateFromPtr(data.data(), nrow, ncol, true, labels, params);
  }

  /**
   * @brief Create dataset from dense matrix pointer (zero-copy for column-major)
   * @param data Pointer to contiguous data (nrow x ncol elements)
   * @param nrow Number of rows (samples)
   * @param ncol Number of columns (features)
   * @param is_row_major True if row-major layout, false if column-major (like arma::mat)
   * @param labels Target labels
   * @param params LightGBM parameters string
   */
  void CreateFromPtr(const double* data, int32_t nrow, int32_t ncol,
                     bool is_row_major, const std::vector<float>& labels,
                     const std::string& params) {
    CheckError(LGBM_DatasetCreateFromMat(
        data,
        C_API_DTYPE_FLOAT64,
        nrow,
        ncol,
        is_row_major ? 1 : 0,
        params.c_str(),
        nullptr,  // no reference dataset
        &handle_
    ));

    // Set labels
    CheckError(LGBM_DatasetSetField(
        handle_,
        "label",
        labels.data(),
        static_cast<int>(labels.size()),
        C_API_DTYPE_FLOAT32
    ));
  }

  DatasetHandle Get() const { return handle_; }

private:
  DatasetHandle handle_{nullptr};
};

/**
 * @brief RAII wrapper for LightGBM Booster
 */
class BoosterWrapper {
public:
  BoosterWrapper() = default;
  ~BoosterWrapper() {
    if (handle_) {
      LGBM_BoosterFree(handle_);
    }
  }

  // Non-copyable
  BoosterWrapper(const BoosterWrapper&) = delete;
  BoosterWrapper& operator=(const BoosterWrapper&) = delete;

  // Movable
  BoosterWrapper(BoosterWrapper&& other) noexcept : handle_(other.handle_) {
    other.handle_ = nullptr;
  }
  BoosterWrapper& operator=(BoosterWrapper&& other) noexcept {
    if (this != &other) {
      if (handle_) {
        LGBM_BoosterFree(handle_);
      }
      handle_ = other.handle_;
      other.handle_ = nullptr;
    }
    return *this;
  }

  /**
   * @brief Create booster from dataset and parameters
   */
  void Create(const DatasetWrapper& dataset, const std::string& params) {
    CheckError(LGBM_BoosterCreate(dataset.Get(), params.c_str(), &handle_));
  }

  /**
   * @brief Train for specified number of iterations
   */
  void Train(int num_iterations) {
    for (int i = 0; i < num_iterations; ++i) {
      int is_finished = 0;
      CheckError(LGBM_BoosterUpdateOneIter(handle_, &is_finished));
      if (is_finished) {
        break;
      }
    }
  }

  /**
   * @brief Get number of classes (for classification)
   */
  int GetNumClasses() const {
    int num_classes = 0;
    CheckError(LGBM_BoosterGetNumClasses(handle_, &num_classes));
    return num_classes;
  }

  /**
   * @brief Predict for dense matrix (row-major)
   * @param data Row-major data matrix
   * @param nrow Number of rows
   * @param ncol Number of columns
   * @param predict_type C_API_PREDICT_NORMAL or C_API_PREDICT_RAW_SCORE
   * @return Predictions
   */
  std::vector<double> Predict(const std::vector<double>& data, int32_t nrow, int32_t ncol,
                               int predict_type = C_API_PREDICT_NORMAL) const {
    return PredictFromPtr(data.data(), nrow, ncol, true, predict_type);
  }

  /**
   * @brief Predict for dense matrix pointer (zero-copy for column-major)
   * @param data Pointer to contiguous data (nrow x ncol elements)
   * @param nrow Number of rows
   * @param ncol Number of columns
   * @param is_row_major True if row-major, false if column-major (like arma::mat)
   * @param predict_type C_API_PREDICT_NORMAL or C_API_PREDICT_RAW_SCORE
   * @return Predictions
   */
  std::vector<double> PredictFromPtr(const double* data, int32_t nrow, int32_t ncol,
                                      bool is_row_major,
                                      int predict_type = C_API_PREDICT_NORMAL) const {
    // Calculate output size
    int64_t out_len = 0;
    CheckError(LGBM_BoosterCalcNumPredict(handle_, nrow, predict_type, 0, -1, &out_len));

    std::vector<double> result(out_len);
    int64_t actual_len = 0;

    CheckError(LGBM_BoosterPredictForMat(
        handle_,
        data,
        C_API_DTYPE_FLOAT64,
        nrow,
        ncol,
        is_row_major ? 1 : 0,
        predict_type,
        0,   // start_iteration
        -1,  // num_iteration (all)
        "",  // parameter
        &actual_len,
        result.data()
    ));

    result.resize(actual_len);
    return result;
  }

  BoosterHandle Get() const { return handle_; }

private:
  BoosterHandle handle_{nullptr};
};

/**
 * @brief Build LightGBM parameters string
 */
class ParamsBuilder {
public:
  ParamsBuilder& SetObjective(const std::string& objective) {
    params_["objective"] = objective;
    return *this;
  }

  ParamsBuilder& SetNumClass(int num_class) {
    params_["num_class"] = std::to_string(num_class);
    return *this;
  }

  ParamsBuilder& SetBoostingType(const std::string& boosting_type) {
    params_["boosting_type"] = boosting_type;
    return *this;
  }

  ParamsBuilder& SetLearningRate(double learning_rate) {
    params_["learning_rate"] = std::to_string(learning_rate);
    return *this;
  }

  ParamsBuilder& SetNumLeaves(int num_leaves) {
    params_["num_leaves"] = std::to_string(num_leaves);
    return *this;
  }

  ParamsBuilder& SetMaxDepth(int max_depth) {
    params_["max_depth"] = std::to_string(max_depth);
    return *this;
  }

  ParamsBuilder& SetMinDataInLeaf(int min_data) {
    params_["min_data_in_leaf"] = std::to_string(min_data);
    return *this;
  }

  ParamsBuilder& SetLambdaL1(double lambda) {
    params_["lambda_l1"] = std::to_string(lambda);
    return *this;
  }

  ParamsBuilder& SetLambdaL2(double lambda) {
    params_["lambda_l2"] = std::to_string(lambda);
    return *this;
  }

  ParamsBuilder& SetVerbosity(int verbosity) {
    params_["verbosity"] = std::to_string(verbosity);
    return *this;
  }

  std::string Build() const {
    std::ostringstream oss;
    bool first = true;
    for (const auto& [key, value] : params_) {
      if (!first) oss << " ";
      oss << key << "=" << value;
      first = false;
    }
    return oss.str();
  }

private:
  std::unordered_map<std::string, std::string> params_;
};

} // namespace epoch_script::transform::lightgbm_utils
