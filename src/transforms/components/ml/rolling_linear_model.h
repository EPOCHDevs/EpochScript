#pragma once
//
// Rolling LIBLINEAR-based Linear Models
//
// Implements rolling/expanding window versions of:
// - rolling_logistic_l1: L1-regularized Logistic Regression
// - rolling_logistic_l2: L2-regularized Logistic Regression
// - rolling_svr_l1: L1-regularized Support Vector Regression
// - rolling_svr_l2: L2-regularized Support Vector Regression
//
// NOTE: Due to CRTP complexity with SFINAE, we split into separate
// classifier and regressor implementations.
//
#include "rolling_ml_base.h"
#include "rolling_ml_metadata.h"
#include "liblinear_base.h"
#include "../statistics/dataframe_armadillo_utils.h"
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/blocked_range.h>

#include <armadillo>

namespace epoch_script::transform {

/**
 * @brief LIBLINEAR model wrapper for rolling transforms
 */
struct LinearModelWrapper {
  liblinear_utils::ModelPtr model;
  int nr_class{2};
  double bias{1.0};
};

/**
 * @brief Output vectors for Rolling Linear Classifier
 */
struct RollingLinearClassifierOutputs {
  std::vector<int64_t> prediction;
  std::vector<double> probability;
  std::vector<double> decision_value;
};

/**
 * @brief Output vectors for Rolling Linear Regressor
 */
struct RollingLinearRegressorOutputs {
  std::vector<double> prediction;
};

/**
 * @brief Rolling Logistic L1 Classifier (L1-regularized)
 *
 * Sparse logistic regression with L1 regularization.
 * Performs walk-forward training and prediction.
 */
class RollingLogisticL1Transform final
    : public RollingMLBaseSupervised<RollingLogisticL1Transform, LinearModelWrapper> {
public:
  using Base = RollingMLBaseSupervised<RollingLogisticL1Transform, LinearModelWrapper>;
  using OutputVectors = RollingLinearClassifierOutputs;

  explicit RollingLogisticL1Transform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_C = cfg.GetOptionValue("C", MetaDataOptionDefinition{1.0}).GetDecimal();
    m_eps = cfg.GetOptionValue("eps", MetaDataOptionDefinition{0.01}).GetDecimal();
    m_bias = cfg.GetOptionValue("bias", MetaDataOptionDefinition{1.0}).GetDecimal();
    liblinear_utils::SetupLogging();
  }

  [[nodiscard]] LinearModelWrapper TrainModel(const arma::mat& X, const arma::vec& y) const {
    return TrainLinearModel(X, y, 6, m_C, m_eps, m_bias);  // L1R_LR = 6
  }

  void Predict(const LinearModelWrapper& wrapper, const arma::mat& X,
               const ml_utils::WindowSpec&, OutputVectors& outputs, size_t offset) const {
    PredictClassifier(wrapper, X, outputs, offset);
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return { GetOutputId("prediction"), GetOutputId("probability"), GetOutputId("decision_value") };
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.prediction.resize(n_rows, -1);
    outputs.probability.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
    outputs.decision_value.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index, const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.prediction));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.probability));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.decision_value));
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  double m_C{1.0}, m_eps{0.01}, m_bias{1.0};

  LinearModelWrapper TrainLinearModel(const arma::mat& X, const arma::vec& y,
                                       int solver_type, double C, double eps, double bias) const {
    std::vector<std::vector<double>> X_vec(X.n_rows);
    for (size_t i = 0; i < X.n_rows; ++i) {
      X_vec[i].resize(X.n_cols);
      for (size_t j = 0; j < X.n_cols; ++j) X_vec[i][j] = X(i, j);
    }
    std::vector<double> y_vec(y.begin(), y.end());

    liblinear_utils::ProblemData prob_data;
    prob_data.Initialize(X_vec, y_vec, bias);

    parameter param{};
    param.solver_type = solver_type;
    param.C = C;
    param.eps = eps;
    param.nr_weight = 0;
    param.weight_label = nullptr;
    param.weight = nullptr;
    param.p = 0.1;
    param.nu = 0.5;
    param.init_sol = nullptr;
    param.regularize_bias = 1;

    model* raw_model = train(prob_data.GetProblem(), &param);
    if (!raw_model) throw std::runtime_error("LIBLINEAR training failed");

    LinearModelWrapper wrapper;
    wrapper.model = liblinear_utils::ModelPtr(raw_model);
    wrapper.nr_class = get_nr_class(raw_model);
    wrapper.bias = bias;
    return wrapper;
  }

  void PredictClassifier(const LinearModelWrapper& wrapper, const arma::mat& X,
                          RollingLinearClassifierOutputs& outputs, size_t offset) const {
    const model* mdl = wrapper.model.get();
    const int nr_class = wrapper.nr_class;
    const double bias = wrapper.bias;
    const size_t n_cols = X.n_cols;
    const size_t dec_size = static_cast<size_t>(std::max(1, nr_class * (nr_class - 1) / 2));

    // Parallel prediction - each row is independent
    oneapi::tbb::parallel_for(
        oneapi::tbb::blocked_range<size_t>(0, X.n_rows),
        [&, mdl, nr_class, bias, n_cols, dec_size, offset](const oneapi::tbb::blocked_range<size_t>& range) {
          // Thread-local buffers
          std::vector<double> prob_estimates(nr_class);
          std::vector<double> dec_values(dec_size);
          std::vector<double> row(n_cols);

          for (size_t i = range.begin(); i < range.end(); ++i) {
            for (size_t j = 0; j < n_cols; ++j) row[j] = X(i, j);
            liblinear_utils::PredictionSample sample(row, bias);

            outputs.prediction[offset + i] = static_cast<int64_t>(
                predict_probability(mdl, sample.Get(), prob_estimates.data()));
            outputs.probability[offset + i] = (nr_class >= 2) ? prob_estimates[1] : prob_estimates[0];
            predict_values(mdl, sample.Get(), dec_values.data());
            outputs.decision_value[offset + i] = dec_values[0];
          }
        });
  }
};

/**
 * @brief Rolling Logistic L2 Classifier (L2-regularized)
 */
class RollingLogisticL2Transform final
    : public RollingMLBaseSupervised<RollingLogisticL2Transform, LinearModelWrapper> {
public:
  using Base = RollingMLBaseSupervised<RollingLogisticL2Transform, LinearModelWrapper>;
  using OutputVectors = RollingLinearClassifierOutputs;

  explicit RollingLogisticL2Transform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_C = cfg.GetOptionValue("C", MetaDataOptionDefinition{1.0}).GetDecimal();
    m_eps = cfg.GetOptionValue("eps", MetaDataOptionDefinition{0.01}).GetDecimal();
    m_bias = cfg.GetOptionValue("bias", MetaDataOptionDefinition{1.0}).GetDecimal();
    liblinear_utils::SetupLogging();
  }

  [[nodiscard]] LinearModelWrapper TrainModel(const arma::mat& X, const arma::vec& y) const {
    return TrainLinearModel(X, y, 0, m_C, m_eps, m_bias);  // L2R_LR = 0
  }

  void Predict(const LinearModelWrapper& wrapper, const arma::mat& X,
               const ml_utils::WindowSpec&, OutputVectors& outputs, size_t offset) const {
    PredictClassifier(wrapper, X, outputs, offset);
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return { GetOutputId("prediction"), GetOutputId("probability"), GetOutputId("decision_value") };
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.prediction.resize(n_rows, -1);
    outputs.probability.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
    outputs.decision_value.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index, const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.prediction));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.probability));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.decision_value));
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  double m_C{1.0}, m_eps{0.01}, m_bias{1.0};

  LinearModelWrapper TrainLinearModel(const arma::mat& X, const arma::vec& y,
                                       int solver_type, double C, double eps, double bias) const {
    std::vector<std::vector<double>> X_vec(X.n_rows);
    for (size_t i = 0; i < X.n_rows; ++i) {
      X_vec[i].resize(X.n_cols);
      for (size_t j = 0; j < X.n_cols; ++j) X_vec[i][j] = X(i, j);
    }
    std::vector<double> y_vec(y.begin(), y.end());

    liblinear_utils::ProblemData prob_data;
    prob_data.Initialize(X_vec, y_vec, bias);

    parameter param{};
    param.solver_type = solver_type;
    param.C = C;
    param.eps = eps;
    param.nr_weight = 0;
    param.weight_label = nullptr;
    param.weight = nullptr;
    param.p = 0.1;
    param.nu = 0.5;
    param.init_sol = nullptr;
    param.regularize_bias = 1;

    model* raw_model = train(prob_data.GetProblem(), &param);
    if (!raw_model) throw std::runtime_error("LIBLINEAR training failed");

    LinearModelWrapper wrapper;
    wrapper.model = liblinear_utils::ModelPtr(raw_model);
    wrapper.nr_class = get_nr_class(raw_model);
    wrapper.bias = bias;
    return wrapper;
  }

  void PredictClassifier(const LinearModelWrapper& wrapper, const arma::mat& X,
                          RollingLinearClassifierOutputs& outputs, size_t offset) const {
    const model* mdl = wrapper.model.get();
    const int nr_class = wrapper.nr_class;
    const double bias = wrapper.bias;
    const size_t n_cols = X.n_cols;
    const size_t dec_size = static_cast<size_t>(std::max(1, nr_class * (nr_class - 1) / 2));

    // Parallel prediction - each row is independent
    oneapi::tbb::parallel_for(
        oneapi::tbb::blocked_range<size_t>(0, X.n_rows),
        [&, mdl, nr_class, bias, n_cols, dec_size, offset](const oneapi::tbb::blocked_range<size_t>& range) {
          // Thread-local buffers
          std::vector<double> prob_estimates(nr_class);
          std::vector<double> dec_values(dec_size);
          std::vector<double> row(n_cols);

          for (size_t i = range.begin(); i < range.end(); ++i) {
            for (size_t j = 0; j < n_cols; ++j) row[j] = X(i, j);
            liblinear_utils::PredictionSample sample(row, bias);

            outputs.prediction[offset + i] = static_cast<int64_t>(
                predict_probability(mdl, sample.Get(), prob_estimates.data()));
            outputs.probability[offset + i] = (nr_class >= 2) ? prob_estimates[1] : prob_estimates[0];
            predict_values(mdl, sample.Get(), dec_values.data());
            outputs.decision_value[offset + i] = dec_values[0];
          }
        });
  }
};

/**
 * @brief Rolling SVR L1 (L1-loss Support Vector Regression)
 */
class RollingSVRL1Transform final
    : public RollingMLBaseSupervised<RollingSVRL1Transform, LinearModelWrapper> {
public:
  using Base = RollingMLBaseSupervised<RollingSVRL1Transform, LinearModelWrapper>;
  using OutputVectors = RollingLinearRegressorOutputs;

  explicit RollingSVRL1Transform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_C = cfg.GetOptionValue("C", MetaDataOptionDefinition{1.0}).GetDecimal();
    m_eps = cfg.GetOptionValue("eps", MetaDataOptionDefinition{0.01}).GetDecimal();
    m_bias = cfg.GetOptionValue("bias", MetaDataOptionDefinition{1.0}).GetDecimal();
    liblinear_utils::SetupLogging();
  }

  [[nodiscard]] LinearModelWrapper TrainModel(const arma::mat& X, const arma::vec& y) const {
    return TrainLinearModel(X, y, 13, m_C, m_eps, m_bias);  // L2R_L1LOSS_SVR_DUAL = 13
  }

  void Predict(const LinearModelWrapper& wrapper, const arma::mat& X,
               const ml_utils::WindowSpec&, OutputVectors& outputs, size_t offset) const {
    PredictRegressor(wrapper, X, outputs, offset);
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return { GetOutputId("prediction") };
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.prediction.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index, const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.prediction));
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  double m_C{1.0}, m_eps{0.01}, m_bias{1.0};

  LinearModelWrapper TrainLinearModel(const arma::mat& X, const arma::vec& y,
                                       int solver_type, double C, double eps, double bias) const {
    std::vector<std::vector<double>> X_vec(X.n_rows);
    for (size_t i = 0; i < X.n_rows; ++i) {
      X_vec[i].resize(X.n_cols);
      for (size_t j = 0; j < X.n_cols; ++j) X_vec[i][j] = X(i, j);
    }
    std::vector<double> y_vec(y.begin(), y.end());

    liblinear_utils::ProblemData prob_data;
    prob_data.Initialize(X_vec, y_vec, bias);

    parameter param{};
    param.solver_type = solver_type;
    param.C = C;
    param.eps = eps;
    param.nr_weight = 0;
    param.weight_label = nullptr;
    param.weight = nullptr;
    param.p = 0.1;
    param.nu = 0.5;
    param.init_sol = nullptr;
    param.regularize_bias = 1;

    model* raw_model = train(prob_data.GetProblem(), &param);
    if (!raw_model) throw std::runtime_error("LIBLINEAR training failed");

    LinearModelWrapper wrapper;
    wrapper.model = liblinear_utils::ModelPtr(raw_model);
    wrapper.nr_class = get_nr_class(raw_model);
    wrapper.bias = bias;
    return wrapper;
  }

  void PredictRegressor(const LinearModelWrapper& wrapper, const arma::mat& X,
                         RollingLinearRegressorOutputs& outputs, size_t offset) const {
    const model* mdl = wrapper.model.get();
    const double bias = wrapper.bias;
    const size_t n_cols = X.n_cols;

    // Parallel prediction - each row is independent
    oneapi::tbb::parallel_for(
        oneapi::tbb::blocked_range<size_t>(0, X.n_rows),
        [&, mdl, bias, n_cols, offset](const oneapi::tbb::blocked_range<size_t>& range) {
          // Thread-local buffer
          std::vector<double> row(n_cols);

          for (size_t i = range.begin(); i < range.end(); ++i) {
            for (size_t j = 0; j < n_cols; ++j) row[j] = X(i, j);
            liblinear_utils::PredictionSample sample(row, bias);
            outputs.prediction[offset + i] = predict(mdl, sample.Get());
          }
        });
  }
};

/**
 * @brief Rolling SVR L2 (L2-loss Support Vector Regression)
 */
class RollingSVRL2Transform final
    : public RollingMLBaseSupervised<RollingSVRL2Transform, LinearModelWrapper> {
public:
  using Base = RollingMLBaseSupervised<RollingSVRL2Transform, LinearModelWrapper>;
  using OutputVectors = RollingLinearRegressorOutputs;

  explicit RollingSVRL2Transform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_C = cfg.GetOptionValue("C", MetaDataOptionDefinition{1.0}).GetDecimal();
    m_eps = cfg.GetOptionValue("eps", MetaDataOptionDefinition{0.01}).GetDecimal();
    m_bias = cfg.GetOptionValue("bias", MetaDataOptionDefinition{1.0}).GetDecimal();
    liblinear_utils::SetupLogging();
  }

  [[nodiscard]] LinearModelWrapper TrainModel(const arma::mat& X, const arma::vec& y) const {
    return TrainLinearModel(X, y, 11, m_C, m_eps, m_bias);  // L2R_L2LOSS_SVR = 11
  }

  void Predict(const LinearModelWrapper& wrapper, const arma::mat& X,
               const ml_utils::WindowSpec&, OutputVectors& outputs, size_t offset) const {
    PredictRegressor(wrapper, X, outputs, offset);
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return { GetOutputId("prediction") };
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.prediction.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index, const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.prediction));
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  double m_C{1.0}, m_eps{0.01}, m_bias{1.0};

  LinearModelWrapper TrainLinearModel(const arma::mat& X, const arma::vec& y,
                                       int solver_type, double C, double eps, double bias) const {
    std::vector<std::vector<double>> X_vec(X.n_rows);
    for (size_t i = 0; i < X.n_rows; ++i) {
      X_vec[i].resize(X.n_cols);
      for (size_t j = 0; j < X.n_cols; ++j) X_vec[i][j] = X(i, j);
    }
    std::vector<double> y_vec(y.begin(), y.end());

    liblinear_utils::ProblemData prob_data;
    prob_data.Initialize(X_vec, y_vec, bias);

    parameter param{};
    param.solver_type = solver_type;
    param.C = C;
    param.eps = eps;
    param.nr_weight = 0;
    param.weight_label = nullptr;
    param.weight = nullptr;
    param.p = 0.1;
    param.nu = 0.5;
    param.init_sol = nullptr;
    param.regularize_bias = 1;

    model* raw_model = train(prob_data.GetProblem(), &param);
    if (!raw_model) throw std::runtime_error("LIBLINEAR training failed");

    LinearModelWrapper wrapper;
    wrapper.model = liblinear_utils::ModelPtr(raw_model);
    wrapper.nr_class = get_nr_class(raw_model);
    wrapper.bias = bias;
    return wrapper;
  }

  void PredictRegressor(const LinearModelWrapper& wrapper, const arma::mat& X,
                         RollingLinearRegressorOutputs& outputs, size_t offset) const {
    const model* mdl = wrapper.model.get();
    const double bias = wrapper.bias;
    const size_t n_cols = X.n_cols;

    // Parallel prediction - each row is independent
    oneapi::tbb::parallel_for(
        oneapi::tbb::blocked_range<size_t>(0, X.n_rows),
        [&, mdl, bias, n_cols, offset](const oneapi::tbb::blocked_range<size_t>& range) {
          // Thread-local buffer
          std::vector<double> row(n_cols);

          for (size_t i = range.begin(); i < range.end(); ++i) {
            for (size_t j = 0; j < n_cols; ++j) row[j] = X(i, j);
            liblinear_utils::PredictionSample sample(row, bias);
            outputs.prediction[offset + i] = predict(mdl, sample.Get());
          }
        });
  }
};

} // namespace epoch_script::transform
