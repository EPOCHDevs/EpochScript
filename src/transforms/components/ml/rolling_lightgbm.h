#pragma once
//
// Rolling LightGBM Transforms
//
// Implements rolling/expanding window LightGBM using the shared rolling ML
// infrastructure for supervised learning.
//
// Uses CRTP supervised base class for shared rolling logic.
//
#include "rolling_ml_base.h"
#include "rolling_ml_metadata.h"
#include "lightgbm_base.h"
#include "../statistics/dataframe_armadillo_utils.h"
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

#include <armadillo>
#include <set>

namespace epoch_script::transform {

/**
 * @brief Output vectors for Rolling LightGBM Classifier
 */
struct RollingLightGBMClassifierOutputs {
  std::vector<int64_t> prediction;
  std::vector<double> probability;
};

/**
 * @brief Output vectors for Rolling LightGBM Regressor
 */
struct RollingLightGBMRegressorOutputs {
  std::vector<double> prediction;
};

/**
 * @brief LightGBM model wrapper for rolling transforms
 *
 * Wraps the booster so it can be passed around after training.
 */
struct LightGBMModel {
  lightgbm_utils::DatasetWrapper dataset;
  lightgbm_utils::BoosterWrapper booster;
  int num_classes{2};
  int num_features{0};
};

/**
 * @brief Rolling LightGBM Classifier Transform
 *
 * Performs gradient boosting classification on a rolling/expanding window
 * basis. Retrains the model as the window advances.
 *
 * Financial Applications:
 * - Adaptive direction prediction
 * - Walk-forward regime classification
 * - Time-varying risk level prediction
 *
 * Key Parameters:
 * - window_size: Training window size (default 252)
 * - step_size: How many rows to advance per retrain (default 1)
 * - window_type: "rolling" or "expanding"
 * - num_estimators: Number of boosting rounds (default 100)
 * - learning_rate: Step size shrinkage (default 0.1)
 * - num_leaves: Maximum tree leaves (default 31)
 */
class RollingLightGBMClassifier final
    : public RollingMLBaseSupervised<RollingLightGBMClassifier, LightGBMModel> {
public:
  using Base = RollingMLBaseSupervised<RollingLightGBMClassifier, LightGBMModel>;
  using OutputVectors = RollingLightGBMClassifierOutputs;

  explicit RollingLightGBMClassifier(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_num_estimators = static_cast<int>(
        cfg.GetOptionValue("num_estimators",
                           MetaDataOptionDefinition{100.0}).GetInteger());

    m_learning_rate = cfg.GetOptionValue("learning_rate",
                                          MetaDataOptionDefinition{0.1}).GetDecimal();

    m_num_leaves = static_cast<int>(
        cfg.GetOptionValue("num_leaves",
                           MetaDataOptionDefinition{31.0}).GetInteger());

    m_min_data_in_leaf = static_cast<int>(
        cfg.GetOptionValue("min_data_in_leaf",
                           MetaDataOptionDefinition{20.0}).GetInteger());

    auto max_depth_str = cfg.GetOptionValue("max_depth",
                                             MetaDataOptionDefinition{std::string("auto")}).GetString();
    m_max_depth = (max_depth_str == "auto") ? -1 : std::stoi(max_depth_str);

    m_boosting_type = cfg.GetOptionValue("boosting_type",
                                          MetaDataOptionDefinition{std::string("gbdt")}).GetString();

    m_lambda_l1 = cfg.GetOptionValue("lambda_l1",
                                      MetaDataOptionDefinition{0.0}).GetDecimal();
    m_lambda_l2 = cfg.GetOptionValue("lambda_l2",
                                      MetaDataOptionDefinition{0.0}).GetDecimal();

    // Pre-build base params at construction time (everything except objective/num_class)
    lightgbm_utils::ParamsBuilder base_builder;
    base_builder
        .SetVerbosity(-1)
        .SetBoostingType(m_boosting_type)
        .SetLearningRate(m_learning_rate)
        .SetNumLeaves(m_num_leaves)
        .SetMaxDepth(m_max_depth)
        .SetMinDataInLeaf(m_min_data_in_leaf)
        .SetLambdaL1(m_lambda_l1)
        .SetLambdaL2(m_lambda_l2);
    m_base_params = base_builder.Build();
  }

  /**
   * @brief Train LightGBM classifier on training window
   *
   * Uses zero-copy column-major data passing - arma::mat is column-major
   * and LightGBM supports is_row_major=0 for direct pointer access.
   */
  [[nodiscard]] LightGBMModel TrainModel(const arma::mat& X, const arma::vec& y) const {
    // Determine number of classes
    std::set<int> unique_classes;
    for (size_t i = 0; i < y.n_elem; ++i) {
      unique_classes.insert(static_cast<int>(y[i]));
    }
    int num_classes = static_cast<int>(unique_classes.size());

    // Must have at least 2 classes to train a classifier
    if (num_classes < 2) {
      throw std::runtime_error(
        "Cannot train classifier: training window contains only " +
        std::to_string(num_classes) + " unique class(es). Need at least 2 classes.");
    }

    // Convert labels to float (small copy, unavoidable)
    std::vector<float> labels(y.begin(), y.end());

    // Append objective params at runtime (num_classes is data-dependent)
    std::string params = m_base_params;
    if (num_classes == 2) {
      params += " objective=binary";
    } else {
      params += " objective=multiclass num_class=" + std::to_string(num_classes);
    }

    LightGBMModel model;
    model.num_classes = num_classes;
    model.num_features = static_cast<int>(X.n_cols);

    // Zero-copy: pass arma::mat memptr directly (column-major)
    model.dataset.CreateFromPtr(X.memptr(), static_cast<int32_t>(X.n_rows),
                                static_cast<int32_t>(X.n_cols), false, labels, params);
    model.booster.Create(model.dataset, params);
    model.booster.Train(m_num_estimators);

    return model;
  }

  /**
   * @brief Predict using trained classifier
   *
   * Uses zero-copy column-major data passing.
   */
  void Predict(const LightGBMModel& model,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    // Zero-copy: pass arma::mat memptr directly (column-major)
    auto preds = model.booster.PredictFromPtr(X.memptr(), static_cast<int32_t>(X.n_rows),
                                               static_cast<int32_t>(X.n_cols), false);

    for (size_t i = 0; i < X.n_rows; ++i) {
      const size_t idx = output_offset + i;

      if (model.num_classes == 2) {
        outputs.probability[idx] = preds[i];
        outputs.prediction[idx] = preds[i] >= 0.5 ? 1 : 0;
      } else {
        double max_prob = -1.0;
        int max_class = 0;
        for (int c = 0; c < model.num_classes; ++c) {
          double p = preds[i * model.num_classes + c];
          if (p > max_prob) {
            max_prob = p;
            max_class = c;
          }
        }
        outputs.prediction[idx] = max_class;
        outputs.probability[idx] = max_prob;
      }
    }
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return {
      GetOutputId("prediction"),
      GetOutputId("probability")
    };
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.prediction.resize(n_rows, -1);
    outputs.probability.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.prediction));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.probability));
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  int m_num_estimators{100};
  double m_learning_rate{0.1};
  int m_num_leaves{31};
  int m_min_data_in_leaf{20};
  int m_max_depth{-1};
  std::string m_boosting_type{"gbdt"};
  double m_lambda_l1{0.0};
  double m_lambda_l2{0.0};
  std::string m_base_params;  // Pre-built at construction time
};

/**
 * @brief Rolling LightGBM Regressor Transform
 *
 * Performs gradient boosting regression on a rolling/expanding window
 * basis. Retrains the model as the window advances.
 *
 * Financial Applications:
 * - Adaptive return prediction
 * - Walk-forward price forecasting
 * - Time-varying factor modeling
 */
class RollingLightGBMRegressor final
    : public RollingMLBaseSupervised<RollingLightGBMRegressor, LightGBMModel> {
public:
  using Base = RollingMLBaseSupervised<RollingLightGBMRegressor, LightGBMModel>;
  using OutputVectors = RollingLightGBMRegressorOutputs;

  explicit RollingLightGBMRegressor(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_num_estimators = static_cast<int>(
        cfg.GetOptionValue("num_estimators",
                           MetaDataOptionDefinition{100.0}).GetInteger());

    m_learning_rate = cfg.GetOptionValue("learning_rate",
                                          MetaDataOptionDefinition{0.1}).GetDecimal();

    m_num_leaves = static_cast<int>(
        cfg.GetOptionValue("num_leaves",
                           MetaDataOptionDefinition{31.0}).GetInteger());

    m_min_data_in_leaf = static_cast<int>(
        cfg.GetOptionValue("min_data_in_leaf",
                           MetaDataOptionDefinition{20.0}).GetInteger());

    auto max_depth_str = cfg.GetOptionValue("max_depth",
                                             MetaDataOptionDefinition{std::string("auto")}).GetString();
    m_max_depth = (max_depth_str == "auto") ? -1 : std::stoi(max_depth_str);

    m_boosting_type = cfg.GetOptionValue("boosting_type",
                                          MetaDataOptionDefinition{std::string("gbdt")}).GetString();

    m_lambda_l1 = cfg.GetOptionValue("lambda_l1",
                                      MetaDataOptionDefinition{0.0}).GetDecimal();
    m_lambda_l2 = cfg.GetOptionValue("lambda_l2",
                                      MetaDataOptionDefinition{0.0}).GetDecimal();

    // Fully pre-build params at construction time (objective is always regression)
    lightgbm_utils::ParamsBuilder params_builder;
    params_builder
        .SetVerbosity(-1)
        .SetObjective("regression")
        .SetBoostingType(m_boosting_type)
        .SetLearningRate(m_learning_rate)
        .SetNumLeaves(m_num_leaves)
        .SetMaxDepth(m_max_depth)
        .SetMinDataInLeaf(m_min_data_in_leaf)
        .SetLambdaL1(m_lambda_l1)
        .SetLambdaL2(m_lambda_l2);
    m_params = params_builder.Build();
  }

  /**
   * @brief Train LightGBM regressor on training window
   *
   * Uses zero-copy column-major data passing - arma::mat is column-major
   * and LightGBM supports is_row_major=0 for direct pointer access.
   */
  [[nodiscard]] LightGBMModel TrainModel(const arma::mat& X, const arma::vec& y) const {
    // Convert labels to float (small copy, unavoidable)
    std::vector<float> labels(y.begin(), y.end());

    LightGBMModel model;
    model.num_classes = 0;  // Regression
    model.num_features = static_cast<int>(X.n_cols);

    // Zero-copy: pass arma::mat memptr directly (column-major)
    model.dataset.CreateFromPtr(X.memptr(), static_cast<int32_t>(X.n_rows),
                                static_cast<int32_t>(X.n_cols), false, labels, m_params);
    model.booster.Create(model.dataset, m_params);
    model.booster.Train(m_num_estimators);

    return model;
  }

  /**
   * @brief Predict using trained regressor
   *
   * Uses zero-copy column-major data passing.
   */
  void Predict(const LightGBMModel& model,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    // Zero-copy: pass arma::mat memptr directly (column-major)
    auto preds = model.booster.PredictFromPtr(X.memptr(), static_cast<int32_t>(X.n_rows),
                                               static_cast<int32_t>(X.n_cols), false);

    for (size_t i = 0; i < X.n_rows; ++i) {
      outputs.prediction[output_offset + i] = preds[i];
    }
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return { GetOutputId("prediction") };
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.prediction.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.prediction));
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  int m_num_estimators{100};
  double m_learning_rate{0.1};
  int m_num_leaves{31};
  int m_min_data_in_leaf{20};
  int m_max_depth{-1};
  std::string m_boosting_type{"gbdt"};
  double m_lambda_l1{0.0};
  double m_lambda_l2{0.0};
  std::string m_params;  // Fully pre-built at construction time
};

} // namespace epoch_script::transform
