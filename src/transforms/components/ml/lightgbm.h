#pragma once
//
// LightGBM Gradient Boosting Transforms for Financial ML
//
// Implements:
// - lightgbm_classifier: Binary/multiclass classification
// - lightgbm_regressor: Return prediction
//
// NOTE: Preprocessing (z-score, min-max, etc.) should be done via separate
// ml_preprocess transforms in the pipeline. This keeps concerns separated
// and allows users to compose their own preprocessing pipelines.
//
#include "lightgbm_base.h"
#include "../statistics/dataframe_armadillo_utils.h"
#include "epoch_frame/aliases.h"
#include <epoch_script/transforms/core/itransform.h>
#include <arrow/array.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <armadillo>
#include <cmath>

namespace epoch_script::transform {

/**
 * @brief LightGBM Classifier Transform
 *
 * Gradient boosting classifier for binary and multiclass classification.
 * Outputs prediction (class) and probability.
 *
 * Financial Applications:
 * - Direction prediction (up/down/flat)
 * - Risk classification (high/medium/low)
 * - Regime classification
 *
 * NOTE: Use ml_zscore or ml_minmax transforms before this for feature scaling.
 */
class LightGBMClassifier final : public ITransform {
public:
  explicit LightGBMClassifier(const TransformConfiguration &cfg) : ITransform(cfg) {
    // Model parameters
    m_num_estimators = static_cast<int>(
        cfg.GetOptionValue("num_estimators", MetaDataOptionDefinition{100.0}).GetInteger());

    m_learning_rate =
        cfg.GetOptionValue("learning_rate", MetaDataOptionDefinition{0.1}).GetDecimal();

    m_num_leaves = static_cast<int>(
        cfg.GetOptionValue("num_leaves", MetaDataOptionDefinition{31.0}).GetInteger());

    m_min_data_in_leaf = static_cast<int>(
        cfg.GetOptionValue("min_data_in_leaf", MetaDataOptionDefinition{20.0}).GetInteger());

    auto max_depth_str = cfg.GetOptionValue("max_depth", MetaDataOptionDefinition{std::string("auto")}).GetString();
    m_max_depth = (max_depth_str == "auto") ? -1 : std::stoi(max_depth_str);

    auto boosting_type = cfg.GetOptionValue("boosting_type", MetaDataOptionDefinition{std::string("gbdt")}).GetString();
    m_boosting_type = boosting_type;

    m_lambda_l1 = cfg.GetOptionValue("lambda_l1", MetaDataOptionDefinition{0.0}).GetDecimal();
    m_lambda_l2 = cfg.GetOptionValue("lambda_l2", MetaDataOptionDefinition{0.0}).GetDecimal();

    // Training parameters
    m_lookback_window = static_cast<size_t>(
        cfg.GetOptionValue("lookback_window", MetaDataOptionDefinition{0.0}).GetInteger());

    m_min_training_samples = static_cast<size_t>(
        cfg.GetOptionValue("min_training_samples", MetaDataOptionDefinition{100.0}).GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    // Get feature columns (SLOT) and target column
    const auto feature_cols = GetInputIds();
    if (feature_cols.empty()) {
      throw std::runtime_error("LightGBM requires at least one feature input");
    }

    const auto target_col = GetInputId("target");

    // Convert to matrix
    arma::mat X = utils::MatFromDataFrame(bars, feature_cols);
    arma::vec y = utils::VecFromDataFrame(bars, target_col);

    if (X.n_rows < m_min_training_samples) {
      throw std::runtime_error("Insufficient training samples. Required: " +
                               std::to_string(m_min_training_samples) +
                               ", Got: " + std::to_string(X.n_rows));
    }

    // Split into training and prediction sets
    arma::mat training_X;
    arma::vec training_y;
    arma::mat prediction_X;
    epoch_frame::IndexPtr prediction_index;

    if (m_lookback_window > 0 && X.n_rows > m_lookback_window) {
      training_X = X.rows(0, m_lookback_window - 1);
      training_y = y.subvec(0, m_lookback_window - 1);
      prediction_X = X.rows(m_lookback_window, X.n_rows - 1);
      prediction_index = bars.index()->iloc(
          {static_cast<int64_t>(m_lookback_window),
           static_cast<int64_t>(X.n_rows)});
    } else {
      training_X = X;
      training_y = y;
      prediction_X = X;
      prediction_index = bars.index();
    }

    // Determine number of classes
    std::set<int> unique_classes;
    for (size_t i = 0; i < training_y.n_elem; ++i) {
      unique_classes.insert(static_cast<int>(training_y[i]));
    }
    int num_classes = static_cast<int>(unique_classes.size());

    // Train model
    auto [dataset, booster] = TrainModel(training_X, training_y, num_classes);

    // Generate predictions
    return GenerateOutputs(prediction_index, booster, prediction_X, num_classes);
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
  size_t m_lookback_window{0};
  size_t m_min_training_samples{100};

  std::pair<lightgbm_utils::DatasetWrapper, lightgbm_utils::BoosterWrapper>
  TrainModel(const arma::mat &X, const arma::vec &y, int num_classes) const {
    // Convert to row-major vector
    std::vector<double> data_vec(X.n_rows * X.n_cols);
    for (size_t i = 0; i < X.n_rows; ++i) {
      for (size_t j = 0; j < X.n_cols; ++j) {
        data_vec[i * X.n_cols + j] = X(i, j);
      }
    }

    std::vector<float> labels(y.begin(), y.end());

    // Build parameters
    lightgbm_utils::ParamsBuilder params_builder;
    params_builder
        .SetVerbosity(-1)  // Suppress output
        .SetBoostingType(m_boosting_type)
        .SetLearningRate(m_learning_rate)
        .SetNumLeaves(m_num_leaves)
        .SetMaxDepth(m_max_depth)
        .SetMinDataInLeaf(m_min_data_in_leaf)
        .SetLambdaL1(m_lambda_l1)
        .SetLambdaL2(m_lambda_l2);

    if (num_classes == 2) {
      params_builder.SetObjective("binary");
    } else {
      params_builder.SetObjective("multiclass");
      params_builder.SetNumClass(num_classes);
    }

    std::string params = params_builder.Build();

    // Create dataset
    lightgbm_utils::DatasetWrapper dataset;
    dataset.Create(data_vec, static_cast<int32_t>(X.n_rows),
                   static_cast<int32_t>(X.n_cols), labels, params);

    // Create and train booster
    lightgbm_utils::BoosterWrapper booster;
    booster.Create(dataset, params);
    booster.Train(m_num_estimators);

    return {std::move(dataset), std::move(booster)};
  }

  epoch_frame::DataFrame GenerateOutputs(
      const epoch_frame::IndexPtr &index,
      const lightgbm_utils::BoosterWrapper &booster,
      const arma::mat &X,
      int num_classes) const {

    // Convert to row-major vector
    std::vector<double> data_vec(X.n_rows * X.n_cols);
    for (size_t i = 0; i < X.n_rows; ++i) {
      for (size_t j = 0; j < X.n_cols; ++j) {
        data_vec[i * X.n_cols + j] = X(i, j);
      }
    }

    // Get predictions
    auto preds = booster.Predict(data_vec, static_cast<int32_t>(X.n_rows),
                                  static_cast<int32_t>(X.n_cols));

    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    std::vector<int64_t> predictions(X.n_rows);
    std::vector<double> probabilities(X.n_rows);

    if (num_classes == 2) {
      // Binary classification - predictions are probabilities of class 1
      for (size_t i = 0; i < X.n_rows; ++i) {
        probabilities[i] = preds[i];
        predictions[i] = preds[i] >= 0.5 ? 1 : 0;
      }
    } else {
      // Multiclass - predictions are per-class probabilities
      int classes_per_row = num_classes;
      for (size_t i = 0; i < X.n_rows; ++i) {
        double max_prob = -1.0;
        int max_class = 0;
        for (int c = 0; c < classes_per_row; ++c) {
          double p = preds[i * classes_per_row + c];
          if (p > max_prob) {
            max_prob = p;
            max_class = c;
          }
        }
        predictions[i] = max_class;
        probabilities[i] = max_prob;
      }
    }

    output_columns.push_back(GetOutputId("prediction"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(predictions));

    output_columns.push_back(GetOutputId("probability"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(probabilities));

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

/**
 * @brief LightGBM Regressor Transform
 *
 * Gradient boosting regressor for return prediction.
 * Outputs continuous prediction value.
 *
 * Financial Applications:
 * - Return prediction
 * - Price forecasting
 * - Factor modeling
 *
 * NOTE: Use ml_zscore or ml_minmax transforms before this for feature scaling.
 */
class LightGBMRegressor final : public ITransform {
public:
  explicit LightGBMRegressor(const TransformConfiguration &cfg) : ITransform(cfg) {
    m_num_estimators = static_cast<int>(
        cfg.GetOptionValue("num_estimators", MetaDataOptionDefinition{100.0}).GetInteger());

    m_learning_rate =
        cfg.GetOptionValue("learning_rate", MetaDataOptionDefinition{0.1}).GetDecimal();

    m_num_leaves = static_cast<int>(
        cfg.GetOptionValue("num_leaves", MetaDataOptionDefinition{31.0}).GetInteger());

    m_min_data_in_leaf = static_cast<int>(
        cfg.GetOptionValue("min_data_in_leaf", MetaDataOptionDefinition{20.0}).GetInteger());

    auto max_depth_str = cfg.GetOptionValue("max_depth", MetaDataOptionDefinition{std::string("auto")}).GetString();
    m_max_depth = (max_depth_str == "auto") ? -1 : std::stoi(max_depth_str);

    auto boosting_type = cfg.GetOptionValue("boosting_type", MetaDataOptionDefinition{std::string("gbdt")}).GetString();
    m_boosting_type = boosting_type;

    m_lambda_l1 = cfg.GetOptionValue("lambda_l1", MetaDataOptionDefinition{0.0}).GetDecimal();
    m_lambda_l2 = cfg.GetOptionValue("lambda_l2", MetaDataOptionDefinition{0.0}).GetDecimal();

    m_lookback_window = static_cast<size_t>(
        cfg.GetOptionValue("lookback_window", MetaDataOptionDefinition{0.0}).GetInteger());

    m_min_training_samples = static_cast<size_t>(
        cfg.GetOptionValue("min_training_samples", MetaDataOptionDefinition{100.0}).GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    const auto feature_cols = GetInputIds();
    if (feature_cols.empty()) {
      throw std::runtime_error("LightGBM requires at least one feature input");
    }

    const auto target_col = GetInputId("target");

    arma::mat X = utils::MatFromDataFrame(bars, feature_cols);
    arma::vec y = utils::VecFromDataFrame(bars, target_col);

    if (X.n_rows < m_min_training_samples) {
      throw std::runtime_error("Insufficient training samples. Required: " +
                               std::to_string(m_min_training_samples) +
                               ", Got: " + std::to_string(X.n_rows));
    }

    arma::mat training_X;
    arma::vec training_y;
    arma::mat prediction_X;
    epoch_frame::IndexPtr prediction_index;

    if (m_lookback_window > 0 && X.n_rows > m_lookback_window) {
      training_X = X.rows(0, m_lookback_window - 1);
      training_y = y.subvec(0, m_lookback_window - 1);
      prediction_X = X.rows(m_lookback_window, X.n_rows - 1);
      prediction_index = bars.index()->iloc(
          {static_cast<int64_t>(m_lookback_window),
           static_cast<int64_t>(X.n_rows)});
    } else {
      training_X = X;
      training_y = y;
      prediction_X = X;
      prediction_index = bars.index();
    }

    auto [dataset, booster] = TrainModel(training_X, training_y);

    return GenerateOutputs(prediction_index, booster, prediction_X);
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
  size_t m_lookback_window{0};
  size_t m_min_training_samples{100};

  std::pair<lightgbm_utils::DatasetWrapper, lightgbm_utils::BoosterWrapper>
  TrainModel(const arma::mat &X, const arma::vec &y) const {
    std::vector<double> data_vec(X.n_rows * X.n_cols);
    for (size_t i = 0; i < X.n_rows; ++i) {
      for (size_t j = 0; j < X.n_cols; ++j) {
        data_vec[i * X.n_cols + j] = X(i, j);
      }
    }

    std::vector<float> labels(y.begin(), y.end());

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

    std::string params = params_builder.Build();

    lightgbm_utils::DatasetWrapper dataset;
    dataset.Create(data_vec, static_cast<int32_t>(X.n_rows),
                   static_cast<int32_t>(X.n_cols), labels, params);

    lightgbm_utils::BoosterWrapper booster;
    booster.Create(dataset, params);
    booster.Train(m_num_estimators);

    return {std::move(dataset), std::move(booster)};
  }

  epoch_frame::DataFrame GenerateOutputs(
      const epoch_frame::IndexPtr &index,
      const lightgbm_utils::BoosterWrapper &booster,
      const arma::mat &X) const {

    std::vector<double> data_vec(X.n_rows * X.n_cols);
    for (size_t i = 0; i < X.n_rows; ++i) {
      for (size_t j = 0; j < X.n_cols; ++j) {
        data_vec[i * X.n_cols + j] = X(i, j);
      }
    }

    auto preds = booster.Predict(data_vec, static_cast<int32_t>(X.n_rows),
                                  static_cast<int32_t>(X.n_cols));

    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    output_columns.push_back(GetOutputId("prediction"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(preds));

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

} // namespace epoch_script::transform
