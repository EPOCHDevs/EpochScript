#pragma once
//
// LIBLINEAR-based Linear Models for Financial ML
//
// Implements 4 variants:
// - logistic_l1: L1-regularized Logistic Regression (sparse)
// - logistic_l2: L2-regularized Logistic Regression (stable)
// - svr_l1: L1-regularized Support Vector Regression
// - svr_l2: L2-regularized Support Vector Regression
//
// NOTE: Preprocessing (z-score, min-max, etc.) should be done via separate
// ml_preprocess transforms in the pipeline. This keeps concerns separated
// and allows users to compose their own preprocessing pipelines.
//
#include "liblinear_base.h"
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
 * @brief Solver types matching LIBLINEAR's enum
 */
enum class LinearSolverType {
  L2R_LR = 0,              // L2-regularized Logistic Regression (logistic_l2)
  L1R_LR = 6,              // L1-regularized Logistic Regression (logistic_l1)
  L2R_L2LOSS_SVR = 11,     // L2-regularized L2-loss SVR (svr_l2)
  L2R_L1LOSS_SVR_DUAL = 13 // L2-regularized L1-loss SVR dual (svr_l1)
};

/**
 * @brief Template Linear Model Transform using LIBLINEAR
 *
 * Template parameter SOLVER specifies the solver type.
 * Classifiers output prediction, probability, and decision_value.
 * Regressors output only prediction.
 *
 * Financial Applications:
 * - Direction prediction (logistic models)
 * - Return prediction (SVR models)
 * - Feature importance via L1 sparsity
 *
 * NOTE: Use ml_zscore or ml_minmax transforms before this for feature scaling.
 */
template <LinearSolverType SOLVER>
class LinearModelTransform final : public ITransform {
public:
  static constexpr bool IsClassifier =
      SOLVER == LinearSolverType::L2R_LR ||
      SOLVER == LinearSolverType::L1R_LR;

  static constexpr bool SupportsProbability =
      SOLVER == LinearSolverType::L2R_LR ||
      SOLVER == LinearSolverType::L1R_LR;

  explicit LinearModelTransform(const TransformConfiguration &cfg) : ITransform(cfg) {
    // Regularization parameter
    m_C = cfg.GetOptionValue("C", MetaDataOptionDefinition{1.0}).GetDecimal();

    // Stopping tolerance
    m_eps = cfg.GetOptionValue("eps", MetaDataOptionDefinition{0.01}).GetDecimal();

    // Bias term (-1 to disable)
    m_bias = cfg.GetOptionValue("bias", MetaDataOptionDefinition{1.0}).GetDecimal();

    // Training window
    m_lookback_window = static_cast<size_t>(
        cfg.GetOptionValue("lookback_window", MetaDataOptionDefinition{252.0}).GetInteger());

    m_min_training_samples = static_cast<size_t>(
        cfg.GetOptionValue("min_training_samples", MetaDataOptionDefinition{100.0}).GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    // Suppress LIBLINEAR output
    liblinear_utils::SuppressOutput();

    // Get feature columns (SLOT) and target column
    const auto feature_cols = GetInputIds();
    if (feature_cols.empty()) {
      throw std::runtime_error("LinearModel requires at least one feature input");
    }

    // Target is the last input when using SLOT pattern
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
      // Train on first lookback_window bars
      training_X = X.rows(0, m_lookback_window - 1);
      training_y = y.subvec(0, m_lookback_window - 1);

      // Predict on remaining bars
      prediction_X = X.rows(m_lookback_window, X.n_rows - 1);
      prediction_index = bars.index()->iloc(
          {static_cast<int64_t>(m_lookback_window),
           static_cast<int64_t>(X.n_rows)});
    } else {
      // Research mode - use all data
      training_X = X;
      training_y = y;
      prediction_X = X;
      prediction_index = bars.index();
    }

    // Train model
    auto model = TrainModel(training_X, training_y);

    // Generate predictions
    return GenerateOutputs(prediction_index, model.get(), prediction_X);
  }

private:
  double m_C{1.0};
  double m_eps{0.01};
  double m_bias{1.0};
  size_t m_lookback_window{252};
  size_t m_min_training_samples{100};

  liblinear_utils::ModelPtr TrainModel(const arma::mat &X, const arma::vec &y) const {
    // Convert arma::mat to std::vector<std::vector<double>>
    std::vector<std::vector<double>> X_vec(X.n_rows);
    for (size_t i = 0; i < X.n_rows; ++i) {
      X_vec[i].resize(X.n_cols);
      for (size_t j = 0; j < X.n_cols; ++j) {
        X_vec[i][j] = X(i, j);
      }
    }

    // Convert arma::vec to std::vector<double>
    std::vector<double> y_vec(y.begin(), y.end());

    // Setup problem data
    liblinear_utils::ProblemData prob_data;
    prob_data.Initialize(X_vec, y_vec, m_bias);

    // Setup parameters
    parameter param{};
    param.solver_type = static_cast<int>(SOLVER);
    param.C = m_C;
    param.eps = m_eps;
    param.nr_weight = 0;
    param.weight_label = nullptr;
    param.weight = nullptr;
    param.p = 0.1; // For SVR
    param.nu = 0.5;
    param.init_sol = nullptr;
    param.regularize_bias = 1;

    // Validate parameters
    const char *error_msg = check_parameter(prob_data.GetProblem(), &param);
    if (error_msg) {
      throw std::runtime_error(std::string("LIBLINEAR parameter error: ") + error_msg);
    }

    // Train model
    model *raw_model = train(prob_data.GetProblem(), &param);
    if (!raw_model) {
      throw std::runtime_error("LIBLINEAR training failed");
    }

    return liblinear_utils::ModelPtr(raw_model);
  }

  epoch_frame::DataFrame GenerateOutputs(
      const epoch_frame::IndexPtr &index,
      const model *mdl,
      const arma::mat &X) const {

    const size_t T = X.n_rows;
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    if constexpr (IsClassifier) {
      // Classifier outputs: prediction, probability, decision_value
      std::vector<int64_t> predictions(T);
      std::vector<double> probabilities(T);
      std::vector<double> decision_values(T);

      int nr_class = get_nr_class(mdl);
      std::vector<double> prob_estimates(nr_class);
      std::vector<double> dec_values(nr_class * (nr_class - 1) / 2);

      for (size_t i = 0; i < T; ++i) {
        std::vector<double> row(X.n_cols);
        for (size_t j = 0; j < X.n_cols; ++j) {
          row[j] = X(i, j);
        }

        liblinear_utils::PredictionSample sample(row, m_bias);

        if constexpr (SupportsProbability) {
          predictions[i] = static_cast<int64_t>(
              predict_probability(mdl, sample.Get(), prob_estimates.data()));
          // Use probability of positive class (typically class 1)
          probabilities[i] = (nr_class >= 2) ? prob_estimates[1] : prob_estimates[0];
        } else {
          predictions[i] = static_cast<int64_t>(predict(mdl, sample.Get()));
          probabilities[i] = 0.5; // No probability support
        }

        // Get decision value
        predict_values(mdl, sample.Get(), dec_values.data());
        decision_values[i] = dec_values[0];
      }

      output_columns.push_back(GetOutputId("prediction"));
      output_arrays.push_back(epoch_frame::factory::array::make_array(predictions));

      output_columns.push_back(GetOutputId("probability"));
      output_arrays.push_back(epoch_frame::factory::array::make_array(probabilities));

      output_columns.push_back(GetOutputId("decision_value"));
      output_arrays.push_back(epoch_frame::factory::array::make_array(decision_values));

    } else {
      // Regressor outputs: prediction only
      std::vector<double> predictions(T);

      for (size_t i = 0; i < T; ++i) {
        std::vector<double> row(X.n_cols);
        for (size_t j = 0; j < X.n_cols; ++j) {
          row[j] = X(i, j);
        }

        liblinear_utils::PredictionSample sample(row, m_bias);
        predictions[i] = predict(mdl, sample.Get());
      }

      output_columns.push_back(GetOutputId("prediction"));
      output_arrays.push_back(epoch_frame::factory::array::make_array(predictions));
    }

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

// Type aliases for the 4 variants
using LogisticL1Transform = LinearModelTransform<LinearSolverType::L1R_LR>;
using LogisticL2Transform = LinearModelTransform<LinearSolverType::L2R_LR>;
using SVRL1Transform = LinearModelTransform<LinearSolverType::L2R_L1LOSS_SVR_DUAL>;
using SVRL2Transform = LinearModelTransform<LinearSolverType::L2R_L2LOSS_SVR>;

} // namespace epoch_script::transform
