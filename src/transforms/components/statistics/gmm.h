#pragma once
//
// Gaussian Mixture Model Transform for Financial Time Series Analysis
//
// NOTE: Preprocessing (z-score, min-max, etc.) should be done via separate
// ml_preprocess transforms in the pipeline. This keeps concerns separated
// and allows users to compose their own preprocessing pipelines.
//
#include "dataframe_armadillo_utils.h"
#include "epoch_frame/aliases.h"
#include <epoch_script/transforms/core/itransform.h>
#include <arrow/array.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

#include <armadillo>
#include <mlpack/core.hpp>
#include <mlpack/methods/gmm/gmm.hpp>

namespace epoch_script::transform {

/**
 * @brief Gaussian Mixture Model Transform for Financial Time Series
 *
 * This transform implements GMM-based static clustering for financial data.
 * Unlike HMM, GMM treats each observation independently (no temporal transitions).
 *
 * Template parameter N_COMPONENTS specifies the number of mixture components.
 * Use specialized versions: gmm_2, gmm_3, gmm_4, gmm_5
 *
 * Financial Applications:
 * - Return distribution modeling (fat tails, multiple regimes)
 * - Static regime classification (without temporal dependencies)
 * - Outlier/anomaly detection (low log-likelihood = unusual observation)
 * - Cross-sectional clustering by return characteristics
 *
 * Key differences from HMM:
 * - No temporal transitions (static clustering)
 * - Outputs log-likelihood for anomaly detection
 * - Simpler training (no sequence structure)
 *
 * NOTE: Use ml_zscore or ml_minmax transforms before this for feature scaling.
 */
template <size_t N_COMPONENTS>
class GMMTransform final : public ITransform {
public:
  static_assert(N_COMPONENTS >= 2 && N_COMPONENTS <= 5,
                "GMM supports 2-5 components");

  explicit GMMTransform(const TransformConfiguration &cfg) : ITransform(cfg) {
    // Core GMM parameters
    m_max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations",
                           epoch_script::MetaDataOptionDefinition{300.0})
            .GetInteger());

    m_tolerance =
        cfg.GetOptionValue("tolerance",
                           epoch_script::MetaDataOptionDefinition{1e-10})
            .GetDecimal();

    // Training options
    m_min_training_samples = static_cast<size_t>(
        cfg.GetOptionValue("min_training_samples",
                           epoch_script::MetaDataOptionDefinition{100.0})
            .GetInteger());

    m_lookback_window = static_cast<size_t>(
        cfg.GetOptionValue("lookback_window",
                           epoch_script::MetaDataOptionDefinition{0.0})
            .GetInteger());

    m_trials = static_cast<size_t>(
        cfg.GetOptionValue("trials",
                           epoch_script::MetaDataOptionDefinition{1.0})
            .GetInteger());

    // Covariance type
    auto cov_type = cfg.GetOptionValue(
        "covariance_type",
        epoch_script::MetaDataOptionDefinition{std::string("full")})
        .GetString();
    m_use_diagonal_covariance = (cov_type == "diagonal");
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    const auto cols = GetInputIds();
    if (cols.empty()) {
      throw std::runtime_error(
          "GMMTransform requires at least one input column.");
    }

    // Convert DataFrame to Armadillo matrix
    arma::mat X = utils::MatFromDataFrame(bars, cols);

    if (X.n_rows < m_min_training_samples) {
      throw std::runtime_error("Insufficient training samples for GMM. "
                               "Required: " + std::to_string(m_min_training_samples) +
                               ", Got: " + std::to_string(X.n_rows));
    }

    // Split into training and prediction sets
    arma::mat training_data;
    arma::mat prediction_data;
    epoch_frame::IndexPtr prediction_index;

    if (m_lookback_window > 0 && X.n_rows > m_lookback_window) {
      // Train on first m_lookback_window bars
      training_data = X.rows(0, m_lookback_window - 1);

      // Predict on remaining bars (bars after training window)
      prediction_data = X.rows(m_lookback_window, X.n_rows - 1);

      // Index for prediction window
      prediction_index = bars.index()->iloc(
          {static_cast<int64_t>(m_lookback_window),
           static_cast<int64_t>(X.n_rows)});
    } else {
      // If no lookback specified, use all data for both training and prediction
      // (Research mode - acceptable look-ahead for exploratory analysis)
      training_data = X;
      prediction_data = X;
      prediction_index = bars.index();
    }

    // Train GMM model
    auto gmm = TrainGMM(training_data);

    // Generate predictions
    return GenerateOutputs(prediction_index, gmm, prediction_data);
  }

private:
  // Core GMM parameters
  size_t m_max_iterations{300};
  double m_tolerance{1e-10};

  // Training parameters
  size_t m_min_training_samples{100};
  size_t m_lookback_window{0}; // 0 = use all available data
  size_t m_trials{1};          // Number of EM restarts
  bool m_use_diagonal_covariance{false};

  // Add small regularization to input data
  arma::mat RegularizeInput(const arma::mat &X) const {
    arma::mat X_reg = X;

    // Check covariance matrix condition
    arma::mat cov = arma::cov(X);
    arma::vec eigval;
    arma::mat eigvec;

    if (!arma::eig_sym(eigval, eigvec, cov)) {
      for (size_t i = 0; i < X.n_cols; ++i) {
        X_reg.col(i) += arma::randn(X.n_rows) * 1e-6;
      }
      return X_reg;
    }

    double min_eig = arma::min(eigval);
    double max_eig = arma::max(eigval);
    double condition_number = (min_eig > 1e-15) ? max_eig / min_eig : 1e15;

    if (condition_number > 1e10 || min_eig < 1e-10) {
      double noise_scale = std::max(1e-6, std::abs(min_eig) + 1e-8);
      for (size_t i = 0; i < X_reg.n_cols; ++i) {
        X_reg.col(i) += arma::randn(X_reg.n_rows) * noise_scale;
      }
    }

    return X_reg;
  }

  mlpack::GMM TrainGMM(const arma::mat &X) const {
    const size_t dimensionality = X.n_cols;

    // Check for feature correlation issues
    arma::mat corr_matrix = arma::cor(X);
    for (size_t i = 0; i < dimensionality; ++i) {
      for (size_t j = i + 1; j < dimensionality; ++j) {
        if (std::abs(corr_matrix(i, j)) > 0.95) {
          throw std::runtime_error(
              "GMM training failed: Features " + std::to_string(i) + " and " +
              std::to_string(j) + " are highly correlated (r=" +
              std::to_string(corr_matrix(i, j)) +
              "). Remove one of the correlated features.");
        }
      }
    }

    // Apply input regularization
    arma::mat X_reg = RegularizeInput(X);

    // Transpose for mlpack (expects observations in columns)
    arma::mat X_transposed = X_reg.t();

    // Initialize GMM
    mlpack::GMM gmm(N_COMPONENTS, dimensionality);

    // Train with retry logic
    constexpr int MAX_RETRIES = 3;
    std::string last_error;

    for (int retry = 0; retry < MAX_RETRIES; ++retry) {
      try {
        // Train GMM using EM algorithm
        gmm.Train(X_transposed, m_trials, false);
        return gmm;
      } catch (const std::exception &e) {
        last_error = std::string(e.what());

        if (retry < MAX_RETRIES - 1) {
          // Increase regularization
          double noise_scale = 1e-5 * std::pow(10, retry);
          for (size_t i = 0; i < X_reg.n_cols; ++i) {
            X_reg.col(i) += arma::randn(X_reg.n_rows) * noise_scale;
          }
          X_transposed = X_reg.t();
          gmm = mlpack::GMM(N_COMPONENTS, dimensionality);
        }
      }
    }

    throw std::runtime_error("GMM training failed after " +
                             std::to_string(MAX_RETRIES) + " attempts: " + last_error);
  }

  epoch_frame::DataFrame GenerateOutputs(const epoch_frame::IndexPtr &index,
                                         const mlpack::GMM &gmm,
                                         const arma::mat &X) const {
    const size_t T = X.n_rows;
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    // Transpose for mlpack
    arma::mat X_t = X.t();

    // 1. Component assignment (most likely component)
    arma::Row<size_t> labels;
    gmm.Classify(X_t, labels);

    std::vector<int64_t> component_vec(T);
    for (size_t i = 0; i < T; ++i) {
      component_vec[i] = static_cast<int64_t>(labels[i]);
    }
    output_columns.push_back(GetOutputId("component"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(component_vec));

    // 2. Component probabilities (posterior for each component)
    for (size_t c = 0; c < N_COMPONENTS; ++c) {
      std::vector<double> prob_vec(T);
      for (size_t i = 0; i < T; ++i) {
        // Compute posterior probability P(component | observation)
        double total_prob = gmm.Probability(X_t.col(i));
        if (total_prob > 1e-300) {
          // P(c|x) = P(x|c) * P(c) / P(x)
          double component_likelihood = gmm.Component(c).Probability(X_t.col(i));
          prob_vec[i] = component_likelihood * gmm.Weights()[c] / total_prob;
        } else {
          prob_vec[i] = 0.0;
        }
      }
      output_columns.push_back(GetOutputId("component_" + std::to_string(c) + "_prob"));
      output_arrays.push_back(epoch_frame::factory::array::make_array(prob_vec));
    }

    // 3. Log-likelihood (for anomaly detection)
    std::vector<double> loglik_vec(T);
    for (size_t i = 0; i < T; ++i) {
      loglik_vec[i] = gmm.LogProbability(X_t.col(i));
    }
    output_columns.push_back(GetOutputId("log_likelihood"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(loglik_vec));

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

// Specialized GMM transforms for 2-5 components
using GMM2Transform = GMMTransform<2>;
using GMM3Transform = GMMTransform<3>;
using GMM4Transform = GMMTransform<4>;
using GMM5Transform = GMMTransform<5>;

} // namespace epoch_script::transform
