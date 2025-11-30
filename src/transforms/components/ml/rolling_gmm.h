#pragma once
//
// Rolling Gaussian Mixture Model Transform
//
// Implements rolling/expanding window GMM using the shared rolling ML
// infrastructure. Retrains the GMM on each window and predicts forward.
//
// Uses CRTP base class for shared rolling logic.
//
#include "rolling_ml_base.h"
#include "rolling_ml_metadata.h"
#include "../statistics/dataframe_armadillo_utils.h"
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

#include <armadillo>
#include <mlpack/core.hpp>
#include <mlpack/methods/gmm/gmm.hpp>

namespace epoch_script::transform {

/**
 * @brief Output vectors for Rolling GMM
 */
template<size_t N_COMPONENTS>
struct RollingGMMOutputs {
  std::vector<int64_t> component;
  std::array<std::vector<double>, N_COMPONENTS> component_probs;
  std::vector<double> log_likelihood;
};

/**
 * @brief Rolling GMM Transform
 *
 * Performs Gaussian Mixture Model clustering on a rolling/expanding window
 * basis. Unlike static GMM, this retrains the model as the window advances,
 * capturing evolving distribution characteristics over time.
 *
 * Template parameter N_COMPONENTS specifies the number of Gaussian components.
 * Use specialized versions: rolling_gmm_2, rolling_gmm_3, etc.
 *
 * Financial Applications:
 * - Adaptive return distribution modeling
 * - Walk-forward regime probability estimation
 * - Time-varying anomaly detection (log-likelihood based)
 * - Dynamic mixture weight tracking
 *
 * Key Parameters:
 * - window_size: Training window size (default 252)
 * - step_size: How many rows to advance per retrain (default 1)
 * - window_type: "rolling" or "expanding"
 * - max_iterations: Maximum EM iterations (default 300)
 * - tolerance: EM convergence tolerance (default 1e-10)
 * - trials: Number of EM restarts (default 1)
 */
template<size_t N_COMPONENTS>
class RollingGMMTransform final
    : public RollingMLBaseUnsupervised<RollingGMMTransform<N_COMPONENTS>, mlpack::GMM> {
public:
  using Base = RollingMLBaseUnsupervised<RollingGMMTransform<N_COMPONENTS>, mlpack::GMM>;
  using OutputVectors = RollingGMMOutputs<N_COMPONENTS>;

  static_assert(N_COMPONENTS >= 2 && N_COMPONENTS <= 5,
                "RollingGMM supports 2-5 components");

  explicit RollingGMMTransform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations",
                           MetaDataOptionDefinition{300.0}).GetInteger());

    m_tolerance = cfg.GetOptionValue("tolerance",
                                      MetaDataOptionDefinition{1e-10}).GetDecimal();

    m_trials = static_cast<size_t>(
        cfg.GetOptionValue("trials",
                           MetaDataOptionDefinition{1.0}).GetInteger());
  }

  /**
   * @brief Train GMM model on training window
   *
   * @param X Training data (observations x features)
   * @return Trained mlpack::GMM
   */
  [[nodiscard]] mlpack::GMM TrainModel(const arma::mat& X) const {
    const size_t dimensionality = X.n_cols;

    // Apply input regularization
    arma::mat X_reg = RegularizeInput(X);

    // Transpose for mlpack (expects observations in columns)
    arma::mat X_t = X_reg.t();

    // Initialize GMM
    mlpack::GMM gmm(N_COMPONENTS, dimensionality);

    // Train with retry logic
    constexpr int MAX_RETRIES = 3;

    for (int retry = 0; retry < MAX_RETRIES; ++retry) {
      try {
        gmm.Train(X_t, m_trials, false);
        return gmm;
      } catch (const std::exception&) {
        if (retry < MAX_RETRIES - 1) {
          // Increase regularization on retry
          double noise_scale = 1e-5 * std::pow(10, retry);
          for (size_t i = 0; i < X_reg.n_cols; ++i) {
            X_reg.col(i) += arma::randn(X_reg.n_rows) * noise_scale;
          }
          X_t = X_reg.t();
          gmm = mlpack::GMM(N_COMPONENTS, dimensionality);
        }
      }
    }

    // Return default GMM if all retries fail
    return mlpack::GMM(N_COMPONENTS, dimensionality);
  }

  /**
   * @brief Predict using trained GMM
   *
   * @param gmm Trained GMM model
   * @param X Prediction data (observations x features)
   * @param window Current window specification
   * @param outputs Output vectors to fill
   * @param output_offset Starting offset in output vectors
   */
  void Predict(const mlpack::GMM& gmm,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    // Transpose for mlpack
    arma::mat X_t = X.t();
    const size_t n_points = X.n_rows;

    // Get component assignments
    arma::Row<size_t> labels;
    gmm.Classify(X_t, labels);

    for (size_t i = 0; i < n_points; ++i) {
      const size_t idx = output_offset + i;

      // Component assignment
      outputs.component[idx] = static_cast<int64_t>(labels[i]);

      // Component probabilities (posteriors)
      double total_prob = gmm.Probability(X_t.col(i));
      for (size_t c = 0; c < N_COMPONENTS; ++c) {
        if (total_prob > 1e-300) {
          double component_likelihood = gmm.Component(c).Probability(X_t.col(i));
          outputs.component_probs[c][idx] = component_likelihood * gmm.Weights()[c] / total_prob;
        } else {
          outputs.component_probs[c][idx] = 0.0;
        }
      }

      // Log-likelihood for anomaly detection
      outputs.log_likelihood[idx] = gmm.LogProbability(X_t.col(i));
    }
  }

  /**
   * @brief Get output column names
   */
  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    std::vector<std::string> names;
    names.push_back(this->GetOutputId("component"));
    for (size_t c = 0; c < N_COMPONENTS; ++c) {
      names.push_back(this->GetOutputId("component_" + std::to_string(c) + "_prob"));
    }
    names.push_back(this->GetOutputId("log_likelihood"));
    return names;
  }

  /**
   * @brief Initialize output vectors
   */
  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.component.resize(n_rows, -1);
    for (size_t c = 0; c < N_COMPONENTS; ++c) {
      outputs.component_probs[c].resize(n_rows, std::numeric_limits<double>::quiet_NaN());
    }
    outputs.log_likelihood.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
  }

  /**
   * @brief Build output DataFrame from accumulated results
   */
  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;

    // Component assignment
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.component));

    // Component probabilities
    for (size_t c = 0; c < N_COMPONENTS; ++c) {
      arrays.push_back(epoch_frame::factory::array::make_array(outputs.component_probs[c]));
    }

    // Log-likelihood
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.log_likelihood));

    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  size_t m_max_iterations{300};
  double m_tolerance{1e-10};
  size_t m_trials{1};

  /**
   * @brief Add regularization to input data for numerical stability
   */
  arma::mat RegularizeInput(const arma::mat& X) const {
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
};

// Specialized Rolling GMM transforms for 2-5 components
using RollingGMM2Transform = RollingGMMTransform<2>;
using RollingGMM3Transform = RollingGMMTransform<3>;
using RollingGMM4Transform = RollingGMMTransform<4>;
using RollingGMM5Transform = RollingGMMTransform<5>;

} // namespace epoch_script::transform
