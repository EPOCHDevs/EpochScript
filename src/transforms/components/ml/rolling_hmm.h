#pragma once
//
// Rolling Hidden Markov Model Transform
//
// Implements rolling/expanding window HMM using the shared rolling ML
// infrastructure. Retrains the HMM on each window and predicts forward.
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
#include <mlpack/methods/hmm/hmm.hpp>

namespace epoch_script::transform {

// Concrete Gaussian HMM type
using HMMGaussian = mlpack::HMM<mlpack::GaussianDistribution<>>;

/**
 * @brief Output vectors for Rolling HMM
 */
template<size_t N_STATES>
struct RollingHMMOutputs {
  std::vector<int64_t> state;
  std::array<std::vector<double>, N_STATES> state_probs;
};

/**
 * @brief Rolling HMM Transform
 *
 * Performs Hidden Markov Model training on a rolling/expanding window
 * basis. Unlike static HMM, this retrains the model as the window advances,
 * capturing evolving state dynamics over time.
 *
 * Template parameter N_STATES specifies the number of hidden states.
 * Use specialized versions: rolling_hmm_2, rolling_hmm_3, etc.
 *
 * Financial Applications:
 * - Adaptive market regime detection
 * - Walk-forward state probability estimation
 * - Time-varying transition dynamics
 * - Dynamic risk state assessment
 *
 * Key Parameters:
 * - window_size: Training window size (default 252)
 * - step_size: How many rows to advance per retrain (default 1)
 * - window_type: "rolling" or "expanding"
 * - max_iterations: Maximum Baum-Welch iterations (default 1000)
 * - tolerance: Convergence tolerance (default 1e-5)
 */
template<size_t N_STATES>
class RollingHMMTransform final
    : public RollingMLBaseUnsupervised<RollingHMMTransform<N_STATES>, HMMGaussian> {
public:
  using Base = RollingMLBaseUnsupervised<RollingHMMTransform<N_STATES>, HMMGaussian>;
  using OutputVectors = RollingHMMOutputs<N_STATES>;

  static_assert(N_STATES >= 2 && N_STATES <= 5,
                "RollingHMM supports 2-5 states");

  explicit RollingHMMTransform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations",
                           MetaDataOptionDefinition{1000.0}).GetInteger());

    m_tolerance = cfg.GetOptionValue("tolerance",
                                      MetaDataOptionDefinition{1e-5}).GetDecimal();
  }

  /**
   * @brief Train HMM model on training window
   *
   * @param X Training data (observations x features)
   * @return Trained HMMGaussian
   */
  [[nodiscard]] HMMGaussian TrainModel(const arma::mat& X) const {
    const size_t dimensionality = X.n_cols;

    // Apply input regularization
    arma::mat X_reg = RegularizeInput(X);

    // Initialize Gaussian HMM
    HMMGaussian hmm(N_STATES, mlpack::GaussianDistribution<>(dimensionality),
                    m_tolerance);

    // Prepare sequence (observations in columns)
    std::vector<arma::mat> sequences;
    sequences.emplace_back(X_reg.t());

    // Train with retry logic
    constexpr int MAX_RETRIES = 3;

    for (int retry = 0; retry < MAX_RETRIES; ++retry) {
      try {
        hmm.Train(sequences);
        return hmm;
      } catch (const std::exception&) {
        if (retry < MAX_RETRIES - 1) {
          // Increase regularization on retry
          double noise_scale = 1e-5 * std::pow(10, retry);
          for (size_t i = 0; i < X_reg.n_cols; ++i) {
            X_reg.col(i) += arma::randn(X_reg.n_rows) * noise_scale;
          }
          sequences.clear();
          sequences.emplace_back(X_reg.t());
          hmm = HMMGaussian(N_STATES, mlpack::GaussianDistribution<>(dimensionality),
                            m_tolerance);
        }
      }
    }

    // Return default HMM if all retries fail
    return HMMGaussian(N_STATES, mlpack::GaussianDistribution<>(dimensionality),
                       m_tolerance);
  }

  /**
   * @brief Predict using trained HMM
   *
   * @param hmm Trained HMM model
   * @param X Prediction data (observations x features)
   * @param window Current window specification
   * @param outputs Output vectors to fill
   * @param output_offset Starting offset in output vectors
   */
  void Predict(const HMMGaussian& hmm,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    // Transpose for mlpack (observations in columns)
    arma::mat X_t = X.t();
    const size_t n_points = X.n_rows;

    // Viterbi path (most likely state sequence)
    arma::Row<size_t> viterbi_path;
    hmm.Predict(X_t, viterbi_path);

    // State probabilities via forward-backward
    arma::mat stateLogProb;
    arma::mat forwardLogProb;
    arma::mat backwardLogProb;
    arma::vec logScales;

    try {
      hmm.LogEstimate(X_t, stateLogProb, forwardLogProb, backwardLogProb, logScales);
    } catch (const std::exception&) {
      // If LogEstimate fails, fill with uniform probabilities
      stateLogProb.set_size(N_STATES, n_points);
      stateLogProb.fill(std::log(1.0 / N_STATES));
    }

    arma::mat state_probs = arma::exp(stateLogProb);

    for (size_t i = 0; i < n_points; ++i) {
      const size_t idx = output_offset + i;

      // State assignment (Viterbi)
      outputs.state[idx] = static_cast<int64_t>(viterbi_path[i]);

      // State probabilities
      for (size_t s = 0; s < N_STATES; ++s) {
        outputs.state_probs[s][idx] = state_probs(s, i);
      }
    }
  }

  /**
   * @brief Get output column names
   */
  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    std::vector<std::string> names;
    names.push_back(this->GetOutputId("state"));
    for (size_t s = 0; s < N_STATES; ++s) {
      names.push_back(this->GetOutputId("state_" + std::to_string(s) + "_prob"));
    }
    return names;
  }

  /**
   * @brief Initialize output vectors
   */
  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.state.resize(n_rows, -1);
    for (size_t s = 0; s < N_STATES; ++s) {
      outputs.state_probs[s].resize(n_rows, std::numeric_limits<double>::quiet_NaN());
    }
  }

  /**
   * @brief Build output DataFrame from accumulated results
   */
  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;

    // State assignment
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.state));

    // State probabilities
    for (size_t s = 0; s < N_STATES; ++s) {
      arrays.push_back(epoch_frame::factory::array::make_array(outputs.state_probs[s]));
    }

    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  size_t m_max_iterations{1000};
  double m_tolerance{1e-5};

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

// Specialized Rolling HMM transforms for 2-5 states
using RollingHMM2Transform = RollingHMMTransform<2>;
using RollingHMM3Transform = RollingHMMTransform<3>;
using RollingHMM4Transform = RollingHMMTransform<4>;
using RollingHMM5Transform = RollingHMMTransform<5>;

} // namespace epoch_script::transform
