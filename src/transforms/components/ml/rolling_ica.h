#pragma once
//
// Rolling ICA Transform
//
// Implements rolling/expanding window Independent Component Analysis
// using the shared rolling ML infrastructure.
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
#include <mlpack/methods/radical/radical.hpp>

namespace epoch_script::transform {

/**
 * @brief ICA model state for prediction
 */
struct ICAModel {
  arma::mat unmixing_matrix;  // W matrix for Y = W * X
  arma::vec mean;             // Training data mean for centering
  size_t n_components{0};
};

/**
 * @brief Output vectors for Rolling ICA
 */
struct RollingICAOutputs {
  std::vector<std::vector<double>> independent_components;
};

/**
 * @brief Rolling ICA Transform
 *
 * Performs Independent Component Analysis on a rolling/expanding window
 * basis using RADICAL algorithm. Retrains as the window advances,
 * capturing evolving independent signal structure over time.
 *
 * Unlike PCA which finds uncorrelated components, ICA finds components
 * that are statistically independent (a stronger condition).
 *
 * Financial Applications:
 * - Time-varying blind source separation of market signals
 * - Rolling extraction of hidden independent factors
 * - Adaptive identification of independent risk sources
 * - Dynamic non-Gaussian structure analysis
 *
 * Key Parameters:
 * - window_size: Training window size (default 252)
 * - step_size: How many rows to advance per retrain (default 1)
 * - window_type: "rolling" or "expanding"
 * - noise_std_dev: RADICAL noise parameter (default 0.175)
 * - replicates: Number of replications (default 30)
 * - angles: Number of angles to consider (default 150)
 */
class RollingICATransform final
    : public RollingMLBaseUnsupervised<RollingICATransform, ICAModel> {
public:
  using Base = RollingMLBaseUnsupervised<RollingICATransform, ICAModel>;
  using OutputVectors = RollingICAOutputs;

  explicit RollingICATransform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_noise_std_dev = cfg.GetOptionValue("noise_std_dev",
                                          MetaDataOptionDefinition{0.175}).GetDecimal();

    m_replicates = static_cast<size_t>(
        cfg.GetOptionValue("replicates",
                           MetaDataOptionDefinition{30.0}).GetInteger());

    m_angles = static_cast<size_t>(
        cfg.GetOptionValue("angles",
                           MetaDataOptionDefinition{150.0}).GetInteger());

    // Max components for output sizing (will be clamped by features)
    m_max_components = 10;
  }

  /**
   * @brief Train ICA model on training window
   *
   * @param X Training data (observations x features)
   * @return ICAModel containing unmixing matrix
   */
  [[nodiscard]] ICAModel TrainModel(const arma::mat& X) const {
    const size_t n_features = X.n_cols;

    // Transpose for mlpack (features as rows)
    arma::mat X_t = X.t();

    // Run RADICAL ICA
    mlpack::Radical radical(m_noise_std_dev, m_replicates, m_angles);
    arma::mat Y;  // Independent components
    arma::mat W;  // Unmixing matrix

    try {
      radical.Apply(X_t, Y, W);
    } catch (const std::exception&) {
      // If ICA fails, return identity unmixing (no separation)
      W = arma::eye(n_features, n_features);
    }

    ICAModel model;
    model.unmixing_matrix = W;
    model.mean = arma::mean(X_t, 1);
    model.n_components = n_features;

    return model;
  }

  /**
   * @brief Transform data using trained ICA
   *
   * @param model Trained ICA model
   * @param X Prediction data (observations x features)
   * @param window Current window specification
   * @param outputs Output vectors to fill
   * @param output_offset Starting offset in output vectors
   */
  void Predict(const ICAModel& model,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    // Transpose for transformation
    arma::mat X_t = X.t();
    const size_t n_points = X.n_rows;

    // Center using training mean
    arma::mat centered = X_t;
    for (size_t i = 0; i < centered.n_cols; ++i) {
      centered.col(i) -= model.mean;
    }

    // Apply unmixing: Y = W * X
    arma::mat transformed = model.unmixing_matrix * centered;

    // Fill outputs
    for (size_t i = 0; i < n_points; ++i) {
      const size_t idx = output_offset + i;

      for (size_t k = 0; k < model.n_components && k < m_max_components; ++k) {
        if (k < outputs.independent_components.size()) {
          outputs.independent_components[k][idx] = transformed(k, i);
        }
      }
    }
  }

  /**
   * @brief Get output column names
   */
  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    std::vector<std::string> names;
    for (size_t k = 0; k < m_max_components; ++k) {
      names.push_back(GetOutputId("ic_" + std::to_string(k)));
    }
    return names;
  }

  /**
   * @brief Initialize output vectors
   */
  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.independent_components.resize(m_max_components);
    for (size_t k = 0; k < m_max_components; ++k) {
      outputs.independent_components[k].resize(n_rows, std::numeric_limits<double>::quiet_NaN());
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

    for (size_t k = 0; k < m_max_components; ++k) {
      arrays.push_back(epoch_frame::factory::array::make_array(outputs.independent_components[k]));
    }

    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  double m_noise_std_dev{0.175};
  size_t m_replicates{30};
  size_t m_angles{150};
  size_t m_max_components{10};
};

} // namespace epoch_script::transform
