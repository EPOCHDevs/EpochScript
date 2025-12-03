#pragma once
//
// Rolling PCA Transform
//
// Implements rolling/expanding window PCA using the shared rolling ML
// infrastructure. Retrains PCA on each window and projects forward.
//
// Uses CRTP base class for shared rolling logic.
// Template parameter N_COMPONENTS specifies the fixed number of PCs.
//
#include "rolling_ml_base.h"
#include "rolling_ml_metadata.h"
#include "../statistics/dataframe_armadillo_utils.h"
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

#include <armadillo>
#include <mlpack/core.hpp>
#include <mlpack/methods/pca/pca.hpp>

namespace epoch_script::transform {

/**
 * @brief PCA model state for prediction
 */
struct PCAModel {
  arma::mat eigenvectors;       // Principal component directions
  arma::vec eigenvalues;        // Explained variance per component
  arma::vec mean;               // Training data mean for centering
  size_t n_components{0};       // Number of components to output
  double explained_variance{0}; // Total explained variance ratio
};

/**
 * @brief Output vectors for Rolling PCA (templated on N components)
 */
template<size_t N>
struct RollingPCAOutputsN {
  std::array<std::vector<double>, N> principal_components;
  std::vector<double> explained_variance_ratio;
};

/**
 * @brief Rolling PCA Transform (N components version)
 *
 * Performs Principal Component Analysis on a rolling/expanding window
 * basis with exactly N components. Retrains PCA as the window advances,
 * capturing evolving covariance structure over time.
 *
 * Financial Applications:
 * - Time-varying factor extraction from correlated assets
 * - Rolling risk factor decomposition (yield curve: 3 factors)
 * - Adaptive dimensionality reduction
 * - Dynamic market driver identification (equity: 5-6 factors)
 *
 * Key Parameters:
 * - window_size: Training window size (default 252)
 * - step_size: How many rows to advance per retrain (default 1)
 * - window_type: "rolling" or "expanding"
 * - scale_data: Standardize features before PCA (default true)
 */
template<size_t N_COMPONENTS>
class RollingPCATransformN final
    : public RollingMLBaseUnsupervised<RollingPCATransformN<N_COMPONENTS>, PCAModel> {
public:
  using Base = RollingMLBaseUnsupervised<RollingPCATransformN<N_COMPONENTS>, PCAModel>;
  using OutputVectors = RollingPCAOutputsN<N_COMPONENTS>;

  explicit RollingPCATransformN(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_scale_data = cfg.GetOptionValue("scale_data",
                                       MetaDataOptionDefinition{true}).GetBoolean();
  }

  /**
   * @brief Train PCA model on training window
   *
   * @param X Training data (observations x features)
   * @return PCAModel containing eigenvectors and mean
   */
  [[nodiscard]] PCAModel TrainModel(const arma::mat& X) const {
    // Transpose for mlpack (features as rows)
    arma::mat X_t = X.t();

    // Apply PCA (scaling handled by scale_data option)
    mlpack::PCA<> pca(m_scale_data);
    arma::mat transformed;
    arma::vec eigVal;
    arma::mat eigVec;
    pca.Apply(X_t, transformed, eigVal, eigVec);

    // Always use N_COMPONENTS (clamped to available)
    const size_t n_comp = std::min(N_COMPONENTS, static_cast<size_t>(eigVal.n_elem));

    // Compute explained variance
    double total_var = arma::sum(eigVal);
    double explained = 0;
    for (size_t i = 0; i < n_comp && i < eigVal.n_elem; ++i) {
      explained += eigVal(i);
    }

    PCAModel model;
    model.eigenvectors = eigVec;
    model.eigenvalues = eigVal;
    model.mean = arma::mean(X_t, 1);
    model.n_components = n_comp;
    model.explained_variance = (total_var > 0) ? explained / total_var : 0;

    return model;
  }

  /**
   * @brief Project data using trained PCA
   */
  void Predict(const PCAModel& model,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    // Transpose for projection
    arma::mat X_t = X.t();
    const size_t n_points = X.n_rows;

    // Center using training mean
    arma::mat centered = X_t;
    for (size_t i = 0; i < centered.n_cols; ++i) {
      centered.col(i) -= model.mean;
    }

    // Project onto principal components
    arma::mat projected = model.eigenvectors.t() * centered;

    // Fill outputs
    for (size_t i = 0; i < n_points; ++i) {
      const size_t idx = output_offset + i;

      for (size_t k = 0; k < N_COMPONENTS && k < model.n_components; ++k) {
        outputs.principal_components[k][idx] = projected(k, i);
      }
      outputs.explained_variance_ratio[idx] = model.explained_variance;
    }
  }

  /**
   * @brief Get output column names
   */
  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    std::vector<std::string> names;
    names.reserve(N_COMPONENTS + 1);
    for (size_t k = 0; k < N_COMPONENTS; ++k) {
      names.push_back(this->GetOutputId("pc_" + std::to_string(k)));
    }
    names.push_back(this->GetOutputId("explained_variance_ratio"));
    return names;
  }

  /**
   * @brief Initialize output vectors
   */
  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    for (size_t k = 0; k < N_COMPONENTS; ++k) {
      outputs.principal_components[k].resize(n_rows, std::numeric_limits<double>::quiet_NaN());
    }
    outputs.explained_variance_ratio.resize(n_rows, std::numeric_limits<double>::quiet_NaN());
  }

  /**
   * @brief Build output DataFrame from accumulated results
   */
  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    arrays.reserve(N_COMPONENTS + 1);

    for (size_t k = 0; k < N_COMPONENTS; ++k) {
      arrays.push_back(epoch_frame::factory::array::make_array(outputs.principal_components[k]));
    }
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.explained_variance_ratio));

    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  bool m_scale_data{true};
};

// Type aliases for the 5 variants (N = 2 to 6)
using RollingPCA2Transform = RollingPCATransformN<2>;
using RollingPCA3Transform = RollingPCATransformN<3>;
using RollingPCA4Transform = RollingPCATransformN<4>;
using RollingPCA5Transform = RollingPCATransformN<5>;
using RollingPCA6Transform = RollingPCATransformN<6>;

} // namespace epoch_script::transform
