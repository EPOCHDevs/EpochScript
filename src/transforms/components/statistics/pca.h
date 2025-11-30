#pragma once
//
// PCA Transform - Principal Component Analysis using mlpack
//
// NOTE: Preprocessing (z-score, min-max, etc.) should be done via separate
// ml_preprocess transforms in the pipeline. This keeps concerns separated
// and allows users to compose their own preprocessing pipelines.
//
#include "dataframe_armadillo_utils.h"
#include "epoch_frame/aliases.h"
#include <epoch_script/transforms/core/itransform.h>
#include "transforms/components/ml/ml_split_utils.h"
#include <arrow/array.h>
#include <arrow/builder.h>
#include <cmath>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

#include <armadillo>
#include <mlpack/core.hpp>
#include <mlpack/methods/pca/pca.hpp>

namespace epoch_script::transform {

/**
 * @brief PCA Transform for Dimensionality Reduction
 *
 * Principal Component Analysis transforms correlated features into
 * uncorrelated principal components, ordered by explained variance.
 *
 * Financial Applications:
 * - Factor extraction from correlated assets
 * - Risk factor decomposition
 * - Portfolio optimization (minimize correlated exposures)
 * - Feature reduction for ML models
 * - Identifying hidden market drivers
 */
class PCATransform final : public ITransform {
public:
  explicit PCATransform(const TransformConfiguration &cfg) : ITransform(cfg) {
    m_n_components = static_cast<size_t>(
        cfg.GetOptionValue("n_components",
                           epoch_script::MetaDataOptionDefinition{0.0})
            .GetInteger());

    m_variance_retained = cfg.GetOptionValue(
                              "variance_retained",
                              epoch_script::MetaDataOptionDefinition{0.0})
                              .GetDecimal();

    m_lookback_window = static_cast<size_t>(
        cfg.GetOptionValue("lookback_window",
                           epoch_script::MetaDataOptionDefinition{0.0})
            .GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    const auto cols = GetInputIds();
    if (cols.size() < 2) {
      throw std::runtime_error(
          "PCATransform requires at least 2 input columns.");
    }

    // Convert DataFrame to Armadillo matrix
    arma::mat X = utils::MatFromDataFrame(bars, cols);
    const size_t n_features = X.n_cols;

    if (X.n_rows < n_features) {
      throw std::runtime_error("PCATransform: More features than observations");
    }

    // Split into training and prediction sets using ml_split_utils
    arma::mat training_data;
    arma::mat prediction_data;
    epoch_frame::IndexPtr prediction_index;

    if (m_lookback_window > 0 && X.n_rows > m_lookback_window) {
      // Use split helper for consistent splitting
      auto split = ml_utils::split_by_count(bars, m_lookback_window);

      training_data = X.rows(0, m_lookback_window - 1);
      prediction_data = X.rows(m_lookback_window, X.n_rows - 1);
      prediction_index = split.test.index();
    } else {
      training_data = X;
      prediction_data = X;
      prediction_index = bars.index();
    }

    // mlpack expects features as rows, observations as columns
    arma::mat training_t = training_data.t();
    arma::mat prediction_t = prediction_data.t();

    // Apply PCA (mlpack's PCA internally handles centering/scaling)
    mlpack::PCA<> pca(false);  // We handle scaling externally via ml_preprocess

    arma::mat transformed_training;
    arma::vec eigVal;
    arma::mat eigvec;

    pca.Apply(training_t, transformed_training, eigVal, eigvec);

    // Determine number of components to keep
    size_t n_components = n_features;
    if (m_n_components > 0 && m_n_components < n_features) {
      n_components = m_n_components;
    } else if (m_variance_retained > 0 && m_variance_retained < 1.0) {
      // Keep components until variance threshold met
      double total_var = arma::sum(eigVal);
      double cumulative_var = 0;
      n_components = 0;
      for (size_t i = 0; i < eigVal.n_elem; ++i) {
        cumulative_var += eigVal(i);
        n_components++;
        if (cumulative_var / total_var >= m_variance_retained) {
          break;
        }
      }
    }

    // Transform prediction data using learned eigenvectors
    // Center prediction data using training mean
    arma::vec train_mean = arma::mean(training_t, 1);
    arma::mat centered_pred = prediction_t;
    for (size_t i = 0; i < centered_pred.n_cols; ++i) {
      centered_pred.col(i) -= train_mean;
    }

    // Project onto principal components
    arma::mat transformed_pred = eigvec.t() * centered_pred;

    // Compute explained variance ratio
    double total_var = arma::sum(eigVal);
    arma::vec explained_var_ratio = eigVal / total_var;

    return GenerateOutputs(prediction_index, transformed_pred, explained_var_ratio, n_components);
  }

private:
  size_t m_n_components{0};         // 0 = use variance_retained or keep all
  double m_variance_retained{0.0};  // 0 = keep all, 0.95 = keep 95% variance
  size_t m_lookback_window{0};

  epoch_frame::DataFrame GenerateOutputs(const epoch_frame::IndexPtr &index,
                                         const arma::mat &transformed,
                                         const arma::vec &explained_var_ratio,
                                         size_t n_components) const {
    const size_t T = transformed.n_cols;  // observations are columns
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    // Output principal components (pc_0, pc_1, ...)
    for (size_t k = 0; k < n_components && k < transformed.n_rows; ++k) {
      std::vector<double> pc_vec(T);
      for (size_t i = 0; i < T; ++i) {
        pc_vec[i] = transformed(k, i);
      }
      output_columns.push_back(GetOutputId("pc_" + std::to_string(k)));
      output_arrays.push_back(epoch_frame::factory::array::make_array(pc_vec));
    }

    // Output explained variance ratio for retained components
    // (constant across time, for reference)
    double cumulative_var = 0;
    for (size_t k = 0; k < n_components && k < explained_var_ratio.n_elem; ++k) {
      cumulative_var += explained_var_ratio(k);
    }

    std::vector<double> var_ratio_vec(T, cumulative_var);
    output_columns.push_back(GetOutputId("explained_variance_ratio"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(var_ratio_vec));

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

} // namespace epoch_script::transform
