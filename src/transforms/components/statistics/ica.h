#pragma once
//
// ICA Transform - Independent Component Analysis using mlpack RADICAL
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
#include <mlpack/methods/radical/radical.hpp>

namespace epoch_script::transform {

/**
 * @brief ICA Transform for Independent Component Analysis
 *
 * Uses RADICAL (Robust, Accurate, Direct ICA aLgorithm) to decompose
 * multivariate signals into statistically independent components.
 *
 * Unlike PCA which finds uncorrelated components, ICA finds components
 * that are statistically independent (a stronger condition).
 *
 * Financial Applications:
 * - Separating mixed market signals
 * - Extracting hidden factors from asset returns
 * - Identifying independent risk sources
 * - Blind source separation of market influences
 * - Finding non-Gaussian structure in returns
 */
class ICATransform final : public ITransform {
public:
  explicit ICATransform(const TransformConfiguration &cfg) : ITransform(cfg) {
    m_noise_std_dev = cfg.GetOptionValue(
                          "noise_std_dev",
                          epoch_script::MetaDataOptionDefinition{0.175})
                          .GetDecimal();

    m_replicates = static_cast<size_t>(
        cfg.GetOptionValue("replicates",
                           epoch_script::MetaDataOptionDefinition{30.0})
            .GetInteger());

    m_angles = static_cast<size_t>(
        cfg.GetOptionValue("angles",
                           epoch_script::MetaDataOptionDefinition{150.0})
            .GetInteger());

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
          "ICATransform requires at least 2 input columns.");
    }

    // Convert DataFrame to Armadillo matrix
    arma::mat X = utils::MatFromDataFrame(bars, cols);
    const size_t n_features = X.n_cols;

    if (X.n_rows < n_features * 2) {
      throw std::runtime_error("ICATransform: Insufficient observations for ICA");
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

    // Run RADICAL ICA
    mlpack::Radical radical(m_noise_std_dev, m_replicates, m_angles);

    arma::mat Y;  // Independent components
    arma::mat W;  // Unmixing matrix
    radical.Apply(training_t, Y, W);

    // Apply learned unmixing matrix to prediction data
    // First, center prediction data using training mean
    arma::vec train_mean = arma::mean(training_t, 1);
    arma::mat centered_pred = prediction_t;
    for (size_t i = 0; i < centered_pred.n_cols; ++i) {
      centered_pred.col(i) -= train_mean;
    }

    // Apply unmixing: Y = W * X
    arma::mat transformed_pred = W * centered_pred;

    return GenerateOutputs(prediction_index, transformed_pred, n_features);
  }

private:
  double m_noise_std_dev{0.175};
  size_t m_replicates{30};
  size_t m_angles{150};
  size_t m_lookback_window{0};

  epoch_frame::DataFrame GenerateOutputs(const epoch_frame::IndexPtr &index,
                                         const arma::mat &transformed,
                                         size_t n_components) const {
    const size_t T = transformed.n_cols;  // observations are columns
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    // Output independent components (ic_0, ic_1, ...)
    for (size_t k = 0; k < n_components && k < transformed.n_rows; ++k) {
      std::vector<double> ic_vec(T);
      for (size_t i = 0; i < T; ++i) {
        ic_vec[i] = transformed(k, i);
      }
      output_columns.push_back(GetOutputId("ic_" + std::to_string(k)));
      output_arrays.push_back(epoch_frame::factory::array::make_array(ic_vec));
    }

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

} // namespace epoch_script::transform
