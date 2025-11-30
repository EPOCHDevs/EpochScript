#pragma once
//
// DBSCAN Clustering Transform using mlpack
//
// NOTE: Preprocessing (z-score, min-max, etc.) should be done via separate
// ml_preprocess transforms in the pipeline. This keeps concerns separated
// and allows users to compose their own preprocessing pipelines.
//
#include "dataframe_armadillo_utils.h"
#include "epoch_frame/aliases.h"
#include <epoch_script/transforms/core/itransform.h>
#include "../ml/ml_split_utils.h"
#include <arrow/array.h>
#include <arrow/builder.h>
#include <cmath>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

#include <armadillo>
#include <mlpack/core.hpp>
#include <mlpack/methods/dbscan/dbscan.hpp>

namespace epoch_script::transform {

/**
 * @brief DBSCAN Clustering Transform for Financial Time Series
 *
 * Density-Based Spatial Clustering of Applications with Noise.
 * Unlike K-Means, DBSCAN:
 * - Does not require specifying number of clusters upfront
 * - Can find arbitrarily shaped clusters
 * - Identifies noise/outliers (points that don't belong to any cluster)
 *
 * Financial Applications:
 * - Anomaly/outlier detection in returns
 * - Regime detection without predefined count
 * - Finding natural groupings in market behavior
 * - Identifying unusual trading patterns
 */
class DBSCANTransform final : public ITransform {
public:
  explicit DBSCANTransform(const TransformConfiguration &cfg) : ITransform(cfg) {
    m_epsilon = cfg.GetOptionValue("epsilon",
                                    epoch_script::MetaDataOptionDefinition{0.5})
                    .GetDecimal();

    m_min_points = static_cast<size_t>(
        cfg.GetOptionValue("min_points",
                           epoch_script::MetaDataOptionDefinition{5.0})
            .GetInteger());

    m_lookback_window = static_cast<size_t>(
        cfg.GetOptionValue("lookback_window",
                           epoch_script::MetaDataOptionDefinition{0.0})
            .GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    const auto cols = GetInputIds();
    if (cols.empty()) {
      throw std::runtime_error(
          "DBSCANTransform requires at least one input column.");
    }

    // Convert DataFrame to Armadillo matrix
    arma::mat X = utils::MatFromDataFrame(bars, cols);

    if (X.n_rows < m_min_points) {
      throw std::runtime_error("DBSCANTransform: Insufficient data points for clustering");
    }

    // Split into training and prediction sets using ml_split_utils
    arma::mat prediction_data;
    epoch_frame::IndexPtr prediction_index;

    if (m_lookback_window > 0 && X.n_rows > m_lookback_window) {
      // Use split helper for consistent splitting
      auto split = ml_utils::split_by_count(bars, m_lookback_window);

      prediction_data = X.rows(m_lookback_window, X.n_rows - 1);
      prediction_index = split.test.index();
    } else {
      prediction_data = X;
      prediction_index = bars.index();
    }

    // mlpack expects features as rows, observations as columns
    arma::mat prediction_t = prediction_data.t();

    // Run DBSCAN on prediction data
    mlpack::DBSCAN<> dbscan(m_epsilon, m_min_points);
    arma::Row<size_t> assignments;
    size_t num_clusters = dbscan.Cluster(prediction_t, assignments);

    return GenerateOutputs(prediction_index, assignments, num_clusters);
  }

private:
  double m_epsilon{0.5};
  size_t m_min_points{5};
  size_t m_lookback_window{0};

  epoch_frame::DataFrame GenerateOutputs(const epoch_frame::IndexPtr &index,
                                         const arma::Row<size_t> &assignments,
                                         size_t num_clusters) const {
    const size_t T = assignments.n_elem;
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    // 1. Cluster label (-1 for noise/outliers)
    std::vector<int64_t> label_vec(T);
    std::vector<int64_t> is_anomaly_vec(T);
    size_t anomaly_count = 0;

    for (size_t i = 0; i < T; ++i) {
      if (assignments(i) == SIZE_MAX) {
        // Noise point (outlier)
        label_vec[i] = -1;
        is_anomaly_vec[i] = 1;
        anomaly_count++;
      } else {
        label_vec[i] = static_cast<int64_t>(assignments(i));
        is_anomaly_vec[i] = 0;
      }
    }

    output_columns.push_back(GetOutputId("cluster_label"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(label_vec));

    // 2. Is anomaly flag (1 = noise/outlier, 0 = in cluster)
    output_columns.push_back(GetOutputId("is_anomaly"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(is_anomaly_vec));

    // 3. Cluster count (constant across all rows, for reference)
    std::vector<int64_t> cluster_count_vec(T, static_cast<int64_t>(num_clusters));
    output_columns.push_back(GetOutputId("cluster_count"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(cluster_count_vec));

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

} // namespace epoch_script::transform
