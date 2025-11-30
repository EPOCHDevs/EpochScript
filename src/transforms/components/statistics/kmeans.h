#pragma once
//
// K-Means Clustering Transform using mlpack
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
#include <mlpack/methods/kmeans/kmeans.hpp>

namespace epoch_script::transform {

/**
 * @brief K-Means Clustering Transform for Financial Time Series
 *
 * This transform implements K-Means clustering using mlpack, supporting
 * multi-dimensional input features for market regime detection.
 *
 * Template parameter K specifies the number of clusters.
 * Use specialized versions: kmeans_2, kmeans_3, kmeans_4, kmeans_5
 *
 * Financial Applications:
 * - Market regime detection (bull/bear/sideways)
 * - Volatility state clustering
 * - Asset grouping by behavior patterns
 * - Risk regime identification
 */
template <size_t K>
class KMeansTransform final : public ITransform {
public:
  static_assert(K >= 2 && K <= 5, "KMeans supports 2-5 clusters");

  explicit KMeansTransform(const TransformConfiguration &cfg) : ITransform(cfg) {
    m_max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations",
                           epoch_script::MetaDataOptionDefinition{1000.0})
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
          "KMeansTransform requires at least one input column.");
    }

    // Convert DataFrame to Armadillo matrix
    arma::mat X = utils::MatFromDataFrame(bars, cols);

    if (X.n_rows < K) {
      throw std::runtime_error("KMeansTransform: Insufficient data points for clustering");
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

    // Train K-Means
    mlpack::KMeans<> kmeans(m_max_iterations);
    arma::mat centroids;
    arma::Row<size_t> training_assignments;
    kmeans.Cluster(training_t, K, training_assignments, centroids);

    // Predict on prediction data (assign each point to nearest centroid)
    const size_t T = prediction_data.n_rows;
    arma::Row<size_t> assignments(T);
    arma::mat distances(K, T);

    for (size_t i = 0; i < T; ++i) {
      arma::vec point = prediction_t.col(i);
      double min_dist = std::numeric_limits<double>::max();
      size_t min_cluster = 0;

      for (size_t k = 0; k < K; ++k) {
        double dist = arma::norm(point - centroids.col(k));
        distances(k, i) = dist;
        if (dist < min_dist) {
          min_dist = dist;
          min_cluster = k;
        }
      }
      assignments(i) = min_cluster;
    }

    return GenerateOutputs(prediction_index, assignments, distances);
  }

private:
  size_t m_max_iterations{1000};
  size_t m_lookback_window{0};

  epoch_frame::DataFrame GenerateOutputs(const epoch_frame::IndexPtr &index,
                                         const arma::Row<size_t> &assignments,
                                         const arma::mat &distances) const {
    const size_t T = assignments.n_elem;
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    // 1. Cluster label
    std::vector<int64_t> label_vec(T);
    for (size_t i = 0; i < T; ++i) {
      label_vec[i] = static_cast<int64_t>(assignments(i));
    }
    output_columns.push_back(GetOutputId("cluster_label"));
    output_arrays.push_back(epoch_frame::factory::array::make_array(label_vec));

    // 2. Distance to each centroid (cluster_0_dist, cluster_1_dist, ...)
    for (size_t k = 0; k < K; ++k) {
      std::vector<double> dist_vec(T);
      for (size_t i = 0; i < T; ++i) {
        dist_vec[i] = distances(k, i);
      }
      output_columns.push_back(GetOutputId("cluster_" + std::to_string(k) + "_dist"));
      output_arrays.push_back(epoch_frame::factory::array::make_array(dist_vec));
    }

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

// Specialized K-Means transforms for 2-5 clusters
using KMeans2Transform = KMeansTransform<2>;
using KMeans3Transform = KMeansTransform<3>;
using KMeans4Transform = KMeansTransform<4>;
using KMeans5Transform = KMeansTransform<5>;

} // namespace epoch_script::transform
