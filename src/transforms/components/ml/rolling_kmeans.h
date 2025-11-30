#pragma once
//
// Rolling K-Means Clustering Transform
//
// Implements rolling/expanding window K-Means clustering using the shared
// rolling ML infrastructure. Retrains on each window and predicts forward.
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
#include <mlpack/methods/kmeans/kmeans.hpp>

namespace epoch_script::transform {

/**
 * @brief Output vectors for Rolling K-Means
 */
template<size_t K>
struct RollingKMeansOutputs {
  std::vector<int64_t> cluster_label;
  std::array<std::vector<double>, K> cluster_distances;
};

/**
 * @brief Rolling K-Means Clustering Transform
 *
 * Performs K-Means clustering on a rolling/expanding window basis,
 * retraining the model as the window advances. This captures evolving
 * market regimes over time.
 *
 * Template parameter K specifies the number of clusters.
 * Use specialized versions: rolling_kmeans_2, rolling_kmeans_3, etc.
 *
 * Financial Applications:
 * - Adaptive market regime detection that evolves with market
 * - Walk-forward clustering for backtesting
 * - Time-varying volatility state identification
 * - Dynamic risk regime classification
 *
 * Key Parameters:
 * - window_size: Training window size (default 252 = ~1 year)
 * - step_size: How many rows to advance per retrain (default 1)
 * - window_type: "rolling" (fixed window) or "expanding" (cumulative)
 * - max_iterations: Maximum K-Means iterations (default 1000)
 */
template<size_t K>
class RollingKMeansTransform final
    : public RollingMLBaseUnsupervised<RollingKMeansTransform<K>, arma::mat> {
public:
  using Base = RollingMLBaseUnsupervised<RollingKMeansTransform<K>, arma::mat>;
  using OutputVectors = RollingKMeansOutputs<K>;

  static_assert(K >= 2 && K <= 5, "RollingKMeans supports 2-5 clusters");

  explicit RollingKMeansTransform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations",
                           MetaDataOptionDefinition{1000.0}).GetInteger());
  }

  /**
   * @brief Train K-Means model on training window
   *
   * @param X Training data (observations x features)
   * @return Centroid matrix (features x K)
   */
  [[nodiscard]] arma::mat TrainModel(const arma::mat& X) const {
    // mlpack expects features as rows, observations as columns
    arma::mat X_t = X.t();

    mlpack::KMeans<> kmeans(m_max_iterations);
    arma::mat centroids;
    arma::Row<size_t> assignments;
    kmeans.Cluster(X_t, K, assignments, centroids);

    return centroids;
  }

  /**
   * @brief Predict cluster assignments using trained centroids
   *
   * @param centroids Trained centroid matrix
   * @param X Prediction data (observations x features)
   * @param window Current window specification
   * @param outputs Output vectors to fill
   * @param output_offset Starting offset in output vectors
   */
  void Predict(const arma::mat& centroids,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    // mlpack expects features as rows, observations as columns
    arma::mat X_t = X.t();
    const size_t n_points = X.n_rows;

    for (size_t i = 0; i < n_points; ++i) {
      arma::vec point = X_t.col(i);
      double min_dist = std::numeric_limits<double>::max();
      size_t min_cluster = 0;

      for (size_t k = 0; k < K; ++k) {
        double dist = arma::norm(point - centroids.col(k));
        outputs.cluster_distances[k][output_offset + i] = dist;
        if (dist < min_dist) {
          min_dist = dist;
          min_cluster = k;
        }
      }
      outputs.cluster_label[output_offset + i] = static_cast<int64_t>(min_cluster);
    }
  }

  /**
   * @brief Get output column names
   */
  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    std::vector<std::string> names;
    names.push_back(this->GetOutputId("cluster_label"));
    for (size_t k = 0; k < K; ++k) {
      names.push_back(this->GetOutputId("cluster_" + std::to_string(k) + "_dist"));
    }
    return names;
  }

  /**
   * @brief Initialize output vectors with NaN
   */
  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.cluster_label.resize(n_rows, -1);
    for (size_t k = 0; k < K; ++k) {
      outputs.cluster_distances[k].resize(n_rows, std::numeric_limits<double>::quiet_NaN());
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

    // Cluster label
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.cluster_label));

    // Distance to each cluster
    for (size_t k = 0; k < K; ++k) {
      arrays.push_back(epoch_frame::factory::array::make_array(outputs.cluster_distances[k]));
    }

    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  size_t m_max_iterations{1000};
};

// Specialized Rolling K-Means transforms for 2-5 clusters
using RollingKMeans2Transform = RollingKMeansTransform<2>;
using RollingKMeans3Transform = RollingKMeansTransform<3>;
using RollingKMeans4Transform = RollingKMeansTransform<4>;
using RollingKMeans5Transform = RollingKMeansTransform<5>;

} // namespace epoch_script::transform
