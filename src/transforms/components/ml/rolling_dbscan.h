#pragma once
//
// Rolling DBSCAN Clustering Transform
//
// Implements rolling/expanding window DBSCAN clustering using the shared
// rolling ML infrastructure. DBSCAN identifies clusters of varying shapes
// and marks outliers/anomalies.
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
#include <mlpack/methods/dbscan/dbscan.hpp>

namespace epoch_script::transform {

/**
 * @brief Output vectors for Rolling DBSCAN
 */
struct RollingDBSCANOutputs {
  std::vector<int64_t> cluster_label;
  std::vector<int64_t> is_anomaly;
  std::vector<int64_t> cluster_count;
};

/**
 * @brief DBSCAN model state for prediction
 *
 * DBSCAN doesn't have traditional "model" like centroids.
 * We store the training data to compute nearest core point distances.
 */
struct DBSCANModel {
  arma::mat core_points;        // Core points from training (features x n_core)
  arma::Row<size_t> core_labels; // Cluster labels for core points
  size_t num_clusters{0};
  double epsilon{0.5};
};

/**
 * @brief Rolling DBSCAN Clustering Transform
 *
 * Performs DBSCAN clustering on a rolling/expanding window basis.
 * Unlike K-Means, DBSCAN automatically determines the number of clusters
 * and identifies outliers/noise points.
 *
 * For prediction, new points are assigned to the cluster of their
 * nearest core point (if within epsilon), otherwise marked as anomaly.
 *
 * Financial Applications:
 * - Adaptive anomaly detection that evolves with market conditions
 * - Walk-forward outlier detection for backtesting
 * - Time-varying regime detection without predefined count
 * - Dynamic unusual pattern identification
 *
 * Key Parameters:
 * - window_size: Training window size (default 252)
 * - step_size: How many rows to advance per retrain (default 1)
 * - window_type: "rolling" or "expanding"
 * - epsilon: Maximum distance between points in same cluster (default 0.5)
 * - min_points: Minimum points to form dense region (default 5)
 */
class RollingDBSCANTransform final
    : public RollingMLBaseUnsupervised<RollingDBSCANTransform, DBSCANModel> {
public:
  using Base = RollingMLBaseUnsupervised<RollingDBSCANTransform, DBSCANModel>;
  using OutputVectors = RollingDBSCANOutputs;

  explicit RollingDBSCANTransform(const TransformConfiguration& cfg)
      : Base(cfg) {
    m_epsilon = cfg.GetOptionValue("epsilon",
                                    MetaDataOptionDefinition{0.5}).GetDecimal();

    m_min_points = static_cast<size_t>(
        cfg.GetOptionValue("min_points",
                           MetaDataOptionDefinition{5.0}).GetInteger());
  }

  /**
   * @brief Train DBSCAN model on training window
   *
   * @param X Training data (observations x features)
   * @return DBSCANModel containing core points for prediction
   */
  [[nodiscard]] DBSCANModel TrainModel(const arma::mat& X) const {
    // mlpack expects features as rows, observations as columns
    arma::mat X_t = X.t();

    mlpack::DBSCAN<> dbscan(m_epsilon, m_min_points);
    arma::Row<size_t> assignments;
    size_t num_clusters = dbscan.Cluster(X_t, assignments);

    // Identify core points (points with at least min_points neighbors)
    // For simplicity, we'll use all clustered points as reference
    // A more sophisticated approach would track actual core points
    std::vector<size_t> core_indices;
    for (size_t i = 0; i < assignments.n_elem; ++i) {
      if (assignments(i) != SIZE_MAX) {  // Not noise
        core_indices.push_back(i);
      }
    }

    DBSCANModel model;
    model.num_clusters = num_clusters;
    model.epsilon = m_epsilon;

    if (!core_indices.empty()) {
      model.core_points.set_size(X_t.n_rows, core_indices.size());
      model.core_labels.set_size(core_indices.size());

      for (size_t i = 0; i < core_indices.size(); ++i) {
        model.core_points.col(i) = X_t.col(core_indices[i]);
        model.core_labels(i) = assignments(core_indices[i]);
      }
    }

    return model;
  }

  /**
   * @brief Predict cluster assignments using trained model
   *
   * Points are assigned to cluster of nearest core point if within
   * epsilon distance, otherwise marked as anomaly.
   *
   * @param model Trained DBSCAN model
   * @param X Prediction data (observations x features)
   * @param window Current window specification
   * @param outputs Output vectors to fill
   * @param output_offset Starting offset in output vectors
   */
  void Predict(const DBSCANModel& model,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    // mlpack expects features as rows, observations as columns
    arma::mat X_t = X.t();
    const size_t n_points = X.n_rows;

    for (size_t i = 0; i < n_points; ++i) {
      arma::vec point = X_t.col(i);

      if (model.core_points.empty()) {
        // No core points found in training - mark all as anomaly
        outputs.cluster_label[output_offset + i] = -1;
        outputs.is_anomaly[output_offset + i] = 1;
        outputs.cluster_count[output_offset + i] = 0;
        continue;
      }

      // Find nearest core point
      double min_dist = std::numeric_limits<double>::max();
      size_t nearest_label = SIZE_MAX;

      for (size_t j = 0; j < model.core_points.n_cols; ++j) {
        double dist = arma::norm(point - model.core_points.col(j));
        if (dist < min_dist) {
          min_dist = dist;
          nearest_label = model.core_labels(j);
        }
      }

      // Assign to cluster if within epsilon, else mark as anomaly
      if (min_dist <= model.epsilon && nearest_label != SIZE_MAX) {
        outputs.cluster_label[output_offset + i] = static_cast<int64_t>(nearest_label);
        outputs.is_anomaly[output_offset + i] = 0;
      } else {
        outputs.cluster_label[output_offset + i] = -1;
        outputs.is_anomaly[output_offset + i] = 1;
      }
      outputs.cluster_count[output_offset + i] = static_cast<int64_t>(model.num_clusters);
    }
  }

  /**
   * @brief Get output column names
   */
  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return {
      GetOutputId("cluster_label"),
      GetOutputId("is_anomaly"),
      GetOutputId("cluster_count")
    };
  }

  /**
   * @brief Initialize output vectors
   */
  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    outputs.cluster_label.resize(n_rows, -1);
    outputs.is_anomaly.resize(n_rows, 1);
    outputs.cluster_count.resize(n_rows, 0);
  }

  /**
   * @brief Build output DataFrame from accumulated results
   */
  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.cluster_label));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.is_anomaly));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.cluster_count));

    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  double m_epsilon{0.5};
  size_t m_min_points{5};
};

} // namespace epoch_script::transform
