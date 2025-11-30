#pragma once
//
// Rolling ML Preprocessing Transforms
//
// Rolling/expanding window versions of preprocessing transforms:
// - rolling_ml_zscore: Rolling standardization
// - rolling_ml_minmax: Rolling min-max scaling
// - rolling_ml_robust: Rolling robust scaling
//
// Uses CRTP base class for shared rolling logic.
//
#include "rolling_ml_base.h"
#include "rolling_ml_metadata.h"
#include "../statistics/dataframe_armadillo_utils.h"
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

#include <armadillo>

namespace epoch_script::transform {

/**
 * @brief Model state for z-score preprocessing
 */
struct ZScoreParams {
  std::vector<double> means;
  std::vector<double> stds;
};

/**
 * @brief Model state for min-max preprocessing
 */
struct MinMaxParams {
  std::vector<double> mins;
  std::vector<double> maxs;
};

/**
 * @brief Model state for robust preprocessing
 */
struct RobustParams {
  std::vector<double> medians;
  std::vector<double> iqrs;
};

/**
 * @brief Output vectors for rolling preprocessing
 */
struct RollingPreprocessOutputs {
  std::vector<std::vector<double>> scaled_columns;
};

/**
 * @brief Rolling Z-Score (Standardization) Transform
 *
 * Performs standardization on a rolling/expanding window basis.
 * Fits mean/std on training window, applies to prediction window.
 * z = (x - mean) / std
 *
 * Financial Applications:
 * - Adaptive feature normalization
 * - Walk-forward standardization for ML
 * - Time-varying scale adjustment
 *
 * Key Parameters:
 * - window_size: Training window size (default 252)
 * - step_size: How many rows to advance per refit (default 1)
 * - window_type: "rolling" or "expanding"
 */
class RollingMLZScore final
    : public RollingMLBaseUnsupervised<RollingMLZScore, ZScoreParams> {
public:
  using Base = RollingMLBaseUnsupervised<RollingMLZScore, ZScoreParams>;
  using OutputVectors = RollingPreprocessOutputs;

  explicit RollingMLZScore(const TransformConfiguration& cfg)
      : Base(cfg) {}

  /**
   * @brief Fit z-score parameters on training window
   */
  [[nodiscard]] ZScoreParams TrainModel(const arma::mat& X) const {
    const size_t n_cols = X.n_cols;
    ZScoreParams params;
    params.means.resize(n_cols);
    params.stds.resize(n_cols);

    for (size_t j = 0; j < n_cols; ++j) {
      params.means[j] = arma::mean(X.col(j));
      params.stds[j] = arma::stddev(X.col(j));
    }

    return params;
  }

  /**
   * @brief Apply z-score transformation
   */
  void Predict(const ZScoreParams& params,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    const size_t n_cols = X.n_cols;
    const size_t n_rows = X.n_rows;

    for (size_t i = 0; i < n_rows; ++i) {
      const size_t idx = output_offset + i;
      for (size_t j = 0; j < n_cols && j < outputs.scaled_columns.size(); ++j) {
        if (params.stds[j] > 1e-10) {
          outputs.scaled_columns[j][idx] = (X(i, j) - params.means[j]) / params.stds[j];
        } else {
          outputs.scaled_columns[j][idx] = X(i, j) - params.means[j];
        }
      }
    }
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    const auto cols = GetInputIds();
    std::vector<std::string> names;
    for (size_t j = 0; j < cols.size(); ++j) {
      names.push_back(GetOutputId("scaled_" + std::to_string(j)));
    }
    return names;
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    const auto cols = GetInputIds();
    outputs.scaled_columns.resize(cols.size());
    for (size_t j = 0; j < cols.size(); ++j) {
      outputs.scaled_columns[j].resize(n_rows, std::numeric_limits<double>::quiet_NaN());
    }
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    for (const auto& col : outputs.scaled_columns) {
      arrays.push_back(epoch_frame::factory::array::make_array(col));
    }
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }
};

/**
 * @brief Rolling Min-Max Scaling Transform
 *
 * Scales features to [0, 1] range on a rolling/expanding window basis.
 * x_scaled = (x - min) / (max - min)
 *
 * Financial Applications:
 * - Adaptive bounded normalization
 * - Walk-forward neural network preparation
 * - Time-varying range adjustment
 */
class RollingMLMinMax final
    : public RollingMLBaseUnsupervised<RollingMLMinMax, MinMaxParams> {
public:
  using Base = RollingMLBaseUnsupervised<RollingMLMinMax, MinMaxParams>;
  using OutputVectors = RollingPreprocessOutputs;

  explicit RollingMLMinMax(const TransformConfiguration& cfg)
      : Base(cfg) {}

  [[nodiscard]] MinMaxParams TrainModel(const arma::mat& X) const {
    const size_t n_cols = X.n_cols;
    MinMaxParams params;
    params.mins.resize(n_cols);
    params.maxs.resize(n_cols);

    for (size_t j = 0; j < n_cols; ++j) {
      params.mins[j] = arma::min(X.col(j));
      params.maxs[j] = arma::max(X.col(j));
    }

    return params;
  }

  void Predict(const MinMaxParams& params,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    const size_t n_cols = X.n_cols;
    const size_t n_rows = X.n_rows;

    for (size_t i = 0; i < n_rows; ++i) {
      const size_t idx = output_offset + i;
      for (size_t j = 0; j < n_cols && j < outputs.scaled_columns.size(); ++j) {
        double range = params.maxs[j] - params.mins[j];
        if (range > 1e-10) {
          outputs.scaled_columns[j][idx] = (X(i, j) - params.mins[j]) / range;
        } else {
          outputs.scaled_columns[j][idx] = 0.0;
        }
      }
    }
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    const auto cols = GetInputIds();
    std::vector<std::string> names;
    for (size_t j = 0; j < cols.size(); ++j) {
      names.push_back(GetOutputId("scaled_" + std::to_string(j)));
    }
    return names;
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    const auto cols = GetInputIds();
    outputs.scaled_columns.resize(cols.size());
    for (size_t j = 0; j < cols.size(); ++j) {
      outputs.scaled_columns[j].resize(n_rows, std::numeric_limits<double>::quiet_NaN());
    }
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    for (const auto& col : outputs.scaled_columns) {
      arrays.push_back(epoch_frame::factory::array::make_array(col));
    }
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }
};

/**
 * @brief Rolling Robust Scaling Transform
 *
 * Scales features using median and IQR on a rolling/expanding window.
 * x_scaled = (x - median) / IQR
 *
 * Financial Applications:
 * - Adaptive outlier-robust normalization
 * - Walk-forward robust scaling
 * - Fat-tailed distribution handling
 */
class RollingMLRobust final
    : public RollingMLBaseUnsupervised<RollingMLRobust, RobustParams> {
public:
  using Base = RollingMLBaseUnsupervised<RollingMLRobust, RobustParams>;
  using OutputVectors = RollingPreprocessOutputs;

  explicit RollingMLRobust(const TransformConfiguration& cfg)
      : Base(cfg) {}

  [[nodiscard]] RobustParams TrainModel(const arma::mat& X) const {
    const size_t n_cols = X.n_cols;
    RobustParams params;
    params.medians.resize(n_cols);
    params.iqrs.resize(n_cols);

    for (size_t j = 0; j < n_cols; ++j) {
      arma::vec col = arma::sort(X.col(j));
      params.medians[j] = arma::median(col);

      size_t n = col.n_elem;
      double q1 = col(static_cast<size_t>(n * 0.25));
      double q3 = col(static_cast<size_t>(n * 0.75));
      params.iqrs[j] = q3 - q1;
    }

    return params;
  }

  void Predict(const RobustParams& params,
               const arma::mat& X,
               [[maybe_unused]] const ml_utils::WindowSpec& window,
               OutputVectors& outputs,
               size_t output_offset) const {
    const size_t n_cols = X.n_cols;
    const size_t n_rows = X.n_rows;

    for (size_t i = 0; i < n_rows; ++i) {
      const size_t idx = output_offset + i;
      for (size_t j = 0; j < n_cols && j < outputs.scaled_columns.size(); ++j) {
        if (params.iqrs[j] > 1e-10) {
          outputs.scaled_columns[j][idx] = (X(i, j) - params.medians[j]) / params.iqrs[j];
        } else {
          outputs.scaled_columns[j][idx] = X(i, j) - params.medians[j];
        }
      }
    }
  }

  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    const auto cols = GetInputIds();
    std::vector<std::string> names;
    for (size_t j = 0; j < cols.size(); ++j) {
      names.push_back(GetOutputId("scaled_" + std::to_string(j)));
    }
    return names;
  }

  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    const auto cols = GetInputIds();
    outputs.scaled_columns.resize(cols.size());
    for (size_t j = 0; j < cols.size(); ++j) {
      outputs.scaled_columns[j].resize(n_rows, std::numeric_limits<double>::quiet_NaN());
    }
  }

  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;
    for (const auto& col : outputs.scaled_columns) {
      arrays.push_back(epoch_frame::factory::array::make_array(col));
    }
    return epoch_frame::make_dataframe(index, arrays, column_names);
  }
};

} // namespace epoch_script::transform
