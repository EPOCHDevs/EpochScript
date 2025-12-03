#pragma once
//
// ML Preprocessing Transforms
//
// Stateful preprocessing transforms for ML pipelines:
// - ml_zscore: Standardization (z-score normalization)
// - ml_minmax: Min-Max scaling to [0,1]
// - ml_robust: Robust scaling using median and IQR
//
// All transforms use split_ratio to fit parameters on training data
// and apply those parameters to transform the full dataset.
//
// Uses epoch_frame operations directly - no Armadillo dependency.
//
#include "ml_split_utils.h"
#include "epoch_frame/aliases.h"
#include <epoch_script/transforms/core/itransform.h>
#include <arrow/array.h>
#include <arrow/compute/api.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <cmath>

namespace epoch_script::transform {

/**
 * @brief ML Z-Score (Standardization) Transform
 *
 * Standardizes features by removing mean and scaling to unit variance.
 * Fits on training portion (split_ratio), applies to full data.
 *
 * z = (x - mean) / std
 *
 * Financial Applications:
 * - Feature normalization for ML models
 * - Making features comparable across different scales
 * - Preparing data for algorithms sensitive to feature scaling
 */
class MLZScore final : public ITransform {
public:
  explicit MLZScore(const TransformConfiguration &cfg) : ITransform(cfg) {
    m_split_ratio = cfg.GetOptionValue("split_ratio", MetaDataOptionDefinition{0.7}).GetDecimal();
    if (m_split_ratio <= 0 || m_split_ratio > 1.0) {
      throw std::runtime_error("split_ratio must be in (0, 1]");
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    const auto cols = GetInputIds();
    if (cols.empty()) {
      throw std::runtime_error("ml_zscore requires at least one input");
    }

    // Split data - get train view for fitting
    auto split = ml_utils::split_by_ratio(bars, m_split_ratio);

    // Select only the input columns
    auto train_df = split.train[cols];
    auto full_df = bars[cols];

    // Fit on training data: compute mean and std per column
     std::vector<epoch_frame::Scalar> means, stds;
    means.reserve(cols.size());
    stds.reserve(cols.size());

    for (const auto& col : cols) {
      const auto& train_col = train_df[col];
      means.push_back(train_col.mean());
      stds.push_back(train_col.stddev(arrow::compute::VarianceOptions(1)));
    }

    // Apply transformation: (x - mean) / std
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    static const epoch_frame::Scalar min_std{1e-10};
    for (size_t j = 0; j < cols.size(); ++j) {
      const auto& series = full_df[cols[j]];
      epoch_frame::Series scaled = (series - means[j]);

      if (stds[j] > min_std) {
        scaled = scaled / stds[j];
      }

      output_columns.push_back(GetOutputId("scaled_" + std::to_string(j)));
      output_arrays.push_back(scaled.array());
    }

    return epoch_frame::make_dataframe(bars.index(), output_arrays, output_columns);
  }

private:
  double m_split_ratio{0.7};
};

/**
 * @brief ML Min-Max Scaling Transform
 *
 * Scales features to [0, 1] range.
 * Fits on training portion (split_ratio), applies to full data.
 *
 * x_scaled = (x - min) / (max - min)
 *
 * Financial Applications:
 * - Normalizing features to bounded range
 * - Neural network input preparation
 * - Features with known bounds
 */
class MLMinMax final : public ITransform {
public:
  explicit MLMinMax(const TransformConfiguration &cfg) : ITransform(cfg) {
    m_split_ratio = cfg.GetOptionValue("split_ratio", MetaDataOptionDefinition{0.7}).GetDecimal();
    if (m_split_ratio <= 0 || m_split_ratio > 1.0) {
      throw std::runtime_error("split_ratio must be in (0, 1]");
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    const auto cols = GetInputIds();
    if (cols.empty()) {
      throw std::runtime_error("ml_minmax requires at least one input");
    }

    // Split data - get train view for fitting
    auto split = ml_utils::split_by_ratio(bars, m_split_ratio);

    // Select only the input columns
    auto train_df = split.train[cols];
    auto full_df = bars[cols];

    // Fit on training data: compute min and max per column
    std::vector<epoch_frame::Scalar> mins, maxs;
    mins.reserve(cols.size());
    maxs.reserve(cols.size());

    for (const auto& col : cols) {
      const auto& train_col = train_df[col];
      mins.push_back(train_col.min());
      maxs.push_back(train_col.max());
    }

    // Apply transformation: (x - min) / (max - min)
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    const epoch_frame::Scalar min_err{1e-10};
    for (size_t j = 0; j < cols.size(); ++j) {
      const auto& series = full_df[cols[j]];
      epoch_frame::Scalar range = maxs[j] - mins[j];
      epoch_frame::Series scaled = series - mins[j];

      if (range > min_err) {
        scaled = scaled / range;
      }

      output_columns.push_back(GetOutputId("scaled_" + std::to_string(j)));
      output_arrays.push_back(scaled.array());
    }

    return epoch_frame::make_dataframe(bars.index(), output_arrays, output_columns);
  }

private:
  double m_split_ratio{0.7};
};

/**
 * @brief ML Robust Scaling Transform
 *
 * Scales features using statistics robust to outliers.
 * Uses median and interquartile range (IQR).
 * Fits on training portion (split_ratio), applies to full data.
 *
 * x_scaled = (x - median) / IQR
 *
 * Financial Applications:
 * - Handling features with outliers
 * - Robust normalization for non-Gaussian data
 * - Fat-tailed financial distributions
 */
class MLRobust final : public ITransform {
public:
  explicit MLRobust(const TransformConfiguration &cfg) : ITransform(cfg) {
    m_split_ratio = cfg.GetOptionValue("split_ratio", MetaDataOptionDefinition{0.7}).GetDecimal();
    if (m_split_ratio <= 0 || m_split_ratio > 1.0) {
      throw std::runtime_error("split_ratio must be in (0, 1]");
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    const auto cols = GetInputIds();
    if (cols.empty()) {
      throw std::runtime_error("ml_robust requires at least one input");
    }

    // Split data - get train view for fitting
    auto split = ml_utils::split_by_ratio(bars, m_split_ratio);

    // Select only the input columns
    auto train_df = split.train[cols];
    auto full_df = bars[cols];

    // Fit on training data: compute median and IQR per column
    std::vector<epoch_frame::Scalar> medians, iqrs;
    medians.reserve(cols.size());
    iqrs.reserve(cols.size());

    for (const auto& col : cols) {
      const auto& train_col = train_df[col];
      epoch_frame::Scalar median = train_col.quantile(arrow::compute::QuantileOptions{0.5});
      epoch_frame::Scalar q1 = train_col.quantile(arrow::compute::QuantileOptions{0.25});
      epoch_frame::Scalar q3 = train_col.quantile(arrow::compute::QuantileOptions{0.75});
      medians.push_back(median);
      iqrs.push_back(q3 - q1);
    }

    // Apply transformation: (x - median) / IQR
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    const epoch_frame::Scalar min_err{1e-10};
    for (size_t j = 0; j < cols.size(); ++j) {
      const auto& series = full_df[cols[j]];
      epoch_frame::Series scaled = series - medians[j];

      if (iqrs[j] > min_err) {
        scaled = scaled / iqrs[j];
      }

      output_columns.push_back(GetOutputId("scaled_" + std::to_string(j)));
      output_arrays.push_back(scaled.array());
    }

    return epoch_frame::make_dataframe(bars.index(), output_arrays, output_columns);
  }

private:
  double m_split_ratio{0.7};
};

} // namespace epoch_script::transform
