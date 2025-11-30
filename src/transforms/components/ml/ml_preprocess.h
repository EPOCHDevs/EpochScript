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
#include "ml_split_utils.h"
#include "../statistics/dataframe_armadillo_utils.h"
#include "epoch_frame/aliases.h"
#include <epoch_script/transforms/core/itransform.h>
#include <arrow/array.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <armadillo>
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

    // Convert to armadillo for efficient computation
    arma::mat train_X = utils::MatFromDataFrame(split.train, cols);
    arma::mat X = utils::MatFromDataFrame(bars, cols);
    const size_t n_cols = X.n_cols;

    // Fit on training data
    std::vector<double> means(n_cols), stds(n_cols);
    for (size_t j = 0; j < n_cols; ++j) {
      means[j] = arma::mean(train_X.col(j));
      stds[j] = arma::stddev(train_X.col(j));
    }

    // Apply to full data
    for (size_t j = 0; j < n_cols; ++j) {
      if (stds[j] > 1e-10) {
        X.col(j) = (X.col(j) - means[j]) / stds[j];
      } else {
        X.col(j) = X.col(j) - means[j];
      }
    }

    return GenerateOutputs(bars.index(), X);
  }

private:
  double m_split_ratio{0.7};

  epoch_frame::DataFrame GenerateOutputs(
      const epoch_frame::IndexPtr &index,
      const arma::mat &X) const {
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    for (size_t j = 0; j < X.n_cols; ++j) {
      std::vector<double> col_vec(X.n_rows);
      for (size_t i = 0; i < X.n_rows; ++i) {
        col_vec[i] = X(i, j);
      }
      output_columns.push_back(GetOutputId("scaled_" + std::to_string(j)));
      output_arrays.push_back(epoch_frame::factory::array::make_array(col_vec));
    }

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
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

    // Convert to armadillo
    arma::mat train_X = utils::MatFromDataFrame(split.train, cols);
    arma::mat X = utils::MatFromDataFrame(bars, cols);
    const size_t n_cols = X.n_cols;

    // Fit on training data
    std::vector<double> mins(n_cols), maxs(n_cols);
    for (size_t j = 0; j < n_cols; ++j) {
      mins[j] = arma::min(train_X.col(j));
      maxs[j] = arma::max(train_X.col(j));
    }

    // Apply to full data
    for (size_t j = 0; j < n_cols; ++j) {
      double range = maxs[j] - mins[j];
      if (range > 1e-10) {
        X.col(j) = (X.col(j) - mins[j]) / range;
      } else {
        X.col(j).zeros();
      }
    }

    return GenerateOutputs(bars.index(), X);
  }

private:
  double m_split_ratio{0.7};

  epoch_frame::DataFrame GenerateOutputs(
      const epoch_frame::IndexPtr &index,
      const arma::mat &X) const {
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    for (size_t j = 0; j < X.n_cols; ++j) {
      std::vector<double> col_vec(X.n_rows);
      for (size_t i = 0; i < X.n_rows; ++i) {
        col_vec[i] = X(i, j);
      }
      output_columns.push_back(GetOutputId("scaled_" + std::to_string(j)));
      output_arrays.push_back(epoch_frame::factory::array::make_array(col_vec));
    }

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

/**
 * @brief ML Robust Scaling Transform
 *
 * Scales features using statistics that are robust to outliers.
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

    // Convert to armadillo
    arma::mat train_X = utils::MatFromDataFrame(split.train, cols);
    arma::mat X = utils::MatFromDataFrame(bars, cols);
    const size_t n_cols = X.n_cols;

    // Fit on training data
    std::vector<double> medians(n_cols), iqrs(n_cols);
    for (size_t j = 0; j < n_cols; ++j) {
      arma::vec col = arma::sort(train_X.col(j));
      medians[j] = arma::median(col);

      // IQR = Q3 - Q1
      size_t n = col.n_elem;
      double q1 = col(static_cast<size_t>(n * 0.25));
      double q3 = col(static_cast<size_t>(n * 0.75));
      iqrs[j] = q3 - q1;
    }

    // Apply to full data
    for (size_t j = 0; j < n_cols; ++j) {
      if (iqrs[j] > 1e-10) {
        X.col(j) = (X.col(j) - medians[j]) / iqrs[j];
      } else {
        X.col(j) = X.col(j) - medians[j];
      }
    }

    return GenerateOutputs(bars.index(), X);
  }

private:
  double m_split_ratio{0.7};

  epoch_frame::DataFrame GenerateOutputs(
      const epoch_frame::IndexPtr &index,
      const arma::mat &X) const {
    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;

    for (size_t j = 0; j < X.n_cols; ++j) {
      std::vector<double> col_vec(X.n_rows);
      for (size_t i = 0; i < X.n_rows; ++i) {
        col_vec[i] = X(i, j);
      }
      output_columns.push_back(GetOutputId("scaled_" + std::to_string(j)));
      output_arrays.push_back(epoch_frame::factory::array::make_array(col_vec));
    }

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

} // namespace epoch_script::transform
