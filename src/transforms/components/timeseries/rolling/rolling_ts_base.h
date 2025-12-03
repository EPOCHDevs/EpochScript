#pragma once
/**
 * @file rolling_ts_base.h
 * @brief CRTP Base Class for Rolling Time Series Transforms
 *
 * Provides common rolling infrastructure for univariate time series models
 * like GARCH and ARIMA. Unlike ML models, these take a single input series
 * and produce forecasts, fitted values, and model diagnostics.
 */

#include <epoch_script/transforms/core/itransform.h>
#include "../../ml/rolling_window_iterator.h"
#include "../../statistics/dataframe_armadillo_utils.h"
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <armadillo>
#include <vector>
#include <string>

namespace epoch_script::transform {

/**
 * @brief Configuration for rolling time series transforms
 */
struct RollingTSConfig {
  size_t window_size{252};          // Training window size
  size_t step_size{1};              // Rows to advance per window
  ml_utils::WindowType window_type{ml_utils::WindowType::Rolling};
  size_t min_training_samples{100}; // Minimum samples required
  size_t forecast_horizon{1};       // How many steps ahead to forecast
};

/**
 * @brief CRTP Base class for Rolling Time Series Transforms
 *
 * For univariate models (GARCH, ARIMA) that train on historical data
 * and produce forecasts and diagnostics.
 *
 * Derived classes must implement:
 * - FitResultType FitModel(const arma::vec& y) const
 * - void ExtractOutputs(const FitResultType& result, const arma::vec& y,
 *                       size_t window_end_idx, OutputVectors& outputs) const
 * - std::vector<std::string> GetOutputColumnNames() const
 * - void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const
 *
 * Template Parameters:
 *   Derived - The derived class (CRTP pattern)
 *   FitResultType - The result type from model fitting
 */
template<typename Derived, typename FitResultType>
class RollingTSBase : public ITransform {
public:
  explicit RollingTSBase(const TransformConfiguration& cfg)
    : ITransform(cfg) {
    // Parse rolling options
    m_config.window_size = static_cast<size_t>(
      cfg.GetOptionValue("window_size",
                         MetaDataOptionDefinition{252.0}).GetInteger());

    m_config.step_size = static_cast<size_t>(
      cfg.GetOptionValue("step_size",
                         MetaDataOptionDefinition{1.0}).GetInteger());

    auto window_type_str = cfg.GetOptionValue(
      "window_type", MetaDataOptionDefinition{std::string("rolling")}).GetSelectOption();
    m_config.window_type = ml_utils::ParseWindowType(window_type_str);

    m_config.min_training_samples = static_cast<size_t>(
      cfg.GetOptionValue("min_training_samples",
                         MetaDataOptionDefinition{100.0}).GetInteger());

    m_config.forecast_horizon = static_cast<size_t>(
      cfg.GetOptionValue("forecast_horizon",
                         MetaDataOptionDefinition{1.0}).GetInteger());

    // Validate: step_size must be >= forecast_horizon
    // This ensures we don't have gaps in forecasts when stepping forward
    if (m_config.step_size < m_config.forecast_horizon) {
      throw std::runtime_error(
          "step_size (" + std::to_string(m_config.step_size) +
          ") must be >= forecast_horizon (" +
          std::to_string(m_config.forecast_horizon) +
          "). Adjust step_size or forecast_horizon.");
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame& bars) const override {
    const auto& derived = static_cast<const Derived&>(*this);

    // Get input column (univariate)
    const std::string input_col = GetInputId();
    if (input_col.empty()) {
      throw std::runtime_error("Rolling time series transform requires an input column");
    }

    // Convert to armadillo vector
    arma::vec y = utils::VecFromDataFrame(bars, input_col);
    const size_t n_rows = y.n_elem;

    // Validate
    if (n_rows < m_config.window_size) {
      throw std::runtime_error("Insufficient data for rolling TS. Required: " +
                               std::to_string(m_config.window_size) +
                               ", Got: " + std::to_string(n_rows));
    }

    if (m_config.window_size < m_config.min_training_samples) {
      throw std::runtime_error("window_size (" + std::to_string(m_config.window_size) +
                               ") must be >= min_training_samples (" +
                               std::to_string(m_config.min_training_samples) + ")");
    }

    // Create rolling window iterator
    ml_utils::RollingWindowIterator iterator(
      n_rows,
      m_config.window_size,
      m_config.step_size,
      m_config.window_type
    );

    // Get output column names from derived
    auto output_names = derived.GetOutputColumnNames();

    // Output rows (after initial training window)
    const size_t output_rows = n_rows - m_config.window_size;

    // Initialize output vectors
    typename Derived::OutputVectors outputs;
    derived.InitializeOutputVectors(outputs, output_rows);

    size_t output_offset = 0;

    // Main rolling loop
    iterator.ForEach([&](const ml_utils::WindowSpec& window) {
      // Extract training data
      arma::vec train_y = y.subvec(window.train_start, window.train_end - 1);

      // Fit model
      FitResultType fit_result;
      bool success = false;

      try {
        fit_result = derived.FitModel(train_y);
        success = derived.IsValidFit(fit_result);
      } catch (const std::exception&) {
        success = false;
      }

      // Extract outputs for prediction window
      if (window.predict_start < n_rows && window.predict_start < window.predict_end) {
        size_t n_predict = std::min(window.predict_end, n_rows) - window.predict_start;

        if (success) {
          derived.ExtractOutputs(fit_result, train_y, window, outputs, output_offset);
        }
        // If failed, outputs remain NaN (initialized state)

        output_offset += n_predict;
      }
    });

    // Build output DataFrame
    epoch_frame::IndexPtr output_index = ml_utils::RollingOutputBuilder::SliceOutputIndex(
      bars.index(), m_config.window_size);

    return derived.BuildOutputDataFrame(output_index, outputs, output_names);
  }

protected:
  RollingTSConfig m_config;

  [[nodiscard]] const RollingTSConfig& GetRollingConfig() const { return m_config; }
};

} // namespace epoch_script::transform
