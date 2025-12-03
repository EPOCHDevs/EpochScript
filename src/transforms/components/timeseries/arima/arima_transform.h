#pragma once
/**
 * @file arima_transform.h
 * @brief ARIMA Transform for time series forecasting
 *
 * Implements ARIMA(p,d,q) models for financial time series analysis.
 *
 * Financial Applications:
 * - Price/return forecasting
 * - Mean reversion analysis
 * - Trend extraction
 * - Residual analysis for alpha generation
 */

#include "arima_core.h"
#include "../../statistics/dataframe_armadillo_utils.h"
#include "../../ml/ml_split_utils.h"
#include <epoch_script/transforms/core/itransform.h>
#include <cmath>
#include <epoch_frame/aliases.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

namespace epoch_script::transform {

/**
 * @brief ARIMA Transform for time series forecasting
 *
 * Outputs:
 * - fitted: Fitted values on original scale
 * - residuals: Model residuals
 * - forecast: Point forecasts (at last observation)
 * - forecast_lower: Lower prediction interval
 * - forecast_upper: Upper prediction interval
 * - aic, bic: Model information criteria
 */
class ARIMATransform final : public ITransform {
public:
  explicit ARIMATransform(const TransformConfiguration &cfg) : ITransform(cfg) {
    // ARIMA order parameters
    m_config.p = static_cast<size_t>(
        cfg.GetOptionValue("p", MetaDataOptionDefinition{1.0}).GetInteger());
    m_config.d = static_cast<size_t>(
        cfg.GetOptionValue("d", MetaDataOptionDefinition{0.0}).GetInteger());
    m_config.q = static_cast<size_t>(
        cfg.GetOptionValue("q", MetaDataOptionDefinition{1.0}).GetInteger());

    m_config.with_constant = cfg.GetOptionValue(
        "with_constant", MetaDataOptionDefinition{true}).GetBoolean();

    // Optimization parameters
    m_config.max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations", MetaDataOptionDefinition{500.0}).GetInteger());
    m_config.tolerance = cfg.GetOptionValue(
        "tolerance", MetaDataOptionDefinition{1e-8}).GetDecimal();

    // Forecasting parameters
    m_config.forecast_horizon = static_cast<size_t>(
        cfg.GetOptionValue("forecast_horizon", MetaDataOptionDefinition{1.0}).GetInteger());
    m_config.confidence_level = cfg.GetOptionValue(
        "confidence_level", MetaDataOptionDefinition{0.95}).GetDecimal();

    // Training parameters
    m_split_ratio = cfg.GetOptionValue(
        "split_ratio", MetaDataOptionDefinition{1.0}).GetDecimal();  // Default: use all data
    m_split_gap = static_cast<size_t>(
        cfg.GetOptionValue("split_gap", MetaDataOptionDefinition{0.0}).GetInteger());  // Purge gap
    m_config.min_training_samples = static_cast<size_t>(
        cfg.GetOptionValue("min_training_samples", MetaDataOptionDefinition{50.0}).GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    // Get input column
    const std::string input_col = GetInputId();
    if (input_col.empty()) {
      throw std::runtime_error("ARIMATransform requires an input column.");
    }

    // Extract series as vector
    arma::vec y = utils::VecFromDataFrame(bars, input_col);

    if (y.n_elem < m_config.min_training_samples) {
      throw std::runtime_error(
          "Insufficient data for ARIMA estimation. Required: " +
          std::to_string(m_config.min_training_samples) +
          ", Got: " + std::to_string(y.n_elem));
    }

    // Compute training size using split utilities pattern
    size_t train_size = ComputeTrainSize(y.n_elem);
    epoch_frame::IndexPtr output_index = bars.index();

    // Extract training data
    arma::vec training_y = (train_size < y.n_elem)
        ? y.subvec(0, train_size - 1)
        : y;

    // Fit ARIMA model
    auto fit_result = timeseries::arima::FitARIMA(training_y, m_config);

    if (!fit_result.converged) {
      throw std::runtime_error("ARIMA estimation failed to converge: " + fit_result.message);
    }

    // If we have a split, re-compute fitted values on full series
    timeseries::arima::ARIMAFitResult full_fit;
    if (train_size < y.n_elem) {
      // Re-fit on full data using trained parameters as starting point
      full_fit = timeseries::arima::FitARIMA(y, m_config);
    } else {
      full_fit = fit_result;
    }

    // Compute forecast
    auto forecast = timeseries::arima::Forecast(full_fit, m_config.forecast_horizon,
                                                 m_config.confidence_level);

    return GenerateOutputs(output_index, y, full_fit, forecast);
  }

private:
  timeseries::arima::ARIMAConfig m_config;
  double m_split_ratio{1.0};  ///< Training split ratio (1.0 = use all data)
  size_t m_split_gap{0};      ///< Purge gap between train and test (Marcos López de Prado)

  /**
   * @brief Compute training size following ml_split_utils pattern
   */
  [[nodiscard]] size_t ComputeTrainSize(size_t n_rows) const {
    if (m_split_ratio >= 1.0) {
      return n_rows;  // Use all data
    }
    return static_cast<size_t>(std::ceil(n_rows * m_split_ratio));
  }

  /**
   * @brief Generate output DataFrame with ARIMA results
   */
  epoch_frame::DataFrame GenerateOutputs(
      const epoch_frame::IndexPtr& index,
      const arma::vec& y,
      const timeseries::arima::ARIMAFitResult& fit,
      const timeseries::arima::ARIMAForecast& forecast) const {

    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;
    const size_t T = y.n_elem;

    // Fitted values - direct conversion, no intermediate vector
    output_columns.push_back(GetOutputId("fitted"));
    output_arrays.push_back(utils::ArrayFromVec(fit.fitted));

    // Residuals (y - fitted) - use Armadillo subtraction, then convert
    arma::vec residuals = y - fit.fitted;
    output_columns.push_back(GetOutputId("residuals"));
    output_arrays.push_back(utils::ArrayFromVec(residuals));

    // Multi-horizon forecasts (forecast_1, forecast_2, ..., forecast_h)
    // Each set of columns represents h-step ahead forecast with confidence intervals
    // Only available at last timestamp to prevent forward bias
    const size_t h = m_config.forecast_horizon;
    for (size_t step = 1; step <= h; ++step) {
      // Point forecast
      double fc_val = (forecast.point.n_elem >= step)
          ? forecast.point(step - 1)
          : std::numeric_limits<double>::quiet_NaN();
      output_columns.push_back(GetOutputId("forecast_" + std::to_string(step)));
      output_arrays.push_back(utils::ArrayWithLastValue(T, fc_val));

      // Lower bound
      double lower_val = (forecast.lower.n_elem >= step)
          ? forecast.lower(step - 1)
          : std::numeric_limits<double>::quiet_NaN();
      output_columns.push_back(GetOutputId("forecast_" + std::to_string(step) + "_lower"));
      output_arrays.push_back(utils::ArrayWithLastValue(T, lower_val));

      // Upper bound
      double upper_val = (forecast.upper.n_elem >= step)
          ? forecast.upper(step - 1)
          : std::numeric_limits<double>::quiet_NaN();
      output_columns.push_back(GetOutputId("forecast_" + std::to_string(step) + "_upper"));
      output_arrays.push_back(utils::ArrayWithLastValue(T, upper_val));
    }

    // Model diagnostics - only available at last timestamp to prevent forward bias
    output_columns.push_back(GetOutputId("aic"));
    output_arrays.push_back(utils::ArrayWithLastValue(T, fit.aic));

    output_columns.push_back(GetOutputId("bic"));
    output_arrays.push_back(utils::ArrayWithLastValue(T, fit.bic));

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

// Type aliases for common ARIMA specifications
using ARIMA110Transform = ARIMATransform;  // Default ARIMA(1,1,0)

}  // namespace epoch_script::transform
