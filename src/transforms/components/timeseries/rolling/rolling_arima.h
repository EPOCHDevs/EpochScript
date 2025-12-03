#pragma once
/**
 * @file rolling_arima.h
 * @brief Rolling ARIMA Transform
 *
 * Implements walk-forward ARIMA estimation using a rolling window approach.
 * Retrains the model as the window advances for adaptive forecasting.
 *
 * Financial Applications:
 * - Adaptive price/return forecasting
 * - Time-varying mean reversion signals
 * - Walk-forward trend estimation
 * - Dynamic residual analysis for alpha
 */

#include "rolling_ts_base.h"
#include "../arima/arima_core.h"
#include "../arima/arima_types.h"

namespace epoch_script::transform {

/**
 * @brief Output vectors for Rolling ARIMA
 */
struct RollingARIMAOutputs {
  std::vector<double> forecast;           // Point forecast
  std::vector<double> forecast_lower;     // Lower confidence bound
  std::vector<double> forecast_upper;     // Upper confidence bound
  std::vector<double> fitted;             // Fitted value at window end
  std::vector<double> residual;           // Residual at window end
  std::vector<double> phi_1;              // AR(1) coefficient (for tracking)
  std::vector<double> aic;                // Model AIC
};

/**
 * @brief Rolling ARIMA Transform
 *
 * Performs ARIMA estimation on a rolling/expanding window basis.
 * At each step, retrains the model and produces forecasts.
 *
 * Key Parameters:
 * - p: AR order (default 1)
 * - d: Differencing order (default 0)
 * - q: MA order (default 1)
 * - window_size: Training window size (default 252)
 * - step_size: Rows to advance per retrain (default 1)
 * - window_type: "rolling" or "expanding"
 * - forecast_horizon: Steps ahead to forecast (default 1)
 * - confidence_level: For prediction intervals (default 0.95)
 */
class RollingARIMATransform final
    : public RollingTSBase<RollingARIMATransform, timeseries::arima::ARIMAFitResult> {
public:
  using Base = RollingTSBase<RollingARIMATransform, timeseries::arima::ARIMAFitResult>;
  using OutputVectors = RollingARIMAOutputs;
  using FitResult = timeseries::arima::ARIMAFitResult;

  explicit RollingARIMATransform(const TransformConfiguration& cfg)
      : Base(cfg) {
    // ARIMA-specific options
    m_arima_config.p = static_cast<size_t>(
        cfg.GetOptionValue("p", MetaDataOptionDefinition{1.0}).GetInteger());
    m_arima_config.d = static_cast<size_t>(
        cfg.GetOptionValue("d", MetaDataOptionDefinition{0.0}).GetInteger());
    m_arima_config.q = static_cast<size_t>(
        cfg.GetOptionValue("q", MetaDataOptionDefinition{1.0}).GetInteger());

    m_arima_config.with_constant = cfg.GetOptionValue(
        "with_constant", MetaDataOptionDefinition{true}).GetBoolean();

    m_arima_config.max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations", MetaDataOptionDefinition{500.0}).GetInteger());
    m_arima_config.tolerance = cfg.GetOptionValue(
        "tolerance", MetaDataOptionDefinition{1e-8}).GetDecimal();

    m_arima_config.forecast_horizon = m_config.forecast_horizon;
    m_arima_config.min_training_samples = m_config.min_training_samples;

    m_confidence_level = cfg.GetOptionValue(
        "confidence_level", MetaDataOptionDefinition{0.95}).GetDecimal();
  }

  /**
   * @brief Fit ARIMA model on training window
   */
  [[nodiscard]] FitResult FitModel(const arma::vec& y) const {
    return timeseries::arima::FitARIMA(y, m_arima_config);
  }

  /**
   * @brief Check if fit result is valid
   */
  [[nodiscard]] bool IsValidFit(const FitResult& result) const {
    return result.converged;
  }

  /**
   * @brief Extract outputs from fit result
   */
  void ExtractOutputs(const FitResult& result,
                      const arma::vec& train_y,
                      const ml_utils::WindowSpec& window,
                      OutputVectors& outputs,
                      size_t output_offset) const {
    // Generate forecast
    auto forecast = timeseries::arima::Forecast(result, m_config.forecast_horizon, m_confidence_level);

    // Get values at window end
    size_t T = result.fitted.n_elem;
    double last_fitted = (T > 0) ? result.fitted(T - 1) : std::numeric_limits<double>::quiet_NaN();
    double last_residual = (T > 0) ? result.residuals(T - 1) : std::numeric_limits<double>::quiet_NaN();

    // Get AR(1) coefficient if available
    double phi1 = (result.params.p() >= 1) ? result.params.phi(0) : 0.0;

    // Number of prediction points
    size_t n_predict = std::min(window.predict_end, static_cast<size_t>(train_y.n_elem) + m_config.step_size)
                       - window.predict_start;

    // Fill outputs
    for (size_t i = 0; i < n_predict && (output_offset + i) < outputs.forecast.size(); ++i) {
      size_t idx = output_offset + i;

      // Use h-step forecast where h = i + 1 (or capped at horizon)
      size_t h = std::min(i + 1, static_cast<size_t>(forecast.point.n_elem));
      if (h > 0) {
        outputs.forecast[idx] = forecast.point(h - 1);
        outputs.forecast_lower[idx] = forecast.lower(h - 1);
        outputs.forecast_upper[idx] = forecast.upper(h - 1);
      }

      outputs.fitted[idx] = last_fitted;
      outputs.residual[idx] = last_residual;
      outputs.phi_1[idx] = phi1;
      outputs.aic[idx] = result.aic;
    }
  }

  /**
   * @brief Get output column names
   */
  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return {
      GetOutputId("forecast"),
      GetOutputId("forecast_lower"),
      GetOutputId("forecast_upper"),
      GetOutputId("fitted"),
      GetOutputId("residual"),
      GetOutputId("phi_1"),
      GetOutputId("aic")
    };
  }

  /**
   * @brief Initialize output vectors
   */
  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    const double nan_val = std::numeric_limits<double>::quiet_NaN();
    outputs.forecast.resize(n_rows, nan_val);
    outputs.forecast_lower.resize(n_rows, nan_val);
    outputs.forecast_upper.resize(n_rows, nan_val);
    outputs.fitted.resize(n_rows, nan_val);
    outputs.residual.resize(n_rows, nan_val);
    outputs.phi_1.resize(n_rows, nan_val);
    outputs.aic.resize(n_rows, nan_val);
  }

  /**
   * @brief Build output DataFrame
   */
  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;

    arrays.push_back(epoch_frame::factory::array::make_array(outputs.forecast));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.forecast_lower));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.forecast_upper));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.fitted));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.residual));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.phi_1));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.aic));

    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  timeseries::arima::ARIMAConfig m_arima_config;
  double m_confidence_level{0.95};
};

} // namespace epoch_script::transform
