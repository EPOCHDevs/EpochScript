#pragma once
/**
 * @file rolling_garch.h
 * @brief Rolling GARCH Transform
 *
 * Implements walk-forward GARCH(1,1) volatility estimation using a
 * rolling window approach. Retrains the model as the window advances.
 *
 * Financial Applications:
 * - Adaptive volatility forecasting
 * - Time-varying VaR estimation
 * - Regime-adaptive risk modeling
 * - Walk-forward volatility trading signals
 */

#include "rolling_ts_base.h"
#include "../garch/garch_core.h"
#include "../garch/garch_types.h"

namespace epoch_script::transform {

/**
 * @brief Output vectors for Rolling GARCH
 */
struct RollingGARCHOutputs {
  std::vector<double> conditional_variance;   // Current period variance estimate
  std::vector<double> forecast_variance;      // h-step ahead forecast
  std::vector<double> volatility;             // sqrt(conditional_variance)
  std::vector<double> forecast_volatility;    // sqrt(forecast_variance)
  std::vector<double> persistence;            // alpha + beta (stability)
  std::vector<double> var_95;                 // 95% VaR (parametric)
  std::vector<double> var_99;                 // 99% VaR (parametric)
};

/**
 * @brief Rolling GARCH Transform
 *
 * Performs GARCH(1,1) estimation on a rolling/expanding window basis.
 * At each step, retrains the model and produces volatility forecasts.
 *
 * Key Parameters:
 * - window_size: Training window size (default 252, one trading year)
 * - step_size: Rows to advance per retrain (default 1)
 * - window_type: "rolling" or "expanding"
 * - forecast_horizon: Steps ahead to forecast (default 1)
 * - distribution: "gaussian" or "studentt"
 */
class RollingGARCHTransform final
    : public RollingTSBase<RollingGARCHTransform, timeseries::garch::GARCHFitResult> {
public:
  using Base = RollingTSBase<RollingGARCHTransform, timeseries::garch::GARCHFitResult>;
  using OutputVectors = RollingGARCHOutputs;
  using FitResult = timeseries::garch::GARCHFitResult;

  explicit RollingGARCHTransform(const TransformConfiguration& cfg)
      : Base(cfg) {
    // GARCH-specific options
    m_garch_config.p = static_cast<size_t>(
        cfg.GetOptionValue("p", MetaDataOptionDefinition{1.0}).GetInteger());
    m_garch_config.q = static_cast<size_t>(
        cfg.GetOptionValue("q", MetaDataOptionDefinition{1.0}).GetInteger());

    auto dist_str = cfg.GetOptionValue(
        "distribution", MetaDataOptionDefinition{std::string("normal")}).GetSelectOption();
    m_garch_config.distribution = timeseries::garch::ParseDistributionType(dist_str);

    m_garch_config.max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations", MetaDataOptionDefinition{500.0}).GetInteger());
    m_garch_config.tolerance = cfg.GetOptionValue(
        "tolerance", MetaDataOptionDefinition{1e-8}).GetDecimal();

    m_garch_config.min_training_samples = m_config.min_training_samples;
  }

  /**
   * @brief Fit GARCH model on training window
   */
  [[nodiscard]] FitResult FitModel(const arma::vec& y) const {
    return timeseries::garch::FitGARCH(y, m_garch_config);
  }

  /**
   * @brief Check if fit result is valid
   */
  [[nodiscard]] bool IsValidFit(const FitResult& result) const {
    return result.converged && result.params.IsStationary();
  }

  /**
   * @brief Extract outputs from fit result
   */
  void ExtractOutputs(const FitResult& result,
                      const arma::vec& train_y,
                      const ml_utils::WindowSpec& window,
                      OutputVectors& outputs,
                      size_t output_offset) const {
    // Get forecast horizon from config
    size_t h = m_config.forecast_horizon;

    // Compute h-step variance forecast
    arma::vec forecast = timeseries::garch::ForecastVariance(result, h);
    double h_step_var = forecast(h - 1);  // h-step ahead

    // Current period variance (last value in fitted series)
    double current_var = result.conditional_variance(result.conditional_variance.n_elem - 1);

    // Persistence
    double persistence = result.params.Persistence();

    // Number of prediction points in this window
    size_t n_predict = std::min(window.predict_end, static_cast<size_t>(train_y.n_elem) + m_config.step_size)
                       - window.predict_start;

    // For step_size > 1, all points in prediction window get same forecast
    for (size_t i = 0; i < n_predict && (output_offset + i) < outputs.conditional_variance.size(); ++i) {
      size_t idx = output_offset + i;

      outputs.conditional_variance[idx] = current_var;
      outputs.forecast_variance[idx] = h_step_var;
      outputs.volatility[idx] = std::sqrt(current_var);
      outputs.forecast_volatility[idx] = std::sqrt(h_step_var);
      outputs.persistence[idx] = persistence;

      // Parametric VaR (assuming zero mean)
      // VaR_alpha = -z_alpha * sigma
      double vol = std::sqrt(h_step_var);
      outputs.var_95[idx] = 1.645 * vol;  // 95% VaR
      outputs.var_99[idx] = 2.326 * vol;  // 99% VaR
    }
  }

  /**
   * @brief Get output column names
   */
  [[nodiscard]] std::vector<std::string> GetOutputColumnNames() const {
    return {
      GetOutputId("conditional_variance"),
      GetOutputId("forecast_variance"),
      GetOutputId("volatility"),
      GetOutputId("forecast_volatility"),
      GetOutputId("persistence"),
      GetOutputId("var_95"),
      GetOutputId("var_99")
    };
  }

  /**
   * @brief Initialize output vectors
   */
  void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const {
    const double nan_val = std::numeric_limits<double>::quiet_NaN();
    outputs.conditional_variance.resize(n_rows, nan_val);
    outputs.forecast_variance.resize(n_rows, nan_val);
    outputs.volatility.resize(n_rows, nan_val);
    outputs.forecast_volatility.resize(n_rows, nan_val);
    outputs.persistence.resize(n_rows, nan_val);
    outputs.var_95.resize(n_rows, nan_val);
    outputs.var_99.resize(n_rows, nan_val);
  }

  /**
   * @brief Build output DataFrame
   */
  [[nodiscard]] epoch_frame::DataFrame BuildOutputDataFrame(
      const epoch_frame::IndexPtr& index,
      const OutputVectors& outputs,
      const std::vector<std::string>& column_names) const {
    std::vector<arrow::ChunkedArrayPtr> arrays;

    arrays.push_back(epoch_frame::factory::array::make_array(outputs.conditional_variance));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.forecast_variance));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.volatility));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.forecast_volatility));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.persistence));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.var_95));
    arrays.push_back(epoch_frame::factory::array::make_array(outputs.var_99));

    return epoch_frame::make_dataframe(index, arrays, column_names);
  }

private:
  timeseries::garch::GARCHConfig m_garch_config;
};

} // namespace epoch_script::transform
