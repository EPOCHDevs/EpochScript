#pragma once
/**
 * @file garch_transform.h
 * @brief GARCH Transform for volatility modeling and forecasting
 *
 * Implements GARCH(p,q) models for financial volatility analysis.
 * Models conditional variance: sigma^2_t = omega + alpha*eps^2_{t-1} + beta*sigma^2_{t-1}
 *
 * Financial Applications:
 * - Option pricing (volatility input)
 * - VaR/CVaR risk management
 * - Volatility forecasting for position sizing
 * - Regime detection via volatility levels
 */

#include "garch_core.h"
#include "../../statistics/dataframe_armadillo_utils.h"
#include "../../ml/ml_split_utils.h"
#include <epoch_script/transforms/core/itransform.h>
#include <cmath>
#include <epoch_frame/aliases.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

namespace epoch_script::transform {

/**
 * @brief GARCH Transform for volatility modeling
 *
 * Template parameter GARCH_TYPE allows different GARCH variants:
 * - GARCHType::GARCH: Standard GARCH(p,q)
 * - GARCHType::EGARCH: Exponential GARCH (future)
 * - GARCHType::TARCH: Threshold GARCH (future)
 * - GARCHType::FIGARCH: Fractionally Integrated GARCH (future)
 *
 * @tparam GARCH_TYPE The type of GARCH model to use
 */
template <timeseries::garch::GARCHType GARCH_TYPE = timeseries::garch::GARCHType::GARCH>
class GARCHTransform final : public ITransform {
public:
  explicit GARCHTransform(const TransformConfiguration &cfg) : ITransform(cfg) {
    // GARCH order parameters
    m_config.p = static_cast<size_t>(
        cfg.GetOptionValue("p", MetaDataOptionDefinition{1.0}).GetInteger());
    m_config.q = static_cast<size_t>(
        cfg.GetOptionValue("q", MetaDataOptionDefinition{1.0}).GetInteger());

    // Distribution type
    auto dist_str = cfg.GetOptionValue(
        "distribution", MetaDataOptionDefinition{std::string("normal")}).GetSelectOption();
    m_config.distribution = timeseries::garch::ParseDistributionType(dist_str);

    m_config.df = cfg.GetOptionValue("df", MetaDataOptionDefinition{8.0}).GetDecimal();

    // Optimization parameters
    m_config.max_iterations = static_cast<size_t>(
        cfg.GetOptionValue("max_iterations", MetaDataOptionDefinition{500.0}).GetInteger());
    m_config.tolerance = cfg.GetOptionValue(
        "tolerance", MetaDataOptionDefinition{1e-8}).GetDecimal();

    // Training parameters
    m_config.forecast_horizon = static_cast<size_t>(
        cfg.GetOptionValue("forecast_horizon", MetaDataOptionDefinition{1.0}).GetInteger());
    m_split_ratio = cfg.GetOptionValue(
        "split_ratio", MetaDataOptionDefinition{1.0}).GetDecimal();  // Default: use all data
    m_split_gap = static_cast<size_t>(
        cfg.GetOptionValue("split_gap", MetaDataOptionDefinition{0.0}).GetInteger());  // Purge gap
    m_config.min_training_samples = static_cast<size_t>(
        cfg.GetOptionValue("min_training_samples", MetaDataOptionDefinition{100.0}).GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &bars) const override {
    // Get input column (returns series)
    const std::string input_col = GetInputId();
    if (input_col.empty()) {
      throw std::runtime_error("GARCHTransform requires an input column (returns).");
    }

    // Extract returns as vector
    arma::vec returns = utils::VecFromDataFrame(bars, input_col);

    if (returns.n_elem < m_config.min_training_samples) {
      throw std::runtime_error(
          "Insufficient data for GARCH estimation. Required: " +
          std::to_string(m_config.min_training_samples) +
          ", Got: " + std::to_string(returns.n_elem));
    }

    // Compute training size using ml_split_utils pattern
    size_t train_size = ComputeTrainSize(returns.n_elem);
    epoch_frame::IndexPtr output_index = bars.index();

    // Extract training data
    arma::vec training_returns = (train_size < returns.n_elem)
        ? returns.subvec(0, train_size - 1)
        : returns;

    // Fit GARCH model
    auto fit_result = timeseries::garch::FitGARCH(training_returns, m_config);

    if (!fit_result.converged) {
      throw std::runtime_error("GARCH estimation failed to converge: " + fit_result.message);
    }

    // Re-compute variance on full series for output alignment
    arma::vec full_variance;
    arma::vec full_std_resid;
    double mean_return = arma::mean(returns);
    arma::vec eps = returns - mean_return;

    full_variance = timeseries::garch::ComputeConditionalVariance(eps, fit_result.params);
    full_std_resid = eps / arma::sqrt(full_variance);

    // Compute volatility forecast
    arma::vec vol_forecast = timeseries::garch::ForecastVariance(fit_result, m_config.forecast_horizon);

    return GenerateOutputs(output_index, full_variance, full_std_resid,
                           vol_forecast, fit_result);
  }

private:
  timeseries::garch::GARCHConfig m_config;
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
   * @brief Generate output DataFrame with GARCH results
   */
  epoch_frame::DataFrame GenerateOutputs(
      const epoch_frame::IndexPtr& index,
      const arma::vec& conditional_variance,
      const arma::vec& standardized_residuals,
      const arma::vec& variance_forecast,
      const timeseries::garch::GARCHFitResult& fit) const {

    std::vector<std::string> output_columns;
    std::vector<arrow::ChunkedArrayPtr> output_arrays;
    const size_t T = conditional_variance.n_elem;

    // Conditional volatility (sqrt of variance) - direct conversion, no intermediate vector
    output_columns.push_back(GetOutputId("conditional_volatility"));
    output_arrays.push_back(utils::ArrayFromVecSqrt(conditional_variance));

    // Multi-horizon volatility forecasts (vol_forecast_1, vol_forecast_2, ..., vol_forecast_h)
    // Each column represents h-step ahead forecast, only available at last timestamp
    const size_t h = m_config.forecast_horizon;
    for (size_t step = 1; step <= h; ++step) {
      double forecast_val = (variance_forecast.n_elem >= step)
          ? std::sqrt(variance_forecast(step - 1))
          : std::numeric_limits<double>::quiet_NaN();
      output_columns.push_back(GetOutputId("vol_forecast_" + std::to_string(step)));
      output_arrays.push_back(utils::ArrayWithLastValue(T, forecast_val));
    }

    // Standardized residuals - direct conversion, no intermediate vector
    output_columns.push_back(GetOutputId("standardized_residuals"));
    output_arrays.push_back(utils::ArrayFromVec(standardized_residuals));

    // Model diagnostics - only available at last timestamp to prevent forward bias
    output_columns.push_back(GetOutputId("aic"));
    output_arrays.push_back(utils::ArrayWithLastValue(T, fit.aic));

    output_columns.push_back(GetOutputId("bic"));
    output_arrays.push_back(utils::ArrayWithLastValue(T, fit.bic));

    output_columns.push_back(GetOutputId("log_likelihood"));
    output_arrays.push_back(utils::ArrayWithLastValue(T, fit.log_likelihood));

    return epoch_frame::make_dataframe(index, output_arrays, output_columns);
  }
};

// Type aliases for common GARCH specifications
using GARCH11Transform = GARCHTransform<timeseries::garch::GARCHType::GARCH>;
// Future variants:
// using EGARCHTransform = GARCHTransform<timeseries::garch::GARCHType::EGARCH>;
// using TARCHTransform = GARCHTransform<timeseries::garch::GARCHType::TARCH>;
// using FIGARCHTransform = GARCHTransform<timeseries::garch::GARCHType::FIGARCH>;

}  // namespace epoch_script::transform
