#pragma once
// Time series econometric transforms registration
// Provides ARIMA and GARCH models for forecasting and volatility
//
// Categories:
// 1. Volatility Models - Conditional variance estimation
//    - rolling_garch: Walk-forward GARCH volatility forecasting
// 2. Forecasting Models - Time series prediction
//    - rolling_arima: Walk-forward ARIMA price/return forecasting
//
// Note: Static ARIMA/GARCH removed - rolling variants are more practical
// for trading applications. See EXTENSION_PLAN.md for future enhancements.

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "rolling/rolling_garch.h"
#include "rolling/rolling_arima.h"

// Metadata definitions
#include "timeseries_metadata.h"

namespace epoch_script::transform::timeseries {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all time series transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // VOLATILITY MODELS - GARCH
    // =========================================================================
    // GARCH (Generalized Autoregressive Conditional Heteroskedasticity)
    // Models conditional variance for volatility forecasting.
    // Uses walk-forward approach - retrains as window advances.

    // rolling_garch: Walk-forward GARCH volatility estimation
    // Input: returns (log returns or simple returns)
    // Options: p (ARCH order), q (GARCH order), window_size, step_size
    // Outputs: conditional_variance, forecast_variance, volatility,
    //          forecast_volatility, persistence, var_95, var_99
    // Use for: Position sizing (inverse vol weighting), option pricing,
    //          VaR calculation, regime detection via volatility levels
    epoch_script::transform::Register<RollingGARCHTransform>("rolling_garch");

    // =========================================================================
    // FORECASTING MODELS - ARIMA
    // =========================================================================
    // ARIMA (Autoregressive Integrated Moving Average)
    // Classical time series forecasting model.
    // Uses walk-forward approach - retrains as window advances.

    // rolling_arima: Walk-forward ARIMA forecasting
    // Input: series (prices or returns)
    // Options: p (AR order), d (differencing), q (MA order),
    //          window_size, step_size, forecast_horizon
    // Outputs: forecast, forecast_upper, forecast_lower,
    //          residual, fitted, aic, bic
    // Use for: Price/return prediction, mean reversion signals,
    //          trend following confirmation, residual analysis
    epoch_script::transform::Register<RollingARIMATransform>("rolling_arima");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // GARCH metadata (from timeseries_metadata.h)
    for (const auto& metadata : MakeGARCHMetaData()) {
        metaRegistry.Register(metadata);
    }

    // ARIMA metadata (from timeseries_metadata.h)
    for (const auto& metadata : MakeARIMAMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::timeseries
