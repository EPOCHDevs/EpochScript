#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

/**
 * @brief Create GARCH metadata for rolling variant only
 *
 * GARCH (Generalized Autoregressive Conditional Heteroskedasticity) models
 * conditional variance for volatility forecasting.
 *
 * NOTE: Static GARCH removed - see EXTENSION_PLAN.md for rationale.
 * Future: garch_report as Reporter transform for research/visualization.
 *
 * Financial Applications:
 * - Option pricing (volatility input)
 * - VaR/CVaR risk management
 * - Position sizing based on volatility
 * - Regime detection via volatility levels
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeGARCHMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Rolling GARCH options
  auto makeRollingGARCHOptions = []() -> MetaDataOptionList {
    return {
      MetaDataOption{
        .id = "p",
        .name = "ARCH Order (p)",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 1,
        .max = 5,
        .desc = "ARCH order - number of lagged squared residuals"
      },
      MetaDataOption{
        .id = "q",
        .name = "GARCH Order (q)",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 1,
        .max = 5,
        .desc = "GARCH order - number of lagged variances"
      },
      MetaDataOption{
        .id = "distribution",
        .name = "Error Distribution",
        .type = epoch_core::MetaDataOptionType::Select,
        .defaultValue = MetaDataOptionDefinition(std::string("normal")),
        .selectOption = {
          {"Normal (Gaussian)", "normal"},
          {"Student's t", "studentt"}
        },
        .desc = "Distribution assumption for standardized residuals"
      },
      MetaDataOption{
        .id = "max_iterations",
        .name = "Max Iterations",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(500.0),
        .min = 100,
        .max = 5000,
        .desc = "Maximum optimization iterations"
      },
      MetaDataOption{
        .id = "tolerance",
        .name = "Convergence Tolerance",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(1e-8),
        .min = 1e-12,
        .max = 1e-4,
        .desc = "Convergence tolerance for optimization"
      },
      MetaDataOption{
        .id = "window_size",
        .name = "Window Size",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(252.0),
        .min = 50,
        .max = 2520,
        .desc = "Training window size (252 = 1 trading year)"
      },
      MetaDataOption{
        .id = "step_size",
        .name = "Step Size",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 1,
        .max = 21,
        .desc = "Rows to advance per retrain (must be >= forecast_horizon)"
      },
      MetaDataOption{
        .id = "window_type",
        .name = "Window Type",
        .type = epoch_core::MetaDataOptionType::Select,
        .defaultValue = MetaDataOptionDefinition(std::string("rolling")),
        .selectOption = {
          {"Rolling", "rolling"},
          {"Expanding", "expanding"}
        },
        .desc = "Rolling uses fixed window, expanding grows from initial"
      },
      MetaDataOption{
        .id = "forecast_horizon",
        .name = "Forecast Horizon",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 1,
        .max = 21,
        .desc = "Steps ahead to forecast"
      },
      MetaDataOption{
        .id = "min_training_samples",
        .name = "Min Training Samples",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(100.0),
        .min = 50,
        .max = 1000,
        .desc = "Minimum samples required for estimation"
      }
    };
  };

  // Rolling GARCH (only variant - static removed)
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_garch",
    .category = epoch_core::TransformCategory::Volatility,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "Rolling GARCH Volatility",
    .options = makeRollingGARCHOptions(),
    .isCrossSectional = false,
    .desc = "Walk-forward GARCH volatility estimation. Retrains model as window advances "
            "for adaptive volatility forecasting in changing market conditions.",
    .inputs = {{epoch_core::IODataType::Decimal, ARG, "Returns", false, false}},
    .outputs = {
      {epoch_core::IODataType::Decimal, "conditional_variance", "Fitted σ²_t at each timestamp. Use for position sizing (inverse volatility weighting).", true, false},
      {epoch_core::IODataType::Decimal, "forecast_variance", "h-step ahead variance forecast. Only valid at step boundaries.", true, false},
      {epoch_core::IODataType::Decimal, "volatility", "sqrt(conditional_variance). Annualize by multiplying by sqrt(252) for daily data.", true, false},
      {epoch_core::IODataType::Decimal, "forecast_volatility", "sqrt(forecast_variance). h-step ahead volatility prediction.", true, false},
      {epoch_core::IODataType::Decimal, "persistence", "alpha + beta sum. Values > 0.9 indicate highly persistent volatility (slow mean reversion).", true, false},
      {epoch_core::IODataType::Decimal, "var_95", "Parametric 95% VaR assuming normal distribution. Multiply by position value for dollar risk.", true, false},
      {epoch_core::IODataType::Decimal, "var_99", "Parametric 99% VaR assuming normal distribution. Multiply by position value for dollar risk.", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"garch", "volatility", "rolling", "walk-forward", "adaptive"},
    .requiresTimeFrame = false,
    .strategyTypes = {"volatility-trading", "risk-management", "adaptive"},
    .relatedTransforms = {"rolling_arima"},
    .usageContext = "Use for adaptive volatility forecasting that adjusts to changing market regimes. "
                    "Output includes VaR estimates. Better than static GARCH for live trading.",
    .limitations = "Computationally more expensive than static GARCH. Requires sufficient data in each window. "
                   "step_size must be >= forecast_horizon."
  });

  return metadataList;
}

/**
 * @brief Create ARIMA metadata for rolling variant only
 *
 * ARIMA (AutoRegressive Integrated Moving Average) models for time series forecasting.
 *
 * NOTE: Static ARIMA removed - see EXTENSION_PLAN.md for rationale.
 * Future: arima_report as Reporter transform for research/visualization.
 *
 * Financial Applications:
 * - Price/return forecasting
 * - Mean reversion signals
 * - Trend extraction
 * - Residual analysis for alpha
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeARIMAMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Rolling ARIMA options
  auto makeRollingARIMAOptions = []() -> MetaDataOptionList {
    return {
      MetaDataOption{
        .id = "p",
        .name = "AR Order (p)",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 0,
        .max = 5,
        .desc = "Autoregressive order - number of lagged observations"
      },
      MetaDataOption{
        .id = "d",
        .name = "Differencing Order (d)",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(0.0),
        .min = 0,
        .max = 2,
        .desc = "Differencing order - number of times to difference for stationarity"
      },
      MetaDataOption{
        .id = "q",
        .name = "MA Order (q)",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 0,
        .max = 5,
        .desc = "Moving average order - number of lagged forecast errors"
      },
      MetaDataOption{
        .id = "with_constant",
        .name = "Include Constant",
        .type = epoch_core::MetaDataOptionType::Boolean,
        .defaultValue = MetaDataOptionDefinition(true),
        .desc = "Include constant/intercept term in model"
      },
      MetaDataOption{
        .id = "max_iterations",
        .name = "Max Iterations",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(500.0),
        .min = 100,
        .max = 5000,
        .desc = "Maximum optimization iterations"
      },
      MetaDataOption{
        .id = "tolerance",
        .name = "Convergence Tolerance",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(1e-8),
        .min = 1e-12,
        .max = 1e-4,
        .desc = "Convergence tolerance for optimization"
      },
      MetaDataOption{
        .id = "confidence_level",
        .name = "Confidence Level",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.95),
        .min = 0.8,
        .max = 0.99,
        .desc = "Confidence level for prediction intervals"
      },
      MetaDataOption{
        .id = "window_size",
        .name = "Window Size",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(252.0),
        .min = 50,
        .max = 2520,
        .desc = "Training window size (252 = 1 trading year)"
      },
      MetaDataOption{
        .id = "step_size",
        .name = "Step Size",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 1,
        .max = 21,
        .desc = "Rows to advance per retrain (must be >= forecast_horizon)"
      },
      MetaDataOption{
        .id = "window_type",
        .name = "Window Type",
        .type = epoch_core::MetaDataOptionType::Select,
        .defaultValue = MetaDataOptionDefinition(std::string("rolling")),
        .selectOption = {
          {"Rolling", "rolling"},
          {"Expanding", "expanding"}
        },
        .desc = "Rolling uses fixed window, expanding grows from initial"
      },
      MetaDataOption{
        .id = "forecast_horizon",
        .name = "Forecast Horizon",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 1,
        .max = 21,
        .desc = "Steps ahead to forecast"
      },
      MetaDataOption{
        .id = "min_training_samples",
        .name = "Min Training Samples",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(50.0),
        .min = 30,
        .max = 1000,
        .desc = "Minimum samples required for estimation"
      }
    };
  };

  // Rolling ARIMA (only variant - static removed)
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_arima",
    .category = epoch_core::TransformCategory::Statistical,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "Rolling ARIMA Forecast",
    .options = makeRollingARIMAOptions(),
    .isCrossSectional = false,
    .desc = "Walk-forward ARIMA forecasting. Retrains model as window advances "
            "for adaptive forecasting in changing market conditions.",
    .inputs = {{epoch_core::IODataType::Decimal, ARG, "Series", false, false}},
    .outputs = {
      {epoch_core::IODataType::Decimal, "forecast", "h-step ahead point forecast. Only valid at step boundaries (every step_size rows).", true, false},
      {epoch_core::IODataType::Decimal, "forecast_lower", "Lower bound of prediction interval at confidence_level. Wider = more uncertainty.", true, false},
      {epoch_core::IODataType::Decimal, "forecast_upper", "Upper bound of prediction interval at confidence_level. Wider = more uncertainty.", true, false},
      {epoch_core::IODataType::Decimal, "fitted", "In-sample fitted value from trained model. Use for assessing model fit quality.", true, false},
      {epoch_core::IODataType::Decimal, "residual", "actual - fitted. Should be ~white noise if model is correctly specified.", true, false},
      {epoch_core::IODataType::Decimal, "phi_1", "AR(1) coefficient. |phi_1| < 1 = stationary. phi_1 < 0 = mean reversion. Track stability over time.", true, false},
      {epoch_core::IODataType::Decimal, "aic", "Akaike Information Criterion. Lower = better fit. Compare across windows to detect regime changes.", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"arima", "forecast", "rolling", "walk-forward", "adaptive"},
    .requiresTimeFrame = false,
    .strategyTypes = {"mean-reversion", "trend-following", "adaptive"},
    .relatedTransforms = {"rolling_garch"},
    .usageContext = "Use for adaptive forecasting that adjusts to changing market dynamics. "
                    "Phi_1 output shows time-varying mean reversion strength. Better for live trading.",
    .limitations = "Computationally more expensive than static ARIMA. Requires sufficient data in each window. "
                   "step_size must be >= forecast_horizon."
  });

  return metadataList;
}

} // namespace epoch_script::transform
