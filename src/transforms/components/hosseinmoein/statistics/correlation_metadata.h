#pragma once
// Correlation and Statistical Transforms Metadata
// Provides metadata for rolling_corr, rolling_cov, beta, ewm_corr, ewm_cov, linear_fit, zscore

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transforms {

using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// ROLLING CORRELATION
// =============================================================================

inline TransformsMetaData MakeRollingCorrMetaData() {
    return TransformsMetaData{
        .id = "rolling_corr",
        .category = TransformCategory::Statistical,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Rolling Correlation",
        .options = {
            epoch_script::MetaDataOption{
                .id = "window",
                .name = "Window Size",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 3,
                .max = 500,
                .step_size = 1,
                .desc = "Rolling window size for correlation calculation",
                .tuningGuidance = "Minimum 10-20 bars for stable estimates. Use 20-50 for tactical correlation "
                                  "(daily regime changes), 60-120 for strategic allocation (monthly trends). "
                                  "Larger windows (200+) for long-term relationship analysis. Balance stability "
                                  "vs responsiveness to regime shifts."}},
        .desc = "Calculates Pearson correlation coefficient between two series over a rolling window. "
                "Values range from -1 (perfect negative correlation) to +1 (perfect positive correlation), "
                "with 0 indicating no linear relationship.",
        .inputs = {{.type = IODataType::Decimal, .id = "x", .name = "Series X"},
                   {.type = IODataType::Decimal, .id = "y", .name = "Series Y"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Correlation"}},
        .tags = {"statistics", "correlation", "rolling", "cross-asset", "pairs-trading", "relationship"},
        .requiresTimeFrame = false,
        .strategyTypes = {"pairs-trading", "lead-lag-analysis", "correlation-trading", "hedge-analysis", "cross-asset"},
        .relatedTransforms = {"rolling_cov", "linear_fit", "ewm_corr", "lag"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Use for lead-lag analysis, pairs trading, and cross-asset relationship monitoring. "
                        "Track evolving correlation between assets for pair selection, hedge effectiveness, "
                        "or diversification analysis. Common use: rolling correlation between stock and sector "
                        "index to detect beta changes, or between two assets in pairs trading to identify "
                        "correlation breakdowns.",
        .limitations = "Only detects linear relationships - may miss non-linear dependencies. Window size critical: "
                       "too small creates noise, too large misses regime changes. Sensitive to outliers. "
                       "Correlation doesn't imply causation. Non-stationary series can show spurious correlations."
    };
}

// =============================================================================
// ROLLING COVARIANCE
// =============================================================================

inline TransformsMetaData MakeRollingCovMetaData() {
    return TransformsMetaData{
        .id = "rolling_cov",
        .category = TransformCategory::Statistical,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Rolling Covariance",
        .options = {
            epoch_script::MetaDataOption{
                .id = "window",
                .name = "Window Size",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 3,
                .max = 500,
                .step_size = 1,
                .desc = "Rolling window size for covariance calculation",
                .tuningGuidance = "Minimum 10-20 bars for stable estimates. Use 20-60 for tactical risk management, "
                                  "120-252 for strategic portfolio allocation. Larger windows provide smoother "
                                  "estimates but lag regime changes."}},
        .desc = "Calculates covariance between two series over a rolling window. Measures how two variables "
                "move together, without normalization like correlation. Units are in product of the input series units.",
        .inputs = {{.type = IODataType::Decimal, .id = "x", .name = "Series X"},
                   {.type = IODataType::Decimal, .id = "y", .name = "Series Y"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Covariance"}},
        .tags = {"statistics", "covariance", "rolling", "cross-asset", "risk-management", "portfolio"},
        .requiresTimeFrame = false,
        .strategyTypes = {"risk-management", "portfolio-optimization", "factor-analysis", "variance-analysis"},
        .relatedTransforms = {"rolling_corr", "ewm_cov", "linear_fit"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Use for risk management, portfolio optimization, and understanding joint variability. "
                        "Unlike correlation, covariance preserves magnitude information useful for portfolio "
                        "variance calculations. Common use: covariance matrix construction for mean-variance "
                        "optimization, or risk factor analysis where absolute comovement matters.",
        .limitations = "Not normalized - values depend on input scales, making comparison across different "
                       "asset pairs difficult. Use rolling_corr for standardized relationship measure. "
                       "Sensitive to outliers. Requires series to be somewhat stationary for meaningful results."
    };
}

// =============================================================================
// BETA (ROLLING)
// =============================================================================

inline TransformsMetaData MakeBetaMetaData() {
    return TransformsMetaData{
        .id = "beta",
        .category = TransformCategory::Statistical,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Beta (Rolling)",
        .options = {
            epoch_script::MetaDataOption{
                .id = "window",
                .name = "Window Size",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(60.0),
                .min = 10,
                .max = 500,
                .step_size = 1,
                .desc = "Rolling window size for beta calculation",
                .tuningGuidance = "Minimum 20-30 bars for stable beta estimates. Use 60-120 for standard equity "
                                  "beta (matches industry practice). Shorter windows (20-40) for tactical trading "
                                  "and rapid regime detection. Longer windows (200-252) for strategic allocation "
                                  "and long-term beta characteristics."}},
        .desc = "Calculates rolling beta coefficient measuring an asset's sensitivity to market movements. "
                "Beta = Cov(asset, market) / Var(market). Beta > 1 indicates higher volatility than market, "
                "< 1 lower, = 1 matches market.",
        .inputs = {{.type = IODataType::Decimal, .id = "asset_returns", .name = "Asset Returns"},
                   {.type = IODataType::Decimal, .id = "market_returns", .name = "Market Returns"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Beta"}},
        .tags = {"statistics", "beta", "rolling", "risk", "capm", "sensitivity", "market-exposure"},
        .requiresTimeFrame = false,
        .strategyTypes = {"risk-management", "portfolio-optimization", "hedging", "beta-analysis", "factor-analysis"},
        .relatedTransforms = {"rolling_cov", "rolling_corr"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Essential for risk management, portfolio construction, and hedging. Track time-varying "
                        "market sensitivity for dynamic allocation. Identify defensive (low beta) vs aggressive "
                        "(high beta) assets. Use for CAPM-based expected returns, hedge ratio calculation, and "
                        "sector rotation based on beta regimes.",
        .limitations = "Assumes linear relationship between asset and market. Beta is backward-looking and may "
                       "not predict future sensitivity. Requires sufficiently long window for stable estimates. "
                       "Market definition affects results (SPY vs sector index)."
    };
}

// =============================================================================
// EWM CORRELATION
// =============================================================================

inline TransformsMetaData MakeEWMCorrMetaData() {
    return TransformsMetaData{
        .id = "ewm_corr",
        .category = TransformCategory::Statistical,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Exponentially Weighted Moving Correlation",
        .options = {
            epoch_script::MetaDataOption{
                .id = "span",
                .name = "Span",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Span for exponential weighting (decay = 2/(1+span)). Higher values give more weight to history.",
                .tuningGuidance = "Use span=10-20 for fast adaptation to correlation shifts (tactical trading). "
                                  "Use span=30-60 for balanced responsiveness and stability. Use span=100+ for "
                                  "strategic allocation with smooth estimates. Roughly equivalent to rolling "
                                  "window of span/2 bars but smoother."}},
        .desc = "Calculates correlation between two series using exponential weighting, giving more weight to "
                "recent observations. Adapts faster to regime changes than simple rolling correlation while "
                "maintaining stability.",
        .inputs = {{.type = IODataType::Decimal, .id = "x", .name = "Series X"},
                   {.type = IODataType::Decimal, .id = "y", .name = "Series Y"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Correlation"}},
        .tags = {"statistics", "correlation", "exponential-weighted", "adaptive", "cross-asset", "dynamic"},
        .requiresTimeFrame = false,
        .strategyTypes = {"adaptive-hedging", "dynamic-pairs-trading", "regime-sensitive-correlation", "real-time-risk"},
        .relatedTransforms = {"rolling_corr", "ewm_cov", "linear_fit"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Use when you need correlation estimates that adapt to changing market regimes faster "
                        "than rolling windows. Ideal for dynamic hedging where recent correlation matters more "
                        "than historical. Exponential weighting provides smooth adaptation without abrupt window "
                        "edge effects.",
        .limitations = "Span parameter requires tuning - too low creates noise, too high lags regime changes. "
                       "No clear lookback period like rolling windows, making interpretation less intuitive. "
                       "Early values (first 10-20 bars) are unstable. Still only captures linear relationships."
    };
}

// =============================================================================
// EWM COVARIANCE
// =============================================================================

inline TransformsMetaData MakeEWMCovMetaData() {
    return TransformsMetaData{
        .id = "ewm_cov",
        .category = TransformCategory::Statistical,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Exponentially Weighted Moving Covariance",
        .options = {
            epoch_script::MetaDataOption{
                .id = "span",
                .name = "Span",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Span for exponential weighting (decay = 2/(1+span)). Higher values give more weight to history.",
                .tuningGuidance = "Use span=10-20 for fast-adapting risk models. Use span=30-60 for balanced "
                                  "adaptation. Use span=100+ for strategic risk analysis with smooth estimates. "
                                  "Roughly corresponds to rolling window of span/2 bars but with smoother transitions."}},
        .desc = "Calculates covariance between two series using exponential weighting, emphasizing recent "
                "observations while smoothly incorporating history. Adapts faster than rolling covariance to "
                "changing relationships.",
        .inputs = {{.type = IODataType::Decimal, .id = "x", .name = "Series X"},
                   {.type = IODataType::Decimal, .id = "y", .name = "Series Y"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Covariance"}},
        .tags = {"statistics", "covariance", "exponential-weighted", "adaptive", "cross-asset", "risk-management"},
        .requiresTimeFrame = false,
        .strategyTypes = {"adaptive-risk-management", "dynamic-hedging", "real-time-portfolio-risk", "risk-parity"},
        .relatedTransforms = {"rolling_cov", "ewm_corr", "rolling_corr"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Use for adaptive risk models where recent comovement is more relevant than distant "
                        "history. Ideal for real-time portfolio risk monitoring, dynamic hedging ratio "
                        "calculation, or fast-adapting risk parity strategies. Exponential weighting provides "
                        "continuous adaptation without the edge effects of rolling windows.",
        .limitations = "Not normalized - depends on input scales. Use ewm_corr for standardized measure. "
                       "Span parameter requires tuning. Early values unstable. No intuitive lookback period "
                       "like rolling windows."
    };
}

// =============================================================================
// LINEAR FIT (ROLLING OLS)
// =============================================================================

inline TransformsMetaData MakeLinearFitMetaData() {
    return TransformsMetaData{
        .id = "linear_fit",
        .category = TransformCategory::Statistical,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Linear Fit (Rolling OLS)",
        .options = {
            epoch_script::MetaDataOption{
                .id = "window",
                .name = "Window Size",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(60.0),
                .min = 10,
                .max = 500,
                .step_size = 1,
                .desc = "Rolling window size for OLS regression",
                .tuningGuidance = "Use 20-60 for tactical pairs trading. Use 60-120 for more stable hedge ratios. "
                                  "Match to expected holding period - shorter windows for intraday, longer for "
                                  "swing trading."}},
        .desc = "Rolling ordinary least squares (OLS) regression. Fits y = slope*x + intercept over a rolling "
                "window. Returns slope, intercept, and residual (y - predicted) at each bar.",
        .inputs = {{.type = IODataType::Decimal, .id = "x", .name = "Independent Variable (X)"},
                   {.type = IODataType::Decimal, .id = "y", .name = "Dependent Variable (Y)"}},
        .outputs = {{.type = IODataType::Decimal, .id = "slope", .name = "Slope (Hedge Ratio)"},
                    {.type = IODataType::Decimal, .id = "intercept", .name = "Intercept"},
                    {.type = IODataType::Decimal, .id = "residual", .name = "Residual (Spread)"}},
        .tags = {"statistics", "regression", "ols", "linear", "pairs-trading", "hedge-ratio"},
        .requiresTimeFrame = false,
        .strategyTypes = {"pairs-trading", "statistical-arbitrage", "regression-analysis", "hedging"},
        .relatedTransforms = {"rolling_corr", "rolling_cov", "beta", "engle_granger"},
        .assetRequirements = {"multi-asset-required"},
        .usageContext = "Essential for pairs trading hedge ratio estimation and spread construction. Use to "
                        "find dynamic relationship between two assets. The slope represents the hedge ratio, "
                        "residual is the spread to trade. Also useful for detrending and factor exposure estimation.",
        .limitations = "Linear relationship only - doesn't capture non-linear dependencies. Window size critical: "
                       "too small creates noisy estimates, too large misses regime changes. Sensitive to outliers. "
                       "Assumes homoscedastic errors."
    };
}

// =============================================================================
// Z-SCORE
// =============================================================================

inline TransformsMetaData MakeZScoreMetaData() {
    return TransformsMetaData{
        .id = "zscore",
        .category = TransformCategory::Statistical,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Z-Score",
        .options = {
            epoch_script::MetaDataOption{
                .id = "window",
                .name = "Window",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 1,
                .max = 10000,
                .step_size = 1,
                .desc = "Rolling window size for calculating mean and standard deviation",
                .tuningGuidance = "Shorter windows (10-15) for responsive mean-reversion entries, captures "
                                  "recent regime quickly but noisier. Standard 20 balances stability with "
                                  "adaptation. Longer windows (40-60) for stable normalization in ML features "
                                  "or cross-asset comparisons. Match window to your mean-reversion timeframe."}},
        .desc = "Rolling z-score of the input series: (x_t - mean)/stddev over the configured window.",
        .inputs = {{.type = IODataType::Number, .id = "SLOT", .name = "Input"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Z-Score"}},
        .tags = {"indicator", "statistics", "normalization", "standardization"},
        .requiresTimeFrame = false,
        .strategyTypes = {"mean-reversion", "statistical-arbitrage", "outlier-detection", "pairs-trading", "ml-feature-engineering"},
        .relatedTransforms = {"bband_percent", "return_vol", "hurst_exponent"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for mean-reversion strategies and outlier detection. Normalizes any indicator to "
                        "standard deviations from rolling mean. Z > +2 indicates overbought (2 std devs above mean), "
                        "Z < -2 oversold. Ideal for comparing signal strength across different assets or making "
                        "indicators stationary for ML models. Common thresholds: +/-1.5 (aggressive), +/-2.0 "
                        "(standard), +/-2.5 (conservative).",
        .limitations = "Assumes distribution is roughly normal - breaks down with skewed or heavy-tailed data. "
                       "Extreme values can persist longer than expected in trending regimes. Window size critical - "
                       "too short creates noise, too long misses regime changes. Not suitable for non-stationary "
                       "series without detrending first."
    };
}

// =============================================================================
// COMBINED METADATA FUNCTION
// =============================================================================

inline std::vector<TransformsMetaData> MakeCorrelationMetaData() {
    return {
        MakeRollingCorrMetaData(),
        MakeRollingCovMetaData(),
        MakeBetaMetaData(),
        MakeEWMCorrMetaData(),
        MakeEWMCovMetaData(),
        MakeLinearFitMetaData(),
        MakeZScoreMetaData()
    };
}

}  // namespace epoch_script::transforms
