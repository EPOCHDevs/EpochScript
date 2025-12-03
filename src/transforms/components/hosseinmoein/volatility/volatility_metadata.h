#pragma once
// Volatility Estimator Metadata
// Provides metadata for range-based and OHLC volatility estimators

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transforms {

using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// GARMAN-KLASS VOLATILITY
// =============================================================================

inline TransformsMetaData MakeGarmanKlassMetaData() {
    return TransformsMetaData{
        .id = "garman_klass",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Garman-Klass Volatility",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(14.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback window for volatility calculation",
                .tuningGuidance = "Shorter periods (7-10) track volatility changes quickly but are noisier. "
                                  "Standard 14-21 for tactical trading. Longer periods (30-60) for strategic allocation."},
            epoch_script::MetaDataOption{
                .id = "trading_days",
                .name = "Trading Days",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(252.0),
                .min = 1,
                .max = 365,
                .step_size = 1,
                .desc = "Number of trading periods per year for annualizing volatility",
                .tuningGuidance = "Use 252 for US stocks, 365 for crypto (24/7 markets), ~260 for international equities."}},
        .desc = "Volatility estimator using open, high, low, and close prices. More efficient than "
                "close-to-close volatility (~8x) by incorporating intraday price range information.",
        .inputs = {},  // Uses OHLC from data source
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Volatility"}},
        .tags = {"indicator", "volatility", "risk", "technical", "range-based", "ohlc"},
        .requiresTimeFrame = true,
        .strategyTypes = {"risk-management", "options-trading", "volatility-targeting", "portfolio-optimization"},
        .relatedTransforms = {"parkinson", "yang_zhang", "return_vol", "ulcer_index"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for risk management and position sizing when you need accurate volatility estimates "
                        "that account for intraday price swings. Superior to close-to-close volatility for "
                        "capturing true market turbulence. Commonly used in options pricing, VaR calculations, "
                        "and dynamic portfolio allocation.",
        .limitations = "Requires OHLC data (not suitable for close-only time series). Assumes continuous trading - "
                       "less accurate for assets with gaps or limited trading hours. Not effective for detecting "
                       "volatility regime changes in real-time."
    };
}

// =============================================================================
// PARKINSON VOLATILITY
// =============================================================================

inline TransformsMetaData MakeParkinsonMetaData() {
    return TransformsMetaData{
        .id = "parkinson",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Parkinson Volatility",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(14.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback window for high-low range volatility estimation",
                .tuningGuidance = "Shorter periods (7-10) capture recent volatility changes quickly. "
                                  "Standard 14-21 for general risk metrics. Longer periods (30-60) for stable estimates."},
            epoch_script::MetaDataOption{
                .id = "trading_periods",
                .name = "Trading Periods Per Year",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(252.0),
                .min = 1,
                .max = 365,
                .step_size = 1,
                .desc = "Annual trading period count for annualizing volatility estimate",
                .tuningGuidance = "Use 252 for US daily equities, 365 for daily crypto (24/7)."}},
        .desc = "Range-based volatility estimator using high and low prices. More efficient than close-to-close "
                "volatility and captures intraday price movements without requiring full OHLC data.",
        .inputs = {},  // Uses High/Low from data source
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Volatility"}},
        .tags = {"indicator", "volatility", "risk", "range-based", "technical"},
        .requiresTimeFrame = true,
        .strategyTypes = {"risk-management", "options-selling", "breakout-detection", "position-sizing"},
        .relatedTransforms = {"garman_klass", "yang_zhang", "return_vol"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for risk management and position sizing with simple but effective volatility estimates. "
                        "More efficient than close-to-close methods (requires fewer data points for same accuracy). "
                        "Ideal for strategies that care about intrabar volatility, such as options selling, "
                        "stop-loss placement, and breakout detection.",
        .limitations = "Assumes no overnight gaps - underestimates volatility in markets with large opening gaps. "
                       "Only requires high/low but ignores open/close information (less complete than Garman-Klass "
                       "or Yang-Zhang). Not suitable for assets with sparse tick data or wide bid-ask spreads."
    };
}

// =============================================================================
// YANG-ZHANG VOLATILITY
// =============================================================================

inline TransformsMetaData MakeYangZhangMetaData() {
    return TransformsMetaData{
        .id = "yang_zhang",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Yang-Zhang Volatility",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(14.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback window for comprehensive OHLC volatility estimation with jump adjustment",
                .tuningGuidance = "Shorter periods (7-10) capture recent volatility regime shifts, useful for "
                                  "dynamic hedging. Standard 14-21 balances accuracy with responsiveness. "
                                  "Longer periods (30-60) for stable strategic risk metrics."},
            epoch_script::MetaDataOption{
                .id = "trading_periods",
                .name = "Trading Periods Per Year",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(252.0),
                .min = 1,
                .max = 365,
                .step_size = 1,
                .desc = "Annual period count for volatility annualization",
                .tuningGuidance = "Use 252 for US equities (standard), ~260 for international markets. "
                                  "Not recommended for 24/7 crypto markets where overnight component is meaningless."}},
        .desc = "Advanced volatility estimator that accounts for opening jumps and combines overnight and intraday "
                "volatility. Most comprehensive of range-based estimators, designed to be robust against price jumps.",
        .inputs = {},  // Uses OHLC from data source
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Volatility"}},
        .tags = {"indicator", "volatility", "risk", "technical", "complex", "ohlc"},
        .requiresTimeFrame = true,
        .strategyTypes = {"options-trading", "risk-management", "sophisticated-portfolio-optimization", "earnings-strategies"},
        .relatedTransforms = {"garman_klass", "parkinson", "return_vol"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for the most accurate volatility estimates in markets with significant overnight gaps "
                        "or opening jumps (e.g., earnings announcements, geopolitical events). Separates overnight "
                        "volatility from intraday volatility for better risk modeling. Preferred for options pricing, "
                        "VaR models, and sophisticated risk management where volatility decomposition matters.",
        .limitations = "Most complex volatility estimator - requires full OHLC data and more computation. Benefit "
                       "over simpler methods (Garman-Klass, Parkinson) is marginal in markets without frequent gaps. "
                       "Overkill for strategies that only need relative volatility ranking. Not suitable for "
                       "continuous 24/7 markets (crypto) where open/close distinction is arbitrary."
    };
}

// =============================================================================
// HODGES-TOMPKINS VOLATILITY
// =============================================================================

inline TransformsMetaData MakeHodgesTompkinsMetaData() {
    return TransformsMetaData{
        .id = "hodges_tompkins",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Hodges-Tompkins Volatility",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback window for bias-corrected volatility estimation",
                .tuningGuidance = "Standard 20-30 for most applications. Shorter periods benefit more from "
                                  "bias correction. For very short windows (5-10), this estimator significantly "
                                  "outperforms simple standard deviation."},
            epoch_script::MetaDataOption{
                .id = "trading_periods",
                .name = "Trading Periods Per Year",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(252.0),
                .min = 1,
                .max = 365,
                .step_size = 1,
                .desc = "Annual period count for volatility annualization",
                .tuningGuidance = "Use 252 for US equities, 365 for crypto."}},
        .desc = "Bias-corrected volatility estimator that provides more accurate estimates especially with "
                "small sample sizes. Corrects for the downward bias in standard deviation estimates.",
        .inputs = {},  // Uses Close from data source
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Volatility"}},
        .tags = {"indicator", "volatility", "risk", "bias-corrected", "technical"},
        .requiresTimeFrame = true,
        .strategyTypes = {"risk-management", "options-trading", "small-sample-analysis"},
        .relatedTransforms = {"garman_klass", "parkinson", "return_vol"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use when you need accurate volatility estimates from limited data or short rolling windows. "
                        "The bias correction is most valuable for windows under 30 periods.",
        .limitations = "Only uses close prices - less information than OHLC-based estimators. "
                       "Benefit diminishes as sample size increases (>60 periods)."
    };
}

// =============================================================================
// ULCER INDEX
// =============================================================================

inline TransformsMetaData MakeUlcerIndexMetaData() {
    return TransformsMetaData{
        .id = "ulcer_index",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Ulcer Index",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(14.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback window for measuring drawdown severity",
                .tuningGuidance = "Standard 14 for short-term pain measurement. Use 21-30 for typical "
                                  "drawdown analysis. Longer periods (60+) for strategic risk assessment."}},
        .desc = "Downside volatility measure that focuses on drawdowns from peaks rather than overall volatility. "
                "Also known as the 'pain index' - measures the psychological pain of holding through drawdowns.",
        .inputs = {},  // Uses Close from data source
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Ulcer Index"}},
        .tags = {"indicator", "volatility", "risk", "drawdown", "downside-risk"},
        .requiresTimeFrame = true,
        .strategyTypes = {"risk-management", "portfolio-optimization", "drawdown-analysis"},
        .relatedTransforms = {"garman_klass", "return_vol", "max_drawdown"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for risk-adjusted performance measurement (Martin ratio = return / ulcer_index). "
                        "Better than standard deviation for risk-averse investors who care more about losing "
                        "money than overall variability. Useful for comparing strategies with different "
                        "drawdown characteristics.",
        .limitations = "Only captures downside risk - ignores upside volatility which may matter for some strategies. "
                       "Less suitable for mean-reversion strategies where drawdowns are expected trading opportunities."
    };
}

// =============================================================================
// COMBINED METADATA FUNCTION
// =============================================================================

inline std::vector<TransformsMetaData> MakeVolatilityEstimatorMetaData() {
    return {
        MakeGarmanKlassMetaData(),
        MakeParkinsonMetaData(),
        MakeYangZhangMetaData(),
        MakeHodgesTompkinsMetaData(),
        MakeUlcerIndexMetaData()
    };
}

}  // namespace epoch_script::transforms
