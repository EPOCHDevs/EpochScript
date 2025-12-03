#pragma once
// Indicator Transforms Metadata
// Provides metadata for technical indicator transforms

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transforms {

using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// MOVING AVERAGE
// =============================================================================

inline TransformsMetaData MakeMAMetaData() {
    return TransformsMetaData{
        .id = "ma",
        .category = TransformCategory::Trend,
        .plotKind = epoch_core::TransformPlotKind::line,
        .name = "Moving Average",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback period for moving average calculation",
                .tuningGuidance = "Shorter periods (5-10) for aggressive signals with more noise. "
                                  "Standard 20 for balanced trend detection. Longer periods (50-200) for major trends."},
            epoch_script::MetaDataOption{
                .id = "type",
                .name = "Type",
                .type = epoch_core::MetaDataOptionType::Select,
                .defaultValue = epoch_script::MetaDataOptionDefinition("sma"),
                .selectOptions = {
                    {.name = "Simple Moving Average (SMA)", .value = "sma"},
                    {.name = "Exponential Moving Average (EMA)", .value = "ema"},
                    {.name = "Hull Moving Average (HMA)", .value = "hma"},
                    {.name = "Kaufman's Adaptive Moving Average (KAMA)", .value = "kama"},
                    {.name = "Double Exponential Moving Average (DEMA)", .value = "dema"},
                    {.name = "Triple Exponential Moving Average (TEMA)", .value = "tema"},
                    {.name = "Triangular Moving Average (TRIMA)", .value = "trima"},
                    {.name = "Weighted Moving Average (WMA)", .value = "wma"},
                    {.name = "Zero Lag Exponential Moving Average (ZLEMA)", .value = "zlema"}},
                .desc = "MA calculation method - each type balances responsiveness vs smoothness differently",
                .tuningGuidance = "SMA: Basic trend and support/resistance. EMA: Standard for trend-following. "
                                  "HMA: Minimize lag. KAMA: Adapts to volatility. DEMA/TEMA: Ultra-responsive."
            }},
        .desc = "Calculates average price over specified period with multiple calculation methods. "
                "Acts as a trend indicator and noise filter.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Input"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Moving Average"}},
        .tags = {"overlay", "moving-average", "trend", "smoothing", "filter", "trend-following"},
        .requiresTimeFrame = false,
        .strategyTypes = {"trend-following", "moving-average-crossover", "support-resistance"},
        .relatedTransforms = {"ema", "sma", "dema", "hma"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Core trend indicator for directional strategies. Use price crossing MA for trend "
                        "change signals, or multiple MAs for crossover systems.",
        .limitations = "All MAs lag price by design. Whipsaws in choppy/ranging markets. "
                       "Not suitable as sole entry signal."
    };
}

// =============================================================================
// BOLLINGER BAND %B
// =============================================================================

inline TransformsMetaData MakeBBandPercentMetaData() {
    return TransformsMetaData{
        .id = "bband_percent",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::bb_percent_b,
        .name = "Bollinger Bands %B",
        .desc = "Measures position within Bollinger Bands as a normalized value between 0 and 1. "
                "Values above 1 or below 0 indicate extreme conditions.",
        .inputs = {
            {.type = IODataType::Decimal, .id = "bbands_lower", .name = "Lower Band"},
            {.type = IODataType::Decimal, .id = "bbands_upper", .name = "Upper Band"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "%B"}},
        .tags = {"indicator", "bollinger", "bands", "oscillator", "normalized", "mean-reversion"},
        .requiresTimeFrame = false,
        .requiredDataSources = {"c"},
        .strategyTypes = {"mean-reversion", "bollinger-squeeze", "overbought-oversold"},
        .relatedTransforms = {"bbands", "bband_width"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for mean-reversion strategies to identify overbought (>1.0) and oversold (<0.0) "
                        "conditions. Best combined with bband_width to detect volatility squeezes before breakouts.",
        .limitations = "Less reliable in strong trending markets. Works best in range-bound conditions."
    };
}

// =============================================================================
// BOLLINGER BAND WIDTH
// =============================================================================

inline TransformsMetaData MakeBBandWidthMetaData() {
    return TransformsMetaData{
        .id = "bband_width",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Bollinger Bands Width",
        .desc = "Measures the width of Bollinger Bands to identify volatility expansion and contraction. "
                "Narrows during low volatility and widens during high volatility.",
        .inputs = {
            {.type = IODataType::Decimal, .id = "bbands_lower", .name = "Lower Band"},
            {.type = IODataType::Decimal, .id = "bbands_middle", .name = "Middle Band"},
            {.type = IODataType::Decimal, .id = "bbands_upper", .name = "Upper Band"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Width"}},
        .tags = {"indicator", "bollinger", "bands", "volatility", "squeeze", "breakout"},
        .requiresTimeFrame = false,
        .strategyTypes = {"breakout", "bollinger-squeeze", "volatility-expansion"},
        .relatedTransforms = {"bbands", "bband_percent", "atr"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Detect volatility squeezes (narrow width) that often precede large price moves. "
                        "Low width values signal consolidation periods and potential breakout opportunities.",
        .limitations = "Width alone doesn't indicate breakout direction. Requires additional confirmation."
    };
}

// =============================================================================
// RETURN VOLATILITY
// =============================================================================

inline TransformsMetaData MakeReturnVolMetaData() {
    return TransformsMetaData{
        .id = "return_vol",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Return Volatility",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback window for volatility calculation",
                .tuningGuidance = "Shorter periods (5-10) for responsive volatility. Standard 20 for balanced. "
                                  "Longer periods (40-60) for stable strategic metrics."}},
        .desc = "Measures the standard deviation of returns over a specified period. "
                "Quantifies historical volatility for risk assessment.",
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Volatility"}},
        .tags = {"indicator", "volatility", "risk", "standard-deviation", "returns", "risk-management"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"c"},
        .strategyTypes = {"risk-management", "position-sizing", "volatility-targeting", "regime-detection"},
        .relatedTransforms = {"atr", "garman_klass", "price_diff_vol", "bbands"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Measure realized volatility for position sizing, risk management, or volatility "
                        "regime detection. Higher volatility suggests reduced position size.",
        .limitations = "Backward-looking - doesn't predict future volatility. Period selection critical."
    };
}

// =============================================================================
// PRICE DIFFERENCE VOLATILITY
// =============================================================================

inline TransformsMetaData MakePriceDiffVolMetaData() {
    return TransformsMetaData{
        .id = "price_diff_vol",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Price Difference Volatility",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback window for volatility calculation",
                .tuningGuidance = "Shorter periods (5-10) for responsive measures. Standard 20 for balanced."}},
        .desc = "Calculates the standard deviation of absolute price changes over a specified period. "
                "Provides a direct measure of price movement volatility.",
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Volatility"}},
        .tags = {"indicator", "volatility", "price-movement", "standard-deviation", "risk"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"c"},
        .strategyTypes = {"risk-management", "position-sizing", "volatility-targeting"},
        .relatedTransforms = {"return_vol", "atr", "bbands"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Alternative volatility measure using absolute price differences instead of percentage "
                        "returns. More intuitive for price-based stops and position sizing in dollars/points.",
        .limitations = "Not normalized by price level - $1 move on $10 stock vs $100 stock treated same. "
                       "Use return_vol for cross-asset comparison."
    };
}

// =============================================================================
// SESSION GAP
// =============================================================================

inline TransformsMetaData MakeSessionGapMetaData() {
    return TransformsMetaData{
        .id = "session_gap",
        .category = TransformCategory::Indicator,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Session Gap",
        .options = {
            epoch_script::MetaDataOption{
                .id = "fill_percent",
                .name = "Fill Percentage",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(100.0),
                .min = 0,
                .max = 100,
                .step_size = 1,
                .desc = "Minimum fill percentage for gap_filled signal",
                .tuningGuidance = "100% (default) for complete fill. 50-80% for partial fill strategies."}},
        .desc = "Detects overnight/session gaps and tracks their fill status.",
        .outputs = {
            {.type = IODataType::Boolean, .id = "gap_filled", .name = "Gap Filled"},
            {.type = IODataType::Decimal, .id = "gap_retrace", .name = "Gap Retrace %"},
            {.type = IODataType::Decimal, .id = "gap_size", .name = "Gap Size"},
            {.type = IODataType::Decimal, .id = "psc", .name = "Prior Session Close"}},
        .tags = {"indicator", "gap", "session", "overnight", "price-action"},
        .requiresTimeFrame = true,
        .strategyTypes = {"gap-fill", "overnight-sentiment", "intraday"},
        .relatedTransforms = {"bar_gap", "previous_high_low"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Track overnight gaps for gap fill strategies. Identifies when price gaps from prior "
                        "session close and monitors retracement toward that level.",
        .limitations = "Only tracks session-to-session gaps. Works best on equity markets with clear sessions."
    };
}

// =============================================================================
// BAR GAP
// =============================================================================

inline TransformsMetaData MakeBarGapMetaData() {
    return TransformsMetaData{
        .id = "bar_gap",
        .category = TransformCategory::Indicator,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Bar Gap",
        .options = {
            epoch_script::MetaDataOption{
                .id = "fill_percent",
                .name = "Fill Percentage",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(100.0),
                .min = 0,
                .max = 100,
                .step_size = 1,
                .desc = "Minimum fill percentage for gap_filled signal",
                .tuningGuidance = "100% for complete fill. Lower values for partial fill detection."},
            epoch_script::MetaDataOption{
                .id = "min_gap_size",
                .name = "Minimum Gap Size",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(0.0),
                .desc = "Minimum gap size to detect (absolute price difference)",
                .tuningGuidance = "Set above typical bid-ask spread to filter noise."}},
        .desc = "Detects gaps between consecutive bars and tracks their fill status.",
        .outputs = {
            {.type = IODataType::Boolean, .id = "gap_filled", .name = "Gap Filled"},
            {.type = IODataType::Decimal, .id = "gap_retrace", .name = "Gap Retrace %"},
            {.type = IODataType::Decimal, .id = "gap_size", .name = "Gap Size"},
            {.type = IODataType::Decimal, .id = "psc", .name = "Prior Bar Close"}},
        .tags = {"indicator", "gap", "intraday", "price-action"},
        .requiresTimeFrame = true,
        .strategyTypes = {"gap-fill", "liquidity-gaps", "intraday"},
        .relatedTransforms = {"session_gap", "fair_value_gap"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Intraday gap detection between any consecutive bars. Useful for identifying "
                        "liquidity gaps and potential fill opportunities.",
        .limitations = "More sensitive than session_gap - may generate many signals. Use min_gap_size filter."
    };
}

// =============================================================================
// INTRADAY RETURNS
// =============================================================================

inline TransformsMetaData MakeIntradayReturnsMetaData() {
    return TransformsMetaData{
        .id = "intraday_returns",
        .category = TransformCategory::Indicator,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Intraday Returns",
        .desc = "Returns within the same trading day, measuring return from day's open to current bar.",
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Intraday Return"}},
        .tags = {"indicator", "returns", "intraday", "momentum"},
        .requiresTimeFrame = true,
        .strategyTypes = {"intraday-momentum", "day-trading"},
        .relatedTransforms = {"forward_returns", "return_vol"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Track cumulative return within the trading day for intraday momentum strategies.",
        .limitations = "Resets each day. Requires session information to determine day boundaries."
    };
}

// =============================================================================
// COMBINED METADATA FUNCTION
// =============================================================================

inline std::vector<TransformsMetaData> MakeIndicatorsMetaData() {
    return {
        MakeMAMetaData(),
        MakeBBandPercentMetaData(),
        MakeBBandWidthMetaData(),
        MakeReturnVolMetaData(),
        MakePriceDiffVolMetaData(),
        MakeSessionGapMetaData(),
        MakeBarGapMetaData(),
        MakeIntradayReturnsMetaData()
    };
}

}  // namespace epoch_script::transforms
