#pragma once
// Tulip Indicator Transforms Metadata
// Provides metadata for Tulip-based technical indicators

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transforms {

using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// ACCELERATION BANDS
// =============================================================================

inline TransformsMetaData MakeAccelerationBandsMetaData() {
    return TransformsMetaData{
        .id = "acceleration_bands",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::bbands,
        .name = "Acceleration Bands",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback period for calculating the moving average baseline and volatility adjustment",
                .tuningGuidance = "Shorter periods (10-15) respond quickly. Standard 20 balanced. "
                                  "Longer periods (30-50) for smoother bands."},
            epoch_script::MetaDataOption{
                .id = "multiplier",
                .name = "Multiplier",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(2.0),
                .min = 0.1,
                .max = 5.0,
                .desc = "Controls band width - higher values create wider bands",
                .tuningGuidance = "Start with 2.0. Increase to 2.5-3.0 for volatile assets. "
                                  "Decrease to 1.5 for low-volatility assets."}},
        .desc = "Three bands that expand and contract based on price volatility. Middle band is a simple "
                "moving average, while upper and lower bands adjust dynamically with price acceleration.",
        .outputs = {
            {.type = IODataType::Decimal, .id = "upper_band", .name = "Upper Band"},
            {.type = IODataType::Decimal, .id = "middle_band", .name = "Middle Band"},
            {.type = IODataType::Decimal, .id = "lower_band", .name = "Lower Band"}},
        .tags = {"overlay", "volatility", "bands", "price-channels", "technical"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"h", "l", "c"},
        .strategyTypes = {"breakout", "trend-following", "volatility-expansion", "momentum"},
        .relatedTransforms = {"bbands", "keltner_channels", "return_vol"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for breakout and volatility expansion strategies. Price breaking above upper band "
                        "signals strong bullish momentum, while breaking below lower band indicates bearish acceleration.",
        .limitations = "Best suited for trending markets. In ranging markets, price may whipsaw between bands."
    };
}

// =============================================================================
// KELTNER CHANNELS
// =============================================================================

inline TransformsMetaData MakeKeltnerChannelsMetaData() {
    return TransformsMetaData{
        .id = "keltner_channels",
        .category = TransformCategory::Volatility,
        .plotKind = epoch_core::TransformPlotKind::bbands,
        .name = "Keltner Channels",
        .options = {
            epoch_script::MetaDataOption{
                .id = "roll_period",
                .name = "Rolling Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 2,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback period for both the EMA centerline and ATR calculation",
                .tuningGuidance = "Shorter periods (10-15) for sensitive bands. Standard 20 balances "
                                  "trend identification with noise reduction."},
            epoch_script::MetaDataOption{
                .id = "band_multiplier",
                .name = "Band Multiplier",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(2.0),
                .min = 0.5,
                .max = 5.0,
                .desc = "Multiplier applied to ATR to set band distance from centerline",
                .tuningGuidance = "Start with 2.0. Increase to 2.5-3.0 for volatile assets. "
                                  "Decrease to 1.5 for tighter mean-reversion signals."}},
        .desc = "Volatility-based envelope indicator that places bands around an exponential moving average. "
                "Uses average true range to set band width, making it responsive to volatility changes.",
        .outputs = {
            {.type = IODataType::Decimal, .id = "upper_band", .name = "Upper Band"},
            {.type = IODataType::Decimal, .id = "lower_band", .name = "Lower Band"}},
        .tags = {"overlay", "volatility", "bands", "price-channels", "technical"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"h", "l", "c"},
        .strategyTypes = {"trend-following", "breakout", "channel-trading", "momentum"},
        .relatedTransforms = {"bbands", "acceleration_bands", "bband_width"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for trend-following and breakout strategies where ATR-based bands provide clearer "
                        "signals than standard deviation. Price consistently above upper band signals strong uptrend.",
        .limitations = "Uses ATR instead of standard deviation, making it less responsive to sudden volatility spikes."
    };
}

// =============================================================================
// DONCHIAN CHANNEL
// =============================================================================

inline TransformsMetaData MakeDonchianChannelMetaData() {
    return TransformsMetaData{
        .id = "donchian_channel",
        .category = TransformCategory::Trend,
        .plotKind = epoch_core::TransformPlotKind::bbands,
        .name = "Donchian Channel",
        .options = {
            epoch_script::MetaDataOption{
                .id = "window",
                .name = "Window",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback period for highest high and lowest low calculation",
                .tuningGuidance = "Shorter windows (10-15) for active breakout trading. Classic 20 balanced. "
                                  "Longer windows (40-55) for position trading."}},
        .desc = "Price channel with upper and lower bands from rolling high/low and a middle line as their average. "
                "Useful for breakouts and trend following.",
        .outputs = {
            {.type = IODataType::Decimal, .id = "bbands_upper", .name = "Upper"},
            {.type = IODataType::Decimal, .id = "bbands_middle", .name = "Middle"},
            {.type = IODataType::Decimal, .id = "bbands_lower", .name = "Lower"}},
        .tags = {"overlay", "trend", "bands", "price-channels", "breakout"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"h", "l"},
        .strategyTypes = {"breakout", "trend-following", "channel-trading", "turtle-trading"},
        .relatedTransforms = {"keltner_channels", "bbands", "previous_high_low"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for breakout and trend-following strategies. Upper band = highest high over window, "
                        "lower band = lowest low. Classic turtle trading entry: buy on upper band break.",
        .limitations = "Lagging indicator - breakouts signal after move starts. High false breakout rate in ranges."
    };
}

// =============================================================================
// ICHIMOKU CLOUD
// =============================================================================

inline TransformsMetaData MakeIchimokuMetaData() {
    return TransformsMetaData{
        .id = "ichimoku",
        .category = TransformCategory::Trend,
        .plotKind = epoch_core::TransformPlotKind::ichimoku,
        .name = "Ichimoku Cloud",
        .options = {
            epoch_script::MetaDataOption{
                .id = "p_tenkan",
                .name = "Tenkan Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(9.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Conversion line period (short-term momentum)",
                .tuningGuidance = "Default 9 from Japanese 1.5-week cycle. Keep ~1/3 of Kijun period ratio."},
            epoch_script::MetaDataOption{
                .id = "p_kijun",
                .name = "Kijun Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(26.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Base line period (medium-term trend)",
                .tuningGuidance = "Default 26 from Japanese monthly cycle. Should be ~3x Tenkan period."},
            epoch_script::MetaDataOption{
                .id = "p_senkou_b",
                .name = "Senkou B Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(52.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Leading span B period (long-term trend)",
                .tuningGuidance = "Default 52. Should be ~2x Kijun period."}},
        .desc = "Multi-line trend system: Tenkan-sen, Kijun-sen, Senkou Span A/B (cloud), and Chikou span.",
        .outputs = {
            {.type = IODataType::Decimal, .id = "tenkan", .name = "Tenkan-sen"},
            {.type = IODataType::Decimal, .id = "kijun", .name = "Kijun-sen"},
            {.type = IODataType::Decimal, .id = "senkou_a", .name = "Senkou A"},
            {.type = IODataType::Decimal, .id = "senkou_b", .name = "Senkou B"},
            {.type = IODataType::Decimal, .id = "chikou", .name = "Chikou Span"}},
        .tags = {"overlay", "trend", "cloud", "multi-line", "price-channels"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"h", "l", "c"},
        .strategyTypes = {"trend-following", "support-resistance", "multi-timeframe-analysis", "position-trading"},
        .relatedTransforms = {"ma", "donchian_channel", "keltner_channels"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for comprehensive trend analysis. Price above cloud = bullish, below = bearish. "
                        "Tenkan-Kijun cross signals trend changes.",
        .limitations = "Complex system with steep learning curve. Default parameters optimized for weekly "
                       "Japanese stock market."
    };
}

// =============================================================================
// CHANDE KROLL STOP
// =============================================================================

inline TransformsMetaData MakeChandeKrollStopMetaData() {
    return TransformsMetaData{
        .id = "chande_kroll_stop",
        .category = TransformCategory::Trend,
        .plotKind = epoch_core::TransformPlotKind::chande_kroll_stop,
        .name = "Chande Kroll Stop",
        .options = {
            epoch_script::MetaDataOption{
                .id = "p_period",
                .name = "Price Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(10.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback period for identifying highest high / lowest low",
                .tuningGuidance = "Shorter periods (5-7) keep stops tighter. Standard 10 balanced."},
            epoch_script::MetaDataOption{
                .id = "q_period",
                .name = "ATR Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(20.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Period for calculating Average True Range",
                .tuningGuidance = "Shorter periods (10-15) more responsive. Standard 20 stable."},
            epoch_script::MetaDataOption{
                .id = "multiplier",
                .name = "Multiplier",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(3.0),
                .min = 0.5,
                .max = 10.0,
                .desc = "ATR multiplier controlling stop distance from price extremes",
                .tuningGuidance = "Lower (1.5-2.5) for tighter stops. Standard 3.0 balanced. "
                                  "Higher (3.5-5.0) for volatile assets."}},
        .desc = "Trend-following indicator that provides dynamic stop-loss levels. Combines volatility and "
                "moving averages to set appropriate stop points for both long and short positions.",
        .outputs = {
            {.type = IODataType::Decimal, .id = "long_stop", .name = "Long Stop"},
            {.type = IODataType::Decimal, .id = "short_stop", .name = "Short Stop"}},
        .tags = {"indicator", "trend", "stop-loss", "risk-management", "technical"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"c", "h", "l"},
        .strategyTypes = {"trend-following", "stop-loss-management", "position-protection", "breakout"},
        .relatedTransforms = {"keltner_channels", "previous_high_low", "swing_highs_lows"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for dynamic stop-loss placement in trend-following strategies. Long_stop provides "
                        "trailing stops for long positions (placed below price), short_stop for short positions.",
        .limitations = "Lagging indicator - stops trail price, so won't prevent all losses in sudden reversals."
    };
}

// =============================================================================
// VORTEX INDICATOR
// =============================================================================

inline TransformsMetaData MakeVortexMetaData() {
    return TransformsMetaData{
        .id = "vortex",
        .category = TransformCategory::Momentum,
        .plotKind = epoch_core::TransformPlotKind::vortex,
        .name = "Vortex Indicator",
        .options = {
            epoch_script::MetaDataOption{
                .id = "period",
                .name = "Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(14.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Lookback period for calculating positive and negative vortex movement",
                .tuningGuidance = "Shorter periods (7-10) detect trend changes quickly but more whipsaws. "
                                  "Standard 14 balanced. Longer periods (21-28) for confirmed trends."}},
        .desc = "Identifies the start of new trends and trend direction using price movement patterns. "
                "Comprised of two lines that cross during trend changes.",
        .outputs = {
            {.type = IODataType::Decimal, .id = "plus_indicator", .name = "VI+"},
            {.type = IODataType::Decimal, .id = "minus_indicator", .name = "VI-"}},
        .tags = {"indicator", "trend", "crossover", "direction", "technical"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"h", "l", "c"},
        .strategyTypes = {"trend-following", "trend-identification", "directional-confirmation", "crossover-trading"},
        .relatedTransforms = {"ma", "swing_highs_lows", "bos_choch"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for trend identification and directional confirmation. VI+ crossing above VI- signals "
                        "uptrend start; VI- crossing above VI+ signals downtrend start.",
        .limitations = "Crossovers can whipsaw in ranging/choppy markets. Both lines oscillate making absolute "
                       "level interpretation difficult."
    };
}

// =============================================================================
// QQE (Quantitative Qualitative Estimation)
// =============================================================================

inline TransformsMetaData MakeQQEMetaData() {
    return TransformsMetaData{
        .id = "qqe",
        .category = TransformCategory::Momentum,
        .plotKind = epoch_core::TransformPlotKind::qqe,
        .name = "Quantitative Qualitative Estimation",
        .options = {
            epoch_script::MetaDataOption{
                .id = "avg_period",
                .name = "Average Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(14.0),
                .min = 1,
                .max = 500,
                .step_size = 1,
                .desc = "Base RSI calculation period before additional smoothing",
                .tuningGuidance = "Standard RSI 14. Shorter periods (7-10) for faster response. "
                                  "Longer periods (21-28) for smoother signals."},
            epoch_script::MetaDataOption{
                .id = "smooth_period",
                .name = "Smoothing Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(5.0),
                .min = 1,
                .max = 100,
                .step_size = 1,
                .desc = "EMA period applied to RSI to create the base QQE line",
                .tuningGuidance = "Shorter smoothing (3-4) maintains more RSI responsiveness. Standard 5 balanced."},
            epoch_script::MetaDataOption{
                .id = "width_factor",
                .name = "Width Factor",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(4.236),
                .min = 1.0,
                .max = 10.0,
                .desc = "Multiplier for threshold band width around RSI MA",
                .tuningGuidance = "Standard 4.236 (Fibonacci-derived) balanced. Lower values (3.0-4.0) more signals."}},
        .desc = "Enhanced RSI-based indicator with smoothing and adaptive bands. Generates potential trading "
                "signals when price crosses the upper or lower threshold lines.",
        .outputs = {
            {.type = IODataType::Decimal, .id = "result", .name = "QQE"},
            {.type = IODataType::Decimal, .id = "rsi_ma", .name = "RSI Moving Average"},
            {.type = IODataType::Decimal, .id = "long_line", .name = "Long Threshold"},
            {.type = IODataType::Decimal, .id = "short_line", .name = "Short Threshold"}},
        .tags = {"indicator", "oscillator", "rsi-based", "adaptive", "signals"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"c"},
        .strategyTypes = {"momentum", "trend-following", "overbought-oversold", "signal-generation"},
        .relatedTransforms = {"ma", "bband_percent", "zscore"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use as a smoother, less noisy alternative to raw RSI for momentum and overbought/oversold "
                        "detection. QQE line crossing above long_line suggests bullish momentum.",
        .limitations = "Still an RSI derivative - shares RSI's lag and tendency to stay overbought/oversold in trends."
    };
}

// =============================================================================
// HURST EXPONENT
// =============================================================================

inline TransformsMetaData MakeHurstExponentMetaData() {
    return TransformsMetaData{
        .id = "hurst_exponent",
        .category = TransformCategory::Momentum,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Hurst Exponent",
        .options = {
            epoch_script::MetaDataOption{
                .id = "min_period",
                .name = "Minimum Period",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(1.0),
                .min = 1,
                .max = 100,
                .step_size = 1,
                .desc = "Minimum lag for R/S analysis calculation",
                .tuningGuidance = "Start with 1 (default). Increase to 2-5 to focus on longer-term persistence."}},
        .desc = "Measures the long-term memory or persistence of a time series. Values above 0.5 indicate "
                "trend-following behavior, while values below 0.5 suggest mean-reverting tendencies.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Input"}},
        .outputs = {{.type = IODataType::Decimal, .id = "result", .name = "Hurst Exponent"}},
        .tags = {"indicator", "fractal", "time-series", "trend", "mean-reversion"},
        .requiresTimeFrame = false,
        .strategyTypes = {"regime-detection", "strategy-selection", "adaptive-trading", "market-microstructure"},
        .relatedTransforms = {"rolling_hurst_exponent", "return_vol", "zscore"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for regime detection and strategy selection. H > 0.5 suggests persistent/trending "
                        "behavior. H < 0.5 indicates mean-reverting behavior. H ~ 0.5 is random walk.",
        .limitations = "Requires substantial data history (100+ bars minimum). Computation is expensive. "
                       "Best used on rolling basis (see rolling_hurst_exponent)."
    };
}

// =============================================================================
// ROLLING HURST EXPONENT
// =============================================================================

inline TransformsMetaData MakeRollingHurstExponentMetaData() {
    return TransformsMetaData{
        .id = "rolling_hurst_exponent",
        .category = TransformCategory::Momentum,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Rolling Hurst Exponent",
        .options = {
            epoch_script::MetaDataOption{
                .id = "window",
                .name = "Window Size",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(100.0),
                .min = 20,
                .max = 1000,
                .step_size = 1,
                .desc = "Rolling window size for Hurst calculation",
                .tuningGuidance = "Minimum 100 bars for stable estimates. Use 150-200 for reliable regime detection. "
                                  "Larger windows (300-500) for strategic allocation."}},
        .desc = "Calculates the Hurst exponent over a rolling window of data. Provides insights into changing "
                "market behavior between trending and mean-reverting regimes.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Input"}},
        .outputs = {{.type = IODataType::Decimal, .id = "result", .name = "Rolling Hurst Exponent"}},
        .tags = {"indicator", "fractal", "time-series", "rolling", "regime-change"},
        .requiresTimeFrame = false,
        .strategyTypes = {"adaptive-trading", "regime-switching", "dynamic-strategy-allocation", "meta-strategy"},
        .relatedTransforms = {"hurst_exponent", "return_vol", "rolling_hurst_exponent"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for adaptive strategy switching that responds to regime changes. Monitors whether the "
                        "market is currently trending (H>0.5) or mean-reverting (H<0.5).",
        .limitations = "Requires large window (100+) for stability but then lags regime changes. "
                       "Computationally intensive."
    };
}

// =============================================================================
// COMBINED METADATA FUNCTION
// =============================================================================

inline std::vector<TransformsMetaData> MakeTulipMetaData() {
    return {
        MakeAccelerationBandsMetaData(),
        MakeKeltnerChannelsMetaData(),
        MakeDonchianChannelMetaData(),
        MakeIchimokuMetaData(),
        MakeChandeKrollStopMetaData(),
        MakeVortexMetaData(),
        MakeQQEMetaData(),
        MakeHurstExponentMetaData(),
        MakeRollingHurstExponentMetaData()
    };
}

}  // namespace epoch_script::transforms
