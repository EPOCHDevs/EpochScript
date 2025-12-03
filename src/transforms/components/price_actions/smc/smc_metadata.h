#pragma once
// SMC (Smart Money Concepts) Transform Metadata
// Provides metadata for institutional trading pattern detection transforms

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transforms {

using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// SWING HIGHS AND LOWS
// =============================================================================

inline TransformsMetaData MakeSwingHighsLowsMetaData() {
    return TransformsMetaData{
        .id = "swing_highs_lows",
        .category = TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::shl,
        .name = "Swing Highs and Lows",
        .options = {
            epoch_script::MetaDataOption{
                .id = "swing_length",
                .name = "Swing Length",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(5.0),
                .min = 1,
                .max = 100,
                .step_size = 1,
                .desc = "Number of bars on each side required to confirm swing point",
                .tuningGuidance = "3-5 for scalping/intraday (more sensitive). 5-10 for swing trading (balanced). "
                                  "10-20 for position trading (major swings only). Higher values reduce noise but increase lag."}},
        .desc = "Identifies swing high and low points in price data by finding local peaks and valleys within a "
                "specified lookback period.",
        .inputs = {},
        .outputs = {{.type = IODataType::Integer, .id = "high_low", .name = "High/Low Direction"},
                    {.type = IODataType::Decimal, .id = "level", .name = "Level"}},
        .tags = {"smc", "price-action", "swing", "pivot", "technical", "market-structure"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"h", "l"},
        .strategyTypes = {"smart-money-concepts", "market-structure", "support-resistance", "price-action"},
        .relatedTransforms = {"bos_choch", "liquidity", "order_blocks", "retracements", "previous_high_low"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Foundation for Smart Money Concepts (SMC) analysis. Identifies key pivot points that form "
                        "market structure. Feed into other SMC transforms (BOS/CHOCH, liquidity, order blocks). "
                        "Also useful for support/resistance levels and trend analysis.",
        .limitations = "Lagging indicator - swing confirmed only after N bars. Shorter swing_length = more noise, "
                       "longer = less responsive. In strong trends, may miss minor swings. Works best on clean price action."
    };
}

// =============================================================================
// BREAK OF STRUCTURE / CHANGE OF CHARACTER
// =============================================================================

inline TransformsMetaData MakeBosChochMetaData() {
    return TransformsMetaData{
        .id = "bos_choch",
        .category = TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::bos_choch,
        .name = "Break of Structure & Change of Character",
        .options = {
            epoch_script::MetaDataOption{
                .id = "close_break",
                .name = "Use Close Price",
                .type = epoch_core::MetaDataOptionType::Boolean,
                .defaultValue = epoch_script::MetaDataOptionDefinition(false),
                .desc = "Use close price for break detection instead of high/low wicks",
                .tuningGuidance = "False (default) = wick-based breaks, more signals but some false. "
                                  "True = close-based breaks, more conservative but misses some valid breaks."}},
        .desc = "Detects Break of Structure (BOS) and Change of Character (CHOCH) patterns, which signal "
                "potential trend changes and market structure shifts.",
        .inputs = {{.type = IODataType::Integer, .id = "high_low", .name = "High/Low Direction", .allowMultipleConnections = false},
                   {.type = IODataType::Decimal, .id = "level", .name = "Level", .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Integer, .id = "bos", .name = "Break of Structure"},
                    {.type = IODataType::Integer, .id = "choch", .name = "Change of Character"},
                    {.type = IODataType::Decimal, .id = "level", .name = "Level"},
                    {.type = IODataType::Integer, .id = "broken_index", .name = "Broken Index"}},
        .tags = {"smc", "price-action", "market-structure", "technical", "trend", "reversal"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"o", "h", "l", "c"},
        .strategyTypes = {"smart-money-concepts", "trend-following", "trend-reversal", "market-structure"},
        .relatedTransforms = {"swing_highs_lows", "liquidity", "order_blocks", "fair_value_gap"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Core SMC concept for trend analysis. BOS (Break of Structure) = price breaks previous swing "
                        "confirming trend continuation. CHOCH (Change of Character) = break of counter-trend structure "
                        "suggesting reversal. Use with swing_highs_lows input.",
        .limitations = "Requires swing_highs_lows as input - can't use standalone. Lagging since waits for structure "
                       "break confirmation. False signals in choppy markets. Best on trending instruments."
    };
}

// =============================================================================
// FAIR VALUE GAP
// =============================================================================

inline TransformsMetaData MakeFairValueGapMetaData() {
    return TransformsMetaData{
        .id = "fair_value_gap",
        .category = TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::fvg,
        .name = "Fair Value Gap",
        .options = {
            epoch_script::MetaDataOption{
                .id = "join_consecutive",
                .name = "Join Consecutive Gaps",
                .type = epoch_core::MetaDataOptionType::Boolean,
                .defaultValue = epoch_script::MetaDataOptionDefinition(true),
                .desc = "Merge adjacent/overlapping FVGs into larger zones",
                .tuningGuidance = "True (default) = cleaner chart with merged zones, better for slower timeframes. "
                                  "False = all individual FVGs shown, useful for scalping/precision entries."}},
        .desc = "Identifies Fair Value Gaps (FVG) where price makes a significant move leaving an empty zone that "
                "often gets filled later, signaling potential reversal zones.",
        .inputs = {},
        .outputs = {{.type = IODataType::Integer, .id = "fvg", .name = "FVG Direction"},
                    {.type = IODataType::Decimal, .id = "top", .name = "Top"},
                    {.type = IODataType::Decimal, .id = "bottom", .name = "Bottom"},
                    {.type = IODataType::Integer, .id = "mitigated_index", .name = "Mitigated Index"}},
        .tags = {"smc", "price-action", "gap", "imbalance", "technical", "institutional-zones"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"o", "h", "l", "c"},
        .strategyTypes = {"smart-money-concepts", "mean-reversion", "support-resistance", "institutional-zones"},
        .relatedTransforms = {"order_blocks", "bos_choch", "swing_highs_lows", "liquidity"},
        .assetRequirements = {"single-asset"},
        .usageContext = "SMC concept: price imbalance zones where market moved too fast, leaving 'gaps' between candle bodies. "
                        "Price often returns to fill these zones (mitigation). Bullish FVG = support zone, Bearish FVG = resistance.",
        .limitations = "Not all FVGs get filled - some remain unfilled in strong trends. Mitigation can be partial. "
                       "Works better on liquid instruments. Intraday FVGs less reliable than higher timeframe FVGs."
    };
}

// =============================================================================
// LIQUIDITY
// =============================================================================

inline TransformsMetaData MakeLiquidityMetaData() {
    return TransformsMetaData{
        .id = "liquidity",
        .category = TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::liquidity,
        .name = "Liquidity",
        .options = {
            epoch_script::MetaDataOption{
                .id = "range_percent",
                .name = "Range Percentage",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(0.001),
                .min = 0.0001,
                .max = 0.1,
                .desc = "Maximum distance between swing points to be considered a cluster (as % of price)",
                .tuningGuidance = "0.001 (0.1%) for stocks/forex. Adjust based on instrument volatility - lower for tight "
                                  "clusters, higher for broader zones. Too low = miss clusters, too high = false clusters."}},
        .desc = "Identifies clusters of swing highs or lows that are close to each other, representing areas where "
                "significant buyer/seller liquidity is present.",
        .inputs = {{.type = IODataType::Integer, .id = "high_low", .name = "High/Low Direction", .allowMultipleConnections = false},
                   {.type = IODataType::Decimal, .id = "level", .name = "Level", .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Decimal, .id = "liquidity", .name = "Liquidity Direction"},
                    {.type = IODataType::Decimal, .id = "level", .name = "Level"},
                    {.type = IODataType::Decimal, .id = "end", .name = "End Index"},
                    {.type = IODataType::Decimal, .id = "swept", .name = "Swept Index"}},
        .tags = {"smc", "price-action", "liquidity", "technical", "cluster", "stop-hunt"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"h", "l"},
        .strategyTypes = {"smart-money-concepts", "stop-hunt-trading", "liquidity-sweeps", "reversal-trading"},
        .relatedTransforms = {"swing_highs_lows", "order_blocks", "bos_choch", "fair_value_gap"},
        .assetRequirements = {"single-asset"},
        .usageContext = "SMC liquidity pools concept: clusters of swing points where stop losses accumulate. 'Smart money' "
                        "sweeps these levels to trigger stops before reversing. Use to anticipate stop hunts and reversal zones.",
        .limitations = "Requires swing_highs_lows input. Not all liquidity pools get swept. Sweep timing unpredictable. "
                       "Works best on liquid markets where stop hunting is common (forex, futures, major stocks)."
    };
}

// =============================================================================
// ORDER BLOCKS
// =============================================================================

inline TransformsMetaData MakeOrderBlocksMetaData() {
    return TransformsMetaData{
        .id = "order_blocks",
        .category = TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::order_blocks,
        .name = "Order Blocks",
        .options = {
            epoch_script::MetaDataOption{
                .id = "close_mitigation",
                .name = "Use Close for Mitigation",
                .type = epoch_core::MetaDataOptionType::Boolean,
                .defaultValue = epoch_script::MetaDataOptionDefinition(false),
                .desc = "Require candle close in OB zone for mitigation, not just wick",
                .tuningGuidance = "False (default) = wick touch mitigates OB, more sensitive. True = require close inside OB, "
                                  "more conservative. Use true on higher timeframes to avoid premature mitigation."}},
        .desc = "Detects bullish and bearish order blocks which represent areas of significant institutional order flow, "
                "often acting as support and resistance zones.",
        .inputs = {{.type = IODataType::Integer, .id = "high_low", .name = "High/Low Direction", .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Integer, .id = "ob", .name = "Order Block Direction"},
                    {.type = IODataType::Decimal, .id = "top", .name = "Top"},
                    {.type = IODataType::Decimal, .id = "bottom", .name = "Bottom"},
                    {.type = IODataType::Decimal, .id = "ob_volume", .name = "Order Block Volume"},
                    {.type = IODataType::Integer, .id = "mitigated_index", .name = "Mitigated Index"},
                    {.type = IODataType::Decimal, .id = "percentage", .name = "Strength Percentage"}},
        .tags = {"smc", "price-action", "order-block", "institutional", "technical", "support-resistance"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"o", "h", "l", "c", "v"},
        .strategyTypes = {"smart-money-concepts", "institutional-trading", "support-resistance", "retracement-entries"},
        .relatedTransforms = {"swing_highs_lows", "fair_value_gap", "bos_choch", "liquidity"},
        .assetRequirements = {"single-asset"},
        .usageContext = "SMC cornerstone: the last opposing candle before strong move = institutional order accumulation zone. "
                        "Bullish OB = last down candle before rally (buy zone). Bearish OB = last up candle before drop (sell zone). "
                        "Price often retraces to OBs before continuing trend.",
        .limitations = "Requires swing_highs_lows input. Not all OBs hold - some get breached in strong momentum. "
                       "Volume data improves accuracy but not always available. Best on trending markets - less reliable in ranges."
    };
}

// =============================================================================
// PREVIOUS HIGH LOW
// =============================================================================

inline TransformsMetaData MakePreviousHighLowMetaData() {
    return TransformsMetaData{
        .id = "previous_high_low",
        .category = TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::previous_high_low,
        .name = "Previous High Low",
        .options = {
            epoch_script::MetaDataOption{
                .id = "interval",
                .name = "Interval",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = epoch_script::MetaDataOptionDefinition(1.0),
                .min = 1,
                .max = 100,
                .step_size = 1,
                .desc = "Number of periods back to reference (1 = previous period)",
                .tuningGuidance = "1 (default) = immediately previous period. 2+ for older reference levels. "
                                  "Usually 1 is most relevant for trading."},
            epoch_script::MetaDataOption{
                .id = "type",
                .name = "Time Frame Type",
                .type = epoch_core::MetaDataOptionType::Select,
                .defaultValue = epoch_script::MetaDataOptionDefinition("day"),
                .selectOptions = {{.name = "Minute", .value = "minute"},
                                  {.name = "Hour", .value = "hour"},
                                  {.name = "Day", .value = "day"},
                                  {.name = "Week", .value = "week"},
                                  {.name = "Month", .value = "month"}},
                .desc = "Type of period to reference",
                .tuningGuidance = "Day = most common for intraday strategies. Week for swing trading breakouts. "
                                  "Hour for scalping. Month for position trading."}},
        .desc = "Identifies the previous high or low levels within a given interval and tracks when current price "
                "breaks these levels.",
        .inputs = {},
        .outputs = {{.type = IODataType::Decimal, .id = "previous_high", .name = "Previous High"},
                    {.type = IODataType::Decimal, .id = "previous_low", .name = "Previous Low"},
                    {.type = IODataType::Boolean, .id = "broken_high", .name = "Broken High"},
                    {.type = IODataType::Boolean, .id = "broken_low", .name = "Broken Low"}},
        .tags = {"smc", "price-action", "high-low", "technical", "breakout", "intraday"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"o", "h", "l", "c", "v"},
        .strategyTypes = {"breakout", "range-trading", "intraday", "support-resistance"},
        .relatedTransforms = {"sessions", "swing_highs_lows", "donchian_channel"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Track key reference levels from previous periods for breakout trading. Previous day high/low "
                        "crucial for intraday strategies. Break above prev high = bullish, below prev low = bearish.",
        .limitations = "Only tracks one previous period - can't access multiple historical levels. Breaks can be false "
                       "(whipsaw). Works best on liquid instruments with clear daily ranges."
    };
}

// =============================================================================
// RETRACEMENTS
// =============================================================================

inline TransformsMetaData MakeRetracementsMetaData() {
    return TransformsMetaData{
        .id = "retracements",
        .category = TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::retracements,
        .name = "Retracements",
        .desc = "Calculates percentage retracements from swing highs and lows, measuring how much price has pulled back "
                "from a prior swing point.",
        .inputs = {{.type = IODataType::Integer, .id = "high_low", .name = "High/Low Direction", .allowMultipleConnections = false},
                   {.type = IODataType::Decimal, .id = "level", .name = "Level", .allowMultipleConnections = false}},
        .outputs = {{.type = IODataType::Integer, .id = "direction", .name = "Direction"},
                    {.type = IODataType::Decimal, .id = "current_retracement", .name = "Current Retracement %"},
                    {.type = IODataType::Decimal, .id = "deepest_retracement", .name = "Deepest Retracement %"}},
        .tags = {"smc", "price-action", "retracement", "fibonacci", "technical", "pullback"},
        .requiresTimeFrame = true,
        .requiredDataSources = {"h", "l"},
        .strategyTypes = {"retracement-trading", "pullback-entries", "trend-following", "smart-money-concepts"},
        .relatedTransforms = {"swing_highs_lows", "bos_choch", "order_blocks"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Measure pullback depth from swing points for retracement entries. Deeper retracements (50-70%) offer "
                        "better risk/reward but fewer triggers. Shallow retracements (20-30%) indicate strong momentum.",
        .limitations = "Requires swing_highs_lows input. Retracement % doesn't guarantee reversal - can retrace 100% (full reversal). "
                       "Doesn't predict how far retracement will go. Works best in trending markets."
    };
}

// =============================================================================
// SESSIONS
// =============================================================================

inline TransformsMetaData MakeSessionsMetaData() {
    return TransformsMetaData{
        .id = "sessions",
        .category = TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::sessions,
        .name = "Trading Sessions",
        .options = {
            epoch_script::MetaDataOption{
                .id = "session_type",
                .name = "Session Type",
                .type = epoch_core::MetaDataOptionType::Select,
                .defaultValue = epoch_script::MetaDataOptionDefinition("London"),
                .selectOptions = {{.name = "Sydney (08:00-17:00 AEDT/AEST)", .value = "Sydney"},
                                  {.name = "Tokyo (09:00-18:00 JST)", .value = "Tokyo"},
                                  {.name = "London (08:00-17:00 GMT/BST)", .value = "London"},
                                  {.name = "New York (09:30-16:00 ET)", .value = "NewYork"},
                                  {.name = "Asian Kill Zone (19:00-23:00 ET)", .value = "AsianKillZone"},
                                  {.name = "London Open Kill Zone (02:00-05:00 ET)", .value = "LondonOpenKillZone"},
                                  {.name = "New York Kill Zone (07:00-10:00 ET)", .value = "NewYorkKillZone"},
                                  {.name = "London Close Kill Zone (10:00-12:00 ET)", .value = "LondonCloseKillZone"}},
                .desc = "Trading session or kill zone to track",
                .tuningGuidance = "Full sessions (London/NY) for broader range breakouts. Kill zones for precise institutional entry "
                                  "windows. London Open (2-5am ET) most volatile for EUR/GBP pairs."}},
        .desc = "Identifies active trading sessions (Sydney, Tokyo, London, New York) and key session-based 'kill zones' "
                "where significant price movements often occur.",
        .inputs = {},
        .outputs = {{.type = IODataType::Boolean, .id = "active", .name = "Session Active"},
                    {.type = IODataType::Decimal, .id = "high", .name = "Session High"},
                    {.type = IODataType::Decimal, .id = "low", .name = "Session Low"},
                    {.type = IODataType::Boolean, .id = "closed", .name = "Session Closed"},
                    {.type = IODataType::Boolean, .id = "opened", .name = "Session Opened"}},
        .tags = {"smc", "price-action", "session", "time-based", "kill-zone", "intraday", "forex"},
        .requiresTimeFrame = false,
        .requiredDataSources = {"o", "h", "l", "c"},
        .strategyTypes = {"intraday", "session-breakout", "time-of-day", "smart-money-concepts"},
        .relatedTransforms = {"previous_high_low", "fair_value_gap", "liquidity"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Time-based filters for intraday forex/futures strategies. Major sessions mark periods of high liquidity "
                        "and volatility. 'Kill zones' are specific windows where institutional orders concentrate.",
        .limitations = "ONLY useful on intraday timeframes (1min-4hr). Meaningless on daily+ bars. Most effective on forex pairs "
                       "and futures. Less relevant for stocks (use market hours instead). Requires accurate timezone handling."
    };
}

// =============================================================================
// COMBINED METADATA FUNCTION
// =============================================================================

inline std::vector<TransformsMetaData> MakeSMCMetaData() {
    return {
        MakeSwingHighsLowsMetaData(),
        MakeBosChochMetaData(),
        MakeFairValueGapMetaData(),
        MakeLiquidityMetaData(),
        MakeOrderBlocksMetaData(),
        MakePreviousHighLowMetaData(),
        MakeRetracementsMetaData(),
        MakeSessionsMetaData()
    };
}

}  // namespace epoch_script::transforms
