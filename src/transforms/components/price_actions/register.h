#pragma once
// Price Action transforms registration
// Provides technical analysis patterns based on price structure
//
// Categories:
// 1. SMC (Smart Money Concepts) - Institutional trading patterns
//    - bos_choch: Break of Structure / Change of Character
//    - fair_value_gap: FVG / imbalance detection
//    - liquidity: Liquidity pool detection
//    - order_blocks: Order block identification
//    - swing_highs_lows: Swing point detection
//    - previous_high_low: Prior session high/low
//    - retracements: Fibonacci retracement levels
//    - sessions: Trading session boundaries
//    - session_time_window: Custom time windows
// 2. Chart Formations - Classical chart patterns
//    - head_and_shoulders: H&S top pattern
//    - inverse_head_and_shoulders: Inverse H&S bottom pattern
//    - double_top_bottom: Double top/bottom patterns
//    - flag: Bull/bear flag patterns
//    - triangles: Ascending/descending/symmetrical triangles
//    - pennant: Pennant continuation patterns
//    - consolidation_box: Rectangle/range patterns
// 3. Infrastructure - Pattern detection utilities
//    - flexible_pivot_detector: Customizable pivot detection

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// SMC Transform implementations
#include "smc/smc_metadata.h"
#include "smc/bos_choch.h"
#include "smc/fvg.h"
#include "smc/liquidity.h"
#include "smc/ob.h"
#include "smc/swing_highs_lows.h"
#include "smc/previous_high_low.h"
#include "smc/retracements.h"
#include "smc/sessions.h"
#include "smc/session_time_window.h"

// Chart Formation implementations
#include "infrastructure/flexible_pivot_detector.h"
#include "chart_formations/head_and_shoulders.h"
#include "chart_formations/inverse_head_and_shoulders.h"
#include "chart_formations/double_top_bottom.h"
#include "chart_formations/flag.h"
#include "chart_formations/triangles.h"
#include "chart_formations/pennant.h"
#include "chart_formations/consolidation_box.h"

namespace epoch_script::transform::price_actions {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all price action transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // SMART MONEY CONCEPTS (SMC)
    // =========================================================================
    // Patterns used by institutional traders. Based on ICT/SMC methodology.

    // bos_choch: Break of Structure / Change of Character
    // Input: OHLC data
    // Outputs: bos_bull, bos_bear, choch_bull, choch_bear (boolean signals)
    // Use for: Trend continuation (BOS) vs reversal (CHOCH) signals,
    //          institutional order flow analysis
    epoch_script::transform::Register<BosChoch>("bos_choch");

    // fair_value_gap: FVG / Imbalance Detection
    // Input: OHLC data
    // Outputs: fvg_bull, fvg_bear, fvg_high, fvg_low, fvg_mitigated
    // Use for: Entry zones, price inefficiency detection,
    //          targets for mean reversion to fair value
    epoch_script::transform::Register<FairValueGap>("fair_value_gap");

    // liquidity: Liquidity Pool Detection
    // Input: OHLC data with swing detection
    // Outputs: buy_side_liquidity, sell_side_liquidity, liquidity_swept
    // Use for: Stop hunt detection, liquidity grab strategies,
    //          anticipating institutional sweeps
    epoch_script::transform::Register<Liquidity>("liquidity");

    // order_blocks: Order Block Identification
    // Input: OHLC data
    // Outputs: ob_bull, ob_bear, ob_high, ob_low, ob_mitigated
    // Use for: Institutional accumulation/distribution zones,
    //          high-probability entry areas
    epoch_script::transform::Register<OrderBlocks>("order_blocks");

    // swing_highs_lows: Swing Point Detection
    // Input: OHLC data
    // Options: swing_length (lookback period)
    // Outputs: swing_high, swing_low, swing_high_price, swing_low_price
    // Use for: Market structure analysis, trend identification,
    //          support/resistance levels
    epoch_script::transform::Register<SwingHighsLows>("swing_highs_lows");

    // previous_high_low: Prior Session High/Low
    // Input: OHLC data
    // Outputs: previous_high, previous_low
    // Use for: Key levels from prior session, opening range breakouts,
    //          PDH/PDL reference levels
    epoch_script::transform::Register<PreviousHighLow>("previous_high_low");

    // retracements: Fibonacci Retracement Levels
    // Input: Swing high/low points
    // Outputs: fib_236, fib_382, fib_500, fib_618, fib_786
    // Use for: Entry/exit targets, pullback levels,
    //          confluence zones for reversals
    epoch_script::transform::Register<Retracements>("retracements");

    // sessions: Trading Session Boundaries
    // Input: Timestamp
    // Outputs: is_asian, is_london, is_new_york, session_name
    // Use for: Session-based strategies, volatility expectations,
    //          time-of-day filtering
    epoch_script::transform::Register<DefaultSessions>("sessions");

    // session_time_window: Custom Time Windows
    // Input: Timestamp
    // Options: start_hour, end_hour, timezone
    // Outputs: in_window (boolean)
    // Use for: Custom session definitions, specific trading hours
    epoch_script::transform::Register<SessionTimeWindow>("session_time_window");

    // =========================================================================
    // CHART FORMATIONS - Classical Patterns
    // =========================================================================
    // Traditional chart patterns from technical analysis.
    // All patterns use flexible_pivot_detector for swing detection.

    // flexible_pivot_detector: Customizable Pivot Detection
    // Input: OHLC data
    // Options: left_bars, right_bars, mode
    // Outputs: pivot_high, pivot_low, pivot_price
    // Use for: Foundation for all chart pattern detection,
    //          customizable swing point identification
    epoch_script::transform::Register<FlexiblePivotDetector>("flexible_pivot_detector");

    // head_and_shoulders: Head and Shoulders Top Pattern
    // Input: OHLC data
    // Outputs: pattern_complete, neckline, target, entry_signal
    // Use for: Major reversal pattern (bullish to bearish),
    //          typically forms at market tops
    epoch_script::transform::Register<HeadAndShoulders>("head_and_shoulders");

    // inverse_head_and_shoulders: Inverse H&S Bottom Pattern
    // Input: OHLC data
    // Outputs: pattern_complete, neckline, target, entry_signal
    // Use for: Major reversal pattern (bearish to bullish),
    //          typically forms at market bottoms
    epoch_script::transform::Register<InverseHeadAndShoulders>("inverse_head_and_shoulders");

    // double_top_bottom: Double Top/Bottom Patterns
    // Input: OHLC data
    // Outputs: double_top, double_bottom, neckline, target
    // Use for: Reversal patterns at swing extremes,
    //          M-shape (top) or W-shape (bottom)
    epoch_script::transform::Register<DoubleTopBottom>("double_top_bottom");

    // flag: Bull/Bear Flag Patterns
    // Input: OHLC data
    // Outputs: bull_flag, bear_flag, flag_pole, target
    // Use for: Continuation patterns after strong moves,
    //          consolidation before trend continuation
    epoch_script::transform::Register<Flag>("flag");

    // triangles: Triangle Patterns
    // Input: OHLC data
    // Outputs: ascending, descending, symmetrical, breakout_direction
    // Use for: Consolidation patterns with converging trendlines,
    //          ascending = bullish bias, descending = bearish bias
    epoch_script::transform::Register<Triangles>("triangles");

    // pennant: Pennant Continuation Patterns
    // Input: OHLC data
    // Outputs: bull_pennant, bear_pennant, apex, target
    // Use for: Short-term continuation after flagpole move,
    //          similar to flags but with converging lines
    epoch_script::transform::Register<Pennant>("pennant");

    // consolidation_box: Rectangle/Range Patterns
    // Input: OHLC data
    // Options: min_touches, tolerance
    // Outputs: in_box, box_high, box_low, breakout
    // Use for: Range trading, breakout setups,
    //          identifying accumulation/distribution ranges
    epoch_script::transform::Register<ConsolidationBox>("consolidation_box");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================
    // SMC transforms metadata
    for (const auto& metadata : epoch_script::transforms::MakeSMCMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::price_actions
