#pragma once
// Technical Indicators transforms registration
// Provides standard market indicators and data manipulation utilities
//
// Categories:
// 1. Price Indicators - Market-data derived metrics
//    - vwap: Volume-Weighted Average Price
//    - trade_count: Number of trades per bar
// 2. Technical Analysis - Bollinger Band variants
//    - bband_percent: Bollinger Band %B indicator
//    - bband_width: Bollinger Band width indicator
// 3. Returns Calculation - Forward/intraday returns
//    - forward_returns: Future returns (for ML targets)
//    - intraday_returns: Same-day returns
// 4. Gap Detection - Price gaps between bars
//    - session_gap: Overnight/session gaps
//    - bar_gap: Gaps between consecutive bars
// 5. Data Manipulation - Lag and fill operations
//    - lag: Shift series by N periods (typed variants)
//    - ffill: Forward fill null values (typed variants)
//    - ma: Generic moving average wrapper

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "vwap.h"
#include "trade_count.h"
#include "bband_variant.h"
#include "forward_returns.h"
#include "intraday_returns.h"
#include "indicators_metadata.h"
#include "session_gap.h"
#include "bar_gap.h"
#include "lag.h"
#include "ffill.h"
#include "moving_average.h"

namespace epoch_script::transform::indicators {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all indicator transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // PRICE INDICATORS - Market Data Derived
    // =========================================================================
    // Single-series transforms derived from OHLCV market data.
    // Require specific columns from the data source.

    // vwap: Volume-Weighted Average Price
    // Input: Requires 'vw' column from data source
    // Outputs: VWAP value per bar
    // Use for: Intraday execution benchmarks, mean-reversion signals,
    //          institutional order flow analysis
    epoch_script::transform::Register<VWAPTransform>("vwap");

    // trade_count: Number of trades per bar
    // Input: Requires 'n' column from data source
    // Outputs: Trade count per bar
    // Use for: Liquidity analysis, volume quality, unusual activity detection
    epoch_script::transform::Register<TradeCountTransform>("trade_count");

    // =========================================================================
    // TECHNICAL ANALYSIS - Bollinger Band Variants
    // =========================================================================
    // Bollinger Band derived indicators for volatility analysis.

    // bband_percent: Bollinger Band %B
    // Input: Price series
    // Options: period, stddev
    // Outputs: %B value (0-1 when price between bands)
    // Use for: Overbought/oversold signals, mean reversion timing
    //          %B > 1 = above upper band, %B < 0 = below lower band
    epoch_script::transform::Register<BollingerBandsPercent>("bband_percent");

    // bband_width: Bollinger Band Width
    // Input: Price series
    // Options: period, stddev
    // Outputs: Width as percentage of middle band
    // Use for: Volatility squeeze detection, breakout anticipation,
    //          regime identification (low width = consolidation)
    epoch_script::transform::Register<BollingerBandsWidth>("bband_width");

    // =========================================================================
    // RETURNS CALCULATION
    // =========================================================================
    // Calculate various return metrics for analysis.

    // forward_returns: Future returns (lookahead)
    // Input: Price series
    // Options: period (how far ahead), return_type (simple/log)
    // Outputs: Returns N periods into the future
    // Use for: ML target variables, strategy evaluation
    // IMPORTANT: Forward-looking - last N bars will be null. Cannot use in live trading.
    epoch_script::transform::Register<ForwardReturns>("forward_returns");

    // intraday_returns: Returns within the same trading day
    // Input: Price series
    // Outputs: Return from day's open to current bar
    // Use for: Intraday momentum, day trading signals
    epoch_script::transform::Register<IntradayReturns>("intraday_returns");

    // =========================================================================
    // GAP DETECTION
    // =========================================================================
    // Detect price gaps for gap trading strategies.

    // session_gap: Overnight/session gaps
    // Input: OHLC data
    // Options: fill_percent (minimum fill % for gap_filled)
    // Outputs: gap_filled, gap_retrace, gap_size, psc (prior session close)
    // Use for: Gap fill strategies, overnight sentiment,
    //          identifies gaps between trading sessions
    epoch_script::transform::Register<SessionGap>("session_gap");

    // bar_gap: Gaps between consecutive bars
    // Input: OHLC data
    // Options: fill_percent, min_gap_size
    // Outputs: gap_filled, gap_retrace, gap_size, psc
    // Use for: Intraday gap detection, liquidity gaps,
    //          identifies gaps between ANY consecutive bars
    epoch_script::transform::Register<BarGap>("bar_gap");

    // =========================================================================
    // DATA MANIPULATION - Lag Operations
    // =========================================================================
    // Shift series by N periods. Typed variants for type safety.

    // lag_number: Lag numeric series by N periods
    // Input: Numeric series
    // Options: period (default 1)
    // Outputs: Series shifted forward (earlier values become null)
    // Use for: Previous bar comparisons, momentum calculations
    epoch_script::transform::Register<LagNumber>("lag_number");

    // lag_string: Lag string series by N periods
    // Input: String series
    // Options: period
    // Outputs: Shifted string series
    // Use for: Previous categorical values
    epoch_script::transform::Register<LagString>("lag_string");

    // lag_boolean: Lag boolean series by N periods
    // Input: Boolean series
    // Options: period
    // Outputs: Shifted boolean series
    // Use for: Previous signal values
    epoch_script::transform::Register<LagBoolean>("lag_boolean");

    // lag_timestamp: Lag timestamp series by N periods
    // Input: Timestamp series
    // Options: period
    // Outputs: Shifted timestamp series
    // Use for: Previous event times
    epoch_script::transform::Register<LagTimestamp>("lag_timestamp");

    // =========================================================================
    // DATA MANIPULATION - Forward Fill
    // =========================================================================
    // Fill null values by propagating last valid observation.
    // Essential for handling sparse data like fundamentals.

    // ffill_number: Forward fill numeric nulls
    // Input: Numeric series with nulls
    // Outputs: Series with nulls filled from last valid value
    // Use for: Aligning quarterly fundamentals to daily prices,
    //          handling missing data points
    epoch_script::transform::Register<FfillNumber>("ffill_number");

    // ffill_string: Forward fill string nulls
    // Input: String series with nulls
    // Outputs: Series with nulls filled
    // Use for: Forward-filling categorical data
    epoch_script::transform::Register<FfillString>("ffill_string");

    // ffill_boolean: Forward fill boolean nulls
    // Input: Boolean series with nulls
    // Outputs: Series with nulls filled
    // Use for: Forward-filling signal states
    epoch_script::transform::Register<FfillBoolean>("ffill_boolean");

    // ffill_timestamp: Forward fill timestamp nulls
    // Input: Timestamp series with nulls
    // Outputs: Series with nulls filled
    // Use for: Forward-filling event timestamps
    epoch_script::transform::Register<FfillTimestamp>("ffill_timestamp");

    // =========================================================================
    // DATA MANIPULATION - Moving Average
    // =========================================================================

    // ma: Generic moving average wrapper
    // Input: Numeric series
    // Options: period, ma_type (sma, ema, etc.)
    // Outputs: Moving average series
    // Use for: Trend smoothing, crossover signals
    // Note: Wraps Tulip indicators for flexibility
    epoch_script::transform::Register<MovingAverage>("ma");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // Forward returns metadata
    for (const auto& metadata : MakeForwardReturnsMetaData()) {
        metaRegistry.Register(metadata);
    }

    // Forward fill metadata
    for (const auto& metadata : MakeFfillMetaData()) {
        metaRegistry.Register(metadata);
    }

    // Indicator transforms metadata (ma, bband_percent, bband_width, return_vol, etc.)
    for (const auto& metadata : MakeIndicatorsMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::indicators
