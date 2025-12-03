#pragma once
// Volatility transforms registration
// Provides basic volatility estimation from price data
//
// Categories:
// 1. Price-Based Volatility - Standard deviation of price changes
//    - price_diff_vol: Rolling std dev of price differences
//    - return_vol: Rolling std dev of returns
//
// Note: For more advanced volatility estimators, see:
// - hosseinmoein/volatility: Hodges-Tompkins, Ulcer Index
// - hosseinmoein/hosseinmoein.h: Garman-Klass, Parkinson, Yang-Zhang

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "volatility.h"

namespace epoch_script::transform::volatility_module {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all volatility transforms and metadata
// =============================================================================

inline void Register() {
    // auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // PRICE-BASED VOLATILITY
    // =========================================================================
    // Simple volatility measures using rolling standard deviation.
    // These use close prices and are quick to compute.

    // price_diff_vol: Rolling volatility of price differences
    // Input: OHLC data (uses close price)
    // Options: period (window size for rolling std dev)
    // Outputs: Rolling standard deviation of price differences
    // Use for: Dollar volatility estimation, position sizing by dollar risk,
    //          comparing volatility across assets with different price levels
    // Formula: std(close[t] - close[t-1]) over period
    epoch_script::transform::Register<PriceDiffVolatility>("price_diff_vol");

    // return_vol: Rolling volatility of returns (realized volatility)
    // Input: OHLC data (uses close price)
    // Options: period (window size for rolling std dev)
    // Outputs: Rolling standard deviation of returns
    // Use for: Standard volatility measure, position sizing,
    //          Sharpe ratio calculation, options pricing
    // Formula: std(pct_change(close)) over period
    // Note: Returns are more comparable across assets than price differences
    epoch_script::transform::Register<ReturnVolatility>("return_vol");

    // =========================================================================
    // RELATED TRANSFORMS (in other modules)
    // =========================================================================
    // For more sophisticated volatility estimators, use:
    //
    // From hosseinmoein/volatility:
    // - hodges_tompkins: Bias-corrected volatility estimator
    //   Better for small sample sizes
    //
    // - ulcer_index: Downside volatility / pain index
    //   Focuses on drawdowns rather than overall volatility
    //
    // From hosseinmoein/hosseinmoein.h:
    // - garman_klass: Uses OHLC data for more efficient estimation
    //   ~8x more efficient than close-to-close
    //
    // - parkinson: Uses high-low range
    //   Good for assets with significant intraday moves
    //
    // - yang_zhang: Accounts for overnight jumps + intraday range
    //   Best estimator when overnight gaps are significant
    //
    // From timeseries:
    // - rolling_garch: Conditional volatility forecasting
    //   For volatility clustering and forecasting

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================
    // Note: Volatility transform metadata is registered inline in registration.cpp
    // or via YAML configuration.
}

}  // namespace epoch_script::transform::volatility_module
