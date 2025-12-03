#pragma once
// Tulip Indicators transforms registration
// Wraps the Tulip Indicators C library for technical analysis
// Dynamically registers 100+ indicators from external library metadata
//
// Categories:
// 1. Moving Averages - Trend smoothing
//    - sma, ema, dema, tema, trima, kama, mama, t3, wma, zlema, etc.
// 2. Oscillators - Momentum indicators
//    - rsi, stoch, stochrsi, macd, cci, mfi, willr, cmo, etc.
// 3. Volatility - Range and volatility measures
//    - atr, natr, bbands, keltner, etc.
// 4. Trend - Trend strength and direction
//    - adx, adxr, aroon, aroonosc, psar, supertrend, etc.
// 5. Volume - Volume-based indicators
//    - obv, ad, adosc, vwma, etc.
// 6. Price Transform - Price calculations
//    - typprice, medprice, wclprice, avgprice, etc.
// 7. Math Operations - Statistical functions
//    - min, max, sum, stddev, var, linreg, etc.
// 8. Candlestick Patterns - Pattern recognition
//    - All candlestick patterns (doji, hammer, engulfing, etc.)
//
// Note: This module uses dynamic registration from the Tulip Indicators
// library metadata. All indicators use the same TulipModelImpl template.

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <candles.h>
#include <indicators.h>
#include <span>
#include <unordered_set>

// Transform implementation
#include "tulip_model.h"
#include "tulip_metadata.h"

namespace epoch_script::transform::tulip {

using namespace epoch_script::transform;

// =============================================================================
// Registration function - registers all Tulip indicators and candle patterns
// =============================================================================

inline void Register() {
    // Skip indicators that have custom implementations elsewhere
    std::unordered_set<std::string> toSkip{"lag"};

    // =========================================================================
    // TECHNICAL INDICATORS
    // =========================================================================
    // Register all Tulip Indicators library transforms.
    // These include: sma, ema, rsi, macd, bbands, atr, adx, stoch, etc.
    // Full list: https://tulipindicators.org/list
    for (auto const& metaData :
         std::span(ti_indicators, ti_indicators + ti_indicator_count())) {
        if (!toSkip.contains(metaData.name)) {
            epoch_script::transform::Register<TulipModelImpl<true>>(metaData.name);
        }
    }

    // Custom Tulip-based indicators not native to the library
    // crossunder is implemented as crossover with swapped inputs
    epoch_script::transform::Register<TulipModelImpl<true>>("crossunder");

    // =========================================================================
    // CANDLESTICK PATTERNS
    // =========================================================================
    // Register all candlestick pattern recognition transforms.
    // These detect: doji, hammer, engulfing, morning_star, etc.
    for (auto const& metaData :
         std::span(tc_candles, tc_candles + tc_candle_count())) {
        epoch_script::transform::Register<TulipModelImpl<false>>(metaData.name);
    }

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================
    // Register metadata for complex indicators that need detailed descriptions
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();
    for (const auto& metadata : epoch_script::transforms::MakeTulipMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::tulip
