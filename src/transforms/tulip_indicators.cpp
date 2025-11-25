#include "common.h"
#include "epoch_core/common_utils.h"
#include <epoch_script/transforms/core/metadata.h>
#include "indicators.h"
#include <epoch_core/ranges_to.h>
#include <vector>
#include <yaml-cpp/yaml.h>

namespace epoch_script::transforms {

struct IndicatorMetaData {
  std::vector<std::string> tags;
  std::string desc;
  epoch_core::TransformCategory category{epoch_core::TransformCategory::Math};
  epoch_core::TransformPlotKind plotKind{epoch_core::TransformPlotKind::Null};

  // Enhanced metadata for RAG/LLM
  std::vector<std::string> strategyTypes{};
  std::vector<std::string> relatedTransforms{};
  std::vector<std::string> assetRequirements{"single-asset"};
  std::string usageContext{};
  std::string limitations{};

  // Flag schema for flag PlotKind transforms
  std::optional<FlagSchema> flagSchema{};
};

std::unordered_map<std::string, IndicatorMetaData>
MakeTulipIndicatorMetaData() {
  std::unordered_map<std::string, IndicatorMetaData> indicatorMetaData;

  // Vector operations and math functions
  indicatorMetaData["abs"] =
      IndicatorMetaData{.tags = {"simple", "abs", "math", "vector"},
                        .desc = "Vector Absolute Value. Returns the absolute "
                                "value of each element in the input."};

  indicatorMetaData["acos"] = IndicatorMetaData{
      .tags = {"simple", "acos", "math", "trigonometric", "vector"},
      .desc = "Vector Arccosine. Calculates the arccosine (inverse cosine) for "
              "each element in the input."};

  indicatorMetaData["add"] = IndicatorMetaData{
      .tags = {"simple", "add", "math", "arithmetic", "vector"},
      .desc = "Vector Addition. Adds two vectors element by element."};

  indicatorMetaData["asin"] = IndicatorMetaData{
      .tags = {"simple", "asin", "math", "trigonometric", "vector"},
      .desc = "Vector Arcsine. Calculates the arcsine (inverse sine) for each "
              "element in the input."};

  indicatorMetaData["atan"] = IndicatorMetaData{
      .tags = {"simple", "atan", "math", "trigonometric", "vector"},
      .desc = "Vector Arctangent. Calculates the arctangent (inverse tangent) "
              "for each element in the input."};

  indicatorMetaData["ceil"] = IndicatorMetaData{
      .tags = {"simple", "ceil", "math", "rounding", "vector"},
      .desc = "Vector Ceiling. Rounds each element up to the nearest integer."};

  indicatorMetaData["cos"] = IndicatorMetaData{
      .tags = {"simple", "cos", "math", "trigonometric", "vector"},
      .desc = "Vector Cosine. Calculates the cosine for each element in the "
              "input."};

  indicatorMetaData["cosh"] = IndicatorMetaData{
      .tags = {"simple", "cosh", "math", "hyperbolic", "vector"},
      .desc = "Vector Hyperbolic Cosine. Calculates the hyperbolic cosine for "
              "each element in the input."};

  indicatorMetaData["crossany"] = IndicatorMetaData{
      .tags = {"math", "crossany", "crossover", "signal"},
      .desc = "Crossany. Returns 1 when the first input "
              "crosses the second input in any direction.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::flag,
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::Activity,
          .text = "Cross (Any Direction)",
          .textIsTemplate = false,
          .color = epoch_core::Color::Info,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  indicatorMetaData["crossover"] = IndicatorMetaData{
      .tags = {"math", "crossover", "signal", "trend"},
      .desc = "Crossover. Returns 1 when the first input "
              "crosses above the second input.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::flag,
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::TrendingUp,
          .text = "Bullish Cross",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  indicatorMetaData["crossunder"] = IndicatorMetaData{
      .tags = {"math", "crossunder", "signal", "trend"},
      .desc = "Crossunder. Returns 1 when the first input "
              "crosses below the second input.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::flag,
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::TrendingDown,
          .text = "Bearish Cross",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  indicatorMetaData["decay"] = IndicatorMetaData{
      .tags = {"math", "decay", "linear"},
      .desc = "Linear Decay. Applies linear decay to each element in the input "
              "over the specified period.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["div"] = IndicatorMetaData{
      .tags = {"simple", "div", "math", "arithmetic", "vector"},
      .desc = "Vector Division. Divides the first vector by the second element "
              "by element."};

  indicatorMetaData["edecay"] = IndicatorMetaData{
      .tags = {"math", "edecay", "exponential"},
      .desc = "Exponential Decay. Applies exponential decay to each element in "
              "the input over the specified period.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["exp"] = IndicatorMetaData{
      .tags = {"simple", "exp", "math", "exponential", "vector"},
      .desc = "Vector Exponential. Calculates e raised to the power of each "
              "element in the input.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::Null};

  indicatorMetaData["floor"] = IndicatorMetaData{
      .tags = {"simple", "floor", "math", "rounding", "vector"},
      .desc = "Vector Floor. Rounds each element down to the nearest integer."};

  indicatorMetaData["ln"] =
      IndicatorMetaData{.tags = {"simple", "ln", "math", "logarithm", "vector"},
                        .desc = "Vector Natural Log. Calculates the natural "
                                "logarithm for each element in the input."};

  indicatorMetaData["log10"] = IndicatorMetaData{
      .tags = {"simple", "log10", "math", "logarithm", "vector"},
      .desc = "Vector Base-10 Log. Calculates the base-10 logarithm for each "
              "element in the input."};

  indicatorMetaData["max"] = IndicatorMetaData{
      .tags = {"math", "max", "maximum", "highest"},
      .desc = "Maximum In Period. Finds the maximum value in the specified "
              "period for each element position.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["md"] = IndicatorMetaData{
      .tags = {"math", "md", "mean-deviation", "statistics"},
      .desc = "Mean Deviation Over Period. Calculates the "
              "mean deviation over the specified period.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["min"] = IndicatorMetaData{
      .tags = {"math", "min", "minimum", "lowest"},
      .desc = "Minimum In Period. Finds the minimum value in the specified "
              "period for each element position.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["mul"] = IndicatorMetaData{
      .tags = {"simple", "mul", "math", "arithmetic", "vector"},
      .desc =
          "Vector Multiplication. Multiplies two vectors element by element."};

  indicatorMetaData["round"] = IndicatorMetaData{
      .tags = {"simple", "round", "math", "rounding", "vector"},
      .desc = "Vector Round. Rounds each element to the nearest integer."};

  indicatorMetaData["sin"] = IndicatorMetaData{
      .tags = {"simple", "sin", "math", "trigonometric", "vector"},
      .desc =
          "Vector Sine. Calculates the sine for each element in the input."};

  indicatorMetaData["sinh"] = IndicatorMetaData{
      .tags = {"simple", "sinh", "math", "hyperbolic", "vector"},
      .desc = "Vector Hyperbolic Sine. Calculates the hyperbolic sine for each "
              "element in the input."};

  indicatorMetaData["sqrt"] =
      IndicatorMetaData{.tags = {"simple", "sqrt", "math", "vector"},
                        .desc = "Vector Square Root. Calculates the square "
                                "root for each element in the input."};

  indicatorMetaData["stddev"] = IndicatorMetaData{
      .tags = {"math", "stddev", "standard-deviation", "statistics",
               "volatility"},
      .desc = "Standard Deviation Over Period. Calculates the standard "
              "deviation over the specified period.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["stderr"] = IndicatorMetaData{
      .tags = {"math", "stderr", "standard-error", "statistics"},
      .desc = "Standard Error Over Period. Calculates the standard error over "
              "the specified period.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["sub"] = IndicatorMetaData{
      .tags = {"simple", "sub", "math", "arithmetic", "vector"},
      .desc = "Vector Subtraction. Subtracts the second vector from the first "
              "element by element."};

  indicatorMetaData["sum"] = IndicatorMetaData{
      .tags = {"math", "sum", "cumulative", "total"},
      .desc = "Sum Over Period. Calculates the sum over the "
              "specified period for each element position.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::Null};

  indicatorMetaData["tan"] = IndicatorMetaData{
      .tags = {"simple", "tan", "math", "trigonometric", "vector"},
      .desc = "Vector Tangent. Calculates the tangent for each element in the "
              "input."};

  indicatorMetaData["tanh"] = IndicatorMetaData{
      .tags = {"simple", "tanh", "math", "hyperbolic", "vector"},
      .desc = "Vector Hyperbolic Tangent. Calculates the hyperbolic tangent "
              "for each element in the input."};

  indicatorMetaData["todeg"] = IndicatorMetaData{
      .tags = {"simple", "todeg", "math", "conversion", "vector"},
      .desc = "Vector Degree Conversion. Converts radian values to degrees for "
              "each element in the input."};

  indicatorMetaData["torad"] = IndicatorMetaData{
      .tags = {"simple", "torad", "math", "conversion", "vector"},
      .desc = "Vector Radian Conversion. Converts degree values to radians for "
              "each element in the input."};

  indicatorMetaData["trunc"] = IndicatorMetaData{
      .tags = {"simple", "trunc", "math", "rounding", "vector"},
      .desc = "Vector Truncate. Truncates the decimal part of each element in "
              "the input."};

  indicatorMetaData["var"] = IndicatorMetaData{
      .tags = {"math", "var", "variance", "statistics", "volatility"},
      .desc = "Variance Over Period. Calculates the variance over the "
              "specified period.",
      .category = epoch_core::TransformCategory::Math,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  // Technical indicators
  indicatorMetaData["ad"] = IndicatorMetaData{
      .tags = {"indicator", "ad", "volume", "accumulation-distribution"},
      .desc =
          "Accumulation/Distribution Line. Volume-based indicator designed to "
          "measure cumulative flow of money into and out of a security.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["adosc"] = IndicatorMetaData{
      .tags = {"indicator", "adosc", "volume", "oscillator"},
      .desc = "Accumulation/Distribution Oscillator. Indicates momentum in the "
              "Accumulation/Distribution Line using two moving averages.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["adx"] = IndicatorMetaData{
      .tags = {"indicator", "adx", "trend", "directional-movement"},
      .desc = "Average Directional Movement Index. Measures the strength of a "
              "trend, regardless of its direction.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line,
      .strategyTypes = {"trend-strength", "regime-detection", "trend-following"},
      .relatedTransforms = {"adxr", "di", "dm", "dx"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Measures trend strength (not direction). ADX >25 = trending, <20 = ranging/choppy. Use as filter: trade trend strategies when ADX >25, mean-reversion when ADX <20. Rising ADX = strengthening trend, falling ADX = weakening trend. Combine with +DI/-DI for direction. High ADX doesn't indicate bullish/bearish, just strong trend.",
      .limitations = "Significant lag - based on smoothed moving averages. Slow to signal trend changes. Can stay high during corrections within trends. No direction info (use DI indicators). Works best with 14-25 period on daily charts. Not suitable for very short timeframes. Requires patience - signals develop slowly."};


  indicatorMetaData["adxr"] = IndicatorMetaData{
      .tags = {"indicator", "adxr", "trend", "directional-movement"},
      .desc = "Average Directional Movement Rating. Smoothed version of ADX, "
              "provides trend direction information.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["ao"] = IndicatorMetaData{
      .tags = {"indicator", "ao", "momentum", "oscillator"},
      .desc = "Awesome Oscillator. Measures market momentum by comparing a "
              "5-period and 34-period simple moving average.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::ao};

  indicatorMetaData["apo"] = IndicatorMetaData{
      .tags = {"indicator", "apo", "moving-average", "oscillator", "momentum"},
      .desc = "Absolute Price Oscillator. Shows the difference between two "
              "exponential moving averages as an absolute value.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["aroon"] = IndicatorMetaData{
      .tags = {"indicator", "aroon", "trend", "oscillator"},
      .desc = "Aroon. Measures the time between highs and lows over a time "
              "period, identifying trends and corrections.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::aroon,
      .strategyTypes = {"trend-identification", "trend-strength", "trend-beginning-detection"},
      .relatedTransforms = {"aroonosc", "adx", "dx"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Two lines: Aroon Up and Aroon Down (0-100). Measures time since highest high / lowest low in period. Up above 70 & Down below 30 = strong uptrend. Down above 70 & Up below 30 = strong downtrend. Both below 50 = consolidation. Crossovers signal trend changes. Better at identifying trend beginnings than ADX. Default 25 period.",
      .limitations = "Based solely on time since highs/lows, ignores price magnitude. Can give early signals before trend established. Consolidation periods (both low) ambiguous. Requires confirmation from other indicators. Less popular than ADX. Works best on trending instruments with clear swings."};


  indicatorMetaData["aroonosc"] = IndicatorMetaData{
      .tags = {"indicator", "aroonosc", "trend", "oscillator"},
      .desc = "Aroon Oscillator. Subtracts Aroon Down from Aroon Up, measuring "
              "the strength of a prevailing trend.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["atr"] = IndicatorMetaData{
      .tags = {"indicator", "atr", "volatility", "average-true-range"},
      .desc = "Average True Range. Measures market volatility by calculating "
              "the average range between price points.",
      .category = epoch_core::TransformCategory::Volatility,
      .plotKind = epoch_core::TransformPlotKind::panel_line,
      .strategyTypes = {"risk-management", "position-sizing", "stop-loss-placement", "volatility-targeting"},
      .relatedTransforms = {"tr", "natr", "bbands", "return_vol"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Essential for position sizing and stop placement. Use ATR multiples for stops (e.g., 2x ATR stop gives breathing room). Higher ATR = more volatile = smaller position or wider stops. Trend filters: rising ATR suggests expansion/breakout, falling ATR suggests consolidation. Not directional - only measures volatility magnitude.",
      .limitations = "Lagging - based on past volatility. Doesn't predict future volatility. Absolute value in price units - not normalized (use NATR for cross-asset comparison). Can be slow to adapt to regime changes with standard 14 period. Gaps affect calculation. Consider shorter periods (7-10) for faster adaptation."};


  indicatorMetaData["avgprice"] = IndicatorMetaData{
      .tags = {"overlay", "avgprice", "price", "average"},
      .desc = "Average Price. Calculates the average of "
              "open, high, low, and close prices.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["bbands"] = IndicatorMetaData{
      .tags = {"overlay", "bbands", "volatility", "bands", "bollinger"},
      .desc = "Bollinger Bands. Volatility bands placed above and below a "
              "moving average, adapting to market conditions.",
      .category = epoch_core::TransformCategory::Volatility,
      .plotKind = epoch_core::TransformPlotKind::bbands,
      .strategyTypes = {"mean-reversion", "breakout", "bollinger-squeeze", "volatility-expansion"},
      .relatedTransforms = {"bband_percent", "bband_width", "atr", "keltner_channels"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Three bands: middle (SMA), upper (+2 stddev), lower (-2 stddev). Mean-reversion: price at bands suggests overbought/oversold. Breakout: squeeze (narrow bands) precedes big moves. Price riding upper band = strong uptrend. Width measures volatility. Combine with %B indicator for normalized position. Use band walks for trend continuation.",
      .limitations = "Not directional on its own - just identifies extremes. Bands can expand indefinitely in trending markets (no fixed overbought/oversold). Squeeze detection requires bband_width. Default 20-period/2-stddev may need adjustment per asset. Can give false signals in strong trends when price 'walks the bands'."};


  indicatorMetaData["bop"] = IndicatorMetaData{
      .tags = {"indicator", "bop", "price", "balance-of-power", "momentum"},
      .desc = "Balance of Power. Measures buying and selling pressure by "
              "comparing closing price to trading range.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["cci"] = IndicatorMetaData{
      .tags = {"indicator", "cci", "momentum", "commodity-channel-index"},
      .desc = "Commodity Channel Index. Identifies cyclical turns in price and "
              "measures variations from the statistical mean.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::cci,
      .strategyTypes = {"mean-reversion", "overbought-oversold", "trend-following", "breakout"},
      .relatedTransforms = {"rsi", "stoch", "willr", "mfi"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Unbounded oscillator measuring deviation from statistical mean. Typical range: -100 to +100, but can go beyond. CCI >+100 = overbought, <-100 = oversold. Unlike RSI/Stoch, extreme readings can persist in trends. Use for: mean-reversion in ranges (fade extremes), trend-following in trends (ride extremes), divergence detection. Zero-line crosses indicate momentum shifts.",
      .limitations = "Unbounded nature makes fixed thresholds less reliable than RSI. Extreme readings normal in strong trends - not always reversal signals. Requires context (trending vs ranging) to interpret correctly. Default 20-period may need adjustment. More volatile than RSI. Consider using with ADX to distinguish trending/ranging regimes."};


  indicatorMetaData["cmo"] = IndicatorMetaData{
      .tags = {"indicator", "cmo", "momentum", "oscillator"},
      .desc = "Chande Momentum Oscillator. Momentum oscillator calculating "
              "relative momentum of positive and negative price movements.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["cvi"] = IndicatorMetaData{
      .tags = {"indicator", "cvi", "volatility", "chaikins"},
      .desc = "Chaikins Volatility. Measures volatility by tracking the "
              "difference between high and low prices over a period.",
      .category = epoch_core::TransformCategory::Volatility,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["dema"] = IndicatorMetaData{
      .tags = {"overlay", "dema", "moving-average", "double-exponential"},
      .desc = "Double Exponential Moving Average. Moving average that reduces "
              "lag with a double smoothing mechanism.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line,
      .strategyTypes = {"trend-following", "low-lag-trend", "fast-moving-average"},
      .relatedTransforms = {"ema", "tema", "hma", "zlema"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Faster-responding MA than EMA. Uses double smoothing (EMA of EMA) to reduce lag while maintaining smoothness. Good for catching trend changes quickly. Use for crossover systems where responsiveness matters. Period typically 10-30. Responds faster than SMA/EMA but slower than HMA/TEMA.",
      .limitations = "More sensitive to noise than SMA/EMA. Can whipsaw in choppy markets. Still lags price, just less than traditional MAs. False signals increase with faster response. Best in trending markets with clear direction. Consider combining with ADX to filter ranging periods."};


  indicatorMetaData["di"] = IndicatorMetaData{
      .tags = {"indicator", "di", "trend", "directional-indicator"},
      .desc = "Directional Indicator. Components of ADX that measure positive "
              "and negative price movement strength.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["dm"] = IndicatorMetaData{
      .tags = {"indicator", "dm", "trend", "directional-movement"},
      .desc = "Directional Movement. Identifies whether prices are trending by "
              "comparing consecutive highs and lows.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["dpo"] = IndicatorMetaData{
      .tags = {"indicator", "dpo", "trend", "detrended-oscillator"},
      .desc = "Detrended Price Oscillator. Eliminates long-term trends to "
              "focus on short to medium-term cycles.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["dx"] = IndicatorMetaData{
      .tags = {"indicator", "dx", "trend", "directional-movement"},
      .desc = "Directional Movement Index. Measures trending strength by "
              "comparing +DI and -DI indicators.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["ema"] = IndicatorMetaData{
      .tags = {"overlay", "ema", "moving-average", "exponential"},
      .desc = "Exponential Moving Average. Moving average that gives more "
              "weight to recent prices, reducing lag.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line,
      .strategyTypes = {"trend-following", "moving-average-crossover", "dynamic-support-resistance"},
      .relatedTransforms = {"sma", "dema", "tema", "hma", "kama"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Faster-reacting trend indicator than SMA due to exponential weighting. Use for trend identification, crossover systems, or dynamic support/resistance. More responsive to recent price action. Common combinations: 12/26 EMA (MACD basis), 8/21 (short-term), 20/50 (medium-term). Price above EMA = uptrend, below = downtrend.",
      .limitations = "Still lags price, just less than SMA. More sensitive to noise and false signals than SMA. Can whipsaw in ranging markets. Early signals may be false breakouts. Consider combining with volume or momentum confirmation."};


  indicatorMetaData["emv"] = IndicatorMetaData{
      .tags = {"indicator", "emv", "volume", "ease-of-movement"},
      .desc = "Ease of Movement. Relates price change to volume, identifying "
              "whether price changes are easy or difficult.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["fisher"] = IndicatorMetaData{
      .tags = {"indicator", "fisher", "transform", "oscillator"},
      .desc = "Fisher Transform. Converts prices to a Gaussian normal "
              "distribution to identify extreme price movements.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::fisher};

  indicatorMetaData["fosc"] = IndicatorMetaData{
      .tags = {"indicator", "fosc", "oscillator", "forecast"},
      .desc = "Forecast Oscillator. Compares price to linear regression "
              "forecast value, indicating when price deviates from trend.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::fosc};

  indicatorMetaData["hma"] = IndicatorMetaData{
      .tags = {"overlay", "hma", "moving-average", "hull"},
      .desc = "Hull Moving Average. Moving average designed to reduce lag and "
              "improve smoothness by using weighted averages.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line,
      .strategyTypes = {"trend-following", "low-lag-trend", "smooth-responsive"},
      .relatedTransforms = {"ema", "wma", "dema", "tema"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Best balance between lag reduction and smoothness. Uses weighted moving average of weighted differences to minimize lag while staying smooth. Excellent for trend identification without excessive noise. Color change (slope change) provides clear trend signals. Popular for swing trading. Period typically 9-21 for shorter-term.",
      .limitations = "Calculation complex - harder to understand intuitively. Can occasionally overshoot in ranging markets. Less well-known than EMA/SMA. Parameter tuning more critical than simple MAs. Requires adequate trend for best performance. Not ideal for very short-term scalping."};


  indicatorMetaData["kama"] = IndicatorMetaData{
      .tags = {"overlay", "kama", "moving-average", "adaptive", "kaufman"},
      .desc = "Kaufman Adaptive Moving Average. Adjusts sensitivity "
              "automatically based on market volatility.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["kvo"] = IndicatorMetaData{
      .tags = {"indicator", "kvo", "volume", "oscillator", "klinger"},
      .desc = "Klinger Volume Oscillator. Compares volume to price trends to "
              "identify reversals and divergence.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["linreg"] = IndicatorMetaData{
      .tags = {"overlay", "linreg", "linear-regression", "trend"},
      .desc = "Linear Regression. Plots a best-fit line through price data, "
              "showing overall direction of price movement.",
      .category = epoch_core::TransformCategory::Statistical,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["linregintercept"] = IndicatorMetaData{
      .tags = {"indicator", "linregintercept", "linear-regression", "trend",
               "statistics"},
      .desc = "Linear Regression Intercept. Calculates the y-intercept values "
              "for linear regression analysis.",
      .category = epoch_core::TransformCategory::Statistical,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["linregslope"] = IndicatorMetaData{
      .tags = {"indicator", "linregslope", "linear-regression", "trend",
               "statistics"},
      .desc = "Linear Regression Slope. Measures the rate of change in linear "
              "regression values, indicating trend strength.",
      .category = epoch_core::TransformCategory::Statistical,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["macd"] = IndicatorMetaData{
      .tags = {"indicator", "macd", "moving-average", "trend", "momentum"},
      .desc = "Moving Average Convergence/Divergence. Trend-following momentum "
              "indicator showing relationship between two moving averages.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::macd,
      .strategyTypes = {"trend-following", "momentum", "divergence-trading", "crossover"},
      .relatedTransforms = {"ema", "ppo", "apo", "trix"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Combines trend and momentum in one indicator. Three components: MACD line (12-26 EMA diff), Signal line (9 EMA of MACD), Histogram (MACD - Signal). Signals: MACD crosses Signal (bullish/bearish), histogram expansion/contraction (momentum strength), divergence (price vs MACD disagreement). Use for trend confirmation and entry timing.",
      .limitations = "Lagging indicator based on EMAs. Default 12/26/9 parameters may not suit all timeframes or assets. Can give false signals in ranging markets. Histogram can be misleading during consolidation. Works best in trending markets. Requires parameter optimization for different instruments."};


  indicatorMetaData["marketfi"] = IndicatorMetaData{
      .tags = {"indicator", "marketfi", "volume", "market-facilitation-index"},
      .desc = "Market Facilitation Index. Measures market readiness to move "
              "prices with minimal volume.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::column};

  indicatorMetaData["mass"] = IndicatorMetaData{
      .tags = {"indicator", "mass", "volatility", "index"},
      .desc = "Mass Index. Identifies potential reversals by examining "
              "high-low range expansion and contraction.",
      .category = epoch_core::TransformCategory::Volatility,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["medprice"] = IndicatorMetaData{
      .tags = {"overlay", "medprice", "price", "average"},
      .desc = "Median Price. Simple average of the high and "
              "low prices for each period.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["mfi"] = IndicatorMetaData{
      .tags = {"indicator", "mfi", "volume", "money-flow-index", "oscillator"},
      .desc = "Money Flow Index. Volume-weighted RSI that measures buying and "
              "selling pressure based on price and volume.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::rsi,
      .strategyTypes = {"mean-reversion", "overbought-oversold", "volume-confirmation", "divergence-trading"},
      .relatedTransforms = {"rsi", "obv", "ad", "stoch"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Volume-weighted RSI - accounts for money flow not just price. MFI >80 = overbought with strong buying volume, <20 = oversold with strong selling volume. More reliable than RSI when volume confirms price moves. Use divergences (price new high but MFI doesn't = bearish). Failure swings (MFI fails to exceed previous high/low) signal reversals. Combine with RSI for confirmation.",
      .limitations = "Requires quality volume data - unreliable with sporadic/low volume. More parameters than RSI = more curve-fitting risk. Can give early signals in strong trends. 80/20 thresholds may need adjustment per asset. Lagging like RSI. Works best on liquid instruments with consistent volume. Consider volume profile analysis for illiquid assets."};


  indicatorMetaData["mom"] = IndicatorMetaData{
      .tags = {"indicator", "mom", "momentum", "rate-of-change"},
      .desc = "Momentum. Measures rate of change in prices by comparing "
              "current price to a previous price.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["msw"] = IndicatorMetaData{
      .tags = {"indicator", "msw", "cycle", "sine-wave"},
      .desc = "Mesa Sine Wave. Identifies market cycles "
              "using sine waves derived from price data.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["natr"] = IndicatorMetaData{
      .tags = {"indicator", "natr", "volatility",
               "normalized-average-true-range"},
      .desc = "Normalized Average True Range. ATR expressed as a percentage of "
              "closing price, allowing comparison across securities.",
      .category = epoch_core::TransformCategory::Volatility,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["nvi"] = IndicatorMetaData{
      .tags = {"indicator", "nvi", "volume", "negative-volume-index"},
      .desc = "Negative Volume Index. Shows price movements on days when "
              "volume decreases, highlighting smart money activity.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["obv"] = IndicatorMetaData{
      .tags = {"indicator", "obv", "volume", "on-balance-volume"},
      .desc = "On Balance Volume. Running total of volume that adds when price "
              "rises and subtracts when price falls.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line,
      .strategyTypes = {"trend-confirmation", "divergence-trading", "volume-analysis", "accumulation-distribution"},
      .relatedTransforms = {"ad", "vwap", "vosc", "pvi", "nvi"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Cumulative volume indicator for confirming trends and spotting divergences. Rising OBV with rising price = healthy uptrend (volume confirms). OBV diverging from price = potential reversal (e.g., price makes new high but OBV doesn't = bearish divergence). Use to identify accumulation/distribution phases. Most powerful for divergence detection.",
      .limitations = "Cumulative nature means scale can be misleading - focus on direction and divergences not absolute values. Doesn't account for volume magnitude of individual bars. Can be distorted by large volume spikes. Best on liquid assets with consistent volume. Requires long history to establish baseline. Consider normalizing or using rate of change."};


  indicatorMetaData["ppo"] = IndicatorMetaData{
      .tags = {"indicator", "ppo", "momentum", "percentage-price-oscillator"},
      .desc = "Percentage Price Oscillator. Shows relationship between two "
              "moving averages as a percentage, similar to MACD.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["psar"] = IndicatorMetaData{
      .tags = {"overlay", "psar", "trend", "parabolic-sar"},
      .desc = "Parabolic SAR. Identifies potential reversals in price "
              "movement, providing entry and exit signals.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::psar};

  indicatorMetaData["pvi"] = IndicatorMetaData{
      .tags = {"indicator", "pvi", "volume", "positive-volume-index"},
      .desc = "Positive Volume Index. Shows price movements on days when "
              "volume increases, highlighting public participation.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["qstick"] = IndicatorMetaData{
      .tags = {"indicator", "qstick", "candlestick", "trend"},
      .desc = "Qstick. Measures the ratio of black to white candlesticks, "
              "indicating buying and selling pressure.",
      .category = epoch_core::TransformCategory::PriceAction,
      .plotKind = epoch_core::TransformPlotKind::qstick};

  indicatorMetaData["roc"] = IndicatorMetaData{
      .tags = {"indicator", "roc", "momentum", "rate-of-change"},
      .desc = "Rate of Change. Measures percentage change between current "
              "price and price n periods ago.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["rocr"] = IndicatorMetaData{
      .tags = {"indicator", "rocr", "momentum", "rate-of-change-ratio"},
      .desc = "Rate of Change Ratio. Calculates the ratio of current price to "
              "price n periods ago, measuring momentum.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["rsi"] = IndicatorMetaData{
      .tags = {"indicator", "rsi", "momentum", "oscillator",
               "relative-strength"},
      .desc = "Relative Strength Index. Momentum oscillator measuring speed "
              "and change of price movements, indicating overbought/oversold "
              "conditions.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::rsi,
      .strategyTypes = {"mean-reversion", "overbought-oversold", "divergence-trading", "momentum"},
      .relatedTransforms = {"stochrsi", "mfi", "willr", "cci"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Core momentum indicator for overbought (>70) and oversold (<30) conditions. Use for mean-reversion entries, divergence signals, or momentum confirmation. Common patterns: RSI >70 = overbought (potential reversal), RSI <30 = oversold (potential bounce). Combine with price action for best results. Divergence (price makes new high but RSI doesn't) signals weakening momentum.",
      .limitations = "Lagging indicator - signals occur after price moves. Can stay overbought/oversold during strong trends. Standard 70/30 thresholds may need adjustment for different assets. Whipsaws in ranging markets. Consider using dynamic overbought/oversold levels or combine with trend filters."};

  indicatorMetaData["sma"] = IndicatorMetaData{
      .tags = {"overlay", "sma", "moving-average", "simple"},
      .desc = "Simple Moving Average. Unweighted mean of previous n data "
              "points, smoothing price data to identify trends.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line,
      .strategyTypes = {"trend-following", "moving-average-crossover", "support-resistance"},
      .relatedTransforms = {"ema", "wma", "dema", "tema", "hma"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Basic trend indicator and dynamic support/resistance. Use price crossing SMA for trend changes, or SMA crossovers for signals (e.g., 50/200 golden cross). Shorter periods (10-20) for responsive signals, longer (50-200) for major trend. Common: 20 SMA for short-term, 50/200 for long-term golden/death cross.",
      .limitations = "Significant lag - all data points weighted equally including old data. Whipsaws in choppy markets. Slower than EMA to react to price changes. Not suitable as sole signal - combine with momentum or volume. Moving averages are inherently lagging by design."};


  indicatorMetaData["stoch"] = IndicatorMetaData{
      .tags = {"indicator", "stoch", "momentum", "oscillator", "stochastic"},
      .desc =
          "Stochastic Oscillator. Compares closing price to price range over a "
          "period, indicating momentum and overbought/oversold conditions.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::stoch,
      .strategyTypes = {"mean-reversion", "overbought-oversold", "momentum", "divergence-trading"},
      .relatedTransforms = {"stochrsi", "rsi", "willr", "mfi"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Two lines: %K (fast) and %D (slow, smoothed). Overbought >80, oversold <20. Signals: K crosses D (bullish/bearish), divergence with price, extreme readings. More sensitive than RSI - reacts faster to price changes. Use for timing entries in range-bound markets or pullback entries in trends. Common settings: 14,3,3 (standard) or 5,3,3 (faster).",
      .limitations = "Very sensitive - many false signals in trending markets. Can remain overbought/oversold during strong trends. Requires smoothing (%D) to reduce noise. Best in ranging/oscillating markets. Not reliable as standalone - combine with trend filter. Parameter sensitivity high - different settings dramatically change signals."};


  indicatorMetaData["stochrsi"] = IndicatorMetaData{
      .tags = {"indicator", "stochrsi", "momentum", "oscillator", "stochastic"},
      .desc = "Stochastic RSI. Applies stochastic formula to RSI values, "
              "creating a more sensitive oscillator.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::rsi,
      .strategyTypes = {"mean-reversion", "overbought-oversold", "momentum", "fast-oscillator"},
      .relatedTransforms = {"rsi", "stoch", "willr"},
      .assetRequirements = {"single-asset"},
      .usageContext = "More sensitive version of RSI using stochastic calculation on RSI values. Oscillates 0-1 (or 0-100). More responsive to price changes than RSI - better for short-term trading. Use >0.8 for overbought, <0.2 for oversold. Generates more signals than RSI. Best with %K and %D lines for crossovers. Popular for crypto and volatile assets.",
      .limitations = "Extremely sensitive - very high false signal rate. Can stay overbought/oversold for extended periods in trends. Requires heavy filtering or confirmation. Not suitable as standalone indicator. Best in strongly ranging markets. Too noisy for position trading. Consider using only in confirmed ranges with other filters."};


  indicatorMetaData["tema"] = IndicatorMetaData{
      .tags = {"overlay", "tema", "moving-average", "triple-exponential"},
      .desc = "Triple Exponential Moving Average. Moving average designed to "
              "smooth price fluctuations and reduce lag.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line,
      .strategyTypes = {"trend-following", "ultra-low-lag", "scalping", "fast-moving-average"},
      .relatedTransforms = {"dema", "ema", "hma", "zlema"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Ultra-responsive MA using triple smoothing (EMA of EMA of EMA). Fastest traditional MA for catching trend changes. Best for short-term trading and scalping. Excellent for quick entries/exits. Use with shorter periods (5-15) for maximum responsiveness. Popular in high-frequency strategies.",
      .limitations = "Most sensitive to noise - highest false signal rate. Whipsaws badly in ranging markets. Requires strong trend filter. Can overshoot in volatile conditions. Best only for very short-term trading. Needs tight stops due to frequent reversals. Not suitable for position trading."};


  indicatorMetaData["tr"] = IndicatorMetaData{
      .tags = {"indicator", "tr", "volatility", "true-range"},
      .desc = "True Range. Measures market volatility by comparing current "
              "price range to previous close.",
      .category = epoch_core::TransformCategory::Volatility,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["trima"] = IndicatorMetaData{
      .tags = {"overlay", "trima", "moving-average", "triangular"},
      .desc = "Triangular Moving Average. Weighted moving average that places "
              "more weight on middle portion of calculation period.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["trix"] = IndicatorMetaData{
      .tags = {"indicator", "trix", "momentum", "oscillator"},
      .desc = "Trix. Triple exponentially smoothed moving average oscillator, "
              "showing percentage rate of change.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["tsf"] = IndicatorMetaData{
      .tags = {"overlay", "tsf", "trend", "time-series-forecast"},
      .desc = "Time Series Forecast. Linear regression projection that extends "
              "the regression line to predict future values.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["typprice"] = IndicatorMetaData{
      .tags = {"overlay", "typprice", "price", "average", "typical"},
      .desc = "Typical Price. Average of high, low, and close prices for each "
              "period, representing a balanced price.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["ultosc"] = IndicatorMetaData{
      .tags = {"indicator", "ultosc", "oscillator", "ultimate-oscillator"},
      .desc = "Ultimate Oscillator. Multi-timeframe momentum oscillator that "
              "uses weighted average of three oscillators.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["vhf"] = IndicatorMetaData{
      .tags = {"indicator", "vhf", "trend", "vertical-horizontal-filter",
               "volatility"},
      .desc = "Vertical Horizontal Filter. Identifies trending and ranging "
              "markets by measuring price direction versus volatility.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["vidya"] = IndicatorMetaData{
      .tags = {"overlay", "vidya", "moving-average", "variable-index"},
      .desc = "Variable Index Dynamic Average. Adapts to volatility by "
              "modifying the smoothing constant used in calculations.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["volatility"] = IndicatorMetaData{
      .tags = {"indicator", "volatility", "risk", "annualized"},
      .desc = "Annualized Historical Volatility. Measures price dispersion "
              "around the mean, expressed as an annualized percentage.",
      .category = epoch_core::TransformCategory::Volatility,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["vosc"] = IndicatorMetaData{
      .tags = {"indicator", "vosc", "volume", "oscillator"},
      .desc = "Volume Oscillator. Shows difference between two volume moving "
              "averages as percentage, indicating volume trends.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["vwma"] = IndicatorMetaData{
      .tags = {"overlay", "vwma", "moving-average", "volume-weighted"},
      .desc =
          "Volume Weighted Moving Average. Moving average that weights price "
          "by volume, giving more importance to high-volume price moves.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line,
      .strategyTypes = {"trend-following", "volume-confirmation", "institutional-flow"},
      .relatedTransforms = {"sma", "ema", "vwap", "obv"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Moving average weighted by volume - gives more weight to high-volume price levels. Better represents institutional/smart money positioning than simple MAs. Use like SMA but with volume confirmation built-in. Breaks more significant than SMA breaks. Good for identifying support/resistance with volume context. Compare to SMA to see volume impact.",
      .limitations = "Requires quality volume data - unreliable on low-volume or manipulated volume. Calculation heavier than simple MAs. Less widely used than SMA/EMA. Volume weighting can distort in illiquid periods. Best on liquid instruments with genuine volume. Consider VWAP for intraday instead."};


  indicatorMetaData["wad"] = IndicatorMetaData{
      .tags = {"indicator", "wad", "volume",
               "williams-accumulation-distribution"},
      .desc = "Williams Accumulation/Distribution. Measures buying/selling "
              "pressure by comparing closing price to midpoint of range.",
      .category = epoch_core::TransformCategory::Volume,
      .plotKind = epoch_core::TransformPlotKind::panel_line};

  indicatorMetaData["wcprice"] = IndicatorMetaData{
      .tags = {"overlay", "wcprice", "price", "weighted-close"},
      .desc = "Weighted Close Price. Average of OHLC prices with extra weight "
              "given to close: (H+L+C+C)/4.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["wilders"] = IndicatorMetaData{
      .tags = {"overlay", "wilders", "moving-average", "smoothing"},
      .desc = "Wilders Smoothing. Specialized moving average using a 1/n "
              "smoothing factor, commonly used in RSI calculations.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  indicatorMetaData["willr"] = IndicatorMetaData{
      .tags = {"indicator", "willr", "momentum", "oscillator", "williams"},
      .desc = "Williams %R. Momentum oscillator that indicates "
              "overbought/oversold conditions relative to high-low range.",
      .category = epoch_core::TransformCategory::Momentum,
      .plotKind = epoch_core::TransformPlotKind::rsi,
      .strategyTypes = {"mean-reversion", "overbought-oversold", "momentum"},
      .relatedTransforms = {"stoch", "rsi", "cci"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Inverted stochastic oscillator ranging from 0 to -100. -20 to 0 = overbought, -80 to -100 = oversold. Shows where close is relative to high-low range. More volatile than RSI. Use for timing entries in trends (buy oversold in uptrend, sell overbought in downtrend). Divergence signals valuable. Default period 14.",
      .limitations = "Inverted scale (-100 to 0) can be confusing. Very similar to Fast Stochastic %K. Can remain in extreme zones during strong trends. High false signal rate without filters. Best combined with trend indicator. Less popular than RSI/Stochastic. Consider using regular Stochastic instead for clearer interpretation."};


  indicatorMetaData["wma"] = IndicatorMetaData{
      .tags = {"overlay", "wma", "moving-average", "weighted"},
      .desc = "Weighted Moving Average. Moving average that assigns more "
              "weight to recent data and less to older data.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line,
      .strategyTypes = {"trend-following", "weighted-trend"},
      .relatedTransforms = {"sma", "ema", "hma", "vwma"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Linear weighted MA giving more importance to recent prices. Faster than SMA, slower than EMA. Weights decrease linearly (most recent = N, oldest = 1). Good middle ground between SMA and EMA. Use for trend following when you want more responsiveness than SMA but more stability than EMA. Common in trading systems as baseline trend.",
      .limitations = "Still lags price significantly. Arbitrarily chosen linear weighting may not suit all markets. Less popular than EMA - fewer traders watching same levels. Doesn't adapt to volatility. Can whipsaw in choppy conditions. For most uses, EMA is preferred for better mathematical properties."};


  indicatorMetaData["zlema"] = IndicatorMetaData{
      .tags = {"overlay", "zlema", "moving-average", "zero-lag"},
      .desc = "Zero-Lag Exponential Moving Average. EMA variant that removes "
              "lag by using linear extrapolation.",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::line};

  return indicatorMetaData;
}

inline MetaDataOption MakeTulipOptions(std::string const &option, std::string const &indicatorName = "") {
  static std::unordered_set<std::string> skip{"open", "high", "low", "close",
                                              "volume"};
  MetaDataOption optionMetaData{.id = option,
                                .name = beautify(option),
                                .type = epoch_core::MetaDataOptionType::Decimal,
                                .defaultValue = std::nullopt,
                                .isRequired = true, // Default to required; set to false when we add a default
                                .selectOption = {}};

  // Contextual defaults based on indicator type and option name
  // Note: Tulip library uses "short period" but beautify() converts to "short_period"

  // MACD: short_period=12, long_period=26, signal_period=9
  if (indicatorName == "macd" && option == "short_period") {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1;
    optionMetaData.max = 10000;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{12.0}};
    optionMetaData.isRequired = false; // Has default
  }
  else if (indicatorName == "macd" && option == "long_period") {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1;
    optionMetaData.max = 10000;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{26.0}};
    optionMetaData.isRequired = false; // Has default
  }
  else if (indicatorName == "macd" && option == "signal_period") {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1;
    optionMetaData.max = 10000;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{9.0}};
    optionMetaData.isRequired = false; // Has default
  }
  // Stochastic: k_period=14, k_slowing_period=3, d_period=3
  else if ((indicatorName == "stoch" || indicatorName == "stochf") && option == "k_period") {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1;
    optionMetaData.max = 10000;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{14.0}};
    optionMetaData.isRequired = false; // Has default
  }
  else if ((indicatorName == "stoch" || indicatorName == "stochf") && option == "k_slowing_period") {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1;
    optionMetaData.max = 10000;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{3.0}};
    optionMetaData.isRequired = false; // Has default
  }
  else if ((indicatorName == "stoch" || indicatorName == "stochf") && option == "d_period") {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1;
    optionMetaData.max = 10000;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{3.0}};
    optionMetaData.isRequired = false; // Has default
  }
  // Generic period parameters - default to 14 for most indicators
  else if (option.find("period") != std::string::npos) {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1; // Period must be at least 1
    optionMetaData.max = 10000;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{14.0}}; // Default 14 for most indicators
    optionMetaData.isRequired = false; // Has default
  }
  // Standard deviation multiplier (used in Bollinger Bands, etc.)
  else if (option == "stddev") {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1;
    optionMetaData.max = 10;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{2.0}}; // Standard 2 std dev
    optionMetaData.isRequired = false; // Has default
  }
  // Parabolic SAR parameters
  else if (option == "acceleration_factor_step") {
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{0.02}};
    optionMetaData.isRequired = false; // Has default
  }
  else if (option == "acceleration_factor_maximum") {
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{0.2}};
    optionMetaData.isRequired = false; // Has default
  }
  // Multiplier/factor parameters (typically decimal)
  else if (option == "multiplier" || option == "factor") {
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{2.0}}; // Common for bands
    optionMetaData.isRequired = false; // Has default
  }
  // Smoothing parameters
  else if (option == "smoothing") {
    optionMetaData.type = epoch_core::MetaDataOptionType::Integer;
    optionMetaData.min = 1;
    optionMetaData.max = 10000;
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{3.0}};
    optionMetaData.isRequired = false; // Has default
  }
  // Volume factor (for volume-based indicators)
  else if (option == "volume_factor") {
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{0.7}};
    optionMetaData.isRequired = false; // Has default
  }
  // Cutoff/threshold parameters
  else if (option == "cutoff") {
    optionMetaData.defaultValue = MetaDataOptionDefinition{MetaDataOptionDefinition::T{0.5}};
    optionMetaData.isRequired = false; // Has default
  }
  // Moving average type parameters remain as strings with no default
  else if (option == "ma_type") {
    optionMetaData.type = epoch_core::MetaDataOptionType::String;
    optionMetaData.isRequired = true; // These must be specified
    optionMetaData.defaultValue = std::nullopt;
  }

  return optionMetaData;
};

inline std::vector<IOMetaData> MakeTulipInputs(auto const &inputs) {
    static std::unordered_set<std::string> skip{"open", "high", "low", "close",
                                                "volume"};
    std::vector<IOMetaData> ioMetaDataList;
    bool useSingleWildCard = inputs.size() == 1;
    for (auto const &[i, input] : std::views::enumerate(inputs)) {
        std::string_view inputStr{input};

        IOMetaData ioMetaData;
        ioMetaData.allowMultipleConnections = false;

        if (inputStr == "real") {
            ioMetaData.id = useSingleWildCard ? ARG : std::format("{}{}", ARG, i);
        } else {
            // skip ohlcv inputs
            continue;
        }
        ioMetaDataList.emplace_back(ioMetaData);
    }

    return ioMetaDataList;
}

inline std::vector<IOMetaData> MakeTulipOutputs(auto const &outputs) {
  std::vector<IOMetaData> ioMetaDataList;
  if (outputs.size() == 1) {
    std::string output{outputs[0]};
    ioMetaDataList.emplace_back(
        IOMetaData{.type = (output == "crossany" || output == "crossover" || output == "crossunder")
                               ? epoch_core::IODataType::Boolean
                               : epoch_core::IODataType::Decimal,
                   .id = "result",
                   .name = "",
                    .allowMultipleConnections = true});
  } else {
    for (auto const &output_view : outputs) {
      std::string output{output_view};
      ioMetaDataList.emplace_back(
          IOMetaData{.type = epoch_core::IODataType::Decimal,
                     .id = output,
                     .name = beautify(output),
                     .allowMultipleConnections = true});
    }
  }

  return ioMetaDataList;
}

std::vector<TransformsMetaData> MakeTulipIndicators() {
  static const std::unordered_map<std::string, IndicatorMetaData>
      indicatorMetaData = MakeTulipIndicatorMetaData();

  std::vector<TransformsMetaData> allIndicators(TI_INDICATOR_COUNT);
  static std::unordered_set<std::string> dataSources{"open", "high", "low",
                                                     "close", "volume"};
    static std::unordered_set<std::string> skipNodes{"lag"};

  std::ranges::transform(
      std::span{ti_indicators, ti_indicators + TI_INDICATOR_COUNT} | std::views::filter([](const ti_indicator_info &tiIndicatorInfo) {
          return !skipNodes.contains(tiIndicatorInfo.name);
      }),
      allIndicators.begin(), [&](const ti_indicator_info &tiIndicatorInfo) {

        const std::span optionSpan{tiIndicatorInfo.option_names,
                                   tiIndicatorInfo.option_names +
                                       tiIndicatorInfo.options};
        const std::span inputSpan{tiIndicatorInfo.input_names,
                                  tiIndicatorInfo.input_names +
                                      tiIndicatorInfo.inputs};
        const std::span outputSpan{tiIndicatorInfo.output_names,
                                   tiIndicatorInfo.output_names +
                                       tiIndicatorInfo.outputs};

        auto metadata = epoch_core::lookupDefault(
            indicatorMetaData, tiIndicatorInfo.name, IndicatorMetaData{});

        std::vector<std::string> requiredDataSources;
        for (auto const &inputPtr : inputSpan) {
          std::string input{inputPtr};
          if (dataSources.contains(input)) {
            requiredDataSources.emplace_back(1, input.front());
          }
        }

        return TransformsMetaData{
            .id = tiIndicatorInfo.name,
            .category = metadata.category,
            .plotKind = metadata.plotKind,
            .name = tiIndicatorInfo.full_name,
            .options = epoch_core::ranges::to<std::vector>(
                optionSpan | std::views::transform([&](const auto& opt) {
                    return MakeTulipOptions(opt, tiIndicatorInfo.name);
                })),
            .isCrossSectional = false,
            .desc = metadata.desc,
            .inputs = MakeTulipInputs(inputSpan),
            .outputs = MakeTulipOutputs(outputSpan),
            .tags = metadata.tags,
            .requiresTimeFrame = requiredDataSources.size() > 0,
            .requiredDataSources = requiredDataSources,
            .flagSchema = metadata.flagSchema,
            .strategyTypes = metadata.strategyTypes,
            .relatedTransforms = metadata.relatedTransforms,
            .assetRequirements = metadata.assetRequirements,
            .usageContext = metadata.usageContext,
            .limitations = metadata.limitations};
      });

  // Add custom indicators that are not native to Tulip library
  // crossunder - implemented as crossover with swapped inputs
  auto crossunderMetadata = epoch_core::lookupDefault(
      indicatorMetaData, "crossunder", IndicatorMetaData{});
  allIndicators.push_back(TransformsMetaData{
      .id = "crossunder",
      .category = crossunderMetadata.category,
      .plotKind = crossunderMetadata.plotKind,
      .name = "Vector Crossunder",
      .options = {},
      .isCrossSectional = false,
      .desc = crossunderMetadata.desc,
      .inputs = {IOMetaDataConstants::DECIMAL_INPUT0_METADATA,
                 IOMetaDataConstants::DECIMAL_INPUT1_METADATA},
      .outputs = {IOMetaData{.type = epoch_core::IODataType::Boolean,
                             .id = "result",
                             .name = "",
                             .allowMultipleConnections = true}},
      .tags = crossunderMetadata.tags,
      .requiresTimeFrame = false,
      .requiredDataSources = {},
      .flagSchema = crossunderMetadata.flagSchema,
      .strategyTypes = crossunderMetadata.strategyTypes,
      .relatedTransforms = crossunderMetadata.relatedTransforms,
      .assetRequirements = crossunderMetadata.assetRequirements,
      .usageContext = crossunderMetadata.usageContext,
      .limitations = crossunderMetadata.limitations});

  return allIndicators;
}
} // namespace epoch_script::transforms