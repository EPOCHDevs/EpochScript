#include "candles.h"
#include "common.h"
#include "epoch_core/common_utils.h"
#include <epoch_script/transforms/core/metadata.h>
#include <epoch_core/ranges_to.h>
#include <vector>
#include <yaml-cpp/yaml.h>

namespace epoch_script::transforms {

struct CandlePatternMetaData {
  std::vector<std::string> tags;
  std::string desc;

  // Enhanced metadata for RAG/LLM
  std::vector<std::string> strategyTypes{};
  std::vector<std::string> relatedTransforms{};
  std::vector<std::string> assetRequirements{"single-asset"};
  std::string usageContext{};
  std::string limitations{};

  // Flag display configuration
  std::optional<FlagSchema> flagSchema{std::nullopt};
};

std::unordered_map<std::string, CandlePatternMetaData>
MakeCandlePatternMetaData() {
  std::unordered_map<std::string, CandlePatternMetaData> patternMetaData;

  patternMetaData["abandoned_baby_bear"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "reversal",
               "abandoned-baby"},
      .desc = "Bearish reversal pattern with a large up candle, followed by a "
              "gapped doji, and a gapped down candle. Signals potential "
              "downward trend.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Abandoned Baby Bear",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["abandoned_baby_bull"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "reversal",
               "abandoned-baby"},
      .desc = "Bullish reversal pattern with a large down candle, followed by "
              "a gapped doji, and a gapped up candle. Signals potential upward "
              "trend.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Abandoned Baby Bull",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["big_black_candle"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "continuation",
               "big-candle"},
      .desc = "Large bearish candle with a long body. Indicates strong selling "
              "pressure and potential downward momentum.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Big Black Candle",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["big_white_candle"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "continuation",
               "big-candle"},
      .desc = "Large bullish candle with a long body. Indicates strong buying "
              "pressure and potential upward momentum.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Big White Candle",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["black_marubozu"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "marubozu", "no-shadow"},
      .desc = "Bearish candle with no upper or lower shadows (wicks). Strong "
              "selling pressure with opening at high and closing at low.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Black Marubozu",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["doji"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "neutral", "indecision", "doji"},
      .desc =
          "Candle with virtually no body where open and close are at the same "
          "level. Indicates market indecision and potential reversal.",
      .strategyTypes = {"reversal", "price-action", "indecision-detection"},
      .relatedTransforms = {"dragonfly_doji", "gravestone_doji", "long_legged_doji", "spinning_top"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Neutral candlestick signaling indecision between buyers and sellers. Context matters: doji after uptrend suggests potential top, after downtrend suggests potential bottom. Often precedes reversals but needs confirmation from next candle. Use with support/resistance levels or other indicators. Multiple dojis signal consolidation.",
      .limitations = "Not directional on its own - requires context and confirmation. Can appear frequently in ranging markets without significance. Different doji types (dragonfly, gravestone, long-legged) have different implications. Body size threshold subjective. Best used as warning signal, not entry trigger. Combine with volume and trend analysis.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Doji",
          .textIsTemplate = false,
          .color = epoch_core::Color::Default,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["dragonfly_doji"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "reversal", "doji"},
      .desc = "Doji with no upper shadow but a long lower shadow. Indicates "
              "rejection of lower prices and potential bullish reversal.",
      .strategyTypes = {"reversal", "price-action", "bullish-reversal", "support-detection"},
      .relatedTransforms = {"hammer", "doji", "gravestone_doji"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Bullish doji variant with T-shape (no upper wick, long lower wick). Sellers pushed price down but buyers drove it back to open. Open/close at high = strong rejection of lows. More bullish than standard doji. Best after downtrend at support. Very similar to hammer but with smaller body (doji). Requires next candle confirmation.",
      .limitations = "Rare pattern - strict doji body requirement. Similar to hammer, can be misidentified. Must appear after downtrend at support. Needs strong confirmation candle. Lower shadow length threshold subjective. False signals in ranging markets. Best with volume spike on lower shadow. Consider hammer as less restrictive alternative.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Dragonfly Doji",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["engulfing_bear"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "reversal", "engulfing"},
      .desc =
          "Bearish pattern where a large down candle completely engulfs the "
          "previous up candle. Strong signal of trend reversal to downside.",
      .strategyTypes = {"reversal", "price-action", "bearish-reversal", "momentum-shift"},
      .relatedTransforms = {"engulfing_bull", "evening_star", "shooting_star", "abandoned_baby_bear"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Powerful two-candle bearish reversal pattern. Second candle's body completely engulfs first candle's body (shadows don't matter). Shows momentum shift: buyers controlled day 1, but sellers overwhelmed them day 2. Best after uptrend at resistance. Larger engulfing candle = stronger signal. High volume on engulfing candle increases reliability. Use for short entries or long exits.",
      .limitations = "Requires clear uptrend context to be valid. Pattern frequency varies by timeframe. Body size matters - small bodies less reliable. Gaps between candles strengthen pattern. False signals increase in choppy markets. Consider engulfing candle size relative to recent range. Best combined with resistance levels, volume confirmation, or other bearish indicators.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Bearish Engulfing",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["engulfing_bull"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "reversal", "engulfing"},
      .desc =
          "Bullish pattern where a large up candle completely engulfs the "
          "previous down candle. Strong signal of trend reversal to upside.",
      .strategyTypes = {"reversal", "price-action", "bullish-reversal", "momentum-shift"},
      .relatedTransforms = {"engulfing_bear", "morning_star", "hammer", "abandoned_baby_bull"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Powerful two-candle bullish reversal pattern. Second candle's body completely engulfs first candle's body (shadows don't matter). Shows momentum shift: sellers controlled day 1, but buyers overwhelmed them day 2. Best after downtrend at support. Larger engulfing candle = stronger signal. High volume on engulfing candle increases reliability. Use for swing trading entries.",
      .limitations = "Requires clear downtrend context to be valid. Pattern frequency varies by timeframe. Body size matters - small bodies less reliable. Gaps between candles strengthen pattern. False signals increase in choppy markets. Consider engulfing candle size relative to recent range. Best combined with support levels, volume confirmation, or other bullish indicators.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Bullish Engulfing",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["evening_doji_star"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "reversal", "star", "doji"},
      .desc = "Bearish reversal pattern with an up candle, followed by a doji "
              "gapped up, then a down candle gapped down. Stronger signal than "
              "Evening Star.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Evening Doji Star",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["evening_star"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "reversal", "star"},
      .desc = "Bearish reversal pattern with an up candle, followed by a small "
              "body candle gapped up, then a down candle gapped down.",
      .strategyTypes = {"reversal", "price-action", "bearish-reversal", "three-candle-pattern"},
      .relatedTransforms = {"evening_doji_star", "morning_star", "engulfing_bear", "shooting_star"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Three-candle bearish reversal pattern. Day 1: bullish continuation. Day 2: small body (star) gaps up showing exhaustion. Day 3: bearish candle closes well into day 1 body. Middle candle can be any color. Gaps strengthen signal but not required. Best at resistance after uptrend. Use for short entries or long exits. Evening Doji Star (middle is doji) is stronger variant.",
      .limitations = "Requires clear uptrend and resistance context. Gaps less common in 24/7 crypto markets. Three-candle patterns slower to develop. Middle candle size subjective. Day 3 candle should close >50% into day 1. False signals without volume confirmation. Best combined with overbought indicators or resistance confluence. Consider waiting for confirmation candle.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Evening Star",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["four_price_doji"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "neutral", "indecision", "doji"},
      .desc = "Special doji where open, high, low, and close are all at the "
              "same price. Extreme indecision in the market.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Four Price Doji",
          .textIsTemplate = false,
          .color = epoch_core::Color::Default,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["gravestone_doji"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "reversal", "doji"},
      .desc = "Doji with no lower shadow but a long upper shadow. Indicates "
              "rejection of higher prices and potential bearish reversal.",
      .strategyTypes = {"reversal", "price-action", "bearish-reversal", "resistance-detection"},
      .relatedTransforms = {"shooting_star", "doji", "dragonfly_doji"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Bearish doji variant with inverted T-shape (no lower wick, long upper wick). Buyers pushed price up but sellers drove it back to open. Open/close at low = strong rejection of highs. More bearish than standard doji. Best after uptrend at resistance. Very similar to shooting star but with perfect doji body. Requires next candle confirmation.",
      .limitations = "Rare pattern - strict doji body requirement. Similar to shooting star, can be misidentified. Must appear after uptrend at resistance. Needs strong confirmation candle. Upper shadow length threshold subjective. False signals in ranging markets. Best with volume spike on upper shadow. Consider shooting star as less restrictive alternative.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Gravestone Doji",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["hammer"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "reversal", "hammer"},
      .desc =
          "Bullish reversal pattern with a small body at the top and a long "
          "lower shadow. Indicates rejection of lower prices in a downtrend.",
      .strategyTypes = {"reversal", "price-action", "support-detection", "bullish-reversal"},
      .relatedTransforms = {"inverted_hammer", "hanging_man", "dragonfly_doji"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Strong bullish reversal signal appearing after downtrends at support levels. Long lower shadow (2-3x body size) shows sellers pushed price down but buyers rejected lower levels. Small body at top (any color, but green stronger) shows closing near high. Requires confirmation (next candle closes higher). Most reliable at key support, with high volume.",
      .limitations = "Must appear in downtrend to be valid - hammer in uptrend is different pattern (hanging man). Needs confirmation from next candle. Lower shadow length threshold subjective. Color matters less than structure, but green preferable. False signals common without support level confluence. Consider trend strength and volume.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Hammer",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["hanging_man"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "reversal", "hanging-man"},
      .desc = "Bearish reversal pattern with a small body at the top and a "
              "long lower shadow, appearing in an uptrend. Warning of a "
              "potential reversal.",
      .strategyTypes = {"reversal", "price-action", "bearish-reversal", "top-detection"},
      .relatedTransforms = {"hammer", "dragonfly_doji", "shooting_star"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Bearish warning signal appearing after uptrends (same structure as hammer but different context). Long lower shadow shows sellers tested lower but buyers defended. However, appears at resistance suggesting exhaustion. Small body at top. Weaker than shooting star - needs strong confirmation. Best when followed by bearish candle closing below hanging man's body.",
      .limitations = "Weakest of reversal patterns - requires strong confirmation. Identical to hammer visually - context determines meaning. Can lead to whipsaws. Many hanging men don't lead to reversals. Best used with other bearish indicators. Red body stronger than green. High volume increases reliability. Consider as warning, not entry signal.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Hanging Man",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["inverted_hammer"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "reversal", "hammer"},
      .desc = "Bullish reversal pattern with a small body at the bottom and a "
              "long upper shadow, appearing after a downtrend.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Inverted Hammer",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["long_legged_doji"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "neutral", "indecision", "doji",
               "volatility"},
      .desc = "Doji with long upper and lower shadows. Indicates significant "
              "volatility and indecision in the market.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Long Legged Doji",
          .textIsTemplate = false,
          .color = epoch_core::Color::Default,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["marubozu"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "neutral", "strong-momentum",
               "marubozu", "no-shadow"},
      .desc = "Candle with no upper or lower shadows. Indicates strong "
              "conviction in the direction of the trend.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Marubozu",
          .textIsTemplate = false,
          .color = epoch_core::Color::Default,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["morning_doji_star"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "reversal", "star", "doji"},
      .desc = "Bullish reversal pattern with a down candle, followed by a doji "
              "gapped down, then an up candle gapped up. Stronger signal than "
              "Morning Star.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Morning Doji Star",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["morning_star"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "reversal", "star"},
      .desc = "Bullish reversal pattern with a down candle, followed by a "
              "small body candle gapped down, then an up candle gapped up.",
      .strategyTypes = {"reversal", "price-action", "bullish-reversal", "three-candle-pattern"},
      .relatedTransforms = {"morning_doji_star", "evening_star", "engulfing_bull", "hammer"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Three-candle bullish reversal pattern. Day 1: bearish continuation. Day 2: small body (star) gaps down showing exhaustion. Day 3: bullish candle closes well into day 1 body. Middle candle can be any color. Gaps strengthen signal but not required. Best at support after downtrend. Use for swing long entries. Morning Doji Star (middle is doji) is stronger variant.",
      .limitations = "Requires clear downtrend and support context. Gaps less common in 24/7 crypto markets. Three-candle patterns slower to develop than single candles. Middle candle size subjective. Day 3 candle should close >50% into day 1. False signals without volume confirmation. Best combined with oversold indicators or support confluence. Consider waiting for confirmation candle.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Morning Star",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["shooting_star"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "reversal",
               "shooting-star"},
      .desc = "Bearish reversal pattern with a small body at the bottom and a "
              "long upper shadow, appearing after an uptrend.",
      .strategyTypes = {"reversal", "price-action", "bearish-reversal", "resistance-detection"},
      .relatedTransforms = {"inverted_hammer", "gravestone_doji", "evening_star"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Bearish reversal signal appearing after uptrends at resistance. Long upper shadow (2-3x body size) shows buyers pushed price up but sellers rejected higher levels. Small body at bottom (any color, but red stronger) shows closing near low. Opposite of hammer. Requires confirmation (next candle closes lower). Most reliable at resistance levels.",
      .limitations = "Must appear in uptrend to be valid - shooting star in downtrend is different pattern (inverted hammer). Needs confirmation from next candle. Upper shadow length threshold subjective. Color matters less than structure, but red preferable. False signals common without resistance confluence. Look-alike to gravestone doji.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Shooting Star",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["spinning_top"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "neutral", "indecision",
               "spinning-top"},
      .desc = "Candle with a small body and longer upper and lower shadows. "
              "Indicates indecision between buyers and sellers.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Spinning Top",
          .textIsTemplate = false,
          .color = epoch_core::Color::Default,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["star"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "neutral", "star", "gap"},
      .desc = "Price gap between the current candle's body and the previous "
              "candle's body. Often a component of more complex patterns.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Star",
          .textIsTemplate = false,
          .color = epoch_core::Color::Default,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  patternMetaData["three_black_crows"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bearish", "reversal", "three-crows"},
      .desc =
          "Bearish reversal pattern with three consecutive black candles with "
          "lower closes. Strong signal of continued downward momentum.",
      .strategyTypes = {"reversal", "price-action", "bearish-reversal", "continuation", "strong-trend"},
      .relatedTransforms = {"three_white_soldiers", "evening_star", "engulfing_bear"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Powerful three-candle bearish reversal. Three consecutive red candles with progressively lower closes. Each candle opens within previous body and closes near low. Shows sustained selling pressure. Best after uptrend at resistance. Signals strong conviction. More reliable than single-candle patterns. Each candle should have small/no upper wicks.",
      .limitations = "Requires clear uptrend context. Three-candle pattern slower to develop - miss early entry. Size of candles matters - small candles less reliable. Can appear mid-downtrend (continuation not reversal). Each candle should close in lower third of range. Opening gaps between candles weaken pattern. Very strong pattern when properly formed, but rare.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Three Black Crows",
          .textIsTemplate = false,
          .color = epoch_core::Color::Error,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["three_white_soldiers"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "reversal",
               "three-soldiers"},
      .desc =
          "Bullish reversal pattern with three consecutive white candles with "
          "higher closes. Strong signal of continued upward momentum.",
      .strategyTypes = {"reversal", "price-action", "bullish-reversal", "continuation", "strong-trend"},
      .relatedTransforms = {"three_black_crows", "morning_star", "engulfing_bull"},
      .assetRequirements = {"single-asset"},
      .usageContext = "Powerful three-candle bullish reversal. Three consecutive green candles with progressively higher closes. Each candle opens within previous body and closes near high. Shows sustained buying pressure. Best after downtrend at support. Signals strong conviction. More reliable than single-candle patterns. Each candle should have small/no lower wicks.",
      .limitations = "Requires clear downtrend context. Three-candle pattern slower to develop. Size of candles matters - small candles less reliable. Can appear mid-uptrend (continuation not reversal). Each candle should close in upper third of range. Opening gaps between candles weaken pattern. Watch for exhaustion after three soldiers (potential top). Very strong when proper but rare.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "Three White Soldiers",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};


  patternMetaData["white_marubozu"] = CandlePatternMetaData{
      .tags = {"candlestick", "pattern", "bullish", "marubozu", "no-shadow"},
      .desc = "Bullish candle with no upper or lower shadows (wicks). Strong "
              "buying pressure with opening at low and closing at high.",
      .flagSchema = FlagSchema{
          .icon = epoch_core::Icon::CandlestickChart,
          .text = "White Marubozu",
          .textIsTemplate = false,
          .color = epoch_core::Color::Success,
          .title = std::nullopt,
          .valueKey = "result"
      }};

  return patternMetaData;
}

std::vector<MetaDataOption> MakeCandleOptions() {
  // Derive defaults from tc_config_default()
  auto defaults = tc_config_default();

  std::vector<MetaDataOption> options;

  // period is integer
  {
    MetaDataOption o{.id = "period",
                     .name = "Period",
                     .type = epoch_core::MetaDataOptionType::Integer,
                     .defaultValue =
                         epoch_script::MetaDataOptionDefinition{
                             static_cast<double>(defaults->period)},
                     .isRequired = true,
                     .min = 1, // Period must be at least 1
                     .max = 1000};
    options.push_back(o);
  }

  // body_none
  {
    MetaDataOption o{.id = "body_none",
                     .name = "Body None Threshold",
                     .type = epoch_core::MetaDataOptionType::Decimal,
                     .defaultValue =
                         epoch_script::MetaDataOptionDefinition{
                             static_cast<double>(defaults->body_none)},
                     .isRequired = true};
    options.push_back(o);
  }

  // body_short
  {
    MetaDataOption o{.id = "body_short",
                     .name = "Body Short Threshold",
                     .type = epoch_core::MetaDataOptionType::Decimal,
                     .defaultValue =
                         epoch_script::MetaDataOptionDefinition{
                             static_cast<double>(defaults->body_short)},
                     .isRequired = true};
    options.push_back(o);
  }

  // body_long
  {
    MetaDataOption o{.id = "body_long",
                     .name = "Body Long Threshold",
                     .type = epoch_core::MetaDataOptionType::Decimal,
                     .defaultValue =
                         epoch_script::MetaDataOptionDefinition{
                             static_cast<double>(defaults->body_long)},
                     .isRequired = true};
    options.push_back(o);
  }

  // wick_none
  {
    MetaDataOption o{.id = "wick_none",
                     .name = "Wick None Threshold",
                     .type = epoch_core::MetaDataOptionType::Decimal,
                     .defaultValue =
                         epoch_script::MetaDataOptionDefinition{
                             static_cast<double>(defaults->wick_none)},
                     .isRequired = true};
    options.push_back(o);
  }

  // wick_long
  {
    MetaDataOption o{.id = "wick_long",
                     .name = "Wick Long Threshold",
                     .type = epoch_core::MetaDataOptionType::Decimal,
                     .defaultValue =
                         epoch_script::MetaDataOptionDefinition{
                             static_cast<double>(defaults->wick_long)},
                     .isRequired = true};
    options.push_back(o);
  }

  // near
  {
    MetaDataOption o{.id = "near",
                     .name = "Near Threshold",
                     .type = epoch_core::MetaDataOptionType::Decimal,
                     .defaultValue =
                         epoch_script::MetaDataOptionDefinition{
                             static_cast<double>(defaults->near)},
                     .isRequired = true};
    options.push_back(o);
  }

  return options;
}

std::vector<TransformsMetaData> MakeTulipCandles() {
  // Create metadata for each candlestick pattern
  auto patternMetaData = MakeCandlePatternMetaData();

  // Iterate over tc_candles until we hit a null element.
  std::vector<TransformsMetaData> allCandles(TC_CANDLE_COUNT);

  for (size_t i = 0; i < TC_CANDLE_COUNT; ++i) {
    const auto &c = tc_candles[i];

    // Look up metadata for this pattern
    auto metadata = epoch_core::lookupDefault(patternMetaData, c.name,
                                              CandlePatternMetaData{});

    allCandles[i] = TransformsMetaData{
        .id = c.name,
        .category = epoch_core::TransformCategory::PriceAction,
        .plotKind = epoch_core::TransformPlotKind::flag,
        .name = c.full_name,
        .options = MakeCandleOptions(),
        .isCrossSectional = false,
        .desc = metadata.desc,
        .inputs = {},
        .outputs = {IOMetaDataConstants::BOOLEAN_OUTPUT_METADATA},
        .tags = metadata.tags,
        .requiresTimeFrame = true,
        .requiredDataSources = {"c", "o", "h", "l"},
        .flagSchema = metadata.flagSchema,
        .strategyTypes = metadata.strategyTypes,
        .relatedTransforms = metadata.relatedTransforms,
        .assetRequirements = metadata.assetRequirements,
        .usageContext = metadata.usageContext,
        .limitations = metadata.limitations};
  }

  return allCandles;
}

} // namespace epoch_script::transforms