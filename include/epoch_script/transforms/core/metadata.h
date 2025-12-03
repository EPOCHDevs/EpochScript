#pragma once
#include "../../core/constants.h"
#include "../../core/metadata_options.h"
#include <glaze/glaze.hpp>
#include <string>
#include <vector>

// Semantic search / palette bucket
CREATE_ENUM(TransformCategory,
            Aggregate,   // aggregate nodes
            ControlFlow, // control flow nodes
            Scalar,      // constants, booleans, editable numbers
            DataSource,  // OHLCV & fundamental feeds
            Math,        // element-wise math & stat functions
            Trend,       // moving-average style trend tools
            Momentum,    // RSI, MACD, Stoch, etc.
            Volatility,  // ATR, Parkinson, Yang-Zhang …
            Volume,      // OBV, VWAP, volume indexes
            PriceAction, // candlestick & chart patterns
            Statistical, // z-score, regression, percentiles
            Factor,      // cross-sectional ranks & spreads
            Utility,     // switches, event_markers, helpers
            Reporter,    // report / visualization sink nodes
            Executor,    // trade / order sink nodes
            EventMarker, // interactive UI event_markers
            ML,
            Portfolio);  // portfolio optimization (HRP, MVO, Risk Parity, etc.)

// Chart helper (omit / null ⇒ not plotted)
CREATE_ENUM(
    TransformPlotKind,
    ao,                 // Awesome Oscillator
    aroon,              // Aroon indicator
    bbands,             // Bollinger Bands helper
    bb_percent_b,       // Bollinger Bands %B
    column,             // column plot
    cci,                // Commodity Channel Index
    chande_kroll_stop,  // Chande Kroll Stop
    elders,             // Elder Ray Index
    fisher,             // Fisher Transform,
    fosc,               // Forcast Oscillator
    h_line,             // horizontal line
    ichimoku,           // Ichimoku Cloud
    line,               // generic overlay
    close_line,         // OHLCV close overlay from bar data sources
    gap,                // Gap indicator
    zone,               // Background/time-range highlight
    panel_line,         // generic overlay, but not on top of the main plot,
    panel_line_percent, // generic overlay, but not on top of the main plot,
    qstick,             // Qstick indicator
    qqe,                // QQE indicator
    order_blocks,       // Order Blocks
    flag,               // flag helper
    macd,               // MACD (histogram + signal)
    retracements,       // Retracement lines
    sessions,           // Sessions
    rsi,                // RSI panel
    psar,               // Parabolic-SAR dots
    atr,                // Average True Range
    shl,                // Swing Highs and Lows
    bos_choch,          // Break of Structure and Change of Character
    fvg,                // Fair Value Gap
    liquidity,          // Liquidity
    stoch,              // Stochastic oscillator
    previous_high_low,  // Previous High and Low
    pivot_point_sr,     // Pivot Point Support/Resistance
    pivot_point_detector, // Pivot Point Detector infrastructure
    vwap,               // VWAP overlay
    vortex,             // Vortex Indicator
    trade_signal,       // Trade Signal Executor
    // Pattern formation and statistical regimes
    head_and_shoulders,
    inverse_head_and_shoulders,
    double_top_bottom,
    pennant_pattern,
    flag_pattern,
    triangle_patterns,
    consolidation_box,
    hmm,                // Hidden Markov Models
    gmm,                // Gaussian Mixture Models
    kmeans,             // K-Means Clustering
    dbscan,             // DBSCAN Clustering
    pca,                // Principal Component Analysis
    ica,                // Independent Component Analysis
    lightgbm,           // LightGBM gradient boosting
    linear_model,       // LIBLINEAR models (logistic, SVR)
    sentiment);         // Sentiment Analysis (ML/NLP)

CREATE_ENUM(IODataType, Decimal, Integer, Number, Boolean, String, Timestamp, Any);

// Note: CardRenderType, CardSlot, Color enums are defined in constants.h
// Note: Use TransformCategory to distinguish between transform types:
//   - Regular transforms: Aggregate, Math, Trend, Momentum, etc.
//   - Reports: category = Executor
//   - EventMarkers: category = EventMarker

namespace epoch_script::transforms {
constexpr auto MARKET_DATA_SOURCE_ID = "market_data_source";
constexpr auto TRADE_SIGNAL_EXECUTOR_ID = "trade_signal_executor";
constexpr auto ASSET_REF_ID = "asset_ref";
struct IOMetaData {
  epoch_core::IODataType type{epoch_core::IODataType::Decimal};
  std::string id{};
  std::string name{};
  bool allowMultipleConnections{false};
  bool isFilter{false};

  void decode(YAML::Node const &);
  YAML::Node encode() const { return {}; }
};

struct TransformCategoryMetaData {
  epoch_core::TransformCategory category;
  std::string name;
  std::string desc;
};
std::vector<TransformCategoryMetaData> MakeTransformCategoryMetaData();

/**
 * @brief Display configuration for flag PlotKind
 * Defines how a flag transform should be rendered (icon, text, color)
 */
struct FlagSchema {
  epoch_core::Icon icon;                          // Icon to display (type-safe enum → Lucide)
  std::string text;                               // Plain text or template with {column_name}
  bool textIsTemplate{false};                     // true = substitute {column} placeholders
  epoch_core::Color color;                        // Semantic color (UI decides shade based on brand)
  std::optional<std::string> title{std::nullopt}; // Optional popup/tooltip title
  std::string valueKey;                           // Output column ID for flag positioning (e.g., "result", "cash_amount")
};

struct TransformsMetaData {
  std::string id;
  epoch_core::TransformCategory category;
  epoch_core::TransformPlotKind plotKind{epoch_core::TransformPlotKind::Null};
  std::string name{};
  MetaDataOptionList options{};
  bool isCrossSectional{false};
  std::string desc{};
  std::vector<IOMetaData> inputs{};
  std::vector<IOMetaData> outputs{};
  bool atLeastOneInputRequired{false};
  std::vector<std::string> tags{};
  bool requiresTimeFrame{false};
  std::vector<std::string> requiredDataSources{};
  bool intradayOnly{false};
  bool allowNullInputs{false};
  bool internalUse{false};  // Compiler-inserted transforms not for direct user use (e.g., static_cast, alias)
  std::string alias{};  // Group name for related transforms (e.g., "static_cast" for static_cast_to_*, "alias" for alias_*)

  // Display configuration for flag PlotKind
  std::optional<FlagSchema> flagSchema{std::nullopt};

  // Enhanced metadata for RAG/LLM strategy construction
  std::vector<std::string> strategyTypes{};  // e.g., "mean-reversion", "breakout", "trend-following"
  std::vector<std::string> relatedTransforms{};  // Cross-references to complementary transforms
  std::vector<std::string> assetRequirements{};  // e.g., "single-asset", "multi-asset-required"
  std::string usageContext{};  // When/why to use this transform
  std::string limitations{};  // Important caveats or constraints

  void decode(YAML::Node const &);
  YAML::Node encode() const { return {}; }
};

using TransformsMetaDataCreator =
    std::function<TransformsMetaData(std::string const &name)>;

struct IOMetaDataConstants {
  // TODO: Move bar attributes to shared headers
  inline static IOMetaData CLOSE_PRICE_METADATA{epoch_core::IODataType::Decimal,
                                                "c", "Close Price", true};
  inline static IOMetaData OPEN_PRICE_METADATA{epoch_core::IODataType::Decimal,
                                               "o", "Open Price", true};
  inline static IOMetaData HIGH_PRICE_METADATA{epoch_core::IODataType::Decimal,
                                               "h", "High Price", true};
  inline static IOMetaData LOW_PRICE_METADATA{epoch_core::IODataType::Decimal,
                                              "l", "Low Price", true};
  inline static IOMetaData VOLUME_METADATA{epoch_core::IODataType::Decimal, "v",
                                           "Volume", true};
  inline static IOMetaData CONTRACT_METADATA{epoch_core::IODataType::String,
                                             "s", "Contract", true};

  inline static IOMetaData ANY_INPUT_METADATA{epoch_core::IODataType::Any, ARG,
                                              "", false};

  inline static IOMetaData ANY_INPUT0_METADATA{epoch_core::IODataType::Any,
                                               ARG0, "", false};

  inline static IOMetaData ANY_INPUT1_METADATA{epoch_core::IODataType::Any,
                                               ARG1, "", false};

  inline static IOMetaData ANY_INPUT2_METADATA{epoch_core::IODataType::Any,
                                               ARG2, "", false};

  inline static IOMetaData DECIMAL_INPUT_METADATA{
      epoch_core::IODataType::Decimal, ARG, "", false};

  inline static IOMetaData DECIMAL_INPUT0_METADATA{
      epoch_core::IODataType::Decimal, ARG0, "", false};

  inline static IOMetaData DECIMAL_INPUT1_METADATA{
      epoch_core::IODataType::Decimal, ARG1, "", false};

  inline static IOMetaData DECIMAL_INPUT2_METADATA{
      epoch_core::IODataType::Decimal, ARG2, "", false};

  inline static IOMetaData NUMBER_INPUT_METADATA{epoch_core::IODataType::Number,
                                                 ARG, "", false};

  inline static IOMetaData NUMBER_INPUT0_METADATA{
      epoch_core::IODataType::Number, ARG0, "", false};

  inline static IOMetaData NUMBER_INPUT1_METADATA{
      epoch_core::IODataType::Number, ARG1, "", false};

  inline static IOMetaData NUMBER_INPUT2_METADATA{
      epoch_core::IODataType::Number, ARG2, "", false};

  inline static IOMetaData ANY_OUTPUT_METADATA{epoch_core::IODataType::Any,
                                               "result", "", true};

  inline static IOMetaData BOOLEAN_INPUT_METADATA{
      epoch_core::IODataType::Boolean, ARG, "", false};

  inline static IOMetaData BOOLEAN_INPUT0_METADATA{
      epoch_core::IODataType::Boolean, ARG0, "", false};

  inline static IOMetaData BOOLEAN_INPUT1_METADATA{
      epoch_core::IODataType::Boolean, ARG1, "", false};

  inline static IOMetaData BOOLEAN_INPUT2_METADATA{
      epoch_core::IODataType::Boolean, ARG2, "", false};

  inline static IOMetaData STRING_INPUT_METADATA{
      epoch_core::IODataType::String, ARG, "", false};

  inline static IOMetaData STRING_INPUT0_METADATA{
      epoch_core::IODataType::String, ARG0, "", false};

  inline static IOMetaData DECIMAL_OUTPUT_METADATA{
      epoch_core::IODataType::Decimal, "result", "", true};

  inline static IOMetaData STRING_OUTPUT_METADATA{
      epoch_core::IODataType::String, "result", "", true};

  inline static IOMetaData BOOLEAN_OUTPUT_METADATA{
      epoch_core::IODataType::Boolean, "result", "", true};

  inline static IOMetaData NUMBER_OUTPUT_METADATA{
      epoch_core::IODataType::Number, "result", "", true};

  inline static IOMetaData INTEGER_OUTPUT_METADATA{
      epoch_core::IODataType::Integer, "result", "", true};

  inline static std::unordered_map<std::string, IOMetaData> MAP{
      {"CLOSE", CLOSE_PRICE_METADATA},
      {"OPEN", OPEN_PRICE_METADATA},
      {"HIGH", HIGH_PRICE_METADATA},
      {"LOW", LOW_PRICE_METADATA},
      {"VOLUME", VOLUME_METADATA},
      {"CONTRACT", CONTRACT_METADATA},
      {"DECIMAL", DECIMAL_INPUT_METADATA},
      {"NUMBER", NUMBER_INPUT_METADATA},
      {"ANY", ANY_INPUT_METADATA},
      {"DECIMAL_RESULT", DECIMAL_OUTPUT_METADATA},
      {"INTEGER_RESULT", INTEGER_OUTPUT_METADATA},
      {"NUMBER_RESULT", NUMBER_OUTPUT_METADATA},
      {"ANY_RESULT", ANY_OUTPUT_METADATA},
      {"BOOLEAN", BOOLEAN_INPUT_METADATA},
      {"BOOLEAN_RESULT", BOOLEAN_OUTPUT_METADATA}};
};

// Shared constants for MetaDataOption select options
struct MetaDataOptionConstants {
  // SessionType select options matching epoch_core::SessionType enum
  inline static std::vector<epoch_script::SelectOption> SESSION_TYPE_OPTIONS = {
      {"Sydney (08:00-17:00 AEDT/AEST)", "Sydney"},
      {"Tokyo (09:00-18:00 JST)", "Tokyo"},
      {"London (08:00-17:00 GMT/BST)", "London"},
      {"New York (09:30-16:00 ET)", "NewYork"},
      {"Asian Kill Zone (19:00-23:00 ET)", "AsianKillZone"},
      {"London Open Kill Zone (02:00-05:00 ET)", "LondonOpenKillZone"},
      {"New York Kill Zone (07:00-10:00 ET)", "NewYorkKillZone"},
      {"London Close Kill Zone (10:00-12:00 ET)", "LondonCloseKillZone"}
  };
};

std::vector<TransformsMetaData> MakeComparativeMetaData();
std::vector<TransformsMetaData> MakeScalarMetaData();
std::vector<TransformsMetaData> MakeDataSource();
std::vector<TransformsMetaData> MakeTradeSignalExecutor();
std::vector<TransformsMetaData> MakeTulipIndicators();
std::vector<TransformsMetaData> MakeTulipCandles();
std::vector<TransformsMetaData> MakeLagMetaData();
std::vector<TransformsMetaData> MakeChartFormationMetaData();
std::vector<TransformsMetaData> MakeCalendarEffectMetaData();
std::vector<TransformsMetaData> MakeStringTransformMetaData();
} // namespace epoch_script::transforms

// Forward declarations for data source metadata factories
// Actual implementations are in header-only files under src/transforms/src/data_sources/
namespace epoch_script::transform {
std::vector<epoch_script::transforms::TransformsMetaData> MakePolygonDataSources();
std::vector<epoch_script::transforms::TransformsMetaData> MakePolygonIndicesDataSources();
std::vector<epoch_script::transforms::TransformsMetaData> MakeFREDDataSource();
std::vector<epoch_script::transforms::TransformsMetaData> MakeSECDataSources();
std::vector<epoch_script::transforms::TransformsMetaData> MakeReferenceStocksDataSources();
} // namespace epoch_script::transform

namespace YAML {
template <> struct convert<epoch_script::transforms::IOMetaData> {
  static bool decode(const Node &node,
                     epoch_script::transforms::IOMetaData &t) {
    t.decode(node);
    return true;
  }
};

template <> struct convert<epoch_script::transforms::TransformsMetaData> {
  static bool decode(const Node &node,
                     epoch_script::transforms::TransformsMetaData &t) {
    t.decode(node);
    return true;
  }
};
} // namespace YAML
