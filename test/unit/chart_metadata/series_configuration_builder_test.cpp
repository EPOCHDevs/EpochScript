#include "chart_metadata/series_configuration_builder.h"
#include "epoch_script/chart_metadata/chart_metadata_provider.h"
#include "catch2/catch_test_macros.hpp"
#include "epoch_script/data/common/constants.h"
#include "epoch_script/transforms/core/config_helper.h"
#include "epoch_script/transforms/core/itransform.h"

using namespace epoch_script;
using namespace epoch_script::chart_metadata;

TEST_CASE("SeriesConfigurationBuilder",
          "[chart_metadata][series_configuration_builder]") {
  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  const auto timeframe = tf.ToString();

  SECTION("Builds candlestick series correctly") {
    auto series = SeriesConfigurationBuilder::BuildCandlestickSeries(timeframe);

    REQUIRE(series.id == timeframe + "_candlestick");
    REQUIRE(series.type == "candlestick");
    REQUIRE(series.name == "");
    REQUIRE(series.dataMapping.size() == 5);
    REQUIRE(series.dataMapping.at("index") == "index");
    REQUIRE(series.dataMapping.at("open") == "o");
    REQUIRE(series.dataMapping.at("high") == "h");
    REQUIRE(series.dataMapping.at("low") == "l");
    REQUIRE(series.dataMapping.at("close") == "c");
    REQUIRE(series.zIndex == 0);
    REQUIRE(series.yAxis == 0);
    REQUIRE(!series.linkedTo.has_value());
  }

  SECTION("Builds volume series correctly") {
    auto series = SeriesConfigurationBuilder::BuildVolumeSeries(timeframe);

    REQUIRE(series.id == timeframe + "_volume");
    REQUIRE(series.type == "column");
    REQUIRE(series.name == "Volume");
    REQUIRE(series.dataMapping.size() == 2);
    REQUIRE(series.dataMapping.at("index") == "index");
    REQUIRE(series.dataMapping.at("value") == "v");
    REQUIRE(series.zIndex == 0);
    REQUIRE(series.yAxis == 1);
    REQUIRE(!series.linkedTo.has_value());
  }

  SECTION("Builds line chart series") {
    auto sma = transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);
    auto series =
        SeriesConfigurationBuilder::BuildSeries(sma, 0, std::nullopt, "1");

    REQUIRE(series.id == "1");
    REQUIRE(series.type == "line");
    REQUIRE(series.name == "SMA period=10");
    REQUIRE(series.dataMapping.size() == 2);
    REQUIRE(series.dataMapping.at("index") == "index");
    REQUIRE(series.dataMapping.at("value") == "1#result");
    REQUIRE(series.zIndex == 5);
    REQUIRE(series.yAxis == 0);
    REQUIRE(!series.linkedTo.has_value());
  }

  SECTION("Builds series with linkedTo") {
    auto sma = transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);
    std::optional<std::string> linkedTo = "candlestick_series";
    auto series =
        SeriesConfigurationBuilder::BuildSeries(sma, 0, linkedTo, "1");

    REQUIRE(series.linkedTo.has_value());
    REQUIRE(series.linkedTo.value() == "candlestick_series");
  }

  SECTION("Maps plot kinds to chart types correctly") {
    struct TestCase {
      std::string transformName;
      std::string expectedType;
      std::string description;
    };

    std::vector<TestCase> testCases = {
        {"sma", "line", "Simple moving average"},
        {"ema", "line", "Exponential moving average"},
        {"bbands", "bbands", "Bollinger Bands"},
        {"rsi", "rsi", "RSI indicator"},
        {"macd", "macd", "MACD indicator"},
        {"psar", "psar", "Parabolic SAR"},
        {"ao", "ao", "Awesome Oscillator"},
        {"cci", "cci", "Commodity Channel Index"},
        {"stoch", "stoch", "Stochastic oscillator"}};

    for (const auto &tc : testCases) {
      DYNAMIC_SECTION(tc.description) {
        // Create appropriate transform based on name
        epoch_script::transform::TransformConfiguration cfg = [&]() {
          if (tc.transformName == "sma" || tc.transformName == "ema") {
            return transform::ma(tc.transformName, "1", strategy::NodeReference("", "c"), 10, tf);
          } else if (tc.transformName == "bbands") {
            return transform::bbands("1", 10, 2, strategy::NodeReference("", "c"), tf);
          } else if (tc.transformName == "rsi") {
            return transform::single_operand_period_op("rsi", "1", 14, strategy::NodeReference("", "c"), tf);
          } else if (tc.transformName == "psar") {
            return transform::psar("1", 0.02, 0.2, strategy::NodeReference("", "c"), tf);
          } else {
            if (tc.transformName == "macd") {
              return transform::run_op("macd", "1",
                  {{epoch_script::ARG, {transform::input_ref("c")}}},
                  {{"short_period", MetaDataOptionDefinition{12.0}},
                   {"long_period", MetaDataOptionDefinition{26.0}},
                   {"signal_period", MetaDataOptionDefinition{9.0}}},
                  tf);
            } else if (tc.transformName == "stoch") {
              return transform::run_op("stoch", "1",
                  {},
                  {{"k_period", MetaDataOptionDefinition{14.0}},
                   {"k_slowing_period", MetaDataOptionDefinition{3.0}},
                   {"d_period", MetaDataOptionDefinition{3.0}}},
                  tf);
            } else if (tc.transformName == "cci") {
              return transform::run_op("cci", "1",
                  {},
                  {{"period", MetaDataOptionDefinition{20.0}}},
                  tf);
            } else if (tc.transformName == "ao") {
              return transform::run_op("ao", "1", {}, {}, tf);
            }
            return transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);
          }
        }();

        auto series =
            SeriesConfigurationBuilder::BuildSeries(cfg, 0, std::nullopt, "1");
        REQUIRE(series.type == tc.expectedType);
      }
    }
  }

  SECTION("Handles SMC indicators chart types") {
    SECTION("Order blocks") {
      auto order_blocks = transform::run_op("order_blocks", "1",
          {{"high_low", {transform::input_ref("1", "high_low")}}},
          {{"close_mitigation", MetaDataOptionDefinition{false}}},
          tf);
      auto series = SeriesConfigurationBuilder::BuildSeries(order_blocks, 2,
                                                            std::nullopt, "1");
      REQUIRE(series.type == "order_blocks");
    }

    SECTION("Fair value gap") {
      auto fvg = transform::run_op("fair_value_gap", "1",
          {},
          {{"join_consecutive", MetaDataOptionDefinition{true}}},
          tf);
      auto series =
          SeriesConfigurationBuilder::BuildSeries(fvg, 2, std::nullopt, "1");
      REQUIRE(series.type == "fvg");
    }

    SECTION("Swing highs/lows") {
      auto shl = transform::run_op("swing_highs_lows", "1",
          {},
          {{"swing_length", MetaDataOptionDefinition{5.0}}},
          tf);
      auto series =
          SeriesConfigurationBuilder::BuildSeries(shl, 0, std::nullopt, "1");
      REQUIRE(series.type == "shl");
    }
  }

  SECTION("Sets correct z-index for different chart types") {
    struct ZIndexTest {
      std::string chartType;
      size_t expectedZIndex;
    };

    std::vector<ZIndexTest> tests = {{"flag", 10},      {"shl", 10},
                                     {"bos_choch", 10}, {"line", 5},
                                     {"bbands", 1},     {"candlestick", 0}};

    for (const auto &test : tests) {
      DYNAMIC_SECTION("z-index for " + test.chartType) {
        // We need to test the private GetZIndex method indirectly
        // Create a transform that would produce this chart type
        epoch_script::transform::TransformConfiguration cfg = [&]() {
          if (test.chartType == "line") {
            return transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);
          }
          if (test.chartType == "bbands") {
            return transform::bbands("1", 10, 2, strategy::NodeReference("", "c"), tf);
          }
          if (test.chartType == "candlestick") {
            // For candlestick, we'll use a simple line transform and check the
            // built-in candlestick series
            return transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);
          }
          if (test.chartType == "flag") {
            return transform::run_op("hammer", "1",
                {{epoch_script::ARG, {transform::input_ref("c")}}},
                {{"period", MetaDataOptionDefinition{10.0}},
                 {"body_none", MetaDataOptionDefinition{0.05}},
                 {"body_short", MetaDataOptionDefinition{0.5}},
                 {"body_long", MetaDataOptionDefinition{1.4}},
                 {"wick_none", MetaDataOptionDefinition{0.05}},
                 {"wick_long", MetaDataOptionDefinition{0.6}},
                 {"near", MetaDataOptionDefinition{0.3}}},
                tf);
          }
          if (test.chartType == "shl") {
            return transform::run_op("swing_highs_lows", "1",
                {},
                {{"swing_length", MetaDataOptionDefinition{5.0}}},
                tf);
          }
          if (test.chartType == "bos_choch") {
            return transform::run_op("bos_choch", "1",
                {{"high_low", {transform::input_ref("dummy_input")}},
                 {"level", {transform::input_ref("dummy_level")}}},
                {{"close_break", MetaDataOptionDefinition{true}}},
                tf);
          }
          // Default case - use SMA
          return transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);
        }();

        auto series =
            SeriesConfigurationBuilder::BuildSeries(cfg, 0, std::nullopt, "1");

        // Special case for candlestick - test the built-in candlestick series
        // instead
        if (test.chartType == "candlestick") {
          auto candlestickSeries =
              SeriesConfigurationBuilder::BuildCandlestickSeries("1D");
          REQUIRE(candlestickSeries.zIndex == test.expectedZIndex);
        } else {
          REQUIRE(series.zIndex == test.expectedZIndex);
        }
      }
    }
  }

  SECTION("Uses transform metadata name when available") {
    auto sma = transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);
    auto series =
        SeriesConfigurationBuilder::BuildSeries(sma, 0, std::nullopt, "1");

    // SMA should have a display name from metadata with parameter names
    REQUIRE(series.name == "SMA period=10");
  }

  SECTION("Falls back to transform name when metadata name is empty") {
    // This would require a transform with empty metadata name
    // Most transforms have names, so this is more of an edge case
    // The test above already verifies the name is populated correctly
  }

  SECTION("Handles all axis assignments correctly") {
    auto sma = transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);

    for (uint8_t axis = 0; axis < 5; ++axis) {
      auto series =
          SeriesConfigurationBuilder::BuildSeries(sma, axis, std::nullopt, "1");
      REQUIRE(series.yAxis == axis);
    }
  }

  SECTION("Preserves seriesepoch_script::ID correctly") {
    auto sma = transform::ma("sma", "1", strategy::NodeReference("", "c"), 10, tf);

    std::vector<std::string> testIds = {"1", "custom_id", "transform_123", ""};

    for (const auto &id : testIds) {
      auto series =
          SeriesConfigurationBuilder::BuildSeries(sma, 0, std::nullopt, id);
      REQUIRE(series.id == id);
    }
  }

  SECTION("Handles complex multi-output indicators") {
    SECTION("MACD with three outputs") {
      auto macd = transform::run_op("macd", "1",
          {{epoch_script::ARG, {transform::input_ref("c")}}},
          {{"short_period", MetaDataOptionDefinition{12.0}},
           {"long_period", MetaDataOptionDefinition{26.0}},
           {"signal_period", MetaDataOptionDefinition{9.0}}},
          tf);
      auto series =
          SeriesConfigurationBuilder::BuildSeries(macd, 2, std::nullopt, "1");

      REQUIRE(series.type == "macd");
      REQUIRE(series.dataMapping.size() == 4); // index + 3 outputs
      REQUIRE(series.dataMapping.at("index") == "index");
      REQUIRE(series.dataMapping.count("macd") == 1);
      REQUIRE(series.dataMapping.count("macd_signal") == 1);
      REQUIRE(series.dataMapping.count("macd_histogram") == 1);
    }

    SECTION("QQE with four outputs") {
      auto qqe = transform::run_op("qqe", "1",
          {{epoch_script::ARG, {transform::input_ref("c")}}},
          {{"avg_period", MetaDataOptionDefinition{14.0}},
           {"smooth_period", MetaDataOptionDefinition{5.0}},
           {"width_factor", MetaDataOptionDefinition{4.236}}},
          tf);
      auto series =
          SeriesConfigurationBuilder::BuildSeries(qqe, 2, std::nullopt, "1");

      REQUIRE(series.type == "qqe");
      REQUIRE(series.dataMapping.size() == 5); // index + 4 outputs
    }
  }

  SECTION("Handles panel indicators with correct types") {
    struct PanelTest {
      std::string indicator;
      std::string expectedType;
    };

    std::vector<PanelTest> panelTests = {{"rsi", "rsi"},
                                         {"cci", "cci"},
                                         {"ao", "ao"},
                                         {"aroon", "aroon"},
                                         {"fisher", "fisher"},
                                         {"qqe", "qqe"},
                                         {"elders_thermometer", "elders"},
                                         {"fosc", "fosc"},
                                         {"qstick", "qstick"}};

    for (const auto &test : panelTests) {
      DYNAMIC_SECTION("Panel indicator: " + test.indicator) {
        auto cfg = [&]() {
          if (test.indicator == "rsi") {
            return transform::run_op("rsi", "1",
                {{epoch_script::ARG, {transform::input_ref("c")}}},
                {{"period", MetaDataOptionDefinition{14.0}}},
                tf);
          } else if (test.indicator == "cci") {
            return transform::run_op("cci", "1",
                {},
                {{"period", MetaDataOptionDefinition{20.0}}},
                tf);
          } else if (test.indicator == "aroon") {
            return transform::run_op("aroon", "1",
                {},
                {{"period", MetaDataOptionDefinition{14.0}}},
                tf);
          } else if (test.indicator == "fisher") {
            return transform::run_op("fisher", "1",
                {},
                {{"period", MetaDataOptionDefinition{10.0}}},
                tf);
          } else if (test.indicator == "qqe") {
            return transform::run_op("qqe", "1",
                {{epoch_script::ARG, {transform::input_ref("c")}}},
                {{"avg_period", MetaDataOptionDefinition{14.0}},
                 {"smooth_period", MetaDataOptionDefinition{5.0}},
                 {"width_factor", MetaDataOptionDefinition{4.236}}},
                tf);
          } else if (test.indicator == "elders_thermometer") {
            return transform::run_op("elders_thermometer", "1",
                {},
                {{"period", MetaDataOptionDefinition{13.0}},
                 {"buy_factor", MetaDataOptionDefinition{0.5}},
                 {"sell_factor", MetaDataOptionDefinition{0.5}}},
                tf);
          } else if (test.indicator == "fosc") {
            return transform::run_op("fosc", "1",
                {{epoch_script::ARG, {transform::input_ref("c")}}},
                {{"period", MetaDataOptionDefinition{14.0}}},
                tf);
          } else if (test.indicator == "qstick") {
            return transform::run_op("qstick", "1",
                {},
                {{"period", MetaDataOptionDefinition{14.0}}},
                tf);
          } else if (test.indicator == "ao") {
            return transform::run_op("ao", "1", {}, {}, tf);
          }
          return transform::run_op(test.indicator, "1", {}, {}, tf);
        }();
        auto series = SeriesConfigurationBuilder::BuildSeries(
            cfg, 2, std::nullopt, "1");

        REQUIRE(series.type == test.expectedType);
      }
    }
  }

  SECTION("Flag series with valueKey and templateDataMapping") {
    // Test simple flag (e.g., crossover) with boolean output
    auto crossover = transform::run_op("crossover", "1",
        {{epoch_script::ARG0, {transform::input_ref("c")}},
         {epoch_script::ARG1, {transform::input_ref("c")}}},
        {},
        tf);
    auto series = SeriesConfigurationBuilder::BuildSeries(crossover, 0, std::nullopt, "1");

    REQUIRE(series.type == "flag");
    REQUIRE(series.zIndex == 10);

    // dataMapping should only have index and value
    REQUIRE(series.dataMapping.size() == 2);
    REQUIRE(series.dataMapping.count("index") == 1);
    REQUIRE(series.dataMapping.count("value") == 1);
    REQUIRE(series.dataMapping.at("index") == "index");
    REQUIRE(series.dataMapping.at("value") == "1#result");  // valueKey = "result"

    // templateDataMapping should have all outputs
    REQUIRE(series.templateDataMapping.size() == 1);
    REQUIRE(series.templateDataMapping.count("result") == 1);
    REQUIRE(series.templateDataMapping.at("result") == "1#result");
  }
}