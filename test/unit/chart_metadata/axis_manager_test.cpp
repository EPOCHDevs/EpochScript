#include "chart_metadata/axis_manager.h"
#include "catch2/catch_test_macros.hpp"
#include "epoch_script/data/common/constants.h"
#include "epoch_script/transforms/core/config_helper.h"
#include "epoch_script/transforms/core/itransform.h"

using namespace epoch_script;
using namespace epoch_script::chart_metadata;

TEST_CASE("AxisManager", "[chart_metadata][axis_manager]") {
  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  const auto timeframe = tf.ToString();
  const auto &C = epoch_script::EpochStratifyXConstants::instance();

  // Price-related inputs
  const std::unordered_set<std::string> priceKeys{C.OPEN(), C.HIGH(), C.LOW(),
                                                  C.CLOSE(), C.CONTRACT()};
  const std::string volumeKey = C.VOLUME();

  SECTION("Initializes with default axes") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // First assignment should create default axes
    auto sma = transform::ma("sma", "1", strategy::InputValue(strategy::NodeReference("", "c")), 10, tf);
    auto [axis, linkedTo] =
        manager.AssignAxis(sma, timeframe, priceKeys, volumeKey, outputHandles);

    auto axes = manager.GetAxes(timeframe);
    REQUIRE(axes.size() == 2);
    REQUIRE(axes[0].index == 0);
    REQUIRE(axes[0].top == 0);
    REQUIRE(axes[0].height == 70);
    REQUIRE(axes[1].index == 1);
    REQUIRE(axes[1].top == 70);
    REQUIRE(axes[1].height == 30);
  }

  SECTION("Assigns price overlays to axis 0") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // Register candlestick series first
    manager.RegisterSeries(timeframe, timeframe + "_candlestick", 0);

    auto sma = transform::ma("sma", "1", strategy::InputValue(strategy::NodeReference("", "c")), 10, tf);
    auto [axis, linkedTo] =
        manager.AssignAxis(sma, timeframe, priceKeys, volumeKey, outputHandles);

    REQUIRE(axis == 0);
    REQUIRE(linkedTo.has_value());
    REQUIRE(linkedTo.value() == timeframe + "_candlestick");
  }

  SECTION("Assigns volume overlays to axis 1") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // Register candlestick and volume series first
    manager.RegisterSeries(timeframe, timeframe + "_candlestick", 0);
    manager.RegisterSeries(timeframe, timeframe + "_volume", 1);

    // Create a volume-based indicator
    auto volume_sma = transform::ma("sma", "1", strategy::InputValue(strategy::NodeReference("", "v")), 10, tf);
    auto [axis, linkedTo] = manager.AssignAxis(volume_sma, timeframe, priceKeys,
                                               volumeKey, outputHandles);

    REQUIRE(axis == 1);
    REQUIRE(linkedTo.has_value());
    REQUIRE(linkedTo.value() == timeframe + "_volume");
  }

  SECTION("Creates new axis for panel indicators") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // RSI should get its own axis
    auto rsi = transform::single_operand_period_op("rsi", "1", 14, strategy::InputValue(strategy::NodeReference("", "c")), tf);
    auto [axis, linkedTo] =
        manager.AssignAxis(rsi, timeframe, priceKeys, volumeKey, outputHandles);

    REQUIRE(axis == 2); // New axis
    REQUIRE(!linkedTo.has_value());

    auto axes = manager.GetAxes(timeframe);
    REQUIRE(axes.size() == 3);
  }

  SECTION("Handles multiple panel indicators") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // Add RSI
    auto rsi = transform::single_operand_period_op("rsi", "1", 14, strategy::InputValue(strategy::NodeReference("", "c")), tf);
    auto [rsiAxis, rsiLinked] =
        manager.AssignAxis(rsi, timeframe, priceKeys, volumeKey, outputHandles);
    REQUIRE(rsiAxis == 2);

    // Add MACD
    auto macd = transform::run_op("macd", "2",
        {{epoch_script::ARG, {transform::input_ref("c")}}},
        {{"short_period", MetaDataOptionDefinition{12.0}},
         {"long_period", MetaDataOptionDefinition{26.0}},
         {"signal_period", MetaDataOptionDefinition{9.0}}},
        tf);

    auto [macdAxis, macdLinked] = manager.AssignAxis(macd, timeframe, priceKeys,
                                                     volumeKey, outputHandles);
    REQUIRE(macdAxis == 3);

    auto axes = manager.GetAxes(timeframe);
    REQUIRE(axes.size() == 4);
  }

  SECTION("Recalculates axis heights correctly") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // Add multiple indicators
    auto rsi = transform::single_operand_period_op("rsi", "1", 14, strategy::InputValue(strategy::NodeReference("", "c")), tf);
    manager.AssignAxis(rsi, timeframe, priceKeys, volumeKey, outputHandles);

    auto macd = transform::run_op("macd", "2",
        {{epoch_script::ARG, {transform::input_ref("c")}}},
        {{"short_period", MetaDataOptionDefinition{12.0}},
         {"long_period", MetaDataOptionDefinition{26.0}},
         {"signal_period", MetaDataOptionDefinition{9.0}}},
        tf);
    manager.AssignAxis(macd, timeframe, priceKeys, volumeKey, outputHandles);

    auto axes = manager.GetAxes(timeframe);
    REQUIRE(axes.size() == 4);

    // Price gets double height: 100/(4+1)*2 = 40
    REQUIRE(axes[0].height == 40);
    REQUIRE(axes[0].top == 0);

    // Others get equal shares: 100/(4+1) = 20
    REQUIRE(axes[1].height == 20);
    REQUIRE(axes[1].top == 40);

    REQUIRE(axes[2].height == 20);
    REQUIRE(axes[2].top == 60);

    REQUIRE(axes[3].height == 20);
    REQUIRE(axes[3].top == 80);
  }

  SECTION("Links chained transforms correctly") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // Register base series
    manager.RegisterSeries(timeframe, timeframe + "_candlestick", 0);
    manager.RegisterSeries(timeframe, timeframe + "_volume", 1);

    // Add SMA
    auto sma = transform::ma("sma", "1", strategy::InputValue(strategy::NodeReference("", "c")), 10, tf);
    auto [smaAxis, smaLinked] =
        manager.AssignAxis(sma, timeframe, priceKeys, volumeKey, outputHandles);
    manager.RegisterSeries(timeframe, "1", smaAxis);

    // Add transform that uses SMA output
    outputHandles[sma.GetOutputId().GetColumnName()] = 2; // Index in series array

    auto min_sma =
        transform::single_operand_period_op("min", "2", 5, strategy::InputValue(sma.GetOutputId()), tf);
    auto [minAxis, minLinked] = manager.AssignAxis(
        min_sma, timeframe, priceKeys, volumeKey, outputHandles);

    REQUIRE(minAxis == smaAxis);
    REQUIRE(minLinked.has_value());
    REQUIRE(minLinked.value() == "1");
  }

  SECTION("Handles different timeframes independently") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    const auto tf1 =
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    const auto tf2 =
        epoch_script::EpochStratifyXConstants::instance().MINUTE_FREQUENCY;
    const auto timeframe1 = tf1.ToString();
    const auto timeframe2 = tf2.ToString();

    // Add indicator to first timeframe
    auto rsi1 = transform::single_operand_period_op("rsi", "1", 14, strategy::InputValue(strategy::NodeReference("", "c")), tf1);
    auto [axis1, linked1] = manager.AssignAxis(rsi1, timeframe1, priceKeys,
                                               volumeKey, outputHandles);

    // Add indicator to second timeframe
    auto rsi2 = transform::single_operand_period_op("rsi", "2", 14, strategy::InputValue(strategy::NodeReference("", "c")), tf2);
    auto [axis2, linked2] = manager.AssignAxis(rsi2, timeframe2, priceKeys,
                                               volumeKey, outputHandles);

    // Both should get axis 2 in their respective timeframes
    REQUIRE(axis1 == 2);
    REQUIRE(axis2 == 2);

    // Axes should be independent
    auto axes1 = manager.GetAxes(timeframe1);
    auto axes2 = manager.GetAxes(timeframe2);

    REQUIRE(axes1.size() == 3);
    REQUIRE(axes2.size() == 3);
  }

  SECTION("GetSeriesIdAtIndex returns correct series") {
    AxisManager manager;

    manager.RegisterSeries(timeframe, "series1", 0);
    manager.RegisterSeries(timeframe, "series2", 1);
    manager.RegisterSeries(timeframe, "series3", 0);

    REQUIRE(manager.GetSeriesIdAtIndex(timeframe, 0) == "series1");
    REQUIRE(manager.GetSeriesIdAtIndex(timeframe, 1) == "series2");
    REQUIRE(manager.GetSeriesIdAtIndex(timeframe, 2) == "series3");
    REQUIRE(manager.GetSeriesIdAtIndex(timeframe, 3) == ""); // Out of bounds
    REQUIRE(manager.GetSeriesIdAtIndex("unknown", 0) ==
            ""); // Unknown timeframe
  }

  SECTION("Handles transforms with no inputs") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // Create a transform with no inputs (like AO - Awesome Oscillator)
    auto ao = transform::run_op("ao", "1", {}, {}, tf);

    auto [axis, linkedTo] =
        manager.AssignAxis(ao, timeframe, priceKeys, volumeKey, outputHandles);

    // Should get its own axis since it's a panel indicator
    REQUIRE(axis == 2);
    REQUIRE(!linkedTo.has_value());
  }

  SECTION("Handles special plot kinds correctly") {
    AxisManager manager;
    std::unordered_map<std::string, int64_t> outputHandles;

    // Register base series
    manager.RegisterSeries(timeframe, timeframe + "_candlestick", 0);

    // Test various plot kinds
    SECTION("Bollinger Bands overlay") {
      auto bbands = transform::bbands("1", 10, 2, strategy::InputValue(strategy::NodeReference("", "c")), tf);
      auto [axis, linkedTo] = manager.AssignAxis(bbands, timeframe, priceKeys,
                                                 volumeKey, outputHandles);
      REQUIRE(axis == 0);
      REQUIRE(linkedTo.has_value());
    }

    SECTION("PSAR overlay") {
      auto psar = transform::psar("1", 0.02, 0.2, strategy::InputValue(strategy::NodeReference("", "c")), tf);
      auto [axis, linkedTo] = manager.AssignAxis(psar, timeframe, priceKeys,
                                                 volumeKey, outputHandles);
      REQUIRE(axis == 0);
      REQUIRE(linkedTo.has_value());
    }

    SECTION("CCI panel indicator") {
      auto cci = transform::run_op("cci", "1",
          {},
          {{"period", MetaDataOptionDefinition{20.0}}},
          tf);
      auto [axis, linkedTo] = manager.AssignAxis(cci, timeframe, priceKeys,
                                                 volumeKey, outputHandles);
      REQUIRE(axis == 2);
      REQUIRE(!linkedTo.has_value());
    }
  }
}