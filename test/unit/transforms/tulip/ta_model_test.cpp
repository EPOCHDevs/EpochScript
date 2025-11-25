//
// Created by adesola on 12/3/24.
//
#include <epoch_script/core/bar_attribute.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/tulip/tulip_model.h" // TulipIndicatorModel
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

TEST_CASE("Tulip Indicator Transforms") {
  SECTION("Moving Average Test") {
    // E.g. an SMA with period=4 on column "src#x"
    TransformConfiguration config = sma(
        "0", input_ref("src", "x"), 4,
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    // Use registry to create the transform
    auto transformBase = MAKE_TRANSFORM(config);
    auto model = dynamic_cast<ITransform *>(transformBase.get());

    SECTION("Successful run") {
      // DataFrame input: src#x => [2, 4, 6, 8, 10]
      // If period=4 => first row of output at index=3 => (2+4+6+8)/4 = 20/4=5,
      // next row= (4+6+8+10)/4=28/4=7
      auto index = epoch_frame::factory::index::make_datetime_index(
          {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
           DateTime{2020y, std::chrono::January, 2d},
           epoch_frame::DateTime{2020y, std::chrono::January, 3d},
           epoch_frame::DateTime{2020y, std::chrono::January, 4d},
           epoch_frame::DateTime{2020y, std::chrono::January, 5d}});

      epoch_frame::DataFrame input =
          make_dataframe<double>(index, {{2.0, 4.0, 6.0, 8.0, 10.0}}, {"src#x"});

      auto expectedIndex = epoch_frame::factory::index::make_datetime_index(
          {epoch_frame::DateTime{2020y, std::chrono::January, 4d},
           epoch_frame::DateTime{2020y, std::chrono::January, 5d}});

      epoch_frame::DataFrame expected = make_dataframe<double>(
          expectedIndex, {{5.0, 7.0}}, {config.GetOutputId().GetColumnName()});

      auto result = model->TransformData(input);
      INFO("Comparing output with expected values\n"
           << result << "\n!=\n"
           << expected);
      REQUIRE(result.equals(expected));
    }

    SECTION("Period beyond data size") {
      // If period=4 but data only has 3 rows => output is empty
      auto index = epoch_frame::factory::index::make_datetime_index(
          {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
           epoch_frame::DateTime{2020y, std::chrono::January, 2d},
           epoch_frame::DateTime{2020y, std::chrono::January, 3d}});

      epoch_frame::DataFrame input =
          make_dataframe<double>(index, {{2.0, 4.0, 6.0}}, {"src#x"});

      // Empty DataFrame with the correct column
      auto emptyIndex = epoch_frame::factory::index::make_datetime_index(
          std::vector<arrow::TimestampScalar>{});
      epoch_frame::DataFrame expected =
          make_dataframe<double>(emptyIndex, {{}}, {config.GetOutputId().GetColumnName()});

      auto result = model->TransformData(input);
      INFO("Comparing output with expected values\n"
           << result << "\n!=\n"
           << expected);
      REQUIRE(result.equals(expected));
    }
  }

  SECTION("CrossOver and CrossAny Test") {
    // DataFrame example with node#column format
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
         epoch_frame::DateTime{2020y, std::chrono::January, 2d},
         epoch_frame::DateTime{2020y, std::chrono::January, 3d},
         epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

    epoch_frame::DataFrame input =
        make_dataframe<double>(index,
                               {
                                   {81.59, 81.06, 82.87, 83.00}, // src#x
                                   {81.85, 81.20, 81.55, 82.91}  // src#y
                               },
                               {"src#x", "src#y"});

    for (auto const &op : {"over", "any"}) {
      DYNAMIC_SECTION(std::format("Cross{} Successful run", op)) {
          // Build transform config: cross + op => crossany or crossover
          TransformConfiguration config = double_operand_op(
              "cross", op, "0",
              input_ref("src", "x"),
              input_ref("src", "y"),
            epoch_script::EpochStratifyXConstants::instance()
                .DAILY_FREQUENCY);

        // Use registry to create the transform
        auto transformBase = MAKE_TRANSFORM(config);
        auto model = dynamic_cast<ITransform *>(transformBase.get());

        // Tulip indicates 1 if a cross occurred on that row => we store as bool
        // For 4 input rows => first output row is from index=1 => we get 3
        // output rows total We'll guess: row1 => false, row2 => true, row3 =>
        // false
        auto outputIndex = epoch_frame::factory::index::make_datetime_index(
            {epoch_frame::DateTime{2020y, std::chrono::January, 2d},
             epoch_frame::DateTime{2020y, std::chrono::January, 3d},
             epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

        epoch_frame::DataFrame expected = make_dataframe<bool>(
            outputIndex, {{false, true, false}}, {config.GetOutputId().GetColumnName()});

        auto result = model->TransformData(input);
        INFO("Comparing output with expected values\n"
             << result << "\n!=\n"
             << expected);
        REQUIRE(result.equals(expected));
      }
    }

    SECTION("Crossunder Test - Detects crossing below") {
      // Crossunder should detect when x crosses BELOW y
      // Using the same input data, crossunder(x, y) should be the opposite of crossover(x, y)
      TransformConfiguration config = crossunder(
          "0",
          input_ref("src", "x"),
          input_ref("src", "y"),
          epoch_script::EpochStratifyXConstants::instance()
              .DAILY_FREQUENCY);

      auto transformBase = MAKE_TRANSFORM(config);
      auto model = dynamic_cast<ITransform *>(transformBase.get());

      // With the input data:
      // x: [81.59, 81.06, 82.87, 83.00]
      // y: [81.85, 81.20, 81.55, 82.91]
      // At index 2: x crosses BELOW y (82.87 > 81.55 => no cross under)
      // At index 3: x crosses ABOVE y (83.00 > 82.91 => no cross under)
      // So crossunder should be all false for this data since x never crosses below y
      auto outputIndex = epoch_frame::factory::index::make_datetime_index(
          {epoch_frame::DateTime{2020y, std::chrono::January, 2d},
           epoch_frame::DateTime{2020y, std::chrono::January, 3d},
           epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

      epoch_frame::DataFrame expected = make_dataframe<bool>(
          outputIndex, {{false, false, false}}, {config.GetOutputId().GetColumnName()});

      auto result = model->TransformData(input);
      INFO("Comparing crossunder output with expected values\n"
           << result << "\n!=\n"
           << expected);
      REQUIRE(result.equals(expected));
    }
  }

  SECTION("MACD Indicator Test") {
    // MACD with periods 5,10,2
    auto &C = epoch_script::EpochStratifyXConstants::instance();
    auto config = run_op("macd", "1",
        {{"SLOT", {input_ref("mds", "c")}}},
        {{"short_period", MetaDataOptionDefinition{5.0}},
         {"long_period", MetaDataOptionDefinition{10.0}},
         {"signal_period", MetaDataOptionDefinition{2.0}}},
        C.DAILY_FREQUENCY);

    // Use registry to create the transform
    auto transformBase = MAKE_TRANSFORM(config);
    auto model = dynamic_cast<ITransform *>(transformBase.get());

    // Suppose we feed a close array: enough data to get a few MACD values
    std::vector<epoch_frame::DateTime> data;
    for (int i = 0; i < 13; i++) {
      data.emplace_back(epoch_frame::DateTime{2020y, std::chrono::January,
                                              chrono_day(1 + i)});
    }
    auto index = epoch_frame::factory::index::make_datetime_index(data);

    std::vector<double> closeValues = {35, 36, 37, 38, 39, 40, 41,
                                       42, 43, 44, 45, 46, 47};
    // Use node#column format for input
    epoch_frame::DataFrame input = make_dataframe<double>(
        index, {closeValues},
        {"mds#c"});

    // For demonstration, we won't calculate real MACD values manually.
    // Just check that the result has 3 columns matching the Tulip naming.
    auto output = model->TransformData(input);

    // We'll just confirm the shape is as expected.
    INFO("Output DataFrame: " << output);
    REQUIRE(output.num_cols() == 3); // if macd is 3-output
    REQUIRE(output.num_rows() > 3);  // Enough data => should produce some rows
  }

  SECTION("MACD Indicator Test with empty result") {
    // MACD with periods 12,26,9 - not enough data to produce result
    auto &C = epoch_script::EpochStratifyXConstants::instance();
    auto config = run_op("macd", "1",
        {{"SLOT", {input_ref("mds", "c")}}},
        {{"short_period", MetaDataOptionDefinition{12.0}},
         {"long_period", MetaDataOptionDefinition{26.0}},
         {"signal_period", MetaDataOptionDefinition{9.0}}},
        C.DAILY_FREQUENCY);

    // Use registry to create the transform
    auto transformBase = MAKE_TRANSFORM(config);
    auto model = dynamic_cast<ITransform *>(transformBase.get());

    // Not enough data with this parameter configuration
    std::vector<epoch_frame::DateTime> data;
    for (int i = 0; i < 13; i++) {
      data.emplace_back(epoch_frame::DateTime{2020y, std::chrono::January,
                                              chrono_day(1 + i)});
    }
    auto index = epoch_frame::factory::index::make_datetime_index(data);

    std::vector<double> closeValues = {35, 36, 37, 38, 39, 40, 41,
                                       42, 43, 44, 45, 46, 47};
    // Use node#column format for input
    epoch_frame::DataFrame input = make_dataframe<double>(
        index, {closeValues},
        {"mds#c"});

    auto output = model->TransformData(input);

    // Confirm that with these parameters we get an empty DataFrame with 3
    // columns
    INFO("Output DataFrame: " << output);
    REQUIRE(output.num_cols() == 3); // if macd is 3-output
    REQUIRE(output.num_rows() == 0); // Not enough data => should be empty
  }

  SECTION("CandleStick Test") {
    auto &C = epoch_script::EpochStratifyXConstants::instance();
    // Doji candlestick pattern detection
    auto config = run_op("doji", "1",
        {},
        {{"period", MetaDataOptionDefinition{10.0}},
         {"body_none", MetaDataOptionDefinition{0.05}},
         {"body_short", MetaDataOptionDefinition{0.5}},
         {"body_long", MetaDataOptionDefinition{1.4}},
         {"wick_none", MetaDataOptionDefinition{0.05}},
         {"wick_long", MetaDataOptionDefinition{0.6}},
         {"near", MetaDataOptionDefinition{0.3}}},
        C.DAILY_FREQUENCY);

    // Use registry to create the transform
    auto transformBase = MAKE_TRANSFORM(config);
    auto model = dynamic_cast<ITransform *>(transformBase.get());

    std::vector<double> openValues = {5, 6, 5, 6, 5, 6, 5, 6,   5, 6, 5,
                                      6, 5, 6, 5, 6, 5, 5, 5.5, 5, 5, 9};
    std::vector<double> highValues = {7, 7, 7, 7, 7, 7, 7, 7,  7, 7, 7,
                                      7, 7, 7, 7, 7, 7, 5, 10, 7, 8, 9};
    std::vector<double> lowValues = {4, 4, 4, 4, 4, 4, 4, 4, 4, 4, 4,
                                     4, 4, 4, 4, 4, 4, 2, 3, 4, 5, 9};
    std::vector<double> closeValues = {6, 5, 6, 5, 6, 5, 6, 5,   6, 5, 6,
                                       5, 6, 5, 6, 5, 6, 5, 5.5, 5, 5, 9};

    std::vector<epoch_frame::DateTime> data;
    for (size_t i = 0; i < openValues.size(); i++) {
      data.emplace_back(epoch_frame::DateTime{2020y, std::chrono::January,
                                              chrono_day(1 + i)});
    }
    auto index = epoch_frame::factory::index::make_datetime_index(data);

    epoch_frame::DataFrame input = make_dataframe<double>(
        index, {openValues, highValues, lowValues, closeValues},
        {C.OPEN(), C.HIGH(), C.LOW(), C.CLOSE()});

    // For demonstration, we won't calculate real MACD values manually.
    // Just check that the result has 3 columns matching the Tulip naming.
    auto output = model->TransformData(input);

    // We'll just confirm the shape is as expected.
    INFO("Output DataFrame: " << output);
    REQUIRE(output.num_cols() == 1);

    std::vector result(closeValues.size(), false);
    std::ranges::fill(result.begin() + 17, result.end(), true);
    REQUIRE(output["1#result"].contiguous_array().to_vector<bool>() == result);
  }
}