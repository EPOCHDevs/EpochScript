//
// Created by adesola on 12/3/24.
//
#include <epoch_script/core/bar_attribute.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/operators/equality.h"
#include "transforms/components/operators/logical.h"
#include "transforms/components/operators/select.h"
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/index_factory.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

/**
 * Helper Functions to Build DataFrames
 */
epoch_frame::DataFrame MakeNumericDataFrame() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

  return make_dataframe<double>(
      index,
      {
          {10.0, 20.0, 30.0, 40.0}, // price
          {1.0, 2.0, 3.0, 4.0},     // actual
          {1.0, 0.0, 3.0, 5.0},     // expected
          {5.0, 10.0, 15.0, 20.0},  // current
          {3.0, 10.0, 20.0, 15.0}   // previous
      },
      {"price#price", "actual#actual", "expected#expected", "current#current", "previous#previous"});
}

epoch_frame::DataFrame MakeBoolDataFrame() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

  return make_dataframe<bool>(index,
                              {
                                  {true, false, true, false}, // bool_a
                                  {false, false, true, true}  // bool_b
                              },
                              {"bool_a#bool_a", "bool_b#bool_b"});
}

epoch_frame::DataFrame MakeSelectDataFrame2() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

  return make_dataframe(
      index,
      {
          {0_scalar, 1_scalar, 0_scalar, 1_scalar},                // selector
          {10.0_scalar, 20.0_scalar, 30.0_scalar, 40.0_scalar},    // option_0
          {100.0_scalar, 200.0_scalar, 300.0_scalar, 400.0_scalar} // option_1
      },
      {arrow::field("selector#selector", arrow::int64()),
       arrow::field("option_0#option_0", arrow::float64()),
       arrow::field("option_1#option_1", arrow::float64())});
}

TEST_CASE("Comparative Transforms") {

  SECTION("Equality Transforms - Comprehensive") {
    epoch_frame::DataFrame input = MakeNumericDataFrame();

    // Vector-based comparisons
    SECTION("Vector Equals (vector_eq)") {
      TransformConfiguration config = vector_op(
          "eq", "7", strategy::InputValue(strategy::NodeReference("actual", "actual")),
          strategy::InputValue(strategy::NodeReference("expected", "expected")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{true, false, true, false}}, {"7#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Vector Not Equals (vector_neq)") {
      TransformConfiguration config = vector_op(
          "neq", "8", strategy::InputValue(strategy::NodeReference("actual", "actual")),
          strategy::InputValue(strategy::NodeReference("expected", "expected")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{false, true, false, true}}, {"8#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Vector Less Than (vector_lt)") {
      TransformConfiguration config = vector_op(
          "lt", "9", strategy::InputValue(strategy::NodeReference("previous", "previous")),
          strategy::InputValue(strategy::NodeReference("current", "current")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{true, false, false, true}}, {"9#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Vector Less Than Equals (vector_lte)") {
      TransformConfiguration config = vector_op(
          "lte", "10", strategy::InputValue(strategy::NodeReference("previous", "previous")),
          strategy::InputValue(strategy::NodeReference("current", "current")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{true, true, false, true}}, {"10#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }
  }

  SECTION("Logical Transforms - Comprehensive") {
    epoch_frame::DataFrame input = MakeBoolDataFrame();

    SECTION("Logical OR (logical_or)") {
      TransformConfiguration config = logical_op(
          "or", "11", strategy::InputValue(strategy::NodeReference("bool_a", "bool_a")),
          strategy::InputValue(strategy::NodeReference("bool_b", "bool_b")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{true, false, true, true}}, {"11#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Logical AND (logical_and)") {
      TransformConfiguration config = logical_op(
          "and", "12", strategy::InputValue(strategy::NodeReference("bool_a", "bool_a")),
          strategy::InputValue(strategy::NodeReference("bool_b", "bool_b")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{false, false, true, false}}, {"12#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Logical NOT (logical_not)") {
      TransformConfiguration config = single_operand_op(
          "logical", "not", "13", strategy::InputValue(strategy::NodeReference("bool_a", "bool_a")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{false, true, false, true}}, {"13#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Logical XOR (logical_xor)") {
      TransformConfiguration config = logical_op(
          "xor", "14", strategy::InputValue(strategy::NodeReference("bool_a", "bool_a")),
          strategy::InputValue(strategy::NodeReference("bool_b", "bool_b")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{true, false, false, true}}, {"14#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Logical AND NOT (logical_and_not)") {
      TransformConfiguration config = logical_op(
          "and_not", "15", strategy::InputValue(strategy::NodeReference("bool_a", "bool_a")),
          strategy::InputValue(strategy::NodeReference("bool_b", "bool_b")),
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<bool>(
          input.index(), {{true, false, false, false}}, {"15#result"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }
  }

  SECTION("Select Transforms - Comprehensive") {
    SECTION("BooleanSelectTransform") {
      auto index = epoch_frame::factory::index::make_datetime_index(
          {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
           epoch_frame::DateTime{2020y, std::chrono::January, 2d},
           epoch_frame::DateTime{2020y, std::chrono::January, 3d},
           epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

      epoch_frame::DataFrame input =
          make_dataframe(index,
                         {
                             {Scalar{true}, Scalar{false}, Scalar{true},
                              Scalar{false}}, // condition
                             {100.0_scalar, 200.0_scalar, 300.0_scalar,
                              400.0_scalar}, // value_if_true
                             {10.0_scalar, 20.0_scalar, 30.0_scalar,
                              40.0_scalar} // value_if_false
                         },
                         {arrow::field("#condition", arrow::boolean()),
                          arrow::field("#value_if_true", arrow::float64()),
                          arrow::field("#value_if_false", arrow::float64())});

      // Use typed variant directly - values are numeric (float64)
      const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
      TransformConfiguration config = run_op(
          "boolean_select_number", "20",
          make_inputs({
              {"condition", input_ref("", "condition")},
              {"true", input_ref("", "value_if_true")},
              {"false", input_ref("", "value_if_false")}
          }),
          {},
          timeframe);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);
      epoch_frame::DataFrame expected = make_dataframe<double>(
          input.index(), {{100.0, 20.0, 300.0, 40.0}}, {"20#value"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Select2 Transform") {
      TransformConfiguration config = select_n(
          21, 2, strategy::InputValue(strategy::NodeReference("selector", "selector")),
          {strategy::InputValue(strategy::NodeReference("option_0", "option_0")),
           strategy::InputValue(strategy::NodeReference("option_1", "option_1"))},
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame input = MakeSelectDataFrame2();
      epoch_frame::DataFrame output = transform->TransformData(input);

      epoch_frame::DataFrame expected = make_dataframe<double>(
          input.index(), {{10.0, 200.0, 30.0, 400.0}}, {"21#value"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Select3 Transform") {
      auto index = epoch_frame::factory::index::make_datetime_index(
          {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
           epoch_frame::DateTime{2020y, std::chrono::January, 2d},
           epoch_frame::DateTime{2020y, std::chrono::January, 3d},
           epoch_frame::DateTime{2020y, std::chrono::January, 4d},
           epoch_frame::DateTime{2020y, std::chrono::January, 5d}});

      epoch_frame::DataFrame input = make_dataframe(
          index,
          {
              {0_scalar, 1_scalar, 2_scalar, 1_scalar, 0_scalar}, // selector
              {10.0_scalar, 20.0_scalar, 30.0_scalar, 40.0_scalar,
               50.0_scalar}, // option_0
              {100.0_scalar, 200.0_scalar, 300.0_scalar, 400.0_scalar,
               500.0_scalar}, // option_1
              {1000.0_scalar, 2000.0_scalar, 3000.0_scalar, 4000.0_scalar,
               5000.0_scalar} // option_2
          },
          {arrow::field("selector#selector", arrow::int64()),
           arrow::field("option_0#option_0", arrow::float64()),
           arrow::field("option_1#option_1", arrow::float64()),
           arrow::field("option_2#option_2", arrow::float64())});

      TransformConfiguration config = select_n(
          22, 3, strategy::InputValue(strategy::NodeReference("selector", "selector")),
          {strategy::InputValue(strategy::NodeReference("option_0", "option_0")),
           strategy::InputValue(strategy::NodeReference("option_1", "option_1")),
           strategy::InputValue(strategy::NodeReference("option_2", "option_2"))},
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      epoch_frame::DataFrame output = transform->TransformData(input);

      epoch_frame::DataFrame expected = make_dataframe<double>(
          input.index(), {{10.0, 200.0, 3000.0, 400.0, 50.0}}, {"22#value"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Select4 Transform (normal usage)") {
      auto index = epoch_frame::factory::index::make_datetime_index(
          {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
           epoch_frame::DateTime{2020y, std::chrono::January, 2d},
           epoch_frame::DateTime{2020y, std::chrono::January, 3d},
           epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

      epoch_frame::DataFrame input = make_dataframe(
          index,
          {
              {0_scalar, 1_scalar, 2_scalar, 3_scalar},             // selector
              {10.0_scalar, 20.0_scalar, 30.0_scalar, 40.0_scalar}, // option_0
              {100.0_scalar, 200.0_scalar, 300.0_scalar,
               400.0_scalar}, // option_1
              {1000.0_scalar, 2000.0_scalar, 3000.0_scalar,
               4000.0_scalar},                                     // option_2
              {-1.0_scalar, -2.0_scalar, -3.0_scalar, -4.0_scalar} // option_3
          },
          {arrow::field("selector#selector", arrow::int64()),
           arrow::field("option_0#option_0", arrow::float64()),
           arrow::field("option_1#option_1", arrow::float64()),
           arrow::field("option_2#option_2", arrow::float64()),
           arrow::field("option_3#option_3", arrow::float64())});

      TransformConfiguration config = select_n(
          23, 4, strategy::InputValue(strategy::NodeReference("selector", "selector")),
          {strategy::InputValue(strategy::NodeReference("option_0", "option_0")),
           strategy::InputValue(strategy::NodeReference("option_1", "option_1")),
           strategy::InputValue(strategy::NodeReference("option_2", "option_2")),
           strategy::InputValue(strategy::NodeReference("option_3", "option_3"))},
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      // row0 => idx=0 => 10
      // row1 => idx=1 => 200
      // row2 => idx=2 => 3000
      // row3 => idx=3 => -4
      epoch_frame::DataFrame output = transform->TransformData(input);

      // row0 => idx=0 => 10
      // row1 => idx=1 => 200
      // row2 => idx=2 => 3000
      // row3 => idx=3 => -4
      epoch_frame::DataFrame expected = make_dataframe<double>(
          input.index(), {{10.0, 200.0, 3000.0, -4.0}}, {"23#value"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }

    SECTION("Select5 Transform (normal usage)") {
      auto index = epoch_frame::factory::index::make_datetime_index(
          {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
           epoch_frame::DateTime{2020y, std::chrono::January, 2d},
           epoch_frame::DateTime{2020y, std::chrono::January, 3d},
           epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

      epoch_frame::DataFrame input = make_dataframe(
          index,
          {
              {0_scalar, 1_scalar, 4_scalar, 3_scalar},             // selector
              {10.0_scalar, 20.0_scalar, 30.0_scalar, 40.0_scalar}, // option_0
              {100.0_scalar, 200.0_scalar, 300.0_scalar,
               400.0_scalar}, // option_1
              {1000.0_scalar, 2000.0_scalar, 3000.0_scalar,
               4000.0_scalar},                                      // option_2
              {-1.0_scalar, -2.0_scalar, -3.0_scalar, -4.0_scalar}, // option_3
              {999.0_scalar, 888.0_scalar, 777.0_scalar, 666.0_scalar}
              // option_4
          },
          {arrow::field("selector#selector", arrow::int64()),
           arrow::field("option_0#option_0", arrow::float64()),
           arrow::field("option_1#option_1", arrow::float64()),
           arrow::field("option_2#option_2", arrow::float64()),
           arrow::field("option_3#option_3", arrow::float64()),
           arrow::field("option_4#option_4", arrow::float64())});

      TransformConfiguration config = select_n(
          24, 5, strategy::InputValue(strategy::NodeReference("selector", "selector")),
          {strategy::InputValue(strategy::NodeReference("option_0", "option_0")),
           strategy::InputValue(strategy::NodeReference("option_1", "option_1")),
           strategy::InputValue(strategy::NodeReference("option_2", "option_2")),
           strategy::InputValue(strategy::NodeReference("option_3", "option_3")),
           strategy::InputValue(strategy::NodeReference("option_4", "option_4"))},
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
      auto transformBase = MAKE_TRANSFORM(config);
      auto transform = dynamic_cast<ITransform *>(transformBase.get());

      // row0 => idx=0 => 10
      // row1 => idx=1 => 200
      // row2 => idx=4 => 777
      // row3 => idx=3 => -4
      epoch_frame::DataFrame output = transform->TransformData(input);

      epoch_frame::DataFrame expected = make_dataframe<double>(
          input.index(), {{10.0, 200.0, 777.0, -4.0}}, {"24#value"});

      INFO("Comparing output with expected values\n"
           << output << "\n!=\n"
           << expected);
      REQUIRE(output.equals(expected));
    }
  }
}
TEST_CASE("Additional Comparative Transforms") {
  SECTION("Vector Greater Than (vector_gt)") {
    epoch_frame::DataFrame input = MakeNumericDataFrame();

    TransformConfiguration config = vector_op(
        "gt", "25", strategy::InputValue(strategy::NodeReference("current", "current")),
        strategy::InputValue(strategy::NodeReference("previous", "previous")),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);
    epoch_frame::DataFrame expected = make_dataframe<bool>(
        input.index(), {{true, false, false, true}}, {"25#result"});

    INFO("Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("Vector Greater Than Equals (vector_gte)") {
    epoch_frame::DataFrame input = MakeNumericDataFrame();

    TransformConfiguration config = vector_op(
        "gte", "26", strategy::InputValue(strategy::NodeReference("current", "current")),
        strategy::InputValue(strategy::NodeReference("previous", "previous")),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);
    epoch_frame::DataFrame expected = make_dataframe<bool>(
        input.index(), {{true, true, false, true}}, {"26#result"});

    INFO("Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("PercentileSelect") {
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
         epoch_frame::DateTime{2020y, std::chrono::January, 2d},
         epoch_frame::DateTime{2020y, std::chrono::January, 3d},
         epoch_frame::DateTime{2020y, std::chrono::January, 4d},
         epoch_frame::DateTime{2020y, std::chrono::January, 5d},
         epoch_frame::DateTime{2020y, std::chrono::January, 6d}});

    epoch_frame::DataFrame input =
        make_dataframe(index,
                       {
                           {10.0_scalar, 15.0_scalar, 8.0_scalar, 20.0_scalar,
                            12.0_scalar, 25.0_scalar}, // value
                           {100.0_scalar, 150.0_scalar, 80.0_scalar,
                            200.0_scalar, 120.0_scalar, 250.0_scalar}, // high
                           {1.0_scalar, 1.5_scalar, 0.8_scalar, 2.0_scalar,
                            1.2_scalar, 2.5_scalar} // low
                       },
                       {arrow::field("#value", arrow::float64()),
                        arrow::field("#high", arrow::float64()),
                        arrow::field("#low", arrow::float64())});

    // Use typed variant directly
    const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    TransformConfiguration config = run_op(
        "percentile_select_number", "30",
        make_inputs({
            {"value", input_ref("", "value")},
            {"high", input_ref("", "high")},
            {"low", input_ref("", "low")}
        }),
        make_options({
            {"lookback", MetaDataOptionDefinition{3.0}},
            {"percentile", MetaDataOptionDefinition{50.0}}
        }),
        timeframe);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // For window size 3, median (50th percentile) in each window:
    // Row 0-2: [10, 15, 8] -> median = 10, value >= median ? high : low
    // 10 >= 10 -> 100
    // 15 >= 10 -> 150
    // 8 >= 10 -> 0.8 (low)

    // Row 3-5: [20, 12, 25] -> median = 20, value >= median ? high : low
    // 20 >= 20 -> 200
    // 12 >= 20 -> 1.2 (low)
    // 25 >= 20 -> 250

    epoch_frame::DataFrame expected = make_dataframe<double>(
        input.index(), {{std::nan(""), std::nan(""), 0.8, 200.0, 120, 250.0}},
        {"30#value"});

    INFO("Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("BooleanBranch") {
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
         epoch_frame::DateTime{2020y, std::chrono::January, 2d},
         epoch_frame::DateTime{2020y, std::chrono::January, 3d},
         epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

    epoch_frame::DataFrame input = make_dataframe<bool>(
        index, {{true, false, true, false}}, {"condition#condition"});

    // Use the helper function instead of direct YAML
    TransformConfiguration config = boolean_branch(
        "31", strategy::InputValue(strategy::NodeReference("condition", "condition")),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    epoch_frame::DataFrame expected = make_dataframe<bool>(
        input.index(),
        {
            {true, false, true, false}, // true branch preserves condition
            {false, true, false, true}  // false branch is the negation
        },
        {"31#true", "31#false"});

    INFO("Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("RatioBranch") {
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
         epoch_frame::DateTime{2020y, std::chrono::January, 2d},
         epoch_frame::DateTime{2020y, std::chrono::January, 3d},
         epoch_frame::DateTime{2020y, std::chrono::January, 4d},
         epoch_frame::DateTime{2020y, std::chrono::January, 5d}});

    epoch_frame::DataFrame input =
        make_dataframe<double>(index, {{0.5, 1.2, 1.5, 0.8, 2.0}}, {"ratio#ratio"});

    // Use the helper function instead of direct YAML
    TransformConfiguration config = ratio_branch(
        "32", strategy::InputValue(strategy::NodeReference("ratio", "ratio")), 1.5, 0.8,
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // threshold_high = 1.5, threshold_low = 0.8
    // high: ratio > 1.5
    // normal: 0.8 <= ratio <= 1.5
    // low: ratio < 0.8

    epoch_frame::DataFrame expected =
        make_dataframe(input.index(),
                       {
                           {Scalar{false}, Scalar{false}, Scalar{false},
                            Scalar{false}, Scalar{true}}, // high branch
                           {Scalar{false}, Scalar{true}, Scalar{true},
                            Scalar{true}, Scalar{false}}, // normal branch
                           {Scalar{true}, Scalar{false}, Scalar{false},
                            Scalar{false}, Scalar{false}}, // low branch
                       },
                       {arrow::field("32#high", arrow::boolean()),
                        arrow::field("32#normal", arrow::boolean()),
                        arrow::field("32#low", arrow::boolean())});

    INFO("Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }
}

TEST_CASE("Value Comparison Operators", "[value_compare]") {
  // Create test data for different value compare scenarios
  auto timeIndex = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d},
       epoch_frame::DateTime{2020y, std::chrono::January, 5d},
       epoch_frame::DateTime{2020y, std::chrono::January, 6d}});

  auto previousIndex = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d},
       epoch_frame::DateTime{2020y, std::chrono::January, 5d}});

  auto previousData = make_dataframe<double>(
      previousIndex, {{10.0, 15.0, 12.0, 20.0, 18.0}}, {"price#price"});

  auto highestData = make_dataframe<double>(
      timeIndex, {{10.0, 15.0, 12.0, 20.0, 18.0, 25.0}}, {"price#price"});

  auto lowestData = make_dataframe<double>(
      timeIndex, {{10.0, 15.0, 8.0, 20.0, 12.0, 25.0}}, {"price#price"});

  SECTION("Previous Value Comparisons") {
    // Simple tests for Previous comparisons - these work correctly
    struct TestCase {
      std::string name;
      std::function<TransformConfiguration(std::string const &, InputVal const &, int64_t,
                                           const epoch_script::TimeFrame &)>
          configFn;
      std::vector<std::optional<bool>> expectedResults;
    };

    std::vector<TestCase> testCases;
    testCases.push_back(TestCase{"GreaterThan", previous_gt, {std::nullopt, true, false, true, false}});
    testCases.push_back(TestCase{"GreaterThanOrEqual", previous_gte, {std::nullopt, true, false, true, false}});
    testCases.push_back(TestCase{"LessThan", previous_lt, {std::nullopt, false, true, false, true}});
    testCases.push_back(TestCase{"LessThanOrEqual", previous_lte, {std::nullopt, false, true, false, true}});
    testCases.push_back(TestCase{"Equals", previous_eq, {std::nullopt, false, false, false, false}});
    testCases.push_back(TestCase{"NotEquals", previous_neq, {std::nullopt, true, true, true, true}});

    for (const auto &test : testCases) {
      SECTION(test.name) {
        TransformConfiguration config =
            test.configFn("test_id", strategy::InputValue(strategy::NodeReference("price", "price")), 1,
                          epoch_script::EpochStratifyXConstants::instance()
                              .DAILY_FREQUENCY);

        auto transformBase = MAKE_TRANSFORM(config);
        auto transform = dynamic_cast<ITransform *>(transformBase.get());

        epoch_frame::DataFrame output = transform->TransformData(previousData);

        std::vector<Scalar> expectedScalars;
        for (const auto &val : test.expectedResults) {
          if (val.has_value()) {
            expectedScalars.push_back(Scalar{val.value()});
          } else {
            expectedScalars.push_back(Scalar{});
          }
        }

        epoch_frame::DataFrame expected =
            make_dataframe(previousData.index(), {expectedScalars},
                           {"test_id#result"}, arrow::boolean());

        INFO("Previous " << test.name
                         << ": Comparing output with expected values\n"
                         << output << "\n!=\n"
                         << expected);
        REQUIRE(output.equals(expected));
      }
    }
  }

  SECTION("Highest Value Comparisons") {
    // Tests for Highest comparisons - fixing expectations based on
    // understanding the implementation
    const int lookback = 3;

    // For highestData = {10.0, 15.0, 12.0, 20.0, 18.0, 25.0}
    // Rolling max with window=3:
    // Index 0, 1: null (insufficient data)
    // Index 2: max of [10.0, 15.0, 12.0] = 15.0
    // Index 3: max of [15.0, 12.0, 20.0] = 20.0
    // Index 4: max of [12.0, 20.0, 18.0] = 20.0
    // Index 5: max of [20.0, 18.0, 25.0] = 25.0

    struct TestCase {
      std::string name;
      std::function<TransformConfiguration(std::string const &, InputVal const &, int64_t,
                                           const epoch_script::TimeFrame &)>
          configFn;
      std::vector<std::optional<bool>> expectedResults;
    };

    // Adjusted expected values based on the above calculation
    std::vector<TestCase> highestTestCases;
    highestTestCases.push_back(TestCase{"GreaterThan", highest_gt, {std::nullopt, std::nullopt, false, false, false, false}});
    highestTestCases.push_back(TestCase{"GreaterThanOrEqual", highest_gte, {std::nullopt, std::nullopt, false, true, false, true}});
    highestTestCases.push_back(TestCase{"LessThan", highest_lt, {std::nullopt, std::nullopt, true, false, true, false}});
    highestTestCases.push_back(TestCase{"LessThanOrEqual", highest_lte, {std::nullopt, std::nullopt, true, true, true, true}});
    highestTestCases.push_back(TestCase{"Equals", highest_eq, {std::nullopt, std::nullopt, false, true, false, true}});
    highestTestCases.push_back(TestCase{"NotEquals", highest_neq, {std::nullopt, std::nullopt, true, false, true, false}});

    for (const auto &test : highestTestCases) {
      SECTION(test.name) {
        TransformConfiguration config =
            test.configFn("test_id", strategy::InputValue(strategy::NodeReference("price", "price")), lookback,
                          epoch_script::EpochStratifyXConstants::instance()
                              .DAILY_FREQUENCY);

        auto transformBase = MAKE_TRANSFORM(config);
        auto transform = dynamic_cast<ITransform *>(transformBase.get());

        epoch_frame::DataFrame output = transform->TransformData(highestData);

        std::vector<Scalar> expectedScalars;
        for (const auto &val : test.expectedResults) {
          if (val.has_value()) {
            expectedScalars.push_back(Scalar{val.value()});
          } else {
            expectedScalars.push_back(Scalar{});
          }
        }

        epoch_frame::DataFrame expected =
            make_dataframe(highestData.index(), {expectedScalars},
                           {"test_id#result"}, arrow::boolean());

        INFO("Highest " << test.name
                        << ": Comparing output with expected values\n"
                        << output << "\n!=\n"
                        << expected);
        REQUIRE(output.equals(expected));
      }
    }
  }

  SECTION("Lowest Value Comparisons") {
    // Tests for Lowest comparisons - fixing expectations based on understanding
    // the implementation
    const int lookback = 3;

    // For lowestData = {10.0, 15.0, 8.0, 20.0, 12.0, 25.0}
    // Rolling min with window=3:
    // Index 0, 1: null (insufficient data)
    // Index 2: min of [10.0, 15.0, 8.0] = 8.0
    // Index 3: min of [15.0, 8.0, 20.0] = 8.0
    // Index 4: min of [8.0, 20.0, 12.0] = 8.0
    // Index 5: min of [20.0, 12.0, 25.0] = 12.0

    struct TestCase {
      std::string name;
      std::function<TransformConfiguration(std::string const &, InputVal const &, int64_t,
                                           const epoch_script::TimeFrame &)>
          configFn;
      std::vector<std::optional<bool>> expectedResults;
    };

    // Adjusted expected values based on the above calculation
    std::vector<TestCase> lowestTestCases;
    lowestTestCases.push_back(TestCase{"GreaterThan", lowest_gt, {std::nullopt, std::nullopt, false, true, true, true}});
    lowestTestCases.push_back(TestCase{"GreaterThanOrEqual", lowest_gte, {std::nullopt, std::nullopt, true, true, true, true}});
    lowestTestCases.push_back(TestCase{"LessThan", lowest_lt, {std::nullopt, std::nullopt, false, false, false, false}});
    lowestTestCases.push_back(TestCase{"LessThanOrEqual", lowest_lte, {std::nullopt, std::nullopt, true, false, false, false}});
    lowestTestCases.push_back(TestCase{"Equals", lowest_eq, {std::nullopt, std::nullopt, true, false, false, false}});
    lowestTestCases.push_back(TestCase{"NotEquals", lowest_neq, {std::nullopt, std::nullopt, false, true, true, true}});

    for (const auto &test : lowestTestCases) {
      SECTION(test.name) {
        TransformConfiguration config =
            test.configFn("test_id", strategy::InputValue(strategy::NodeReference("price", "price")), lookback,
                          epoch_script::EpochStratifyXConstants::instance()
                              .DAILY_FREQUENCY);

        auto transformBase = MAKE_TRANSFORM(config);
        auto transform = dynamic_cast<ITransform *>(transformBase.get());

        epoch_frame::DataFrame output = transform->TransformData(lowestData);

        std::vector<Scalar> expectedScalars;
        for (const auto &val : test.expectedResults) {
          if (val.has_value()) {
            expectedScalars.push_back(Scalar{val.value()});
          } else {
            expectedScalars.push_back(Scalar{});
          }
        }

        epoch_frame::DataFrame expected =
            make_dataframe(lowestData.index(), {expectedScalars},
                           {"test_id#result"}, arrow::boolean());

        INFO("Lowest " << test.name
                       << ": Comparing output with expected values\n"
                       << output << "\n!=\n"
                       << expected);
        REQUIRE(output.equals(expected));
      }
    }
  }
}

TEST_CASE("Type Casting in Equality Operators", "[equality][type_cast]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

  SECTION("Bool vs Double - Not Equal (neq)") {
    // Create a DataFrame with boolean and double columns
    epoch_frame::DataFrame input = make_dataframe(
        index,
        {
            {Scalar{true}, Scalar{false}, Scalar{true},
             Scalar{false}},                                      // bool_column
            {1.0_scalar, 0.0_scalar, 1.0_scalar, 1.0_scalar}      // double_column
        },
        {arrow::field("bool_column#bool_column", arrow::boolean()),
         arrow::field("double_column#double_column", arrow::float64())});

    TransformConfiguration config = vector_op(
        "neq", "100", strategy::InputValue(strategy::NodeReference("bool_column", "bool_column")),
        strategy::InputValue(strategy::NodeReference("double_column", "double_column")),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // true (cast to 1.0) != 1.0 => false
    // false (cast to 0.0) != 0.0 => false
    // true (cast to 1.0) != 1.0 => false
    // false (cast to 0.0) != 1.0 => true
    epoch_frame::DataFrame expected = make_dataframe<bool>(
        input.index(), {{false, false, false, true}}, {"100#result"});

    INFO("Bool vs Double (neq): Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("Bool vs Double - Equal (eq)") {
    // Create a DataFrame with boolean and double columns
    epoch_frame::DataFrame input = make_dataframe(
        index,
        {
            {Scalar{true}, Scalar{false}, Scalar{true},
             Scalar{false}},                                      // bool_column
            {1.0_scalar, 0.0_scalar, 0.0_scalar, 0.0_scalar}      // double_column
        },
        {arrow::field("bool_column#bool_column", arrow::boolean()),
         arrow::field("double_column#double_column", arrow::float64())});

    TransformConfiguration config = vector_op(
        "eq", "101", strategy::InputValue(strategy::NodeReference("bool_column", "bool_column")),
        strategy::InputValue(strategy::NodeReference("double_column", "double_column")),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // true (cast to 1.0) == 1.0 => true
    // false (cast to 0.0) == 0.0 => true
    // true (cast to 1.0) == 0.0 => false
    // false (cast to 0.0) == 0.0 => true
    epoch_frame::DataFrame expected = make_dataframe<bool>(
        input.index(), {{true, true, false, true}}, {"101#result"});

    INFO("Bool vs Double (eq): Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("Double vs Bool - Not Equal (neq) - reversed order") {
    // Test with reversed input order to ensure casting works both ways
    epoch_frame::DataFrame input = make_dataframe(
        index,
        {
            {1.0_scalar, 0.0_scalar, 1.0_scalar, 1.0_scalar},     // double_column
            {Scalar{true}, Scalar{false}, Scalar{true},
             Scalar{false}}                                       // bool_column
        },
        {arrow::field("double_column#double_column", arrow::float64()),
         arrow::field("bool_column#bool_column", arrow::boolean())});

    TransformConfiguration config = vector_op(
        "neq", "102", strategy::InputValue(strategy::NodeReference("double_column", "double_column")),
        strategy::InputValue(strategy::NodeReference("bool_column", "bool_column")),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // 1.0 (cast to true) != true => false
    // 0.0 (cast to false) != false => false
    // 1.0 (cast to true) != true => false
    // 1.0 (cast to true) != false => true
    epoch_frame::DataFrame expected = make_dataframe<bool>(
        input.index(), {{false, false, false, true}}, {"102#result"});

    INFO("Double vs Bool (neq): Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }
}

TEST_CASE("FirstNonNull Transform (Coalesce)") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

  SECTION("Basic coalesce - returns first non-null") {
    // Create DataFrame with nulls in first column (using NaN for null)
    std::vector<std::vector<double>> data = {
        {std::nan(""), 5.0, std::nan(""), std::nan("")},  // SLOT0
        {std::nan(""), std::nan(""), 10.0, 15.0},  // SLOT1
        {20.0, 25.0, 30.0, 35.0}   // SLOT2
    };
    epoch_frame::DataFrame input = make_dataframe<double>(index, data, {"SLOT0#SLOT0", "SLOT1#SLOT1", "SLOT2#SLOT2"});

    // Use typed first_non_null_number helper
    const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    TransformConfiguration config = typed_first_non_null("first_non_null_number", 200,
                                                         {strategy::InputValue(strategy::NodeReference("SLOT0", "SLOT0")),
                                                          strategy::InputValue(strategy::NodeReference("SLOT1", "SLOT1")),
                                                          strategy::InputValue(strategy::NodeReference("SLOT2", "SLOT2"))},
                                                         timeframe);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // Expected: first non-null from each row
    // Row 0: SLOT0=null, SLOT1=null, SLOT2=20.0 => 20.0
    // Row 1: SLOT0=5.0, ... => 5.0
    // Row 2: SLOT0=null, SLOT1=10.0, ... => 10.0
    // Row 3: SLOT0=null, SLOT1=15.0, ... => 15.0
    epoch_frame::DataFrame expected = make_dataframe<double>(
        index, {{20.0, 5.0, 10.0, 15.0}}, {"200#value"});

    INFO("First non-null test:\n" << output << "\n!=\n" << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("All nulls - returns null") {
    std::vector<std::vector<double>> data = {
        {std::nan(""), std::nan(""), std::nan(""), std::nan("")},  // SLOT0
        {std::nan(""), std::nan(""), std::nan(""), std::nan("")},  // SLOT1
        {std::nan(""), std::nan(""), std::nan(""), std::nan("")}   // SLOT2
    };
    epoch_frame::DataFrame input = make_dataframe<double>(index, data, {"SLOT0#SLOT0", "SLOT1#SLOT1", "SLOT2#SLOT2"});

    // Use typed first_non_null_number helper
    const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    TransformConfiguration config = typed_first_non_null("first_non_null_number", 201,
                                                         {strategy::InputValue(strategy::NodeReference("SLOT0", "SLOT0")),
                                                          strategy::InputValue(strategy::NodeReference("SLOT1", "SLOT1")),
                                                          strategy::InputValue(strategy::NodeReference("SLOT2", "SLOT2"))},
                                                         timeframe);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // All nulls => result should be null
    auto result_series = output["201#value"];
    REQUIRE(result_series.array()->null_count() == 4);
  }

  SECTION("First column has values - returns first column") {
    epoch_frame::DataFrame input = make_dataframe<double>(
        index,
        {
            {1.0, 2.0, 3.0, 4.0},    // SLOT0
            {10.0, 20.0, 30.0, 40.0},  // SLOT1
            {100.0, 200.0, 300.0, 400.0}  // SLOT2
        },
        {"SLOT0#SLOT0", "SLOT1#SLOT1", "SLOT2#SLOT2"});

    // Use typed first_non_null_number helper
    const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    TransformConfiguration config = typed_first_non_null("first_non_null_number", 202,
                                                         {strategy::InputValue(strategy::NodeReference("SLOT0", "SLOT0")),
                                                          strategy::InputValue(strategy::NodeReference("SLOT1", "SLOT1")),
                                                          strategy::InputValue(strategy::NodeReference("SLOT2", "SLOT2"))},
                                                         timeframe);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // All values in SLOT0 are non-null, so should return SLOT0
    epoch_frame::DataFrame expected = make_dataframe<double>(
        index, {{1.0, 2.0, 3.0, 4.0}}, {"202#value"});

    INFO("First column values:\n" << output << "\n!=\n" << expected);
    REQUIRE(output.equals(expected));
  }
}

TEST_CASE("ConditionalSelect Transform (Case When)") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

  SECTION("Basic case when - first condition matches") {
    using epoch_frame::Scalar;
    epoch_frame::DataFrame input = make_dataframe(
        index,
        {
            {Scalar(true), Scalar(false), Scalar(false), Scalar(false)},  // SLOT0 - condition1
            {10.0_scalar, 20.0_scalar, 30.0_scalar, 40.0_scalar},  // SLOT1 - value1
            {Scalar(false), Scalar(false), Scalar(false), Scalar(false)},  // SLOT2 - condition2
            {100.0_scalar, 200.0_scalar, 300.0_scalar, 400.0_scalar}  // SLOT3 - value2
        },
        {arrow::field("SLOT0#SLOT0", arrow::boolean()),
         arrow::field("SLOT1#SLOT1", arrow::float64()),
         arrow::field("SLOT2#SLOT2", arrow::boolean()),
         arrow::field("SLOT3#SLOT3", arrow::float64())});

    // Use typed conditional_select_number - values are numeric (float64)
    const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    TransformConfiguration config = typed_conditional_select("conditional_select_number", 300,
                                                             {strategy::InputValue(strategy::NodeReference("SLOT0", "SLOT0")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT1", "SLOT1")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT2", "SLOT2")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT3", "SLOT3"))},
                                                             timeframe);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // Row 0: condition1=true => value1=10.0
    // Rows 1-3: both conditions false => null (no default)
    std::vector<std::vector<double>> expected_data = {{10.0, std::nan(""), std::nan(""), std::nan("")}};
    epoch_frame::DataFrame expected = make_dataframe<double>(index, expected_data, {"300#value"});

    INFO("First condition matches:\n" << output << "\n!=\n" << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("Second condition matches") {
    using epoch_frame::Scalar;
    epoch_frame::DataFrame input = make_dataframe(
        index,
        {
            {Scalar(false), Scalar(false), Scalar(true), Scalar(false)},  // SLOT0 - condition1
            {10.0_scalar, 20.0_scalar, 30.0_scalar, 40.0_scalar},  // SLOT1 - value1
            {Scalar(false), Scalar(true), Scalar(false), Scalar(true)},   // SLOT2 - condition2
            {100.0_scalar, 200.0_scalar, 300.0_scalar, 400.0_scalar}  // SLOT3 - value2
        },
        {arrow::field("SLOT0#SLOT0", arrow::boolean()),
         arrow::field("SLOT1#SLOT1", arrow::float64()),
         arrow::field("SLOT2#SLOT2", arrow::boolean()),
         arrow::field("SLOT3#SLOT3", arrow::float64())});

    // Use typed conditional_select_number helper
    const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    TransformConfiguration config = typed_conditional_select("conditional_select_number", 301,
                                                             {strategy::InputValue(strategy::NodeReference("SLOT0", "SLOT0")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT1", "SLOT1")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT2", "SLOT2")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT3", "SLOT3"))},
                                                             timeframe);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // Row 0: both false => null
    // Row 1: condition2=true => value2=200.0
    // Row 2: condition1=true => value1=30.0 (first match wins)
    // Row 3: condition2=true => value2=400.0
    std::vector<std::vector<double>> expected_data = {{std::nan(""), 200.0, 30.0, 400.0}};
    epoch_frame::DataFrame expected = make_dataframe<double>(index, expected_data, {"301#value"});

    INFO("Second condition matches:\n" << output << "\n!=\n" << expected);
    REQUIRE(output.equals(expected));
  }

  SECTION("With default value") {
    using epoch_frame::Scalar;
    epoch_frame::DataFrame input = make_dataframe(
        index,
        {
            {Scalar(false), Scalar(true), Scalar(false), Scalar(false)},  // SLOT0 - condition1
            {10.0_scalar, 20.0_scalar, 30.0_scalar, 40.0_scalar},  // SLOT1 - value1
            {Scalar(false), Scalar(false), Scalar(true), Scalar(false)},  // SLOT2 - condition2
            {100.0_scalar, 200.0_scalar, 300.0_scalar, 400.0_scalar},  // SLOT3 - value2
            {999.0_scalar, 999.0_scalar, 999.0_scalar, 999.0_scalar}   // SLOT4 - default
        },
        {arrow::field("SLOT0#SLOT0", arrow::boolean()),
         arrow::field("SLOT1#SLOT1", arrow::float64()),
         arrow::field("SLOT2#SLOT2", arrow::boolean()),
         arrow::field("SLOT3#SLOT3", arrow::float64()),
         arrow::field("SLOT4#SLOT4", arrow::float64())});

    // Use typed conditional_select_number helper with default value (odd number of inputs)
    const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    TransformConfiguration config = typed_conditional_select("conditional_select_number", 302,
                                                             {strategy::InputValue(strategy::NodeReference("SLOT0", "SLOT0")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT1", "SLOT1")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT2", "SLOT2")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT3", "SLOT3")),
                                                              strategy::InputValue(strategy::NodeReference("SLOT4", "SLOT4"))},
                                                             timeframe);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    epoch_frame::DataFrame output = transform->TransformData(input);

    // Row 0: both false => default=999.0
    // Row 1: condition1=true => value1=20.0
    // Row 2: condition2=true => value2=300.0
    // Row 3: both false => default=999.0
    std::vector<std::vector<double>> expected_data = {{999.0, 20.0, 300.0, 999.0}};
    epoch_frame::DataFrame expected = make_dataframe<double>(index, expected_data, {"302#value"});

    INFO("With default value:\n" << output << "\n!=\n" << expected);
    REQUIRE(output.equals(expected));
  }
}
