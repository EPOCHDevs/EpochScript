//
// Created by Claude Code
//
#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/core/constants.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/operators/validation.h"
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// Helper function to create a test dataframe for zero/one testing
DataFrame createTestDataFrameForZeroOne() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

  // Use node#column format for column name
  return make_dataframe<double>(index, {{0.0, 1.0, 5.0, 0.0}}, {"src#value"});
}

TEST_CASE("IsZero Transform", "[validation]") {
  auto input = createTestDataFrameForZeroOne();
  auto index = input.index();
  const auto &timeframe =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  // Use input_ref with node#column reference
  auto config = is_zero_cfg("is_zero_test", input_ref("src", "value"), timeframe);
  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);
  DataFrame expected = make_dataframe<bool>(
      index, {{true, false, false, true}}, {config.GetOutputId().GetColumnName()});

  INFO("Comparing is_zero output\n" << output << "\n!=\n" << expected);
  REQUIRE(output.equals(expected));
}

TEST_CASE("IsOne Transform", "[validation]") {
  auto input = createTestDataFrameForZeroOne();
  auto index = input.index();
  const auto &timeframe =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  // Use input_ref with node#column reference
  auto config = is_one_cfg("is_one_test", input_ref("src", "value"), timeframe);
  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);
  DataFrame expected = make_dataframe<bool>(
      index, {{false, true, false, false}}, {config.GetOutputId().GetColumnName()});

  INFO("Comparing is_one output\n" << output << "\n!=\n" << expected);
  REQUIRE(output.equals(expected));
}
