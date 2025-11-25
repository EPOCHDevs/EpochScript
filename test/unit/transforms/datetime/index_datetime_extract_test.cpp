//
// Created by Claude Code
// Test for index_datetime_extract transform
//

#include <epoch_script/core/constants.h>
#include <epoch_script/core/bar_attribute.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/datetime/index_datetime_extract.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/datetime.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// Helper function to create a test dataframe with specific dates
DataFrame createIndexDatetimeExtractTestDataFrame() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 15d, 14h, 30min, 45s},
       epoch_frame::DateTime{2021y, std::chrono::March, 20d, 9h, 15min, 30s},
       epoch_frame::DateTime{2022y, std::chrono::December, 31d, 23h, 59min, 59s}});

  return make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"price"});
}

TEST_CASE("index_datetime_extract - year component", "[datetime][extract][index]") {
  auto input = createIndexDatetimeExtractTestDataFrame();
  auto index = input.index();

  auto config = index_datetime_extract_cfg("year_extract", "year",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  REQUIRE(output.size() == 3);
  REQUIRE(output.contains(config.GetOutputId().GetColumnName()));

  auto series = output[config.GetOutputId().GetColumnName()];
  REQUIRE(series.size() == 3);

  // Verify year values
  auto year_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());
  REQUIRE(year_array->Value(0) == 2020);
  REQUIRE(year_array->Value(1) == 2021);
  REQUIRE(year_array->Value(2) == 2022);
}

TEST_CASE("index_datetime_extract - month component", "[datetime][extract][index]") {
  auto input = createIndexDatetimeExtractTestDataFrame();

  auto config = index_datetime_extract_cfg("month_extract", "month",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto month_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  // January=1, March=3, December=12
  REQUIRE(month_array->Value(0) == 1);
  REQUIRE(month_array->Value(1) == 3);
  REQUIRE(month_array->Value(2) == 12);
}

TEST_CASE("index_datetime_extract - day component", "[datetime][extract][index]") {
  auto input = createIndexDatetimeExtractTestDataFrame();

  auto config = index_datetime_extract_cfg("day_extract", "day",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto day_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  REQUIRE(day_array->Value(0) == 15);
  REQUIRE(day_array->Value(1) == 20);
  REQUIRE(day_array->Value(2) == 31);
}

TEST_CASE("index_datetime_extract - time components", "[datetime][extract][index]") {
  auto input = createIndexDatetimeExtractTestDataFrame();

  SECTION("hour component") {
    auto config = index_datetime_extract_cfg("hour_extract", "hour",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());
    DataFrame output = transform->TransformData(input);

    auto series = output[config.GetOutputId().GetColumnName()];
    auto hour_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

    REQUIRE(hour_array->Value(0) == 14);
    REQUIRE(hour_array->Value(1) == 9);
    REQUIRE(hour_array->Value(2) == 23);
  }

  SECTION("minute component") {
    auto config = index_datetime_extract_cfg("minute_extract", "minute",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());
    DataFrame output = transform->TransformData(input);

    auto series = output[config.GetOutputId().GetColumnName()];
    auto minute_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

    REQUIRE(minute_array->Value(0) == 30);
    REQUIRE(minute_array->Value(1) == 15);
    REQUIRE(minute_array->Value(2) == 59);
  }

  SECTION("second component") {
    auto config = index_datetime_extract_cfg("second_extract", "second",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());
    DataFrame output = transform->TransformData(input);

    auto series = output[config.GetOutputId().GetColumnName()];
    auto second_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

    REQUIRE(second_array->Value(0) == 45);
    REQUIRE(second_array->Value(1) == 30);
    REQUIRE(second_array->Value(2) == 59);
  }
}

TEST_CASE("index_datetime_extract - day_of_week component", "[datetime][extract][index]") {
  // Create index with known weekdays
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2025y, std::chrono::January, 6d},   // Monday
       epoch_frame::DateTime{2025y, std::chrono::January, 10d},  // Friday
       epoch_frame::DateTime{2025y, std::chrono::January, 12d}}); // Sunday

  auto input = make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"price"});

  auto config = index_datetime_extract_cfg("dow_extract", "day_of_week",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto dow_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  // ISO standard: Monday=0, Sunday=6
  REQUIRE(dow_array->Value(0) == 0);  // Monday
  REQUIRE(dow_array->Value(1) == 4);  // Friday
  REQUIRE(dow_array->Value(2) == 6);  // Sunday
}

TEST_CASE("index_datetime_extract - quarter component", "[datetime][extract][index]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 15d},   // Q1
       epoch_frame::DateTime{2020y, std::chrono::April, 10d},     // Q2
       epoch_frame::DateTime{2020y, std::chrono::July, 20d},      // Q3
       epoch_frame::DateTime{2020y, std::chrono::October, 5d}});  // Q4

  auto input = make_dataframe<double>(index, {{10.0, 20.0, 30.0, 40.0}}, {"price"});

  auto config = index_datetime_extract_cfg("quarter_extract", "quarter",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto quarter_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  REQUIRE(quarter_array->Value(0) == 1);
  REQUIRE(quarter_array->Value(1) == 2);
  REQUIRE(quarter_array->Value(2) == 3);
  REQUIRE(quarter_array->Value(3) == 4);
}

TEST_CASE("index_datetime_extract - is_leap_year component", "[datetime][extract][index]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},   // Leap year
       epoch_frame::DateTime{2021y, std::chrono::January, 1d},   // Not leap year
       epoch_frame::DateTime{2024y, std::chrono::January, 1d}}); // Leap year

  auto input = make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"price"});

  auto config = index_datetime_extract_cfg("leap_extract", "is_leap_year",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto leap_array = std::static_pointer_cast<arrow::BooleanArray>(series.contiguous_array().value());

  REQUIRE(leap_array->Value(0) == true);   // 2020 is leap year
  REQUIRE(leap_array->Value(1) == false);  // 2021 is not leap year
  REQUIRE(leap_array->Value(2) == true);   // 2024 is leap year
}

TEST_CASE("index_datetime_extract - day_of_year component", "[datetime][extract][index]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},     // Day 1
       epoch_frame::DateTime{2020y, std::chrono::February, 29d},   // Day 60 (leap year)
       epoch_frame::DateTime{2020y, std::chrono::December, 31d}}); // Day 366

  auto input = make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"price"});

  auto config = index_datetime_extract_cfg("doy_extract", "day_of_year",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto doy_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  REQUIRE(doy_array->Value(0) == 1);
  REQUIRE(doy_array->Value(1) == 60);
  REQUIRE(doy_array->Value(2) == 366);
}

TEST_CASE("index_datetime_extract - week component", "[datetime][extract][index]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 6d},    // Week 2
       epoch_frame::DateTime{2020y, std::chrono::June, 15d},      // Week 25
       epoch_frame::DateTime{2020y, std::chrono::December, 28d}}); // Week 53

  auto input = make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"price"});

  auto config = index_datetime_extract_cfg("week_extract", "week",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto week_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  // ISO weeks
  REQUIRE(week_array->Value(0) == 2);
  REQUIRE(week_array->Value(1) == 25);
  REQUIRE(week_array->Value(2) == 53);
}

TEST_CASE("index_datetime_extract - default component is year", "[datetime][extract][index]") {
  auto input = createIndexDatetimeExtractTestDataFrame();

  // Don't specify component option - should default to year
  auto config = index_datetime_extract_cfg("default_extract", "year",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto year_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  // Should extract year by default
  REQUIRE(year_array->Value(0) == 2020);
  REQUIRE(year_array->Value(1) == 2021);
  REQUIRE(year_array->Value(2) == 2022);
}
