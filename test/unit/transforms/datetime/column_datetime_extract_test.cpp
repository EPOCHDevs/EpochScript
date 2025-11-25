//
// Created by Claude Code
// Test for column_datetime_extract transform
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
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/datetime.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// Helper function to create a test dataframe with timestamp column
DataFrame createTestDataFrameWithTimestamp() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d}});

  // Create timestamp column with different dates
  std::vector<epoch_frame::DateTime> timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 15d, 14h, 30min, 45s},
      epoch_frame::DateTime{2021y, std::chrono::March, 20d, 9h, 15min, 30s},
      epoch_frame::DateTime{2022y, std::chrono::December, 31d, 23h, 59min, 59s}};

  auto price_df = make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"node#price"});
  auto timestamp_df = make_dataframe<DateTime>(index, {timestamps}, {"node#observation_date"});

  // Combine the two dataframes by merging their tables
  std::vector<arrow::ChunkedArrayPtr> combined_columns;
  std::vector<std::string> combined_names;
  for (const auto& name : price_df.column_names()) {
    combined_columns.push_back(price_df[name].array());
    combined_names.push_back(name);
  }
  for (const auto& name : timestamp_df.column_names()) {
    combined_columns.push_back(timestamp_df[name].array());
    combined_names.push_back(name);
  }

  return make_dataframe(index, combined_columns, combined_names);
}

TEST_CASE("column_datetime_extract - year component", "[datetime][extract][column]") {
  auto input = createTestDataFrameWithTimestamp();
  auto index = input.index();

  auto config = column_datetime_extract_cfg("year_extract", strategy::InputValue(strategy::NodeReference("node", "observation_date")), "year",
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

TEST_CASE("column_datetime_extract - month component", "[datetime][extract][column]") {
  auto input = createTestDataFrameWithTimestamp();

  auto config = column_datetime_extract_cfg("month_extract", strategy::InputValue(strategy::NodeReference("node", "observation_date")), "month",
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

TEST_CASE("column_datetime_extract - day component", "[datetime][extract][column]") {
  auto input = createTestDataFrameWithTimestamp();

  auto config = column_datetime_extract_cfg("day_extract", strategy::InputValue(strategy::NodeReference("node", "observation_date")), "day",
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

TEST_CASE("column_datetime_extract - time components", "[datetime][extract][column]") {
  auto input = createTestDataFrameWithTimestamp();

  SECTION("hour component") {
    auto config = column_datetime_extract_cfg("hour_extract", strategy::InputValue(strategy::NodeReference("node", "observation_date")), "hour",
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
    auto config = column_datetime_extract_cfg("minute_extract", strategy::InputValue(strategy::NodeReference("node", "observation_date")), "minute",
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
    auto config = column_datetime_extract_cfg("second_extract", strategy::InputValue(strategy::NodeReference("node", "observation_date")), "second",
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

TEST_CASE("column_datetime_extract - quarter component", "[datetime][extract][column]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d},
       epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

  std::vector<epoch_frame::DateTime> timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 15d},   // Q1
      epoch_frame::DateTime{2020y, std::chrono::April, 10d},     // Q2
      epoch_frame::DateTime{2020y, std::chrono::July, 20d},      // Q3
      epoch_frame::DateTime{2020y, std::chrono::October, 5d}};  // Q4

  auto price_df = make_dataframe<double>(index, {{10.0, 20.0, 30.0, 40.0}}, {"node#price"});
  auto timestamp_df = make_dataframe<DateTime>(index, {timestamps}, {"node#period_end"});

  std::vector<arrow::ChunkedArrayPtr> combined_columns;
  std::vector<std::string> combined_names;
  for (const auto& name : price_df.column_names()) {
    combined_columns.push_back(price_df[name].array());
    combined_names.push_back(name);
  }
  for (const auto& name : timestamp_df.column_names()) {
    combined_columns.push_back(timestamp_df[name].array());
    combined_names.push_back(name);
  }
  auto input = make_dataframe(index, combined_columns, combined_names);

  auto config = column_datetime_extract_cfg("quarter_extract", strategy::InputValue(strategy::NodeReference("node", "period_end")), "quarter",
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

TEST_CASE("column_datetime_extract - is_leap_year component", "[datetime][extract][column]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d}});

  std::vector<epoch_frame::DateTime> timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 1d},   // Leap year
      epoch_frame::DateTime{2021y, std::chrono::January, 1d},   // Not leap year
      epoch_frame::DateTime{2024y, std::chrono::January, 1d}}; // Leap year

  auto price_df = make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"node#price"});
  auto timestamp_df = make_dataframe<DateTime>(index, {timestamps}, {"node#fiscal_year_end"});

  std::vector<arrow::ChunkedArrayPtr> combined_columns;
  std::vector<std::string> combined_names;
  for (const auto& name : price_df.column_names()) {
    combined_columns.push_back(price_df[name].array());
    combined_names.push_back(name);
  }
  for (const auto& name : timestamp_df.column_names()) {
    combined_columns.push_back(timestamp_df[name].array());
    combined_names.push_back(name);
  }
  auto input = make_dataframe(index, combined_columns, combined_names);

  auto config = column_datetime_extract_cfg("leap_extract", strategy::InputValue(strategy::NodeReference("node", "fiscal_year_end")), "is_leap_year",
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

TEST_CASE("column_datetime_extract - default component is year", "[datetime][extract][column]") {
  auto input = createTestDataFrameWithTimestamp();

  // Don't specify component option - should default to year
  auto config = column_datetime_extract_cfg("default_extract", strategy::InputValue(strategy::NodeReference("node", "observation_date")), "year",
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
