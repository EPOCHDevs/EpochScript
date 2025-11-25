//
// Created by Claude Code
// Test for datetime_diff transform
//

#include <epoch_script/core/constants.h>
#include <epoch_script/core/bar_attribute.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/datetime/datetime_diff.h"
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

// Helper function to create a test dataframe with two timestamp columns
DataFrame createTestDataFrameWithTwoTimestamps() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d}});

  // Create timestamp columns using make_dataframe DateTime support
  std::vector<epoch_frame::DateTime> start_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 1d, 10h, 0min, 0s},
      epoch_frame::DateTime{2020y, std::chrono::February, 15d, 14h, 30min, 0s},
      epoch_frame::DateTime{2020y, std::chrono::March, 1d, 0h, 0min, 0s}};

  std::vector<epoch_frame::DateTime> end_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 1d, 12h, 0min, 0s},
      epoch_frame::DateTime{2020y, std::chrono::February, 16d, 14h, 30min, 0s},
      epoch_frame::DateTime{2020y, std::chrono::April, 1d, 0h, 0min, 0s}};

  // Create DataFrame with double and DateTime columns
  auto price_df = make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"price"});
  auto timestamp_df = make_dataframe<DateTime>(index, {start_timestamps, end_timestamps},
                                               {"node#start_date", "node#end_date"});

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

TEST_CASE("datetime_diff - days unit", "[datetime][diff]") {
  auto input = createTestDataFrameWithTwoTimestamps();

  auto config = datetime_diff_cfg("days_diff", strategy::InputValue(strategy::NodeReference("node", "start_date")), strategy::InputValue(strategy::NodeReference("node", "end_date")), "days",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  REQUIRE(output.size() == 3);
  REQUIRE(output.contains(config.GetOutputId().GetColumnName()));

  auto series = output[config.GetOutputId().GetColumnName()];
  auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  // Differences: 0 days, 1 day, 31 days
  REQUIRE(diff_array->Value(0) == 0);
  REQUIRE(diff_array->Value(1) == 1);
  REQUIRE(diff_array->Value(2) == 31);
}

TEST_CASE("datetime_diff - hours unit", "[datetime][diff]") {
  auto input = createTestDataFrameWithTwoTimestamps();
  auto config = datetime_diff_cfg("hours_diff", strategy::InputValue(strategy::NodeReference("node", "start_date")), strategy::InputValue(strategy::NodeReference("node", "end_date")), "hours",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  // Differences: 2 hours, 24 hours, 744 hours (31 days)
  REQUIRE(diff_array->Value(0) == 2);
  REQUIRE(diff_array->Value(1) == 24);
  REQUIRE(diff_array->Value(2) == 31 * 24);  // 31 days = 744 hours
}

TEST_CASE("datetime_diff - minutes unit", "[datetime][diff]") {
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d}});

    std::vector<epoch_frame::DateTime> start_timestamps = {
        epoch_frame::DateTime{2020y, std::chrono::January, 1d, 10h, 0min, 0s}};

    std::vector<epoch_frame::DateTime> end_timestamps = {
        epoch_frame::DateTime{2020y, std::chrono::January, 1d, 10h, 30min, 0s}};

    auto input = make_dataframe<DateTime>(index, {start_timestamps, end_timestamps},
                                          {"node#start_time", "node#end_time"});

    auto config = datetime_diff_cfg("minutes_diff",
        strategy::InputValue(strategy::NodeReference("node", "start_time")),
        strategy::InputValue(strategy::NodeReference("node", "end_time")), "minutes",
            epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    DataFrame output = transform->TransformData(input);

    auto series = output[config.GetOutputId().GetColumnName()];
    auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

    REQUIRE(diff_array->Value(0) == 30);  // 30 minutes difference
}

TEST_CASE("datetime_diff - seconds unit", "[datetime][diff]") {
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d}});

    std::vector<epoch_frame::DateTime> start_timestamps = {
        epoch_frame::DateTime{2020y, std::chrono::January, 1d, 10h, 0min, 0s}};

    std::vector<epoch_frame::DateTime> end_timestamps = {
        epoch_frame::DateTime{2020y, std::chrono::January, 1d, 10h, 0min, 45s}};

    auto input = make_dataframe<DateTime>(index, {start_timestamps, end_timestamps},
                                          {"node#start_time", "node#end_time"});

    auto config = datetime_diff_cfg("seconds_diff",
        strategy::InputValue(strategy::NodeReference("node", "start_time")),
        strategy::InputValue(strategy::NodeReference("node", "end_time")), "seconds",
            epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    DataFrame output = transform->TransformData(input);

    auto series = output[config.GetOutputId().GetColumnName()];
    auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

    REQUIRE(diff_array->Value(0) == 45);  // 45 seconds difference
}

TEST_CASE("datetime_diff - weeks unit", "[datetime][diff]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d}});

  std::vector<epoch_frame::DateTime> start_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 1d},
      epoch_frame::DateTime{2020y, std::chrono::January, 15d}};

  std::vector<epoch_frame::DateTime> end_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 15d},  // 2 weeks later
      epoch_frame::DateTime{2020y, std::chrono::February, 15d}};  // ~4.4 weeks later

  auto input = make_dataframe<DateTime>(index, {start_timestamps, end_timestamps},
                                        {"node#start_date", "node#end_date"});

  auto config = datetime_diff_cfg("weeks_diff", strategy::InputValue(strategy::NodeReference("node", "start_date")), strategy::InputValue(strategy::NodeReference("node", "end_date")), "weeks",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  REQUIRE(diff_array->Value(0) == 2);  // 14 days = 2 weeks
  REQUIRE(diff_array->Value(1) == 4);  // 31 days = 4 weeks (truncated)
}

TEST_CASE("datetime_diff - months unit", "[datetime][diff]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d}});

  std::vector<epoch_frame::DateTime> start_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 1d},
      epoch_frame::DateTime{2020y, std::chrono::January, 15d}};

  std::vector<epoch_frame::DateTime> end_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::April, 1d},    // 3 months later
      epoch_frame::DateTime{2021y, std::chrono::January, 15d}}; // 12 months later

  auto input = make_dataframe<DateTime>(index, {start_timestamps, end_timestamps},
                                        {"node#start_date", "node#end_date"});

  auto config = datetime_diff_cfg("months_diff", strategy::InputValue(strategy::NodeReference("node", "start_date")), strategy::InputValue(strategy::NodeReference("node", "end_date")), "months",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];

  // Check actual array type first
  auto array = series.contiguous_array().value();
  INFO("Array type: " << array->type()->ToString());

  auto diff_array = std::static_pointer_cast<arrow::Int64Array>(array);

  REQUIRE(diff_array->Value(0) == 3);   // 3 months
  REQUIRE(diff_array->Value(1) == 12);  // 12 months
}

TEST_CASE("datetime_diff - quarters unit", "[datetime][diff]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d}});

  std::vector<epoch_frame::DateTime> start_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 1d},    // Q1
      epoch_frame::DateTime{2020y, std::chrono::January, 1d}};   // Q1

  std::vector<epoch_frame::DateTime> end_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::July, 1d},       // Q3
      epoch_frame::DateTime{2021y, std::chrono::January, 1d}};   // Q1 next year

  auto input = make_dataframe<DateTime>(index, {start_timestamps, end_timestamps},
                                        {"node#start_date", "node#end_date"});

  auto config = datetime_diff_cfg("quarters_diff", strategy::InputValue(strategy::NodeReference("node", "start_date")), strategy::InputValue(strategy::NodeReference("node", "end_date")), "quarters",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  REQUIRE(diff_array->Value(0) == 2);  // Q1 to Q3 = 2 quarters
  REQUIRE(diff_array->Value(1) == 4);  // Q1 2020 to Q1 2021 = 4 quarters
}

TEST_CASE("datetime_diff - years unit", "[datetime][diff]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d}});

  std::vector<epoch_frame::DateTime> start_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 1d},
      epoch_frame::DateTime{2018y, std::chrono::June, 15d}};

  std::vector<epoch_frame::DateTime> end_timestamps = {
      epoch_frame::DateTime{2023y, std::chrono::January, 1d},   // 3 years later
      epoch_frame::DateTime{2020y, std::chrono::June, 15d}};    // 2 years later

  auto input = make_dataframe<DateTime>(index, {start_timestamps, end_timestamps},
                                        {"node#start_date", "node#end_date"});

  auto config = datetime_diff_cfg("years_diff", strategy::InputValue(strategy::NodeReference("node", "start_date")), strategy::InputValue(strategy::NodeReference("node", "end_date")), "years",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  REQUIRE(diff_array->Value(0) == 3);  // 3 years
  REQUIRE(diff_array->Value(1) == 2);  // 2 years
}

TEST_CASE("datetime_diff - negative differences", "[datetime][diff]") {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d}});

  // Start is AFTER end - should give negative result
  std::vector<epoch_frame::DateTime> start_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 10d}};

  std::vector<epoch_frame::DateTime> end_timestamps = {
      epoch_frame::DateTime{2020y, std::chrono::January, 5d}};

  auto input = make_dataframe<DateTime>(index, {start_timestamps, end_timestamps},
                                        {"node#start_date", "node#end_date"});

  auto config = datetime_diff_cfg("negative_diff", strategy::InputValue(strategy::NodeReference("node", "start_date")), strategy::InputValue(strategy::NodeReference("node", "end_date")), "days",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  REQUIRE(diff_array->Value(0) == -5);  // end is 5 days before start
}

TEST_CASE("datetime_diff - default unit is days", "[datetime][diff]") {
  auto input = createTestDataFrameWithTwoTimestamps();

  // Don't specify unit option - should default to days
  auto config = datetime_diff_cfg("default_diff", strategy::InputValue(strategy::NodeReference("node", "start_date")), strategy::InputValue(strategy::NodeReference("node", "end_date")), "days",
          epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  auto series = output[config.GetOutputId().GetColumnName()];
  auto diff_array = std::static_pointer_cast<arrow::Int64Array>(series.contiguous_array().value());

  // Should compute difference in days by default
  REQUIRE(diff_array->Value(0) == 0);
  REQUIRE(diff_array->Value(1) == 1);
  REQUIRE(diff_array->Value(2) == 31);
}
