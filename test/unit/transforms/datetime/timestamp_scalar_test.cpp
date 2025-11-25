//
// Created by Claude Code
// Test for timestamp_scalar transform
//

#include <epoch_script/core/constants.h>
#include <epoch_script/core/bar_attribute.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/datetime/timestamp_scalar.h"
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

// Helper function to create a simple test dataframe
DataFrame createTimestampScalarTestDataFrame() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
       epoch_frame::DateTime{2020y, std::chrono::January, 2d},
       epoch_frame::DateTime{2020y, std::chrono::January, 3d}});

  return make_dataframe<double>(index, {{10.0, 20.0, 30.0}}, {"price"});
}

TEST_CASE("timestamp_scalar - Valid timestamp string", "[datetime][scalar]") {
  auto input = createTimestampScalarTestDataFrame();

  TransformConfiguration config =
      TransformConfiguration{TransformDefinition{YAML::Load(std::format(
          R"(
type: timestamp_scalar
id: cutoff_date
options:
  value: "2020-01-01 00:00:00"
timeframe: {}
)",
          epoch_script::EpochStratifyXConstants::instance()
              .DAILY_FREQUENCY.Serialize()))}};

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  DataFrame output = transform->TransformData(input);

  // Scalar optimization: output should be single-row DataFrame
  REQUIRE(output.size() == 1);
  REQUIRE(output.contains(config.GetOutputId().GetColumnName()));

  // Verify the timestamp value is correct
  auto series = output[config.GetOutputId().GetColumnName()];
  REQUIRE(series.size() == 1);

  // Extract the timestamp value and verify it matches expected nanoseconds
  auto timestamp_ns = series.array()->GetScalar(0).ValueOrDie();
  epoch_frame::DateTime expected_dt = epoch_frame::DateTime::from_str("2020-01-01 00:00:00", "UTC");
  int64_t expected_nanos = expected_dt.m_nanoseconds.count();

  auto timestamp_scalar = std::dynamic_pointer_cast<arrow::TimestampScalar>(timestamp_ns);
  REQUIRE(timestamp_scalar != nullptr);
  REQUIRE(timestamp_scalar->value == expected_nanos);
}

TEST_CASE("timestamp_scalar - Different timestamp values", "[datetime][scalar]") {
  auto input = createTimestampScalarTestDataFrame();

  SECTION("Midday timestamp") {
    TransformConfiguration config =
        TransformConfiguration{TransformDefinition{YAML::Load(std::format(
            R"(
type: timestamp_scalar
id: event_time
options:
  value: "2021-03-15 14:30:00"
timeframe: {}
)",
            epoch_script::EpochStratifyXConstants::instance()
                .DAILY_FREQUENCY.Serialize()))}};

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());
    DataFrame output = transform->TransformData(input);

    REQUIRE(output.size() == 1);
    REQUIRE(output.contains(config.GetOutputId().GetColumnName()));

    auto timestamp_ns = output[config.GetOutputId().GetColumnName()].array()->GetScalar(0).ValueOrDie();
    epoch_frame::DateTime expected_dt = epoch_frame::DateTime::from_str("2021-03-15 14:30:00", "UTC");
    int64_t expected_nanos = expected_dt.m_nanoseconds.count();

    auto timestamp_scalar = std::dynamic_pointer_cast<arrow::TimestampScalar>(timestamp_ns);
    REQUIRE(timestamp_scalar->value == expected_nanos);
  }

  SECTION("End of day timestamp") {
    TransformConfiguration config =
        TransformConfiguration{TransformDefinition{YAML::Load(std::format(
            R"(
type: timestamp_scalar
id: close_time
options:
  value: "2022-12-31 23:59:59"
timeframe: {}
)",
            epoch_script::EpochStratifyXConstants::instance()
                .DAILY_FREQUENCY.Serialize()))}};

    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());
    DataFrame output = transform->TransformData(input);

    REQUIRE(output.size() == 1);

    auto timestamp_ns = output[config.GetOutputId().GetColumnName()].array()->GetScalar(0).ValueOrDie();
    epoch_frame::DateTime expected_dt = epoch_frame::DateTime::from_str("2022-12-31 23:59:59", "UTC");
    int64_t expected_nanos = expected_dt.m_nanoseconds.count();

    auto timestamp_scalar = std::dynamic_pointer_cast<arrow::TimestampScalar>(timestamp_ns);
    REQUIRE(timestamp_scalar->value == expected_nanos);
  }
}

TEST_CASE("timestamp_scalar - Invalid format throws error", "[datetime][scalar]") {
  auto input = createTimestampScalarTestDataFrame();

  SECTION("Date-only format (supported)") {
    // Test that date-only format (YYYY-MM-DD) is automatically converted to YYYY-MM-DD 00:00:00
    TransformConfiguration config =
        TransformConfiguration{TransformDefinition{YAML::Load(std::format(
            R"(
type: timestamp_scalar
id: date_only
options:
  value: "2020-01-01"
timeframe: {}
)",
            epoch_script::EpochStratifyXConstants::instance()
                .DAILY_FREQUENCY.Serialize()))}};

    // Should NOT throw - date-only format is supported
    REQUIRE_NOTHROW(MAKE_TRANSFORM(config));

    // Verify it produces correct timestamp value (2020-01-01 00:00:00 UTC)
    auto transform = MAKE_TRANSFORM(config);
    auto output = transform->TransformData(createTimestampScalarTestDataFrame());
    REQUIRE(output.size() == 1);
    REQUIRE(output.contains(config.GetOutputId().GetColumnName()));

    // Get the timestamp value and verify it's 2020-01-01 00:00:00 UTC
    auto column_name = output.column_names().at(0);
    Series series = output[column_name];
    auto timestamp_scalar = series.iloc(0);

    // Expected: 2020-01-01 00:00:00 UTC in nanoseconds
    auto expected_nanos = epoch_frame::DateTime::from_str("2020-01-01 00:00:00", "UTC").m_nanoseconds.count();
    auto ts = timestamp_scalar.timestamp();
    REQUIRE(ts.value == expected_nanos);
  }

  SECTION("Wrong separator (T instead of space)") {
    TransformConfiguration config =
        TransformConfiguration{TransformDefinition{YAML::Load(std::format(
            R"(
type: timestamp_scalar
id: bad_date
options:
  value: "2020-01-01T14:30:00"
timeframe: {}
)",
            epoch_script::EpochStratifyXConstants::instance()
                .DAILY_FREQUENCY.Serialize()))}};

    REQUIRE_THROWS_WITH(
        MAKE_TRANSFORM(config),
        Catch::Matchers::ContainsSubstring("Invalid timestamp format")
    );
  }

  SECTION("Invalid format string") {
    TransformConfiguration config =
        TransformConfiguration{TransformDefinition{YAML::Load(std::format(
            R"(
type: timestamp_scalar
id: bad_date
options:
  value: "not-a-date"
timeframe: {}
)",
            epoch_script::EpochStratifyXConstants::instance()
                .DAILY_FREQUENCY.Serialize()))}};

    REQUIRE_THROWS_WITH(
        MAKE_TRANSFORM(config),
        Catch::Matchers::ContainsSubstring("Invalid timestamp format")
    );
  }
}

TEST_CASE("timestamp_scalar - Returns single row (scalar optimization)", "[datetime][scalar]") {
  // This test explicitly verifies the scalar optimization behavior
  auto input = createTimestampScalarTestDataFrame(); // 3 rows

  TransformConfiguration config =
      TransformConfiguration{TransformDefinition{YAML::Load(std::format(
          R"(
type: timestamp_scalar
id: scalar_test
options:
  value: "2020-06-15 12:00:00"
timeframe: {}
)",
          epoch_script::EpochStratifyXConstants::instance()
              .DAILY_FREQUENCY.Serialize()))}};

  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());
  DataFrame output = transform->TransformData(input);

  // Key assertion: scalar returns single row, not broadcasted to input size
  REQUIRE(output.size() == 1);  // NOT 3!

  INFO("Scalar optimization: timestamp_scalar returns 1 row, not " << input.size() << " rows");
}
