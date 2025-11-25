//
// Created by Claude Code
// Unit tests for StaticCast transforms
//

#include <catch2/catch_test_macros.hpp>
#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/core/constants.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/operators/static_cast.h"
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <arrow/type.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

TEST_CASE("[static_cast] StaticCastToInteger with normal input", "[operators]") {
  const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  // Create input with integers
  auto index = epoch_frame::factory::index::make_datetime_index({
    epoch_frame::DateTime{2024y, std::chrono::January, 1d},
    epoch_frame::DateTime{2024y, std::chrono::January, 2d},
    epoch_frame::DateTime{2024y, std::chrono::January, 3d}
  });
  strategy::NodeReference in{"node", "input"};

  auto input_df = make_dataframe<int64_t>(index, {{10, 20, 30}}, {in.GetColumnName()});

  auto config = static_cast_to_integer_cfg("static_cast_test", in, timeframe);
  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  auto result = transform->TransformData(input_df);

  REQUIRE(result.num_rows() == 3);
  REQUIRE(result["static_cast_test#result"].array()->type()->id() == arrow::Type::INT64);
  REQUIRE(result["static_cast_test#result"].iloc(0).as_int64() == 10);
  REQUIRE(result["static_cast_test#result"].iloc(1).as_int64() == 20);
  REQUIRE(result["static_cast_test#result"].iloc(2).as_int64()== 30);
}

TEST_CASE("[static_cast] StaticCastToInteger with null type input", "[operators]") {
  const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  strategy::NodeReference in{"node", "input"};

  // Create input with null type (simulates empty DataFrame from missing data)
  auto index = epoch_frame::factory::index::make_datetime_index({
    epoch_frame::DateTime{2024y, std::chrono::January, 1d},
    epoch_frame::DateTime{2024y, std::chrono::January, 2d},
    epoch_frame::DateTime{2024y, std::chrono::January, 3d}
  });
  auto null_array = arrow::MakeArrayOfNull(arrow::null(), 3).ValueOrDie();
  auto chunked_null = std::make_shared<arrow::ChunkedArray>(null_array);
  auto input_df = epoch_frame::make_dataframe(index, {chunked_null}, {in.GetColumnName()});


  auto config = static_cast_to_integer_cfg("static_cast_test", in, timeframe);
  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  auto result = transform->TransformData(input_df);

  REQUIRE(result.num_rows() == 3);
  // Should have converted to INT64 type with all nulls
  REQUIRE(result["static_cast_test#result"].array()->type()->id() == arrow::Type::INT64);
  REQUIRE(result["static_cast_test#result"].iloc(0).is_null());
  REQUIRE(result["static_cast_test#result"].iloc(1).is_null());
  REQUIRE(result["static_cast_test#result"].iloc(2).is_null());
}

TEST_CASE("[static_cast] StaticCastToDecimal with normal input", "[operators]") {
  const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto index = epoch_frame::factory::index::make_datetime_index({
    epoch_frame::DateTime{2024y, std::chrono::January, 1d},
    epoch_frame::DateTime{2024y, std::chrono::January, 2d}
  });
  strategy::NodeReference in{"node", "input"};

  auto input_df = make_dataframe<double>(index, {{1.5, 2.5}}, {in.GetColumnName()});

  auto config = static_cast_to_decimal_cfg("static_cast_test", in, timeframe);
  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  auto result = transform->TransformData(input_df);

  REQUIRE(result.num_rows() == 2);
  REQUIRE(result["static_cast_test#result"].array()->type()->id() == arrow::Type::DOUBLE);
  REQUIRE(result["static_cast_test#result"].iloc(0).as_double() == 1.5);
  REQUIRE(result["static_cast_test#result"].iloc(1).as_double() == 2.5);
}

TEST_CASE("[static_cast] StaticCastToBoolean with null type input", "[operators]") {
  const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  strategy::NodeReference in{"node", "input"};

  auto index = epoch_frame::factory::index::make_datetime_index({
    epoch_frame::DateTime{2024y, std::chrono::January, 1d},
    epoch_frame::DateTime{2024y, std::chrono::January, 2d}
  });
  auto null_array = arrow::MakeArrayOfNull(arrow::null(), 2).ValueOrDie();
  auto chunked_null = std::make_shared<arrow::ChunkedArray>(null_array);
  auto input_df = epoch_frame::make_dataframe(index, {chunked_null}, {in.GetColumnName()});

  auto config = static_cast_to_boolean_cfg("static_cast_test", in, timeframe);
  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  auto result = transform->TransformData(input_df);

  REQUIRE(result.num_rows() == 2);
  REQUIRE(result["static_cast_test#result"].array()->type()->id() == arrow::Type::BOOL);
  REQUIRE(result["static_cast_test#result"].iloc(0).is_null());
  REQUIRE(result["static_cast_test#result"].iloc(1).is_null());
}

TEST_CASE("[static_cast] StaticCastToString with null type input", "[operators]") {
  const auto &timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  strategy::NodeReference in{"node", "input"};

  auto index = epoch_frame::factory::index::make_datetime_index({
    epoch_frame::DateTime{2024y, std::chrono::January, 1d}
  });
  auto null_array = arrow::MakeArrayOfNull(arrow::null(), 1).ValueOrDie();
  auto chunked_null = std::make_shared<arrow::ChunkedArray>(null_array);
  auto input_df = epoch_frame::make_dataframe(index, {chunked_null}, {in.GetColumnName()});

  auto config = static_cast_to_string_cfg("static_cast_test", in, timeframe);
  auto transformBase = MAKE_TRANSFORM(config);
  auto transform = dynamic_cast<ITransform *>(transformBase.get());

  auto result = transform->TransformData(input_df);

  REQUIRE(result.num_rows() == 1);
  REQUIRE(result["static_cast_test#result"].array()->type()->id() == arrow::Type::STRING);
  REQUIRE(result["static_cast_test#result"].iloc(0).is_null());
}
