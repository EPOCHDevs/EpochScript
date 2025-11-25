//
// Created by adesola on 12/3/24.
//
#include <epoch_script/core/bar_attribute.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/cummulative/cum_op.h"
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

TEST_CASE("Cummulative Transforms") {
    strategy::NodeReference ref("", "input_column");
  SECTION("CumProdOperation") {
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
         epoch_frame::DateTime{2020y, std::chrono::January, 2d},
         epoch_frame::DateTime{2020y, std::chrono::January, 3d},
         epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

    // Shared Setup: Define an input DataFrame with a numeric column
    epoch_frame::DataFrame input =
        make_dataframe<double>(index, {{1.0, 2.0, 3.0, 4.0}}, {ref.GetColumnName()});

    TransformConfiguration config = cum_prod(
        "20", strategy::InputValue(strategy::NodeReference(ref)),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    // Use registry to create the transform
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<CumProdOperation *>(transformBase.get());

    // Expected Output: Cumulative product [1.0, 2.0, 6.0, 24.0]
    epoch_frame::DataFrame expected = make_dataframe<double>(
        index, {{1.0, 2.0, 6.0, 24.0}}, {config.GetOutputId().GetColumnName()});

    // Apply transform
    epoch_frame::DataFrame output = transform->TransformData(input);

    // Verify output
    INFO("Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected));
  }
}