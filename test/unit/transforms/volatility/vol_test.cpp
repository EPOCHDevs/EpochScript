//
// Created by adesola on 12/3/24.
//
#include "epoch_frame/factory/index_factory.h"
#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/volatility/volatility.h"
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_script/strategy/registration.h>

using namespace epoch_script;
using namespace epoch_script::transform;
using namespace epoch_script::transforms;
using namespace epoch_core;

// Helper function to create test data
epoch_frame::DataFrame createTestData() {
  // We'll define 6 daily close prices as an example
  //   index = 0..5
  //   close = [100, 101, 103, 102, 105, 110]
  std::vector<double> close = {100.0, 101.0, 103.0, 102.0, 105.0, 110.0};

  return epoch_frame::make_dataframe<double>(
      epoch_frame::factory::index::from_range(close.size()), {close},
      {epoch_script::EpochStratifyXConstants::instance().CLOSE()});
}

TEST_CASE("ReturnVolatility Transform", "[Indicator]") {
  TransformConfiguration config = rolling_volatility(
      "1", 3,
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  // Use registry to create the transform
  auto transformBase = MAKE_TRANSFORM(config);
  auto model = dynamic_cast<ITransform *>(transformBase.get());

  // Build sample input DataFrame
  epoch_frame::DataFrame input = createTestData();

  std::vector<double> expectedVol = {
      std::nan(""), std::nan(""), std::nan(""),
      0.015030, // row3 approximate
      0.020386, // row4 approximate
      0.029293  // row5 approximate
  };

  epoch_frame::DataFrame expected = epoch_frame::make_dataframe<double>(
      input.index(), {expectedVol}, {config.GetOutputId().GetColumnName()});

  // Run transform & compare
  epoch_frame::DataFrame output = model->TransformData(input);

  INFO(output << "\n!==\n" << expected);
  REQUIRE(output.equals(expected));
}

TEST_CASE("PriceDiffVolatility Transform", "[Indicator]") {
  TransformConfiguration config = price_diff_volatility(
      "2", 3,
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

  // Use registry to create the transform
  auto transformBase = MAKE_TRANSFORM(config);
  auto model = dynamic_cast<ITransform *>(transformBase.get());

  // Build sample input DataFrame
  epoch_frame::DataFrame input = createTestData();

  // Expected price diffs: [NaN, 1, 2, -1, 3, 5]
  // Rolling stddev with window=3:
  // [NaN, NaN, NaN, 1.53, 2.08, 3.06] (approximate values)
  std::vector<double> expectedVol = {
      std::nan(""), std::nan(""), std::nan(""),
      1.527525, // std dev of [1, 2, -1]
      2.081666, // std dev of [2, -1, 3]
      3.055050  // std dev of [-1, 3, 5]
  };

  epoch_frame::DataFrame expected = epoch_frame::make_dataframe<double>(
      input.index(), {expectedVol}, {config.GetOutputId().GetColumnName()});

  // Run transform & compare
  epoch_frame::DataFrame output = model->TransformData(input);

  INFO(output << "\n!==\n" << expected);
  REQUIRE(output.equals(expected));
}