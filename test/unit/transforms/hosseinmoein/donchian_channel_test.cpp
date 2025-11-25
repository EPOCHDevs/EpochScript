#include <DataFrame/DataFrame.h>
#include <DataFrame/DataFrameFinancialVisitors.h>
#include <DataFrame/DataFrameTypes.h>
#include <catch.hpp>
#include <filesystem>

#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/scalar.h"

#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/config_helper.h>

#include "transforms/components/hosseinmoein/indicators/donchian_channel.h"

using namespace epoch_frame;
using namespace epoch_script::transform;

TEST_CASE("DonchianChannel", "[hosseinmoein][donchian]") {
  auto C = epoch_script::EpochStratifyXConstants::instance();
  auto path = std::format("{}/hmdf/IBM.csv",
                          SMC_TEST_DATA_DIR);

  hmdf::StdDataFrame<std::string> df;
  df.read(path.c_str(), hmdf::io_format::csv2);

  auto index_arr =
      Series{factory::array::make_array(df.get_index())}.str().strptime(
          arrow::compute::StrptimeOptions{"%Y-%m-%d", arrow::TimeUnit::NANO});
  auto index = factory::index::make_index(
      index_arr.value(), MonotonicDirection::Increasing, "Date");
  auto vol = df.get_column<int64_t>("IBM_Volume");
  auto input_df = make_dataframe<double>(
      index,
      {df.get_column<double>("IBM_Close"), df.get_column<double>("IBM_High"),
       df.get_column<double>("IBM_Low"), df.get_column<double>("IBM_Open"),
       std::vector<double>(vol.begin(), vol.end())},
      {C.CLOSE(), C.HIGH(), C.LOW(), C.OPEN(), C.VOLUME()});

  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  const int64_t window = 20;
  auto cfg = run_op("donchian_channel", "donchian_id",
      {},
      {{"window", epoch_script::MetaDataOptionDefinition{static_cast<double>(window)}}},
      tf);

  DonchianChannel dc{cfg};
  auto out = dc.TransformData(input_df);

  auto upper = input_df[C.HIGH()].rolling_agg({.window_size = window}).max();
  auto lower = input_df[C.LOW()].rolling_agg({.window_size = window}).min();
  auto middle = (upper + lower) * Scalar{0.5};

  REQUIRE(out[cfg.GetOutputId("bbands_upper").GetColumnName()].contiguous_array().is_equal(
      upper.contiguous_array()));
  REQUIRE(out[cfg.GetOutputId("bbands_lower").GetColumnName()].contiguous_array().is_equal(
      lower.contiguous_array()));
  REQUIRE(out[cfg.GetOutputId("bbands_middle").GetColumnName()].contiguous_array().is_equal(
      middle.contiguous_array()));
}
