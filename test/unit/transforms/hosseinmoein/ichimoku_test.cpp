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

#include "transforms/components/hosseinmoein/indicators/ichimoku.h"

using namespace epoch_frame;
using namespace epoch_script::transform;

TEST_CASE("Ichimoku", "[hosseinmoein][ichimoku]") {
  auto C = epoch_script::EpochStratifyXConstants::instance();
  auto path = std::format("{}/hmdf/IBM.csv", SMC_TEST_DATA_DIR);

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

  const int64_t p_tenkan = 9, p_kijun = 26, p_senkou_b = 52;
  auto cfg = run_op("ichimoku", "ichimoku_id",
      {},
      {{"p_tenkan", epoch_script::MetaDataOptionDefinition{static_cast<double>(p_tenkan)}},
       {"p_kijun", epoch_script::MetaDataOptionDefinition{static_cast<double>(p_kijun)}},
       {"p_senkou_b", epoch_script::MetaDataOptionDefinition{static_cast<double>(p_senkou_b)}}},
      tf);

  Ichimoku ich{cfg};
  auto out = ich.TransformData(input_df);

  auto roll_max = [&](const Series &s, int64_t w) {
    return s.rolling_agg({.window_size = w}).max();
  };
  auto roll_min = [&](const Series &s, int64_t w) {
    return s.rolling_agg({.window_size = w}).min();
  };

  auto tenkan = (roll_max(input_df[C.HIGH()], p_tenkan) +
                 roll_min(input_df[C.LOW()], p_tenkan)) *
                Scalar{0.5};
  auto kijun = (roll_max(input_df[C.HIGH()], p_kijun) +
                roll_min(input_df[C.LOW()], p_kijun)) *
               Scalar{0.5};
  auto senkou_a = (tenkan + kijun) * Scalar{0.5};
  senkou_a = senkou_a.shift(-p_kijun);
  auto senkou_b = (roll_max(input_df[C.HIGH()], p_senkou_b) +
                   roll_min(input_df[C.LOW()], p_senkou_b)) *
                  Scalar{0.5};
  senkou_b = senkou_b.shift(-p_kijun);
  auto chikou = input_df[C.CLOSE()].shift(p_kijun);

  REQUIRE(out[cfg.GetOutputId("tenkan").GetColumnName()].contiguous_array().is_equal(
      tenkan.contiguous_array()));
  REQUIRE(out[cfg.GetOutputId("kijun").GetColumnName()].contiguous_array().is_equal(
      kijun.contiguous_array()));
  REQUIRE(out[cfg.GetOutputId("senkou_a").GetColumnName()].contiguous_array().is_equal(
      senkou_a.contiguous_array()));
  REQUIRE(out[cfg.GetOutputId("senkou_b").GetColumnName()].contiguous_array().is_equal(
      senkou_b.contiguous_array()));
  REQUIRE(out[cfg.GetOutputId("chikou").GetColumnName()].contiguous_array().is_equal(
      chikou.contiguous_array()));
}
