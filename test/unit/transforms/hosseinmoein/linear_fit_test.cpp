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

#include "transforms/components/hosseinmoein/statistics/linear_fit.h"

using namespace epoch_frame;
using namespace epoch_script::transform;

TEST_CASE("LinearFit rolling (slope/intercept/residual)",
          "[hosseinmoein][linear_fit]") {
  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  const int64_t window = 20;
  auto cfg = run_op("linear_fit", "linfit_id",
      {{"x", {input_ref("x")}}, {"y", {input_ref("y")}}},
      {{"window", epoch_script::MetaDataOptionDefinition{static_cast<double>(window)}}},
      tf);

  // Build synthetic x, y with UTC datetime index (required by LinearFit)
  const size_t N = 200;
  constexpr int64_t BASE_NS = 1577836800000000000LL; // 2020-01-01 UTC in nanoseconds
  constexpr int64_t DAY_NS = 86400000000000LL;
  std::vector<int64_t> ticks(N);
  for (size_t i = 0; i < N; ++i) {
    ticks[i] = BASE_NS + static_cast<int64_t>(i) * DAY_NS;
  }
  auto index = factory::index::make_datetime_index(ticks, "i", "UTC");
  std::vector<double> xvec(N), yvec(N);
  for (size_t i = 0; i < N; ++i) {
    xvec[i] = static_cast<double>(i);
    yvec[i] = 2.0 * xvec[i] + 3.0;
  }
  auto df_xy = make_dataframe<double>(index, {xvec, yvec}, {"#x", "#y"});

  LinearFit lin{cfg};
  auto out = lin.TransformData(df_xy);

  // Expected with HMDF windowed linfit
  std::vector<double> slope_exp(N, std::numeric_limits<double>::quiet_NaN());
  std::vector<double> intercept_exp(N,
                                    std::numeric_limits<double>::quiet_NaN());
  std::vector<double> residual_exp(N, std::numeric_limits<double>::quiet_NaN());

  for (size_t i = 0; i < N; ++i) {
    size_t start =
        (i + 1 >= static_cast<size_t>(window)) ? (i + 1 - window) : 0;
    std::vector<double> xw(xvec.begin() + start, xvec.begin() + i + 1);
    std::vector<double> yw(yvec.begin() + start, yvec.begin() + i + 1);
    hmdf::StdDataFrame<int64_t> tmp;
    tmp.load_index(
        hmdf::StdDataFrame<int64_t>::gen_sequence_index(0, xw.size(), 1));
    tmp.load_column("x", xw);
    tmp.load_column("y", yw);
    hmdf::linfit_v<double, int64_t> v;
    tmp.single_act_visit<double, double>("x", "y", v);
    slope_exp[i] = v.get_slope();
    intercept_exp[i] = v.get_intercept();
    residual_exp[i] = v.get_residual();
  }

  CHECK(out[cfg.GetOutputId("slope").GetColumnName()].contiguous_array().is_equal(
      Array{factory::array::make_contiguous_array(slope_exp)}));
  CHECK(out[cfg.GetOutputId("intercept").GetColumnName()].contiguous_array().is_equal(
      Array{factory::array::make_contiguous_array(intercept_exp)}));
  CHECK(out[cfg.GetOutputId("residual").GetColumnName()].contiguous_array().is_equal(
      Array{factory::array::make_contiguous_array(residual_exp)}));
}
