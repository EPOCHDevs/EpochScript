#include <DataFrame/DataFrame.h>
#include <DataFrame/DataFrameFinancialVisitors.h>
#include <DataFrame/DataFrameTypes.h>
#include <catch.hpp>
#include <filesystem>

#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"

#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/config_helper.h>

// If you already have ADF/KPSS transforms, include their headers here
// #include "transforms/src/hosseinmoein/statistics/adf_test.h"
// #include "transforms/src/hosseinmoein/statistics/stationary_check.h"

using namespace epoch_frame;
using namespace epoch_script::transform;

TEST_CASE("KPSS vs HMDF", "[hosseinmoein][kpss]") {
  auto C = epoch_script::EpochStratifyXConstants::instance();
  auto path = std::format("{}/test_data/hmdf/IBM.csv",
                          std::filesystem::current_path().string());

  hmdf::StdDataFrame<std::string> df;
  df.read(path.c_str(), hmdf::io_format::csv2);

  auto index_arr =
      Series{factory::array::make_array(df.get_index())}.str().strptime(
          arrow::compute::StrptimeOptions{"%Y-%m-%d", arrow::TimeUnit::NANO});
  auto index = factory::index::make_index(
      index_arr.value(), MonotonicDirection::Increasing, "Date");
  auto input_df = make_dataframe<double>(
      index, {df.get_column<double>("IBM_Close")}, {C.CLOSE()});

  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  const int64_t window = 60;
  const double alpha = 0.05;

  // Build expected rolling KPSS statistic using HMDF
  const auto vals = input_df[C.CLOSE()].contiguous_array().to_vector<double>();
  std::vector<double> exp(vals.size(),
                          std::numeric_limits<double>::quiet_NaN());
  for (size_t i = 0; i < vals.size(); ++i) {
    if (i + 1 < static_cast<size_t>(window))
      continue;
    size_t start = i + 1 - static_cast<size_t>(window);
    hmdf::StdDataFrame<int64_t> tmp;
    tmp.load_index(
        hmdf::StdDataFrame<int64_t>::gen_sequence_index(0, window, 1));
    std::vector<double> w(vals.begin() + start, vals.begin() + i + 1);
    tmp.load_column("x", w);
    hmdf::StationaryCheckVisitor<double> v(alpha);
    tmp.single_act_visit<double>("x", v);
    exp[i] = v.get_result();
  }

  auto cfg = run_op("stationary_check", "kpss_id",
      {{epoch_script::ARG, {input_ref(C.CLOSE())}}},
      {{"window", epoch_script::MetaDataOptionDefinition{static_cast<double>(window)}},
       {"alpha", epoch_script::MetaDataOptionDefinition{alpha}}},
      tf);

  // StationaryCheck transform must exist and output 'result'
  StationaryCheck kpss{cfg};
  auto out = kpss.TransformData(input_df);
  auto lhs = out[cfg.GetOutputId("result")].contiguous_array();
  auto rhs = Array{factory::array::make_contiguous_array(exp)};
  REQUIRE(lhs.is_equal(rhs));
}

TEST_CASE("ADF vs HMDF", "[hosseinmoein][adf]") {
  // If you add an ADF transform, mirror KPSS structure here comparing to HMDF
  // ADF visitor
  SUCCEED("ADF test placeholder: implement once ADF transform is available");
}
