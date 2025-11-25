#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/transforms/core/config_helper.h>
#include "transforms/components/hosseinmoein/volatility/volatility.h"
#include <DataFrame/DataFrame.h>
#include <DataFrame/DataFrameFinancialVisitors.h>
#include <catch.hpp>
#include <epoch_frame/factory/index_factory.h>

#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>

TEST_CASE("VolatilityTest", "[volatility]") {
  using namespace hmdf;
  using namespace epoch_frame;
  using namespace epoch_script::transform;

  auto C = epoch_script::EpochStratifyXConstants::instance();
  auto path = std::format("{}/hmdf/IBM.csv",
                          SMC_TEST_DATA_DIR);

  StdDataFrame<std::string> df;
  df.read(path.c_str(), io_format::csv2);

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

  // Test parameters
  int64_t period = 20;
  int64_t trading_days = 252;
  double multiplier = 4.0;
  double band_multiplier = 2.0;
  int64_t roll_period = 20;
  int64_t ulcer_period = 14;
  auto timeframe = C.DAILY_FREQUENCY;

  SECTION("Acceleration Bands") {
    aband_v<double, std::string> abands;
    df.single_act_visit<double, double, double>("IBM_Close", "IBM_High",
                                                "IBM_Low", abands);
    const auto cfg = abands_cfg("abands_id", period, multiplier, timeframe);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto accelerationBand =
        dynamic_cast<AccelerationBands *>(transformBase.get());
    REQUIRE(accelerationBand);

    auto result = accelerationBand->TransformData(input_df);
    for (std::string const &col : {"upper_band", "middle_band", "lower_band"}) {
      auto lhs = result[cfg.GetOutputId(col).GetColumnName()].contiguous_array();

      std::vector<double> rhs;
      if (col == "upper_band") {
        rhs = abands.get_upper_band();
      } else if (col == "middle_band") {
        rhs = abands.get_result();
      } else if (col == "lower_band") {
        rhs = abands.get_lower_band();
      }
      auto rhs_arr = Array(factory::array::make_contiguous_array(rhs));

      INFO(lhs->Diff(*rhs_arr.value()));
      CHECK(lhs.is_equal(rhs_arr));
    }
  }

  SECTION("Garman Klass") {
    gk_vol_v_t gk_vol(period, trading_days);
    df.single_act_visit<double, double, double, double>(
        "IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", gk_vol);

    const auto cfg = garman_klass_cfg("gk_id", period, trading_days, timeframe);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto garmanKlass = dynamic_cast<GarmanKlass *>(transformBase.get());
    REQUIRE(garmanKlass);

    auto result = garmanKlass->TransformData(input_df);
    auto lhs = result[cfg.GetOutputId().GetColumnName()].contiguous_array();
    auto rhs =
        Array(factory::array::make_contiguous_array(gk_vol.get_result()));

    INFO(lhs->Diff(*rhs.value()));
    CHECK(lhs.is_equal(rhs));
  }

  SECTION("Hodges Tompkins") {
    hodges_tompkins_vol_v ht_vol(period, trading_days);
    df.single_act_visit<double>("IBM_Close", ht_vol);

    const auto cfg =
        hodges_tompkins_cfg("ht_id", period, trading_days, timeframe);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto hodgesTompkins = dynamic_cast<HodgesTompkins *>(transformBase.get());
    REQUIRE(hodgesTompkins);

    auto result = hodgesTompkins->TransformData(input_df);
    auto lhs = result[cfg.GetOutputId().GetColumnName()].contiguous_array();
    auto rhs =
        Array(factory::array::make_contiguous_array(ht_vol.get_result()));

    INFO(lhs->Diff(*rhs.value()));
    CHECK(lhs.is_equal(rhs));
  }

  SECTION("Keltner Channels") {
    keltner_channels_v kc_vol(roll_period, band_multiplier);
    df.single_act_visit<double, double, double>("IBM_Low", "IBM_High",
                                                "IBM_Close", kc_vol);

    const auto cfg =
        keltner_channels_cfg("kc_id", roll_period, band_multiplier, timeframe);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto keltnerChannels = dynamic_cast<KeltnerChannels *>(transformBase.get());
    REQUIRE(keltnerChannels);

    auto result = keltnerChannels->TransformData(input_df);
    for (std::string const &col : {"upper_band", "lower_band"}) {
      auto lhs = result[cfg.GetOutputId(col).GetColumnName()].contiguous_array();

      std::vector<double> rhs;
      if (col == "upper_band") {
        rhs = kc_vol.get_upper_band();
      } else if (col == "lower_band") {
        rhs = kc_vol.get_lower_band();
      }
      auto rhs_arr = Array(factory::array::make_contiguous_array(rhs));

      INFO(lhs->Diff(*rhs_arr.value()));
      CHECK(lhs.is_equal(rhs_arr));
    }
  }

  SECTION("Parkinson") {
    hmdf::ParkinsonVolVisitor<double, std::string> p_visitor(period,
                                                             trading_days);
    df.single_act_visit<double, double>("IBM_Low", "IBM_High", p_visitor);

    const auto cfg = parkinson_cfg("p_id", period, trading_days, timeframe);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto parkinson = dynamic_cast<Parkinson *>(transformBase.get());
    REQUIRE(parkinson);

    auto result = parkinson->TransformData(input_df);
    auto lhs = result[cfg.GetOutputId().GetColumnName()].contiguous_array();
    auto rhs =
        Array(factory::array::make_contiguous_array(p_visitor.get_result()));

    INFO(lhs->Diff(*rhs.value()));
    CHECK(lhs.is_equal(rhs));
  }

  SECTION("Ulcer Index") {
    hmdf::UlcerIndexVisitor<double, std::string> ui_visitor(ulcer_period,
                                                            false);
    df.single_act_visit<double>("IBM_Close", ui_visitor);

    const auto cfg = ulcer_index_cfg("ui_id", ulcer_period, false, timeframe);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto ulcerIndex = dynamic_cast<UlcerIndex *>(transformBase.get());
    REQUIRE(ulcerIndex);

    auto result = ulcerIndex->TransformData(input_df);
    auto lhs = result[cfg.GetOutputId().GetColumnName()].contiguous_array();
    auto rhs =
        Array(factory::array::make_contiguous_array(ui_visitor.get_result()));

    INFO(lhs->Diff(*rhs.value()));
    CHECK(lhs.is_equal(rhs));
  }

  SECTION("Yang Zhang") {
    yz_vol_v_t yz_vol(period, trading_days);
    df.single_act_visit<double, double, double, double>(
        "IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", yz_vol);

    const auto cfg = yang_zhang_cfg("yz_id", period, trading_days, timeframe);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto yangZhang = dynamic_cast<YangZhang *>(transformBase.get());
    REQUIRE(yangZhang);

    auto result = yangZhang->TransformData(input_df);
    auto lhs = result[cfg.GetOutputId().GetColumnName()].contiguous_array();
    auto rhs =
        Array(factory::array::make_contiguous_array(yz_vol.get_result()));

    INFO(lhs->Diff(*rhs.value()));
    CHECK(lhs.is_equal(rhs));
  }
}
