#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/scalar.h"
#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/transforms/core/config_helper.h>
#include "transforms/components/hosseinmoein/indicators/indicators.h"
#include <DataFrame/DataFrame.h>
#include <DataFrame/DataFrameFinancialVisitors.h>
#include <DataFrame/DataFrameTypes.h>
#include <catch.hpp>
#include <cstdint>
#include <epoch_frame/factory/index_factory.h>

#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>

TEST_CASE("IndicatorsTest", "[indicators]") {
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
  // Create DataFrame with BOTH raw column names (c, h, l, o, v) AND #-prefixed columns (#c, #h, #l, #o, #v)
  // - Raw names used by transforms that access columns directly via C.CLOSE() etc.
  // - #-prefixed names used by transforms that access via GetInputId() (which returns #handle)
  auto close_data = df.get_column<double>("IBM_Close");
  auto high_data = df.get_column<double>("IBM_High");
  auto low_data = df.get_column<double>("IBM_Low");
  auto open_data = df.get_column<double>("IBM_Open");
  auto vol_data = std::vector<double>(vol.begin(), vol.end());

  auto input_df = make_dataframe<double>(
      index,
      {close_data, high_data, low_data, open_data, vol_data,
       close_data, high_data, low_data, open_data, vol_data},
      {std::string(C.CLOSE()), std::string(C.HIGH()), std::string(C.LOW()),
       std::string(C.OPEN()), std::string(C.VOLUME()),
       "#" + std::string(C.CLOSE()), "#" + std::string(C.HIGH()), "#" + std::string(C.LOW()),
       "#" + std::string(C.OPEN()), "#" + std::string(C.VOLUME())});

  // Test parameters
  int64_t period = 20;
  double multiplier = 2.0;
  int64_t ck_period = 10;
  int64_t atr_period = 20;
  double ck_multiplier = 3.0;
  size_t elders_period = 20;
  int64_t psl_period = 20;
  int64_t qqe_period = 14;
  int64_t smooth_period = 5;
  int64_t vortex_period = 14;

  SECTION("PivotPointSR") {
    PivotPointSRVisitor<double, std::string> pivot;
    df.single_act_visit<double, double, double>("IBM_Low", "IBM_High",
                                                "IBM_Close", pivot);
    const auto cfg = pivot_point_sr_cfg(
        "pivot_sr_id",
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto pivotPointSR = dynamic_cast<PivotPointSR *>(transformBase.get());
    REQUIRE(pivotPointSR);

    auto result = pivotPointSR->TransformData(input_df);

    std::vector<std::string> columns = {"pivot",    "resist_1",  "resist_2",
                                        "resist_3", "support_1", "support_2",
                                        "support_3"};
    for (const auto &col : columns) {
      auto lhs = result[cfg.GetOutputId(col).GetColumnName()].contiguous_array();

      std::vector<double> rhs;
      if (col == "pivot") {
        rhs = pivot.get_result();
      } else if (col == "resist_1") {
        rhs = pivot.get_resist_1();
      } else if (col == "resist_2") {
        rhs = pivot.get_resist_2();
      } else if (col == "resist_3") {
        rhs = pivot.get_resist_3();
      } else if (col == "support_1") {
        rhs = pivot.get_support_1();
      } else if (col == "support_2") {
        rhs = pivot.get_support_2();
      } else if (col == "support_3") {
        rhs = pivot.get_support_3();
      }
      auto rhs_arr = Array(factory::array::make_contiguous_array(rhs));

      INFO(lhs->Diff(*rhs_arr.value()));
      CHECK(lhs.is_equal(rhs_arr));
    }
  }

  SECTION("HurstExponent") {
    auto df_index_int =
        input_df.index()->array().cast(arrow::int64()).to_vector<int64_t>();
    auto close = df.get_column<double>("IBM_Close");
    std::vector<int64_t> window_index_seq(period);
    std::ranges::iota(window_index_seq, 0);

    auto cfg = hurst_exponent_cfg(
        "hurst_id", period, input_ref(C.CLOSE()),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto hurstExp = dynamic_cast<HurstExponent *>(transformBase.get());
    REQUIRE(hurstExp);
    auto expanded_result = hurstExp->TransformData(input_df);
    auto expanded_lhs =
        expanded_result[cfg.GetOutputId("result").GetColumnName()].contiguous_array();

    cfg = rolling_hurst_exponent_cfg(
        "rolling_hurst_id", period, input_ref(C.CLOSE()),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    transformBase = MAKE_TRANSFORM(cfg);
    auto rollingHurstExp =
        dynamic_cast<RollingHurstExponent *>(transformBase.get());
    REQUIRE(rollingHurstExp);
    auto rolling_result = rollingHurstExp->TransformData(input_df);
    auto rolling_lhs =
        rolling_result[cfg.GetOutputId("result").GetColumnName()].contiguous_array();

    for (int64_t i = 0; i < static_cast<int64_t>(df.get_index().size()); i++) {
      if (i < period) {
        INFO("[" << i << "] result expanded: " << expanded_lhs[i]);
        CHECK(expanded_lhs[i].is_null());
      } else {
        StdDataFrame<int64_t> expanded_df;
        std::vector data(close.begin(), close.begin() + i + 1);
        expanded_df.load_index(
            StdDataFrame<int64_t>::gen_sequence_index(0, data.size(), 1));
        expanded_df.load_column("IBM_Close", data);

        HurstExponentVisitor<double> expanded_hurst({1, 2, 4, 8});
        expanded_df.single_act_visit<double>("IBM_Close", expanded_hurst);
        auto expanded_rhs = expanded_hurst.get_result();

        CHECK(expanded_lhs[i] == Scalar{expanded_rhs});
      }

      StdDataFrame<int64_t> rolling_df;
      auto data = (i >= period - 1)
                      ? std::vector(close.begin() + (i - period + 1),
                                    close.begin() + i + 1)
                      : std::vector(close.begin(), close.begin() + i + 1);
      rolling_df.load_index(
          StdDataFrame<int64_t>::gen_sequence_index(0, data.size(), 1));
      rolling_df.load_column("IBM_Close", data);

      HurstExponentVisitor<double> rolling_hurst(
          RollingHurstExponent::lagGrid(period));
      rolling_df.single_act_visit<double>("IBM_Close", rolling_hurst);
      auto rolling_rhs = rolling_hurst.get_result();

      INFO("[" << i << "] result rolling: " << rolling_lhs[i]);
      INFO("[" << i << "] expected rolling: " << rolling_rhs);
      CHECK(rolling_lhs[i] == Scalar{rolling_rhs});
    }
  }

  SECTION("ChandeKrollStop") {
    cksp_v<double, std::string> ck_stop(ck_period, atr_period, ck_multiplier);
    df.single_act_visit<double, double, double>("IBM_Low", "IBM_High",
                                                "IBM_Close", ck_stop);
    const auto cfg = chande_kroll_cfg(
        "ck_stop_id", ck_period, atr_period, ck_multiplier,
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto chandeKroll = dynamic_cast<ChandeKrollStop *>(transformBase.get());
    REQUIRE(chandeKroll);

    auto result = chandeKroll->TransformData(input_df);

    std::vector<std::string> columns = {"long_stop", "short_stop"};
    for (const auto &col : columns) {
      auto lhs = result[cfg.GetOutputId(col).GetColumnName()].contiguous_array();

      std::vector<double> rhs;
      if (col == "long_stop") {
        rhs = ck_stop.get_long_stop();
      } else if (col == "short_stop") {
        rhs = ck_stop.get_short_stop();
      }
      auto rhs_arr = Array(factory::array::make_contiguous_array(rhs));

      INFO(lhs->Diff(*rhs_arr.value()));
      CHECK(lhs.is_equal(rhs_arr));
    }
  }

  SECTION("EldersThermometer") {
    ether_v<double, std::string> elders{elders_period, 0.1, 0.5};
    df.single_act_visit<double, double>("IBM_Low", "IBM_High", elders);
    const auto cfg = elders_thermometer_cfg(
        "elders_id", elders_period, 0.1, 0.5,
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto eldersTherm = dynamic_cast<EldersThermometer *>(transformBase.get());
    REQUIRE(eldersTherm);

    auto result = eldersTherm->TransformData(input_df);
    for (std::string const &column : {"result", "ema", "buy_signal", "sell_signal"}) {
      auto lhs = result[cfg.GetOutputId(column).GetColumnName()].contiguous_array();
      arrow::ArrayPtr rhs;
      if (column == "result") {
        rhs = factory::array::make_contiguous_array(elders.get_result());
      } else if (column == "ema") {
        rhs = factory::array::make_contiguous_array(elders.get_result_ma());
      } else if (column == "buy_signal") {
        rhs = factory::array::make_contiguous_array(elders.get_buy_signal());
      } else if (column == "sell_signal") {
        rhs = factory::array::make_contiguous_array(elders.get_sell_signal());
      }

      INFO(lhs->Diff(*rhs));
      CHECK(lhs.is_equal(Array{rhs}));
    }
  }

  SECTION("PriceDistance") {
    pdist_v<double, std::string> price_dist;
    df.single_act_visit<double, double, double, double>(
        "IBM_Low", "IBM_High", "IBM_Open", "IBM_Close", price_dist);
    const auto cfg = price_distance_cfg(
        "price_dist_id",
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto priceDistance = dynamic_cast<PriceDistance *>(transformBase.get());
    REQUIRE(priceDistance);

    auto result = priceDistance->TransformData(input_df);
    auto lhs = result[cfg.GetOutputId("result").GetColumnName()].contiguous_array();

    auto rhs = price_dist.get_result();
    auto rhs_arr = Array(factory::array::make_contiguous_array(rhs));

    INFO(lhs->Diff(*rhs_arr.value()));
    CHECK(lhs.is_equal(rhs_arr));
  }

  SECTION("PSL") {
    PSLVisitor<double, std::string> psl_visitor(psl_period);
    df.single_act_visit<double, double>("IBM_Close", "IBM_Open", psl_visitor);
    const auto cfg = psl_cfg(
        "psl_id", psl_period,
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto pslTransform = dynamic_cast<PSL *>(transformBase.get());
    REQUIRE(pslTransform);

    auto result = pslTransform->TransformData(input_df);
    auto lhs = result[cfg.GetOutputId("result").GetColumnName()].contiguous_array();

    auto rhs = psl_visitor.get_result();
    auto rhs_arr = factory::array::make_contiguous_array(rhs);

    INFO(lhs->Diff(*rhs_arr));
    CHECK(lhs.is_equal(Array(rhs_arr)));
  }

  SECTION("QuantQualEstimation") {
    qqe_v<double, std::string> qqe_visitor(qqe_period, smooth_period, 4.236);
    df.single_act_visit<double>("IBM_Close", qqe_visitor);
    const auto cfg = qqe_cfg(
        "qqe_id", qqe_period, smooth_period, 4.236,
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto qqeTransform =
        dynamic_cast<QuantQualEstimation *>(transformBase.get());
    REQUIRE(qqeTransform);

    auto result = qqeTransform->TransformData(input_df);

    std::vector<std::string> columns = {"result", "rsi_ma", "long_line",
                                        "short_line"};
    for (const auto &col : columns) {
      auto lhs = result[cfg.GetOutputId(col).GetColumnName()].contiguous_array();

      std::vector<double> rhs;
      if (col == "result") {
        rhs = qqe_visitor.get_result();
      } else if (col == "rsi_ma") {
        rhs = qqe_visitor.get_rsi_ma();
      } else if (col == "long_line") {
        rhs = qqe_visitor.get_long_line();
      } else if (col == "short_line") {
        rhs = qqe_visitor.get_short_line();
      }
      auto rhs_arr = Array(factory::array::make_contiguous_array(rhs));

      INFO(lhs->Diff(*rhs_arr.value()));
      CHECK(lhs.is_equal(rhs_arr));
    }
  }

  SECTION("Vortex") {
    vtx_v<double, std::string> vortex_visitor(vortex_period);
    df.single_act_visit<double, double, double>("IBM_Low", "IBM_High",
                                                "IBM_Close", vortex_visitor);
    const auto cfg = vortex_cfg(
        "vortex_id", vortex_period,
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);
    auto transformBase = MAKE_TRANSFORM(cfg);
    auto vortexTransform = dynamic_cast<Vortex *>(transformBase.get());
    REQUIRE(vortexTransform);

    auto result = vortexTransform->TransformData(input_df);

    std::vector<std::string> columns = {"plus_indicator", "minus_indicator"};
    for (const auto &col : columns) {
      auto lhs = result[cfg.GetOutputId(col).GetColumnName()].contiguous_array();

      std::vector<double> rhs;
      if (col == "plus_indicator") {
        rhs = vortex_visitor.get_plus_indicator();
      } else if (col == "minus_indicator") {
        rhs = vortex_visitor.get_minus_indicator();
      }
      auto rhs_arr = Array(factory::array::make_contiguous_array(rhs));

      INFO(lhs->Diff(*rhs_arr.value()));
      CHECK(lhs.is_equal(rhs_arr));
    }
  }
}
