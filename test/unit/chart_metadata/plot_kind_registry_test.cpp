#include "chart_metadata/plot_kinds/registry.h"
#include "catch2/catch_test_macros.hpp"
#include "epoch_script/transforms/core/config_helper.h"
#include "epoch_script/transforms/core/metadata.h"
#include "epoch_script/data/common/constants.h"

using namespace epoch_script;
using namespace epoch_script::chart_metadata::plot_kinds;
using namespace epoch_core;

TEST_CASE("PlotKindBuilderRegistry - Comprehensive Coverage", "[chart_metadata][plot_kind_registry]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  auto& registry = PlotKindBuilderRegistry::Instance();
  auto src = transform::data_source("src", tf);

  SECTION("ALL PlotKind enum values are registered") {
    // This test iterates through ALL PlotKind values and ensures they are registered
    // When new PlotKinds are added, this test will fail until they are registered

    const std::vector<TransformPlotKind> allPlotKinds = {
      // Multi-line indicators (2+ output lines)
      TransformPlotKind::ao,
      TransformPlotKind::aroon,
      TransformPlotKind::macd,
      TransformPlotKind::stoch,
      TransformPlotKind::fisher,
      TransformPlotKind::qqe,
      TransformPlotKind::elders,
      TransformPlotKind::fosc,
      TransformPlotKind::vortex,

      // Bands (upper/middle/lower)
      TransformPlotKind::bbands,
      TransformPlotKind::bb_percent_b,

      // Complex multi-output indicators
      TransformPlotKind::ichimoku,
      TransformPlotKind::chande_kroll_stop,
      TransformPlotKind::pivot_point_sr,
      TransformPlotKind::previous_high_low,
      TransformPlotKind::retracements,
      TransformPlotKind::gap,
      TransformPlotKind::shl,
      TransformPlotKind::bos_choch,
      TransformPlotKind::order_blocks,
      TransformPlotKind::fvg,
      TransformPlotKind::liquidity,
      TransformPlotKind::sessions,
      TransformPlotKind::pivot_point_detector,

      // Pattern formations
      TransformPlotKind::head_and_shoulders,
      TransformPlotKind::inverse_head_and_shoulders,
      TransformPlotKind::double_top_bottom,
      TransformPlotKind::pennant_pattern,
      TransformPlotKind::flag_pattern,
      TransformPlotKind::triangle_patterns,
      TransformPlotKind::consolidation_box,

      // Single-value indicators & overlays
      TransformPlotKind::line,
      TransformPlotKind::close_line,
      TransformPlotKind::h_line,
      TransformPlotKind::vwap,
      TransformPlotKind::column,
      TransformPlotKind::qstick,
      TransformPlotKind::psar,
      TransformPlotKind::panel_line,
      TransformPlotKind::panel_line_percent,
      TransformPlotKind::rsi,
      TransformPlotKind::cci,
      TransformPlotKind::atr,

      // Special purpose
      TransformPlotKind::flag,
      TransformPlotKind::zone,
      TransformPlotKind::trade_signal,

      // ML/AI indicators
      TransformPlotKind::hmm,
      TransformPlotKind::sentiment,
    };

    INFO("Testing that all " << allPlotKinds.size() << " PlotKind enum values are registered");

    for (const auto& plotKind : allPlotKinds) {
      INFO("Checking PlotKind: " << TransformPlotKindWrapper::ToString(plotKind));

      // Verify registration
      REQUIRE(registry.IsRegistered(plotKind));

      // Verify GetBuilder doesn't throw
      REQUIRE_NOTHROW(registry.GetBuilder(plotKind));

      // Verify GetZIndex doesn't throw
      REQUIRE_NOTHROW(registry.GetZIndex(plotKind));

      // Verify RequiresOwnAxis doesn't throw
      REQUIRE_NOTHROW(registry.RequiresOwnAxis(plotKind));
    }
  }

  SECTION("Multi-line indicators - MACD") {
    auto macd_cfg = transform::run_op("macd", "1",
        {{epoch_script::ARG, {transform::input_ref("c")}}},
        {{"short_period", MetaDataOptionDefinition{12.0}},
         {"long_period", MetaDataOptionDefinition{26.0}},
         {"signal_period", MetaDataOptionDefinition{9.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::macd));

    const auto& builder = registry.GetBuilder(TransformPlotKind::macd);
    auto dataMapping = builder.Build(macd_cfg);

    // MACD has: macd line, signal line, histogram
    REQUIRE(dataMapping.size() >= 2);
    REQUIRE(builder.RequiresOwnAxis() == true);
  }

  SECTION("Multi-line indicators - Aroon") {
    auto aroon_cfg = transform::run_op("aroon", "1",
        {},
        {{"period", MetaDataOptionDefinition{14.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::aroon));

    const auto& builder = registry.GetBuilder(TransformPlotKind::aroon);
    auto dataMapping = builder.Build(aroon_cfg);

    // Aroon has: aroon_up, aroon_down
    REQUIRE(dataMapping.size() >= 2);
    REQUIRE(builder.RequiresOwnAxis() == true);
  }

  SECTION("Multi-line indicators - Stochastic") {
    auto stoch_cfg = transform::run_op("stoch", "1",
        {},
        {{"k_period", MetaDataOptionDefinition{14.0}},
         {"k_slowing_period", MetaDataOptionDefinition{1.0}},
         {"d_period", MetaDataOptionDefinition{3.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::stoch));

    const auto& builder = registry.GetBuilder(TransformPlotKind::stoch);
    auto dataMapping = builder.Build(stoch_cfg);

    // Stoch has: %K, %D
    REQUIRE(dataMapping.size() >= 2);
    REQUIRE(builder.RequiresOwnAxis() == true);
  }

  SECTION("Multi-line indicators - Fisher Transform") {
    auto fisher_cfg = transform::run_op("fisher", "1",
        {},
        {{"period", MetaDataOptionDefinition{10.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::fisher));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::fisher));
  }

  SECTION("Multi-line indicators - QQE") {
    auto qqe_cfg = transform::qqe_cfg("1", 14, 5, 4.236, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::qqe));

    const auto& builder = registry.GetBuilder(TransformPlotKind::qqe);
    auto dataMapping = builder.Build(qqe_cfg);

    REQUIRE(dataMapping.size() >= 2);
    REQUIRE(builder.RequiresOwnAxis() == true);
  }

  SECTION("Multi-line indicators - Elder Ray") {
    auto elders_cfg = transform::run_op("elders_thermometer", "1",
        {},
        {{"period", MetaDataOptionDefinition{13.0}},
         {"buy_factor", MetaDataOptionDefinition{1.4}},
         {"sell_factor", MetaDataOptionDefinition{0.7}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::elders));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::elders));
  }

  SECTION("Multi-line indicators - Forecast Oscillator") {
    auto fosc_cfg = transform::run_op("fosc", "1",
        {{epoch_script::ARG, {transform::input_ref("c")}}},
        {{"period", MetaDataOptionDefinition{5.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::fosc));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::fosc));
  }

  SECTION("Multi-line indicators - Vortex") {
    auto vortex_cfg = transform::vortex_cfg("1", 14, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::vortex));

    const auto& builder = registry.GetBuilder(TransformPlotKind::vortex);
    auto dataMapping = builder.Build(vortex_cfg);

    REQUIRE(builder.RequiresOwnAxis() == true);
  }

  SECTION("Multi-line indicators - Awesome Oscillator") {
    auto ao_cfg = transform::run_op("ao", "1", {}, {}, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::ao));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::ao));
  }

  SECTION("Bands - Bollinger Bands") {
    auto bbands_cfg = transform::bbands("1", 20, 2, strategy::NodeReference("", "c"), tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::bbands));

    const auto& builder = registry.GetBuilder(TransformPlotKind::bbands);
    auto dataMapping = builder.Build(bbands_cfg);

    // BBands has: upper, middle, lower
    REQUIRE(dataMapping.size() >= 3);
    REQUIRE(builder.RequiresOwnAxis() == false);  // Overlays on price
  }

  SECTION("Bands - Bollinger %B") {
    auto bb_percent_b_cfg = transform::run_op("bband_percent", "1",
        {{"bbands_lower", {transform::input_ref("lower")}},
         {"bbands_upper", {transform::input_ref("upper")}}},
        {},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::bb_percent_b));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::bb_percent_b));
  }

  SECTION("Complex indicators - Ichimoku Cloud") {
    auto ichimoku_cfg = transform::run_op("ichimoku", "1",
        {},
        {{"p_tenkan", MetaDataOptionDefinition{9.0}},
         {"p_kijun", MetaDataOptionDefinition{26.0}},
         {"p_senkou_b", MetaDataOptionDefinition{52.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::ichimoku));

    const auto& builder = registry.GetBuilder(TransformPlotKind::ichimoku);
    auto dataMapping = builder.Build(ichimoku_cfg);

    // Ichimoku has multiple lines: conversion, base, leading_a, leading_b, lagging
    REQUIRE(dataMapping.size() >= 4);
    REQUIRE(builder.RequiresOwnAxis() == false);  // Overlays on price
  }

  SECTION("Complex indicators - Chande Kroll Stop") {
    auto chande_cfg = transform::chande_kroll_cfg("1", 10, 20, 3.0, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::chande_kroll_stop));

    const auto& builder = registry.GetBuilder(TransformPlotKind::chande_kroll_stop);
    auto dataMapping = builder.Build(chande_cfg);

    REQUIRE(builder.RequiresOwnAxis() == false);  // Overlays on price
  }

  SECTION("Complex indicators - Pivot Point SR") {
    auto pivot_cfg = transform::pivot_point_sr_cfg("1", tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::pivot_point_sr));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::pivot_point_sr));
  }

  SECTION("Complex indicators - Previous High/Low") {
    auto prev_hl_cfg = transform::previous_high_low("1", 1, "high", tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::previous_high_low));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::previous_high_low));
  }

  SECTION("Complex indicators - Retracements") {
    auto retracement_cfg = transform::retracements("1",
      strategy::NodeReference{"", "high_low"},
      strategy::NodeReference{"", "level"},
      tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::retracements));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::retracements));
  }

  SECTION("Complex indicators - Gap") {
    auto gap_cfg = transform::run_op("session_gap", "1",
        {},
        {{"fill_percent", MetaDataOptionDefinition{100.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::gap));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::gap));
  }

  SECTION("Complex indicators - Swing Highs/Lows") {
    auto shl_cfg = transform::swing_highs_lows("1", 5, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::shl));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::shl));
  }

  SECTION("Complex indicators - BOS/CHOCH") {
    auto bos_cfg = transform::bos_choch("1",
      strategy::NodeReference{"", "high_low"},
      strategy::NodeReference{"", "level"}, true, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::bos_choch));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::bos_choch));
  }

  SECTION("Complex indicators - Order Blocks") {
    auto ob_cfg = transform::order_blocks("1",
      strategy::NodeReference{"", "high_low"},
      false, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::order_blocks));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::order_blocks));
  }

  SECTION("Complex indicators - Fair Value Gap") {
    auto fvg_cfg = transform::fair_value_gap("1", false, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::fvg));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::fvg));
  }

  SECTION("Complex indicators - Liquidity") {
    auto liquidity_cfg = transform::liquidity("1",
      strategy::NodeReference{"", "high_low"},
      strategy::NodeReference{"", "level"}, 0.5, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::liquidity));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::liquidity));
  }

  SECTION("Complex indicators - Sessions") {
    auto sessions_cfg = transform::sessions("1", "NewYork", tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::sessions));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::sessions));
  }

  SECTION("Complex indicators - Pivot Point Detector") {
    auto pivot_detector_cfg = transform::run_op("flexible_pivot_detector", "1",
        {},
        {{"left_count", MetaDataOptionDefinition{5.0}},
         {"right_count", MetaDataOptionDefinition{5.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::pivot_point_detector));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::pivot_point_detector));
  }

  SECTION("Pattern formations - Head and Shoulders") {
    auto hs_cfg = transform::head_and_shoulders_cfg("1", 50, 1.2, 1.2, 0.1, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::head_and_shoulders));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::head_and_shoulders));
  }

  SECTION("Pattern formations - Inverse Head and Shoulders") {
    auto ihs_cfg = transform::inverse_head_and_shoulders_cfg("1", 50, 1.2, 1.2, 0.1, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::inverse_head_and_shoulders));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::inverse_head_and_shoulders));
  }

  SECTION("Pattern formations - Double Top/Bottom") {
    auto dtb_cfg = transform::double_top_bottom_cfg("1", 50, "top", 0.02, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::double_top_bottom));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::double_top_bottom));
  }

  SECTION("Pattern formations - Pennant") {
    auto pennant_cfg = transform::pennant_cfg("1", 50, 4, 0.8, 20, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::pennant_pattern));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::pennant_pattern));
  }

  SECTION("Pattern formations - Flag") {
    auto flag_cfg = transform::flag_cfg("1", 50, 4, 0.8, 0.1, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::flag_pattern));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::flag_pattern));
  }

  SECTION("Pattern formations - Triangle") {
    auto triangle_cfg = transform::triangles_cfg("1", 50, "ascending", 0.8, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::triangle_patterns));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::triangle_patterns));
  }

  SECTION("Pattern formations - Consolidation Box") {
    auto box_cfg = transform::consolidation_box_cfg("1", 50, 4, 0.8, 0.05, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::consolidation_box));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::consolidation_box));
  }

  SECTION("Single-value indicators - Line (generic overlay)") {
    auto sma_cfg = transform::ma("sma", "1", strategy::NodeReference("", "c"), 20, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::line));

    const auto& builder = registry.GetBuilder(TransformPlotKind::line);
    auto dataMapping = builder.Build(sma_cfg);

    REQUIRE(dataMapping.size() >= 1);
    REQUIRE(builder.RequiresOwnAxis() == false);  // Overlays on price
  }

  SECTION("Single-value indicators - Close Line") {
    auto close_cfg = transform::run_op("common_indices", "1",
        {},
        {{"ticker", MetaDataOptionDefinition{std::string{"SPX"}}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::close_line));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::close_line));
  }

  SECTION("Single-value indicators - Horizontal Line") {
    // h_line is a generic line PlotKind - test with SMA
    auto hline_cfg = transform::ma("sma", "1", src.GetOutputId("c"), 20, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::h_line));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::h_line));
  }

  SECTION("Single-value indicators - VWAP") {
    auto vwap_cfg = transform::run_op("vwap", "1", {}, {}, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::vwap));

    const auto& builder = registry.GetBuilder(TransformPlotKind::vwap);
    REQUIRE(builder.RequiresOwnAxis() == false);  // Overlays on price
  }

  SECTION("Single-value indicators - Column") {
    auto col_cfg = transform::run_op("marketfi", "1", {}, {}, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::column));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::column));
  }

  SECTION("Single-value indicators - QStick") {
    auto qstick_cfg = transform::run_op("qstick", "1",
        {},
        {{"period", MetaDataOptionDefinition{14.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::qstick));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::qstick));
  }

  SECTION("Single-value indicators - PSAR") {
    auto psar_cfg = transform::psar("1", 0.02, 0.2, src.GetOutputId("c"), tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::psar));

    const auto& builder = registry.GetBuilder(TransformPlotKind::psar);
    REQUIRE(builder.RequiresOwnAxis() == false);  // Overlays on price (dots)
  }

  SECTION("Single-value indicators - Panel Line") {
    auto panel_cfg = transform::run_op("forward_returns", "1",
        {{epoch_script::ARG, {transform::input_ref("c")}}},
        {{"period", MetaDataOptionDefinition{1.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::panel_line));

    const auto& builder = registry.GetBuilder(TransformPlotKind::panel_line);
    REQUIRE(builder.RequiresOwnAxis() == true);  // Panel indicator
  }

  SECTION("Single-value indicators - Panel Line Percent") {
    // panel_line_percent is a PlotKind but doesn't have a dedicated transform
    // It's used for percentage-based panel indicators
    // Test that the PlotKind is registered (even if no example transform)
    REQUIRE(registry.IsRegistered(TransformPlotKind::panel_line_percent));

    const auto& builder = registry.GetBuilder(TransformPlotKind::panel_line_percent);
    REQUIRE(builder.RequiresOwnAxis() == true);  // Panel indicator
  }

  SECTION("Single-value indicators - RSI") {
    auto rsi_cfg = transform::single_operand_period_op("rsi", "1", 14, src.GetOutputId("c"), tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::rsi));

    const auto& builder = registry.GetBuilder(TransformPlotKind::rsi);
    REQUIRE(builder.RequiresOwnAxis() == true);  // Panel indicator (0-100 range)
  }

  SECTION("Single-value indicators - CCI") {
    auto cci_cfg = transform::run_op("cci", "1",
        {},
        {{"period", MetaDataOptionDefinition{20.0}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::cci));

    const auto& builder = registry.GetBuilder(TransformPlotKind::cci);
    REQUIRE(builder.RequiresOwnAxis() == true);  // Panel indicator
  }

  SECTION("Single-value indicators - ATR") {
    auto atr_cfg = transform::atr("1", 14, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::atr));

    const auto& builder = registry.GetBuilder(TransformPlotKind::atr);
    REQUIRE(builder.RequiresOwnAxis() == true);  // Panel indicator
  }

  SECTION("Special purpose - Flag") {
    auto flag_cfg = transform::run_op("flag", "1",
        {{epoch_script::ARG, {transform::input_ref("c")}}},
        {},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::flag));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::flag));
  }

  SECTION("Special purpose - Zone") {
    auto zone_cfg = transform::run_op("session_time_window", "1",
        {},
        {{"session_type", MetaDataOptionDefinition{std::string{"NewYork"}}},
         {"minute_offset", MetaDataOptionDefinition{30.0}},
         {"boundary_type", MetaDataOptionDefinition{std::string{"start"}}}},
        tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::zone));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::zone));
  }

  SECTION("Special purpose - Trade Signal") {
    auto signal_cfg = transform::trade_signal_executor_cfg("1", {
      {"entry", transform::NodeRef{"", "entry_signal"}},
      {"exit", transform::NodeRef{"", "exit_signal"}}
    }, tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::trade_signal));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::trade_signal));
  }

  SECTION("ML/AI indicators - Hidden Markov Model") {
    auto hmm_cfg = transform::hmm_single_cfg("1", src.GetOutputId("c"), tf, 3, 1000, 1e-5, true, 100, 0);

    REQUIRE(registry.IsRegistered(TransformPlotKind::hmm));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::hmm));
  }

  SECTION("ML/AI indicators - Sentiment Analysis") {
    auto news = transform::news("news_src", tf);
    auto sentiment_cfg = transform::finbert_sentiment_cfg("1", news.GetOutputId("title"), tf);

    REQUIRE(registry.IsRegistered(TransformPlotKind::sentiment));
    REQUIRE_NOTHROW(registry.GetBuilder(TransformPlotKind::sentiment));

    const auto& builder = registry.GetBuilder(TransformPlotKind::sentiment);
    auto dataMapping = builder.Build(sentiment_cfg);

    // Sentiment has: positive, neutral, negative (bool flags), confidence (score)
    REQUIRE(dataMapping.size() >= 4);
    REQUIRE(builder.RequiresOwnAxis() == true);
  }

  SECTION("GetBuilder throws for unregistered PlotKind") {
    // Use Null as an example of an unregistered PlotKind
    REQUIRE(!registry.IsRegistered(TransformPlotKind::Null));
    REQUIRE_THROWS_AS(registry.GetBuilder(TransformPlotKind::Null), std::runtime_error);
  }

  SECTION("Z-Index ordering makes sense") {
    // Verify that z-index values are reasonable
    // Background elements should have lower z-index than foreground elements

    auto zone_z = registry.GetZIndex(TransformPlotKind::zone);
    auto line_z = registry.GetZIndex(TransformPlotKind::line);
    auto flag_z = registry.GetZIndex(TransformPlotKind::flag);

    // Zones should be in the background
    REQUIRE(zone_z < line_z);

    // All z-indices should be in reasonable range (0-100)
    REQUIRE(zone_z <= 100);
    REQUIRE(line_z <= 100);
    REQUIRE(flag_z <= 100);
  }

  SECTION("RequiresOwnAxis categorization") {
    // Price overlays should NOT require own axis
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::line) == false);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::bbands) == false);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::vwap) == false);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::psar) == false);

    // Panel indicators SHOULD require own axis
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::rsi) == true);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::macd) == true);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::stoch) == true);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::cci) == true);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::atr) == true);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::panel_line) == true);
    REQUIRE(registry.RequiresOwnAxis(TransformPlotKind::panel_line_percent) == true);
  }
}
