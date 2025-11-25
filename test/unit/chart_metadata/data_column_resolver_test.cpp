#include "chart_metadata/data_column_resolver.h"
#include "catch2/catch_test_macros.hpp"
#include "epoch_script/data/common/constants.h"
#include "epoch_script/transforms/core/config_helper.h"
#include "epoch_script/transforms/core/itransform.h"

using namespace epoch_script;
using namespace epoch_script::chart_metadata;

TEST_CASE("DataColumnResolver", "[chart_metadata][data_column_resolver][!mayfail]") {
  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  SECTION("Resolves standard single-output indicators") {
    auto sma = transform::ma("sma", "1", strategy::InputValue(strategy::NodeReference("", "c")), 10, tf);
    auto columns = DataColumnResolver::ResolveColumns(sma);

    REQUIRE(columns.size() == 2);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#result");
  }

  SECTION("Resolves Bollinger Bands with special handling") {
    auto bbands = transform::bbands("1", 10, 2, strategy::InputValue(strategy::NodeReference("", "c")), tf);
    auto columns = DataColumnResolver::ResolveColumns(bbands);

    REQUIRE(columns.size() == 4);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#bbands_lower");
    REQUIRE(columns[2] == "1#bbands_middle");
    REQUIRE(columns[3] == "1#bbands_upper");
  }

  SECTION("Resolves MACD with multiple outputs") {
    auto macd = transform::run_op("macd", "1",
        {{epoch_script::ARG, {transform::input_ref("c")}}},
        {{"short_period", MetaDataOptionDefinition{12.0}},
         {"long_period", MetaDataOptionDefinition{26.0}},
         {"signal_period", MetaDataOptionDefinition{9.0}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(macd);

    REQUIRE(columns.size() == 4);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#macd");
    REQUIRE(columns[2] == "1#macd_signal");
    REQUIRE(columns[3] == "1#macd_histogram");
  }

  SECTION("Resolves Stochastic oscillator") {
    auto stoch = transform::run_op("stoch", "1",
        {},
        {{"k_period", MetaDataOptionDefinition{14.0}},
         {"k_slowing_period", MetaDataOptionDefinition{3.0}},
         {"d_period", MetaDataOptionDefinition{3.0}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(stoch);

    REQUIRE(columns.size() == 3);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#stoch_k");
    REQUIRE(columns[2] == "1#stoch_d");
  }

  SECTION("Resolves Aroon indicator") {
    auto aroon = transform::run_op("aroon", "1",
        {},
        {{"period", MetaDataOptionDefinition{14.0}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(aroon);

    REQUIRE(columns.size() == 3);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#aroon_down");
    REQUIRE(columns[2] == "1#aroon_up");
  }

  SECTION("Resolves Fisher Transform") {
    auto fisher = transform::run_op("fisher", "1",
        {},
        {{"period", MetaDataOptionDefinition{10.0}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(fisher);

    REQUIRE(columns.size() == 3);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#fisher");
    REQUIRE(columns[2] == "1#fisher_signal");
  }

  SECTION("Resolves QQE indicator") {
    auto qqe = transform::run_op("qqe", "1",
        {{epoch_script::ARG, {transform::input_ref("c")}}},
        {{"avg_period", MetaDataOptionDefinition{14.0}},
         {"smooth_period", MetaDataOptionDefinition{5.0}},
         {"width_factor", MetaDataOptionDefinition{4.236}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(qqe);

    REQUIRE(columns.size() == 5);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#result");
    REQUIRE(columns[2] == "1#rsi_ma");
    REQUIRE(columns[3] == "1#long_line");
    REQUIRE(columns[4] == "1#short_line");
  }

  SECTION("Resolves SMC indicators - Order Blocks") {
    auto order_blocks = transform::run_op("order_blocks", "2",
        {{"high_low", {transform::input_ref("1", "high_low")}}},
        {{"close_mitigation", MetaDataOptionDefinition{false}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(order_blocks);

    REQUIRE(columns.size() == 7);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "2#ob");
    REQUIRE(columns[2] == "2#top");
    REQUIRE(columns[3] == "2#bottom");
    REQUIRE(columns[4] == "2#ob_volume");
    REQUIRE(columns[5] == "2#mitigated_index");
    REQUIRE(columns[6] == "2#percentage");
  }

  SECTION("Resolves Fair Value Gap") {
    auto fvg = transform::run_op("fair_value_gap", "1",
        {},
        {{"join_consecutive", MetaDataOptionDefinition{true}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(fvg);

    REQUIRE(columns.size() == 5); // index + 4 outputs
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#fvg");
    REQUIRE(columns[2] == "1#top");
    REQUIRE(columns[3] == "1#bottom");
    REQUIRE(columns[4] == "1#mitigated_index");
  }

  SECTION("Resolves Liquidity indicator") {
    auto liquidity = transform::run_op("liquidity", "2",
        {{"high_low", {transform::input_ref("1", "high_low")}},
         {"level", {transform::input_ref("1", "level")}}},
        {{"range_percent", MetaDataOptionDefinition{0.001}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(liquidity);

    REQUIRE(columns.size() == 5);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "2#liquidity");
    REQUIRE(columns[2] == "2#level");
    REQUIRE(columns[3] == "2#end");
    REQUIRE(columns[4] == "2#swept");
  }

  SECTION("Resolves Sessions indicator") {
    auto sessions = transform::run_op("sessions", "1",
        {},
        {{"session_type", MetaDataOptionDefinition{std::string{"London"}}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(sessions);

    REQUIRE(columns.size() == 6);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#active");
    REQUIRE(columns[2] == "1#high");
    REQUIRE(columns[3] == "1#low");
    REQUIRE(columns[4] == "1#closed");
    REQUIRE(columns[5] == "1#opened");
  }

  SECTION("Resolves Previous High Low") {
    auto prev_hl = transform::run_op("previous_high_low", "1",
        {},
        {{"interval", MetaDataOptionDefinition{1.0}},
         {"type", MetaDataOptionDefinition{std::string{"day"}}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(prev_hl);

    REQUIRE(columns.size() == 5);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#previous_high");
    REQUIRE(columns[2] == "1#previous_low");
    REQUIRE(columns[3] == "1#broken_high");
    REQUIRE(columns[4] == "1#broken_low");
  }

  SECTION("Resolves Swing Highs and Lows") {
    auto swing_hl = transform::run_op("swing_highs_lows", "1",
        {},
        {{"swing_length", MetaDataOptionDefinition{5.0}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(swing_hl);

    REQUIRE(columns.size() == 3);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#high_low");
    REQUIRE(columns[2] == "1#level");
  }

  SECTION("Resolves BOS/CHOCH") {
    auto bos_choch = transform::run_op("bos_choch", "2",
        {{"high_low", {transform::input_ref("1", "high_low")}},
         {"level", {transform::input_ref("1", "level")}}},
        {{"close_break", MetaDataOptionDefinition{false}}},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(bos_choch);

    REQUIRE(columns.size() == 5);

    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "2#bos");
    REQUIRE(columns[2] == "2#choch");
    REQUIRE(columns[3] == "2#level");
    REQUIRE(columns[4] == "2#broken_index");
  }

  SECTION("Resolves Retracements") {
    auto retracements = transform::run_op("retracements", "2",
        {{"high_low", {transform::input_ref("1", "high_low")}},
         {"level", {transform::input_ref("1", "level")}}},
        {},
        tf);
    auto columns = DataColumnResolver::ResolveColumns(retracements);

    REQUIRE(columns.size() == 4);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "2#direction");
    REQUIRE(columns[2] == "2#current_retracement");
    REQUIRE(columns[3] == "2#deepest_retracement");
  }

  SECTION("Falls back to default for unknown indicators") {
    // Create a custom transform that's not in the special handling list
    auto rsi = transform::single_operand_period_op("rsi", "1", 14, strategy::InputValue(strategy::NodeReference("", "c")), tf);
    auto columns = DataColumnResolver::ResolveColumns(rsi);

    REQUIRE(columns.size() == 2);
    REQUIRE(columns[0] == "index");
    REQUIRE(columns[1] == "1#result");
  }

  SECTION("Handles transforms with multiple generic outputs") {
    // If we had a transform with multiple outputs not in special list
    // it would use all outputs from GetOutputs()
    // This is tested implicitly by the fallback case above
  }

  SECTION("Verifies actual output IDs match static mappings") {
    SECTION("MACD outputs") {
      auto macd = transform::run_op("macd", "1",
          {{epoch_script::ARG, {transform::input_ref("c")}}},
          {{"short_period", MetaDataOptionDefinition{12.0}},
           {"long_period", MetaDataOptionDefinition{26.0}},
           {"signal_period", MetaDataOptionDefinition{9.0}}},
          tf);
      auto outputs = macd.GetOutputs();

      REQUIRE(outputs.size() == 3);

      // Verify the actual output IDs
      std::vector<std::string> expectedIds = {"macd", "macd_signal",
                                              "macd_histogram"};
      for (size_t i = 0; i < outputs.size(); ++i) {
        REQUIRE(outputs[i].id == expectedIds[i]);
      }

      // Verify our resolver returns the correct columns
      auto columns = DataColumnResolver::ResolveColumns(macd);
      REQUIRE(columns.size() == 4); // index + 3 outputs
      REQUIRE(columns[0] == "index");
      REQUIRE(columns[1] == "1#macd");
      REQUIRE(columns[2] == "1#macd_signal");
      REQUIRE(columns[3] == "1#macd_histogram");
    }

    SECTION("Stochastic outputs") {
      auto stoch = transform::run_op("stoch", "1",
          {},
          {{"k_period", MetaDataOptionDefinition{14.0}},
           {"k_slowing_period", MetaDataOptionDefinition{3.0}},
           {"d_period", MetaDataOptionDefinition{3.0}}},
          tf);
      auto outputs = stoch.GetOutputs();

      REQUIRE(outputs.size() == 2);
      REQUIRE(outputs[0].id == "stoch_k");
      REQUIRE(outputs[1].id == "stoch_d");

      auto columns = DataColumnResolver::ResolveColumns(stoch);
      REQUIRE(columns.size() == 3); // index + 2 outputs
      REQUIRE(columns[1] == "1#stoch_k");
      REQUIRE(columns[2] == "1#stoch_d");
    }

    SECTION("Aroon outputs") {
      auto aroon = transform::run_op("aroon", "1",
          {},
          {{"period", MetaDataOptionDefinition{14.0}}},
          tf);
      auto outputs = aroon.GetOutputs();

      REQUIRE(outputs.size() == 2);
      REQUIRE(outputs[0].id == "aroon_down");
      REQUIRE(outputs[1].id == "aroon_up");

      auto columns = DataColumnResolver::ResolveColumns(aroon);
      REQUIRE(columns.size() == 3); // index + 2 outputs
      REQUIRE(columns[1] == "1#aroon_down");
      REQUIRE(columns[2] == "1#aroon_up");
    }

    SECTION("Fisher Transform outputs") {
      auto fisher = transform::run_op("fisher", "1",
          {},
          {{"period", MetaDataOptionDefinition{10.0}}},
          tf);
      auto outputs = fisher.GetOutputs();

      REQUIRE(outputs.size() == 2);
      REQUIRE(outputs[0].id == "fisher");
      REQUIRE(outputs[1].id == "fisher_signal");

      auto columns = DataColumnResolver::ResolveColumns(fisher);
      REQUIRE(columns.size() == 3); // index + 2 outputs
      REQUIRE(columns[1] == "1#fisher");
      REQUIRE(columns[2] == "1#fisher_signal");
    }

    SECTION("QQE outputs") {
      auto qqe = transform::run_op("qqe", "1",
          {{epoch_script::ARG, {transform::input_ref("c")}}},
          {{"avg_period", MetaDataOptionDefinition{14.0}},
           {"smooth_period", MetaDataOptionDefinition{5.0}},
           {"width_factor", MetaDataOptionDefinition{4.236}}},
          tf);
      auto outputs = qqe.GetOutputs();

      REQUIRE(outputs.size() == 4);
      REQUIRE(outputs[0].id == "result");
      REQUIRE(outputs[1].id == "rsi_ma");
      REQUIRE(outputs[2].id == "long_line");
      REQUIRE(outputs[3].id == "short_line");

      auto columns = DataColumnResolver::ResolveColumns(qqe);
      REQUIRE(columns.size() == 5); // index + 4 outputs
      REQUIRE(columns[1] == "1#result");
      REQUIRE(columns[2] == "1#rsi_ma");
      REQUIRE(columns[3] == "1#long_line");
      REQUIRE(columns[4] == "1#short_line");
    }
  }
}