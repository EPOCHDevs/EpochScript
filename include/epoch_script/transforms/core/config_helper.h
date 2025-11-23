// test_helpers.h
#pragma once

#include "transform_configuration.h"
#include <string>
#include <vector>

namespace epoch_script::transform {
inline auto no_operand_period_op(std::string const &op, auto &&id,
                                 int64_t period,
                                 epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{YAML::Load(std::format(
      "{{ type: {}, id: {}, options: {{ period: {} }}, timeframe: {} }}", op,
      id, period, timeframe.Serialize()))}};
}

inline auto
single_operand_period_op(std::string const &op, auto &&id, int64_t period,
                         std::string const &input,
                         epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format("{{ type: {}, id: {}, inputs: {{ 'SLOT': '{}' }}, "
                             "options: {{ period: {} }}, timeframe: {} }}",
                             op, id, input, period, timeframe.Serialize()))}};
}

inline auto run_op(std::string const &op, auto &&id, YAML::Node const &input,
                   YAML::Node const &options,
                   epoch_script::TimeFrame const &timeframe) {
  YAML::Node inputs_yaml;
  inputs_yaml["type"] = op;
  inputs_yaml["id"] = id;
  inputs_yaml["timeframe"] = YAML::Load(timeframe.Serialize());
  inputs_yaml["inputs"] = input;
  inputs_yaml["options"] = options;

  return TransformConfiguration{TransformDefinition{inputs_yaml}};
}

auto single_operand_op(std::string const &type, std::string const &op,
                       auto &&id, std::string const &input, int64_t value,
                       epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{YAML::Load(std::format(
      "{{ type: {}_{}, id: {}, inputs: {{ 'SLOT': '{}' }}, options: "
      "{{ value: {} }}, timeframe: {} }}",
      type, op, id, input, value, timeframe.Serialize()))}};
}

inline auto single_operand_op(std::string const &type, std::string const &op,
                              auto &&id, std::string const &input,
                              epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{YAML::Load(std::format(
      "{{ type: {}_{}, id: {}, inputs: {{ 'SLOT': '{}' }}, timeframe: {} }}",
      type, op, id, input, timeframe.Serialize()))}};
}

inline auto double_operand_op = [](std::string const &type,
                                   std::string const &op, auto &&id,
                                   std::string const &input1,
                                   std::string const &input2,
                                   epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{YAML::Load(
      std::format("{{ type: {}{}, id: {}, inputs: {{ 'SLOT0': '{}', 'SLOT1': "
                  "'{}' }}, timeframe: {} }}",
                  type, op, id, input1, input2, timeframe.Serialize()))}};
};

inline auto single_input_op = [](std::string const &op, std::string const &id,
                                 std::string const &input,
                                 const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: {}
id: {}
inputs:
  "SLOT": "{}"
timeframe: {}
)",
                             op, id, input, timeframe.Serialize()))}};
};

inline auto no_input_op = [](std::string const &op, std::string const &id,
                             const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: {}
id: {}
timeframe: {}
)",
                             op, id, timeframe.Serialize()))}};
};

// Scalar helpers
inline auto number_op = [](std::string const &id, double value,
                           epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{YAML::Load(std::format(
      "{{ type: number, id: {}, options: {{ value: {} }}, timeframe: {} }}", id,
      value, timeframe.Serialize()))}};
};

// Helper functions for all scalar constants
inline auto pi_op = [](std::string const &id,
                       epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: pi
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto e_op = [](std::string const &id,
                      epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: e
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto phi_op = [](std::string const &id,
                        epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: phi
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto sqrt2_op = [](std::string const &id,
                          epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: sqrt2
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto sqrt3_op = [](std::string const &id,
                          epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: sqrt3
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto sqrt5_op = [](std::string const &id,
                          epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: sqrt5
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto ln2_op = [](std::string const &id,
                        epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: ln2
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto ln10_op = [](std::string const &id,
                         epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: ln10
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto log2e_op = [](std::string const &id,
                          epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: log2e
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto log10e_op = [](std::string const &id,
                           epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: log10e
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto zero_op = [](std::string const &id,
                         epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: zero
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto one_op = [](std::string const &id,
                        epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: one
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto negative_one_op = [](std::string const &id,
                                 epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{
      TransformDefinition{YAML::Load(std::format(R"(
type: negative_one
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()))}};
};

inline auto atr = [](auto &&...args) {
  return no_operand_period_op("atr", args...);
};

inline auto bbands = [](std::string const &id, int period, int stddev,
                        std::string const &input,
                        const epoch_script::TimeFrame &timeframe) {
  YAML::Node inputs_yaml;
  inputs_yaml[epoch_script::ARG] = input;
  YAML::Node options_yaml;
  options_yaml["period"] = period;
  options_yaml["stddev"] = stddev;
  return run_op("bbands", id, inputs_yaml, options_yaml, timeframe);
};

inline auto bbands_percent = [](std::string const &id,
                                const std::string &bbands_lower,
                                const std::string &bbands_upper,
                                const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{YAML::Load(
      std::format(R"(
type: bband_percent
id: {}
inputs:
  "bbands_lower": "{}"
  "bbands_upper": "{}"
timeframe: {}
)",
                  id, bbands_lower, bbands_upper, timeframe.Serialize()))}};
};

// Generates a TransformConfiguration for BollingerBandsWidth.
// Example usage:
//   auto cfg = bbands_width("my_id", "bband_lower", "bband_middle",
//   "bband_upper", someTimeFrame);
inline auto bbands_width =
    [](std::string const &id, std::string const &bband_lower,
       std::string const &bband_middle, std::string const &bband_upper,
       const epoch_script::TimeFrame &timeframe) {
      return TransformConfiguration{TransformDefinition{YAML::Load(std::format(
          R"(
type: bband_width
id: {}
inputs:
  "bbands_lower": "{}"
  "bbands_middle": "{}"
  "bbands_upper": "{}"
timeframe: {}
)",
          id, bband_lower, bband_middle, bband_upper, timeframe.Serialize()))}};
    };

inline auto psar = [](std::string const &id, double acceleration_factor_step,
                      double acceleration_factor_maximum,
                      std::string const &input,
                      const epoch_script::TimeFrame &timeframe) {
  YAML::Node inputs_yaml;
  inputs_yaml[epoch_script::ARG] = input;
  YAML::Node options_yaml;
  options_yaml["acceleration_factor_step"] = acceleration_factor_step;
  options_yaml["acceleration_factor_maximum"] = acceleration_factor_maximum;
  return run_op("psar", id, inputs_yaml, options_yaml, timeframe);
};

inline auto crossany = [](std::string const &id, std::string const &input1,
                          std::string const &input2,
                          epoch_script::TimeFrame const &timeframe) {
  return double_operand_op("cross", "any", id, input1, input2, timeframe);
};

inline auto crossover = [](std::string const &id, std::string const &input1,
                           std::string const &input2,
                           epoch_script::TimeFrame const &timeframe) {
  return double_operand_op("cross", "over", id, input1, input2, timeframe);
};

inline auto crossunder = [](std::string const &id, std::string const &input1,
                            std::string const &input2,
                            epoch_script::TimeFrame const &timeframe) {
  return double_operand_op("cross", "under", id, input1, input2, timeframe);
};

inline auto cs_momentum = [](int64_t id, std::string const &input,
                             epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format("{{ type: cs_momentum, id: {}, inputs: {{ 'SLOT': "
                             "'{}' }}, timeframe: {} }}",
                             id, input, timeframe.Serialize()))}};
};

inline auto cs_topk = [](int64_t id, std::string const &input, int64_t k,
                         epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: top_k
id: {}
inputs:
  "SLOT": "{}"
options:
  k: {}
timeframe: {}
)",
                             id, input, k, timeframe.Serialize()))}};
};

inline auto cs_bottomk = [](int64_t id, std::string const &input, int64_t k,
                            epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: bottom_k
id: {}
inputs:
  "SLOT": "{}"
options:
  k: {}
timeframe: {}
)",
                             id, input, k, timeframe.Serialize()))}};
};

inline auto cs_topk_percentile =
    [](int64_t id, std::string const &input, int64_t k,
       epoch_script::TimeFrame const &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: top_k_percent
id: {}
inputs:
  "SLOT": "{}"
options:
  k: {}
timeframe: {}
)",
                                 id, input, k, timeframe.Serialize()))}};
    };

inline auto cs_bottomk_percentile =
    [](int64_t id, std::string const &input, int64_t k,
       epoch_script::TimeFrame const &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: bottom_k_percent
id: {}
inputs:
  "SLOT": "{}"
options:
  k: {}
timeframe: {}
)",
                                 id, input, k, timeframe.Serialize()))}};
    };

inline auto cum_prod = [](auto &&...args) {
  return single_input_op("cum_prod", args...);
};

inline auto lag = [](auto &&...args) {
  return single_operand_period_op("lag", args...);
};

inline auto stddev = [](auto &&...args) {
  return single_operand_period_op("stddev", args...);
};

inline auto roc = [](auto &&...args) {
  return single_operand_period_op("roc", args...);
};

inline auto logical_op = [](auto &&...args) {
  return double_operand_op("logical_", args...);
};

inline auto vector_op = [](auto &&...args) {
  return double_operand_op("", args...);
};

inline auto vector_mul = [](auto &&...args) {
  return double_operand_op("", "mul", args...);
};

inline auto vector_add = [](auto &&...args) {
  return double_operand_op("", "add", args...);
};

inline auto ma = [](std::string const &type, auto const &id,
                    std::string const &input, int64_t period,
                    epoch_script::TimeFrame const &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: {}
id: {}
inputs:
  "SLOT": {}
options:
  period: {}
timeframe: {}
)",
                             type, id, input, period, timeframe.Serialize()))}};
};

inline auto sma = [](auto &&...args) { return ma("sma", args...); };

// NOTE: boolean_select helper removed - use typed variants:
// boolean_select_string, boolean_select_number, boolean_select_boolean, boolean_select_timestamp

inline auto select_n = [](int64_t id, int n, std::string const &index,
                          const std::vector<std::string> &options,
                          const epoch_script::TimeFrame &timeframe) {
  // Updated for typed switch transforms: switch{N}_number
  YAML::Node inputs_yaml;
  inputs_yaml["type"] = "switch" + std::to_string(n) + "_number";
  inputs_yaml["id"] = id;
  inputs_yaml["timeframe"] = YAML::Load(timeframe.Serialize());

  inputs_yaml["inputs"]["index"] = index;
  for (int i = 0; i < n; ++i) {
    inputs_yaml["inputs"]["SLOT" + std::to_string(i)] = options[i];
  }

  return TransformConfiguration{TransformDefinition{inputs_yaml}};
};

// NOTE: first_non_null helper removed - use typed variants:
// first_non_null_string, first_non_null_number, first_non_null_boolean, first_non_null_timestamp

// NOTE: conditional_select untyped helper removed - use typed variants with this helper:
// Helper for typed conditional_select variants (conditional_select_number, etc.)
// inputs: vector of input IDs in order [cond0, val0, cond1, val1, ..., optional_default]
inline auto typed_conditional_select = [](const std::string &type, int64_t id,
                                          const std::vector<std::string> &inputs,
                                          const epoch_script::TimeFrame &timeframe) {
  YAML::Node inputs_yaml;
  inputs_yaml["type"] = type;  // e.g., "conditional_select_number"
  inputs_yaml["id"] = id;
  inputs_yaml["timeframe"] = YAML::Load(timeframe.Serialize());

  // VARARGS inputs - use ARG key with vector value
  inputs_yaml["inputs"][epoch_script::ARG] = inputs;

  return TransformConfiguration{TransformDefinition{inputs_yaml}};
};

// Helper for typed first_non_null variants (first_non_null_number, etc.)
// inputs: vector of input IDs to check in order
inline auto typed_first_non_null = [](const std::string &type, int64_t id,
                                      const std::vector<std::string> &inputs,
                                      const epoch_script::TimeFrame &timeframe) {
  YAML::Node inputs_yaml;
  inputs_yaml["type"] = type;  // e.g., "first_non_null_number"
  inputs_yaml["id"] = id;
  inputs_yaml["timeframe"] = YAML::Load(timeframe.Serialize());

  // VARARGS inputs - use ARG key with vector value
  inputs_yaml["inputs"][epoch_script::ARG] = inputs;

  return TransformConfiguration{TransformDefinition{inputs_yaml}};
};

inline auto rolling_volatility =
    [](std::string const &id, int64_t period,
       const epoch_script::TimeFrame &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: return_vol
id: {}
options:
  "period": "{}"
timeframe: {}
)",
                                 id, period, timeframe.Serialize()))}};
    };

inline auto price_diff_volatility =
    [](std::string const &id, int64_t period,
       const epoch_script::TimeFrame &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: price_diff_vol
id: {}
options:
  "period": "{}"
timeframe: {}
)",
                                 id, period, timeframe.Serialize()))}};
    };

inline auto swing_highs_lows = [](std::string const &id, int64_t swing_length,
                                  const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: swing_highs_lows
id: {}
options:
  "swing_length": {}
timeframe: {}
)",
                             id, swing_length, timeframe.Serialize()))}};
};

inline auto order_blocks =
    [](std::string const &id, std::string const &high_low,
       bool close_mitigation, const epoch_script::TimeFrame &timeframe) {
      YAML::Node config = YAML::Load(std::format(
          R"(
type: order_blocks
id: {}
options:
  "close_mitigation": {}
timeframe: {}
)",
          id, close_mitigation ? "true" : "false", timeframe.Serialize()));

      YAML::Node inputs;
      inputs["high_low"] = high_low;
      config["inputs"] = inputs;

      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto bos_choch = [](std::string const &id, std::string const &high_low,
                           std::string const &level, bool close_break,
                           const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: bos_choch
id: {}
options:
  "close_break": {}
timeframe: {}
)",
                             id, close_break, timeframe.Serialize()));

  YAML::Node inputs;
  inputs["high_low"] = high_low;
  inputs["level"] = level;
  config["inputs"] = inputs;

  return TransformConfiguration{TransformDefinition{config}};
};

inline auto liquidity = [](std::string const &id, std::string const &high_low,
                           std::string const &level, double range_percent,
                           const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: liquidity
id: {}
options:
  "range_percent": {}
timeframe: {}
)",
                             id, range_percent, timeframe.Serialize()));

  YAML::Node inputs;
  inputs["high_low"] = high_low;
  inputs["level"] = level;

  config["inputs"] = inputs;

  return TransformConfiguration{TransformDefinition{config}};
};

inline auto retracements =
    [](std::string const &id, std::string const &high_low,
       std::string const &level, const epoch_script::TimeFrame &timeframe) {
      YAML::Node config = YAML::Load(std::format(R"(
type: retracements
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()));

      YAML::Node inputs;
      inputs["high_low"] = high_low;
      inputs["level"] = level;

      config["inputs"] = inputs;

      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto fair_value_gap = [](std::string const &id, bool join_consecutive,
                                const epoch_script::TimeFrame &timeframe) {
  YAML::Node node;
  node["type"] = "fair_value_gap";
  node["id"] = id;
  node["timeframe"] = YAML::Load(timeframe.Serialize());
  node["options"]["join_consecutive"] = join_consecutive;
  return TransformConfiguration{TransformDefinition{node}};
};

inline auto sessions = [](std::string const &id,
                          std::string const &session_name,
                          const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: sessions
id: {}
options:
  "session_type": {}
timeframe: {}
)",
                             id, session_name, timeframe.Serialize()))}};
};

// Session Time Window - Detects proximity to session boundaries
inline auto session_time_window = [](std::string const &id,
                                      std::string const &session_type,
                                      int64_t minute_offset,
                                      std::string const &boundary_type,
                                      const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: session_time_window
id: {}
options:
  "session_type": {}
  "minute_offset": {}
  "boundary_type": {}
timeframe: {}
)",
                             id, session_type, minute_offset, boundary_type,
                             timeframe.Serialize()))}};
};

inline auto previous_high_low = [](std::string const &id, int64_t interval,
                                   std::string const &type,
                                   const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: previous_high_low
id: {}
options:
  interval: {}
  type: {}
timeframe: {}
)",
                             id, interval, type, timeframe.Serialize()))}};
};

// NOTE: percentile_select helper removed - use typed variants:
// percentile_select_string, percentile_select_number, percentile_select_boolean, percentile_select_timestamp

inline auto boolean_branch = [](std::string const &id,
                                std::string const &condition,
                                const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: boolean_branch
id: {}
inputs:
  "condition": {}
timeframe: {}
outputs:
  - id: "true"
    name: "True Branch"
  - id: "false"
    name: "False Branch"
)",
                             id, condition, timeframe.Serialize()))}};
};

inline auto ratio_branch = [](std::string const &id, std::string const &ratio,
                              double threshold_high, double threshold_low,
                              const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{YAML::Load(std::format(
      R"(
type: ratio_branch
id: {}
inputs:
  "ratio": {}
options:
  threshold_high: {}
  threshold_low: {}
timeframe: {}
outputs:
  - id: "high"
    name: "High Branch"
  - id: "normal"
    name: "Normal Branch"
  - id: "low"
    name: "Low Branch"
  - id: "ratio"
    name: "Ratio Value"
)",
      id, ratio, threshold_high, threshold_low, timeframe.Serialize()))}};
};

inline auto previous_gt = [](std::string const &id, std::string const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: previous_gt
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto previous_gte = [](std::string const &id, std::string const &input,
                              int64_t periods,
                              const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: previous_gte
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto previous_lt = [](std::string const &id, std::string const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: previous_lt
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto previous_lte = [](std::string const &id, std::string const &input,
                              int64_t periods,
                              const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: previous_lte
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto previous_eq = [](std::string const &id, std::string const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: previous_eq
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto previous_neq = [](std::string const &id, std::string const &input,
                              int64_t periods,
                              const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: previous_neq
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto highest_gt = [](std::string const &id, std::string const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: highest_gt
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto highest_gte = [](std::string const &id, std::string const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: highest_gte
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto highest_lt = [](std::string const &id, std::string const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: highest_lt
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto highest_lte = [](std::string const &id, std::string const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: highest_lte
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto highest_eq = [](std::string const &id, std::string const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: highest_eq
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto highest_neq = [](std::string const &id, std::string const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: highest_neq
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto lowest_gt = [](std::string const &id, std::string const &input,
                           int64_t periods,
                           const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: lowest_gt
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto lowest_gte = [](std::string const &id, std::string const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: lowest_gte
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto lowest_lt = [](std::string const &id, std::string const &input,
                           int64_t periods,
                           const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: lowest_lt
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto lowest_lte = [](std::string const &id, std::string const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: lowest_lte
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto lowest_eq = [](std::string const &id, std::string const &input,
                           int64_t periods,
                           const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: lowest_eq
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

inline auto lowest_neq = [](std::string const &id, std::string const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  return TransformConfiguration{TransformDefinition{
      YAML::Load(std::format(R"(
type: lowest_neq
id: {}
inputs:
  "SLOT": "{}"
options:
  periods: {}
timeframe: {}
)",
                             id, input, periods, timeframe.Serialize()))}};
};

// Aggregate transform helpers
inline auto aggregate_transform =
    [](std::string const &agg_type, std::string const &id,
       const std::vector<std::string> &inputs,
       epoch_script::TimeFrame const &timeframe) {
      YAML::Node inputs_yaml;
      inputs_yaml["SLOT"] = inputs;
      return run_op("agg_" + agg_type, id, inputs_yaml, YAML::Node{},
                    timeframe);
    };

inline auto agg_sum = [](std::string const &id,
                         const std::vector<std::string> &inputs,
                         epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("sum", id, inputs, timeframe);
};

inline auto agg_mean = [](std::string const &id,
                          const std::vector<std::string> &inputs,
                          epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("mean", id, inputs, timeframe);
};

inline auto agg_min = [](std::string const &id,
                         const std::vector<std::string> &inputs,
                         epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("min", id, inputs, timeframe);
};

inline auto agg_max = [](std::string const &id,
                         const std::vector<std::string> &inputs,
                         epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("max", id, inputs, timeframe);
};

inline auto agg_all_of = [](std::string const &id,
                            const std::vector<std::string> &inputs,
                            epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("all_of", id, inputs, timeframe);
};

inline auto agg_any_of = [](std::string const &id,
                            const std::vector<std::string> &inputs,
                            epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("any_of", id, inputs, timeframe);
};

inline auto agg_none_of = [](std::string const &id,
                             const std::vector<std::string> &inputs,
                             epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("none_of", id, inputs, timeframe);
};

inline auto agg_all_equal = [](std::string const &id,
                               const std::vector<std::string> &inputs,
                               epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("all_equal", id, inputs, timeframe);
};

inline auto agg_all_unique = [](std::string const &id,
                                const std::vector<std::string> &inputs,
                                epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("all_unique", id, inputs, timeframe);
};

inline auto abands_cfg = [](std::string const &id, int64_t period,
                            double multiplier,
                            const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: acceleration_bands
id: {}
options:
  "period": {}
  "multiplier": {}
timeframe: {}
)",
                             id, period, multiplier, timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

inline auto chande_kroll_cfg = [](std::string const &id, int64_t p_period,
                                  int64_t q_period, double multiplier,
                                  const epoch_script::TimeFrame &timeframe) {
  YAML::Node config = YAML::Load(std::format(R"(
type: chande_kroll_stop
id: {}
options:
  "p_period": {}
  "q_period": {}
  "multiplier": {}
timeframe: {}
)",
                                             id, p_period, q_period, multiplier,
                                             timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

inline auto garman_klass_cfg = [](std::string const &id, int64_t period,
                                  int64_t trading_days,
                                  const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: garman_klass
id: {}
options:
  "period": {}
  "trading_days": {}
timeframe: {}
)",
                             id, period, trading_days, timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

inline auto hodges_tompkins_cfg =
    [](std::string const &id, int64_t period, int64_t trading_periods,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config = YAML::Load(std::format(R"(
type: hodges_tompkins
id: {}
options:
  "period": {}
  "trading_periods": {}
timeframe: {}
)",
                                                 id, period, trading_periods,
                                                 timeframe.Serialize()));
      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto keltner_channels_cfg =
    [](std::string const &id, int64_t roll_period, double band_multiplier,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config = YAML::Load(std::format(
          R"(
type: keltner_channels
id: {}
options:
  "roll_period": {}
  "band_multiplier": {}
timeframe: {}
)",
          id, roll_period, band_multiplier, timeframe.Serialize()));
      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto parkinson_cfg = [](std::string const &id, int64_t period,
                               int64_t trading_days,
                               const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: parkinson
id: {}
options:
  "period": {}
  "trading_periods": {}
timeframe: {}
)",
                             id, period, trading_days, timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

inline auto ulcer_index_cfg = [](std::string const &id, int64_t period,
                                 bool use_sum,
                                 const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: ulcer_index
id: {}
options:
  "period": {}
  "use_sum": {}
timeframe: {}
)",
                             id, period, use_sum, timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

inline auto yang_zhang_cfg = [](std::string const &id, int64_t period,
                                int64_t trading_days,
                                const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: yang_zhang
id: {}
options:
  "period": {}
  "trading_periods": {}
timeframe: {}
)",
                             id, period, trading_days, timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

// Indicators

inline auto pivot_point_sr_cfg =
    [](std::string const &id, const epoch_script::TimeFrame &timeframe) {
      YAML::Node config = YAML::Load(std::format(R"(
type: pivot_point_sr
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()));
      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto hurst_exponent_cfg =
    [](std::string const &id, int64_t min_period, std::string const &input,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config =
          YAML::Load(std::format(R"(
type: hurst_exponent
id: {}
inputs:
  "SLOT": {}
options:
  "min_period": {}
timeframe: {}
)",
                                 id, input, min_period, timeframe.Serialize()));
      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto rolling_hurst_exponent_cfg =
    [](std::string const &id, int64_t period, std::string const &input,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config =
          YAML::Load(std::format(R"(
type: rolling_hurst_exponent
id: {}
inputs:
  "SLOT": {}
options:
  "window": {}
timeframe: {}
)",
                                 id, input, period, timeframe.Serialize()));
      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto elders_thermometer_cfg =
    [](std::string const &id, int64_t period, double buy_factor,
       double sell_factor, const epoch_script::TimeFrame &timeframe) {
      YAML::Node config = YAML::Load(std::format(
          R"(
type: elders_thermometer
id: {}
options:
  "period": {}
  "buy_factor": {}
  "sell_factor": {}
timeframe: {}
)",
          id, period, buy_factor, sell_factor, timeframe.Serialize()));
      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto peaks_and_valleys_cfg =
    [](std::string const &id, const epoch_script::TimeFrame &timeframe) {
      YAML::Node config = YAML::Load(std::format(R"(
type: peaks_and_valleys
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()));
      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto price_distance_cfg =
    [](std::string const &id, const epoch_script::TimeFrame &timeframe) {
      YAML::Node config = YAML::Load(std::format(R"(
type: price_distance
id: {}
timeframe: {}
)",
                                                 id, timeframe.Serialize()));
      return TransformConfiguration{TransformDefinition{config}};
    };

inline auto psl_cfg = [](std::string const &id, int64_t period,
                         const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: psl
id: {}
options:
  "period": {}
timeframe: {}
)",
                             id, period, timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

inline auto qqe_cfg = [](std::string const &id, int64_t avg_period,
                         int64_t smooth_period, double width_factor,
                         const epoch_script::TimeFrame &timeframe) {
  YAML::Node config = YAML::Load(std::format(
      R"(
type: qqe
id: {}
options:
  "avg_period": {}
  "smooth_period": {}
  "width_factor": {}
timeframe: {}
)",
      id, avg_period, smooth_period, width_factor, timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

inline auto vortex_cfg = [](std::string const &id, int64_t period,
                            const epoch_script::TimeFrame &timeframe) {
  YAML::Node config =
      YAML::Load(std::format(R"(
type: vortex
id: {}
options:
  "period": {}
timeframe: {}
)",
                             id, period, timeframe.Serialize()));
  return TransformConfiguration{TransformDefinition{config}};
};

// Trade Executor helpers
inline auto trade_executor_adapter_cfg =
    [](std::string const &id, std::string const &input,
       const epoch_script::TimeFrame &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: trade_executor_adapter
id: {}
inputs:
  "SLOT": "{}"
timeframe: {}
)",
                                 id, input, timeframe.Serialize()))}};
    };

inline auto trade_signal_executor_cfg =
    [](std::string const &id,
       const std::unordered_map<std::string, std::string> &inputs,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config;
      config["type"] = "trade_signal_executor";
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());

      YAML::Node inputsNode;
      for (const auto &[key, value] : inputs) {
        inputsNode[key] = value;
      }
      config["inputs"] = inputsNode;

      return TransformConfiguration{TransformDefinition{config}};
    };
inline auto data_source = [](std::string const &id,
                             const epoch_script::TimeFrame &timeframe) {
  YAML::Node config = YAML::Load(std::format(R"(
type: market_data_source
id: {}
options: {{}}
timeframe: {}
)",
                                             id, timeframe.Serialize()));

  return TransformConfiguration{TransformDefinition{config}};
};

// Scalar aggregation config helpers
// Generic helper for <agg_type>_scalar with common options
inline auto scalar_aggregation_cfg =
    [](std::string const &agg_type, std::string const &id,
       std::string const &input, const epoch_script::TimeFrame &timeframe,
       YAML::Node const &options = {}) {
      YAML::Node config;
      config["type"] = "scalar_" + agg_type;
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());
      config["inputs"]["SLOT"] = input;
      // Pass required options explicitly; do not rely on metadata defaults
      config["options"] = options;
      if (!options["skip_nulls"] && !agg_type.starts_with("count_") &&
          agg_type != "kurtosis" && agg_type != "skew") {
        config["options"]["skip_nulls"] = false;
      }
      if (!options["min_count"] && !agg_type.starts_with("count") &&
          agg_type != "kurtosis" && agg_type != "skew") {
        config["options"]["min_count"] = 1;
      }
      if ((agg_type == "stddev" || agg_type == "variance") &&
          !options["ddof"]) {
        config["options"]["ddof"] = 1;
      }
      if ((agg_type == "quantile" || agg_type == "tdigest") &&
          !options["quantile"]) {
        config["options"]["quantile"] = 0.5;
      }
      return TransformConfiguration{TransformDefinition{config}};
    };

// stddev(id, input) with ddof option
inline auto stddev_scalar_cfg = []<class... T>(int64_t ddof, T &&...args) {
  YAML::Node options;
  options["ddof"] = ddof;
  return scalar_aggregation_cfg("stddev", std::forward<T>(args)..., options);
};

// variance(id, input) with ddof option
inline auto variance_scalar_cfg = []<class... T>(int64_t ddof, T &&...args) {
  YAML::Node options;
  options["ddof"] = ddof;
  return scalar_aggregation_cfg("variance", std::forward<T>(args)..., options);
};

// quantile(id, input) with quantile option
inline auto quantile_scalar_cfg = []<class... T>(double quantile, T &&...args) {
  YAML::Node options;
  options["quantile"] = quantile;
  return scalar_aggregation_cfg("quantile", std::forward<T>(args)..., options);
};

inline auto tdigest_scalar_cfg = []<class... T>(double quantile, T &&...args) {
  YAML::Node options;
  options["quantile"] = quantile;
  return scalar_aggregation_cfg("tdigest", std::forward<T>(args)..., options);
};

// sum(id, input)
inline auto sum_scalar_cfg = []<class... T>(T &&...args) {
  return scalar_aggregation_cfg("sum", std::forward<T>(args)...);
};

// mean(id, input) with options
inline auto mean_scalar_cfg = []<class... T>(bool skip_nulls, int min_count,
                                             T &&...args) {
  YAML::Node options;
  options["skip_nulls"] = skip_nulls;
  options["min_count"] = min_count;
  return scalar_aggregation_cfg("mean", std::forward<T>(args)..., options);
};

inline auto count_all_scalar_cfg = []<class... T>(T &&...args) {
  return scalar_aggregation_cfg("count_all", std::forward<T>(args)...);
};

// =========================
// HMM configuration helpers
// =========================
// HMM helper with dynamic number of states
// Returns: state (int), prob (list[double]), transition_matrix (list[double])
inline auto hmm_cfg =
    [](std::string const &id, const std::vector<std::string> &inputs,
       const epoch_script::TimeFrame &timeframe, int n_states = 3,
       std::size_t max_iterations = 1000, double tolerance = 1e-5,
       bool compute_zscore = true, std::size_t min_training_samples = 100,
       std::size_t lookback_window = 0) {
      YAML::Node cfg;
      cfg["type"] = "hmm_" + std::to_string(n_states);
      cfg["id"] = id;
      cfg["timeframe"] = YAML::Load(timeframe.Serialize());
      cfg["inputs"]["SLOT"] = inputs; // multiple input series supported

      // Note: n_states is NOT an option - it's encoded in the type name
      cfg["options"]["max_iterations"] = static_cast<int64_t>(max_iterations);
      cfg["options"]["tolerance"] = tolerance;
      cfg["options"]["compute_zscore"] = compute_zscore;
      cfg["options"]["min_training_samples"] =
          static_cast<int64_t>(min_training_samples);
      cfg["options"]["lookback_window"] = static_cast<int64_t>(lookback_window);

      return TransformConfiguration{TransformDefinition{cfg}};
    };

// Convenience single-input HMM helper
inline auto hmm_single_cfg =
    [](std::string const &id, std::string const &input,
       const epoch_script::TimeFrame &timeframe, int n_states = 3,
       std::size_t max_iterations = 1000, double tolerance = 1e-5,
       bool compute_zscore = true, std::size_t min_training_samples = 100,
       std::size_t lookback_window = 0) {
      return hmm_cfg(id, std::vector<std::string>{input}, timeframe, n_states,
                     max_iterations, tolerance, compute_zscore,
                     min_training_samples, lookback_window);
    };

// ========================================
// Chart Formation configuration helpers
// ========================================

// Triangles - Detects ascending, descending, and symmetrical triangle patterns
inline auto triangles_cfg =
    [](std::string const &id, int64_t lookback,
       std::string const &triangle_type, double r_squared_min,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config;
      config["type"] = "triangles";
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());
      config["options"]["lookback"] = lookback;
      config["options"]["triangle_type"] = triangle_type;
      config["options"]["r_squared_min"] = r_squared_min;
      return TransformConfiguration{TransformDefinition{config}};
    };

// Flag - Detects bull and bear flag patterns
inline auto flag_cfg =
    [](std::string const &id, int64_t lookback, int64_t min_pivot_points,
       double r_squared_min, double slope_parallel_tolerance,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config;
      config["type"] = "flag";
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());
      config["options"]["lookback"] = lookback;
      config["options"]["min_pivot_points"] = min_pivot_points;
      config["options"]["r_squared_min"] = r_squared_min;
      config["options"]["slope_parallel_tolerance"] = slope_parallel_tolerance;
      return TransformConfiguration{TransformDefinition{config}};
    };

// Pennant - Detects brief consolidation patterns
inline auto pennant_cfg =
    [](std::string const &id, int64_t lookback, int64_t min_pivot_points,
       double r_squared_min, int64_t max_duration,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config;
      config["type"] = "pennant";
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());
      config["options"]["lookback"] = lookback;
      config["options"]["min_pivot_points"] = min_pivot_points;
      config["options"]["r_squared_min"] = r_squared_min;
      config["options"]["max_duration"] = max_duration;
      return TransformConfiguration{TransformDefinition{config}};
    };

// Head and Shoulders - Detects bearish reversal pattern
inline auto head_and_shoulders_cfg =
    [](std::string const &id, int64_t lookback, double head_ratio_before,
       double head_ratio_after, double neckline_slope_max,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config;
      config["type"] = "head_and_shoulders";
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());
      config["options"]["lookback"] = lookback;
      config["options"]["head_ratio_before"] = head_ratio_before;
      config["options"]["head_ratio_after"] = head_ratio_after;
      config["options"]["neckline_slope_max"] = neckline_slope_max;
      return TransformConfiguration{TransformDefinition{config}};
    };

// Inverse Head and Shoulders - Detects bullish reversal pattern
inline auto inverse_head_and_shoulders_cfg =
    [](std::string const &id, int64_t lookback, double head_ratio_before,
       double head_ratio_after, double neckline_slope_max,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config;
      config["type"] = "inverse_head_and_shoulders";
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());
      config["options"]["lookback"] = lookback;
      config["options"]["head_ratio_before"] = head_ratio_before;
      config["options"]["head_ratio_after"] = head_ratio_after;
      config["options"]["neckline_slope_max"] = neckline_slope_max;
      return TransformConfiguration{TransformDefinition{config}};
    };

// Double Top/Bottom - Detects M/W reversal patterns
inline auto double_top_bottom_cfg =
    [](std::string const &id, int64_t lookback,
       std::string const &pattern_type, double similarity_tolerance,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config;
      config["type"] = "double_top_bottom";
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());
      config["options"]["lookback"] = lookback;
      config["options"]["pattern_type"] = pattern_type;
      config["options"]["similarity_tolerance"] = similarity_tolerance;
      return TransformConfiguration{TransformDefinition{config}};
    };

// ConsolidationBox - Detects horizontal consolidation boxes
inline auto consolidation_box_cfg =
    [](std::string const &id, int64_t lookback, int64_t min_pivot_points,
       double r_squared_min, double max_slope,
       const epoch_script::TimeFrame &timeframe) {
      YAML::Node config;
      config["type"] = "consolidation_box";
      config["id"] = id;
      config["timeframe"] = YAML::Load(timeframe.Serialize());
      config["options"]["lookback"] = lookback;
      config["options"]["min_pivot_points"] = min_pivot_points;
      config["options"]["r_squared_min"] = r_squared_min;
      config["options"]["max_slope"] = max_slope;
      return TransformConfiguration{TransformDefinition{config}};
    };

// =========================
// Event Marker configuration helpers
// =========================

// Event Marker with Filter - Uses boolean column to filter rows
// Accepts EventMarkerSchema object directly - NO YAML!
inline auto event_marker_cfg =
    [](std::string const &id, epoch_script::EventMarkerSchema const &schema,
       const std::vector<std::string> &inputs,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
        .type = "event_marker",
        .id = id,
        .options = {{"schema", epoch_script::MetaDataOptionDefinition{epoch_script::MetaDataOptionDefinition::T{schema}}}},
        .timeframe = timeframe,
        .inputs = {{"SLOT", inputs}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };


// =========================
// String operation helpers
// =========================

inline auto string_case_cfg = [](std::string const &id, std::string const &operation,
                                  std::string const &input,
                                  const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_case",
      .id = id,
      .options = {{"operation", MetaDataOptionDefinition{operation}}},
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<std::string>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_trim_cfg = [](std::string const &id, std::string const &operation,
                                  std::string const &input, std::string const &trim_chars,
                                  const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_trim",
      .id = id,
      .options = {
          {"operation", MetaDataOptionDefinition{operation}},
          {"trim_chars", MetaDataOptionDefinition{trim_chars}}
      },
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<std::string>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_contains_cfg = [](std::string const &id, std::string const &operation,
                                      std::string const &input, std::string const &pattern,
                                      const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_contains",
      .id = id,
      .options = {
          {"operation", MetaDataOptionDefinition{operation}},
          {"pattern", MetaDataOptionDefinition{pattern}}
      },
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<std::string>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_check_cfg = [](std::string const &id, std::string const &operation,
                                   std::string const &input,
                                   const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_check",
      .id = id,
      .options = {{"operation", MetaDataOptionDefinition{operation}}},
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<std::string>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_replace_cfg = [](std::string const &id, std::string const &input,
                                     std::string const &pattern, std::string const &replacement,
                                     const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_replace",
      .id = id,
      .options = {
          {"pattern", MetaDataOptionDefinition{pattern}},
          {"replacement", MetaDataOptionDefinition{replacement}}
      },
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<std::string>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_length_cfg = [](std::string const &id, std::string const &input,
                                    const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_length",
      .id = id,
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<std::string>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_reverse_cfg = [](std::string const &id, std::string const &input,
                                     const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_reverse",
      .id = id,
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<std::string>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// =========================
// ML/AI Transform Helpers
// =========================

// FinBERT Sentiment Analysis - AWS SageMaker sentiment analysis
inline auto finbert_sentiment_cfg = [](std::string const &id,
                                        std::string const &input,
                                        const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "finbert_sentiment",
      .id = id,
      .options = {},
      .timeframe = timeframe,
      .inputs = {{ARG, std::vector<std::string>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// =========================
// Datetime operation helpers
// =========================

// Index Datetime Extract - Extract datetime component from bar index
inline auto index_datetime_extract_cfg =
    [](std::string const &id, std::string const &component,
       const epoch_script::TimeFrame &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: index_datetime_extract
id: {}
options:
  component: "{}"
timeframe: {}
)",
                                 id, component, timeframe.Serialize()))}};
    };

// Column Datetime Extract - Extract datetime component from timestamp column
inline auto column_datetime_extract_cfg =
    [](std::string const &id, std::string const &input,
       std::string const &component, const epoch_script::TimeFrame &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: column_datetime_extract
id: {}
inputs:
  SLOT: "{}"
options:
  component: "{}"
timeframe: {}
)",
                                 id, input, component, timeframe.Serialize()))}};
    };

// Datetime Diff - Calculate time difference between two timestamps
inline auto datetime_diff_cfg =
    [](std::string const &id, std::string const &start_input,
       std::string const &end_input, std::string const &unit,
       const epoch_script::TimeFrame &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: datetime_diff
id: {}
inputs:
  SLOT0: "{}"
  SLOT1: "{}"
options:
  unit: "{}"
timeframe: {}
)",
                                 id, start_input, end_input, unit,
                                 timeframe.Serialize()))}};
    };

// Timestamp Scalar - Create constant timestamp value
inline auto timestamp_scalar_cfg =
    [](std::string const &id, std::string const &value,
       const epoch_script::TimeFrame &timeframe) {
      return TransformConfiguration{TransformDefinition{
          YAML::Load(std::format(R"(
type: timestamp_scalar
id: {}
options:
  value: "{}"
timeframe: {}
)",
                                 id, value, timeframe.Serialize()))}};
    };

// Validation transform helpers
inline auto is_null_cfg = [](std::string const &id, std::string const &input,
                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("is_null", id, input, timeframe);
};

inline auto is_valid_cfg = [](std::string const &id, std::string const &input,
                               const epoch_script::TimeFrame &timeframe) {
  return single_input_op("is_valid", id, input, timeframe);
};

inline auto is_zero_cfg = [](std::string const &id, std::string const &input,
                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("is_zero", id, input, timeframe);
};

inline auto is_one_cfg = [](std::string const &id, std::string const &input,
                             const epoch_script::TimeFrame &timeframe) {
  return single_input_op("is_one", id, input, timeframe);
};

// Static cast transform helpers (compiler-inserted type materializers)
inline auto static_cast_to_integer_cfg = [](std::string const &id, std::string const &input,
                                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_integer", id, input, timeframe);
};

inline auto static_cast_to_decimal_cfg = [](std::string const &id, std::string const &input,
                                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_decimal", id, input, timeframe);
};

inline auto static_cast_to_boolean_cfg = [](std::string const &id, std::string const &input,
                                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_boolean", id, input, timeframe);
};

inline auto static_cast_to_string_cfg = [](std::string const &id, std::string const &input,
                                             const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_string", id, input, timeframe);
};

inline auto static_cast_to_timestamp_cfg = [](std::string const &id, std::string const &input,
                                                const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_timestamp", id, input, timeframe);
};

// GroupBy aggregate transform helpers
inline auto groupby_numeric_agg = [](std::string const &id,
                                      std::string const &agg_type,
                                      std::string const &group_key,
                                      std::string const &value,
                                      epoch_script::TimeFrame const &timeframe) {
  YAML::Node inputs_yaml;
  inputs_yaml["group_key"] = group_key;
  inputs_yaml["value"] = value;

  YAML::Node options_yaml;
  options_yaml["agg"] = agg_type;

  return run_op("groupby_numeric_agg", id, inputs_yaml, options_yaml, timeframe);
};

inline auto groupby_boolean_agg = [](std::string const &id,
                                      std::string const &agg_type,
                                      std::string const &group_key,
                                      std::string const &value,
                                      epoch_script::TimeFrame const &timeframe) {
  YAML::Node inputs_yaml;
  inputs_yaml["group_key"] = group_key;
  inputs_yaml["value"] = value;

  YAML::Node options_yaml;
  options_yaml["agg"] = agg_type;

  return run_op("groupby_boolean_agg", id, inputs_yaml, options_yaml, timeframe);
};

inline auto groupby_any_agg = [](std::string const &id,
                                  std::string const &agg_type,
                                  std::string const &group_key,
                                  std::string const &value,
                                  epoch_script::TimeFrame const &timeframe) {
  YAML::Node inputs_yaml;
  inputs_yaml["group_key"] = group_key;
  inputs_yaml["value"] = value;

  YAML::Node options_yaml;
  options_yaml["agg"] = agg_type;

  return run_op("groupby_any_agg", id, inputs_yaml, options_yaml, timeframe);
};

// Convenience helpers for specific numeric aggregations
inline auto groupby_sum = [](std::string const &id,
                              std::string const &group_key,
                              std::string const &value,
                              epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "sum", group_key, value, timeframe);
};

inline auto groupby_mean = [](std::string const &id,
                               std::string const &group_key,
                               std::string const &value,
                               epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "mean", group_key, value, timeframe);
};

inline auto groupby_count = [](std::string const &id,
                                std::string const &group_key,
                                std::string const &value,
                                epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "count", group_key, value, timeframe);
};

inline auto groupby_first = [](std::string const &id,
                                std::string const &group_key,
                                std::string const &value,
                                epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "first", group_key, value, timeframe);
};

inline auto groupby_last = [](std::string const &id,
                               std::string const &group_key,
                               std::string const &value,
                               epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "last", group_key, value, timeframe);
};

inline auto groupby_min = [](std::string const &id,
                              std::string const &group_key,
                              std::string const &value,
                              epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "min", group_key, value, timeframe);
};

inline auto groupby_max = [](std::string const &id,
                              std::string const &group_key,
                              std::string const &value,
                              epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "max", group_key, value, timeframe);
};

// Convenience helpers for boolean aggregations
inline auto groupby_allof = [](std::string const &id,
                                std::string const &group_key,
                                std::string const &value,
                                epoch_script::TimeFrame const &timeframe) {
  return groupby_boolean_agg(id, "AllOf", group_key, value, timeframe);
};

inline auto groupby_anyof = [](std::string const &id,
                                std::string const &group_key,
                                std::string const &value,
                                epoch_script::TimeFrame const &timeframe) {
  return groupby_boolean_agg(id, "AnyOf", group_key, value, timeframe);
};

inline auto groupby_noneof = [](std::string const &id,
                                 std::string const &group_key,
                                 std::string const &value,
                                 epoch_script::TimeFrame const &timeframe) {
  return groupby_boolean_agg(id, "NoneOf", group_key, value, timeframe);
};

// Convenience helpers for any aggregations
inline auto groupby_isequal = [](std::string const &id,
                                  std::string const &group_key,
                                  std::string const &value,
                                  epoch_script::TimeFrame const &timeframe) {
  return groupby_any_agg(id, "IsEqual", group_key, value, timeframe);
};

inline auto groupby_isunique = [](std::string const &id,
                                   std::string const &group_key,
                                   std::string const &value,
                                   epoch_script::TimeFrame const &timeframe) {
  return groupby_any_agg(id, "IsUnique", group_key, value, timeframe);
};

} // namespace epoch_script::transform