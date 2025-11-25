// test_helpers.h
#pragma once

#include "transform_configuration.h"
#include <string>
#include <vector>

namespace epoch_script::transform {

// Shorthand aliases for InputValue construction
using NodeRef = epoch_script::strategy::NodeReference;
using InputVal = epoch_script::strategy::InputValue;

// Helper to create InputVal reference from column name (e.g., input_ref("c") -> #c)
inline InputVal input_ref(std::string const& col) {
  return InputVal(NodeRef("", col));
}

// Helper to create InputVal reference with node id (e.g., input_ref("src", "c") -> src#c)
inline InputVal input_ref(std::string const& node_id, std::string const& col) {
  return InputVal(NodeRef(node_id, col));
}

inline auto no_operand_period_op(std::string const &op, auto &&id,
                                 int64_t period,
                                 epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = op,
      .id = std::string(id),
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
}

inline auto
single_operand_period_op(std::string const &op, auto &&id, int64_t period,
                         InputVal const &input,
                         epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = op,
      .id = std::string(id),
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}}},
      .timeframe = timeframe,
      .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
}

// Type aliases for convenience
using InputsMap = std::unordered_map<std::string, std::vector<InputVal>>;
using OptionsMap = std::unordered_map<std::string, MetaDataOptionDefinition>;

// Helper to construct InputsMap from key-value pairs
// Usage: make_inputs({{"key", input_ref("col")}, {"key2", input_ref("col2")}})
inline InputsMap make_inputs(std::initializer_list<std::pair<std::string, InputVal>> init) {
  InputsMap result;
  for (const auto& [k, v] : init) {
    result[k] = {v};
  }
  return result;
}

// Helper to construct InputsMap with vector values
inline InputsMap make_inputs_vec(std::initializer_list<std::pair<std::string, std::vector<InputVal>>> init) {
  InputsMap result;
  for (const auto& [k, v] : init) {
    result[k] = v;
  }
  return result;
}

// Helper to construct OptionsMap
inline OptionsMap make_options(std::initializer_list<std::pair<std::string, MetaDataOptionDefinition>> init) {
  OptionsMap result;
  for (const auto& [k, v] : init) {
    result[k] = v;
  }
  return result;
}

inline TransformConfiguration run_op(std::string const &op, std::string const &id,
                   InputsMap inputs,
                   OptionsMap options,
                   epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = op,
      .id = id,
      .options = std::move(options),
      .timeframe = timeframe,
      .inputs = std::move(inputs)
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
}

auto single_operand_op(std::string const &type, std::string const &op,
                       auto &&id, InputVal const &input, int64_t value,
                       epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = type + "_" + op,
      .id = std::string(id),
      .options = {{"value", MetaDataOptionDefinition{static_cast<double>(value)}}},
      .timeframe = timeframe,
      .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
}

inline auto single_operand_op(std::string const &type, std::string const &op,
                              auto &&id, InputVal const &input,
                              epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = type + "_" + op,
      .id = std::string(id),
      .timeframe = timeframe,
      .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
}

inline auto double_operand_op = [](std::string const &type,
                                   std::string const &op, auto &&id,
                                   InputVal const &input1,
                                   InputVal const &input2,
                                   epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = type + op,
      .id = std::string(id),
      .timeframe = timeframe,
      .inputs = {{"SLOT0", std::vector<InputVal>{input1}}, {"SLOT1", std::vector<InputVal>{input2}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto single_input_op = [](std::string const &op, std::string const &id,
                                 InputVal const &input,
                                 const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = op,
      .id = id,
      .timeframe = timeframe,
      .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto no_input_op = [](std::string const &op, std::string const &id,
                             const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = op,
      .id = id,
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Scalar helpers
inline auto number_op = [](std::string const &id, double value,
                           epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = "number",
      .id = id,
      .options = {{"value", MetaDataOptionDefinition{value}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Helper functions for all scalar constants - use no_input_op
inline auto pi_op = [](std::string const &id,
                       epoch_script::TimeFrame const &timeframe) {
  return no_input_op("pi", id, timeframe);
};

inline auto e_op = [](std::string const &id,
                      epoch_script::TimeFrame const &timeframe) {
  return no_input_op("e", id, timeframe);
};

inline auto phi_op = [](std::string const &id,
                        epoch_script::TimeFrame const &timeframe) {
  return no_input_op("phi", id, timeframe);
};

inline auto sqrt2_op = [](std::string const &id,
                          epoch_script::TimeFrame const &timeframe) {
  return no_input_op("sqrt2", id, timeframe);
};

inline auto sqrt3_op = [](std::string const &id,
                          epoch_script::TimeFrame const &timeframe) {
  return no_input_op("sqrt3", id, timeframe);
};

inline auto sqrt5_op = [](std::string const &id,
                          epoch_script::TimeFrame const &timeframe) {
  return no_input_op("sqrt5", id, timeframe);
};

inline auto ln2_op = [](std::string const &id,
                        epoch_script::TimeFrame const &timeframe) {
  return no_input_op("ln2", id, timeframe);
};

inline auto ln10_op = [](std::string const &id,
                         epoch_script::TimeFrame const &timeframe) {
  return no_input_op("ln10", id, timeframe);
};

inline auto log2e_op = [](std::string const &id,
                          epoch_script::TimeFrame const &timeframe) {
  return no_input_op("log2e", id, timeframe);
};

inline auto log10e_op = [](std::string const &id,
                           epoch_script::TimeFrame const &timeframe) {
  return no_input_op("log10e", id, timeframe);
};

inline auto zero_op = [](std::string const &id,
                         epoch_script::TimeFrame const &timeframe) {
  return no_input_op("zero", id, timeframe);
};

inline auto one_op = [](std::string const &id,
                        epoch_script::TimeFrame const &timeframe) {
  return no_input_op("one", id, timeframe);
};

inline auto negative_one_op = [](std::string const &id,
                                 epoch_script::TimeFrame const &timeframe) {
  return no_input_op("negative_one", id, timeframe);
};

inline auto atr = [](auto &&...args) {
  return no_operand_period_op("atr", args...);
};

inline auto bbands = [](std::string const &id, int period, int stddev,
                        InputVal const &input,
                        const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "bbands",
      .id = id,
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}},
                  {"stddev", MetaDataOptionDefinition{static_cast<double>(stddev)}}},
      .timeframe = timeframe,
      .inputs = {{epoch_script::ARG, std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto bbands_percent = [](std::string const &id,
                                InputVal const &bbands_lower,
                                InputVal const &bbands_upper,
                                const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "bband_percent",
      .id = id,
      .timeframe = timeframe,
      .inputs = {{"bbands_lower", std::vector<InputVal>{bbands_lower}},
                 {"bbands_upper", std::vector<InputVal>{bbands_upper}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Generates a TransformConfiguration for BollingerBandsWidth.
// Example usage:
//   auto cfg = bbands_width("my_id", bband_lower, bband_middle, bband_upper, someTimeFrame);
inline auto bbands_width =
    [](std::string const &id, InputVal const &bband_lower,
       InputVal const &bband_middle, InputVal const &bband_upper,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "bband_width",
          .id = id,
          .timeframe = timeframe,
          .inputs = {{"bbands_lower", std::vector<InputVal>{bband_lower}},
                     {"bbands_middle", std::vector<InputVal>{bband_middle}},
                     {"bbands_upper", std::vector<InputVal>{bband_upper}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto psar = [](std::string const &id, double acceleration_factor_step,
                      double acceleration_factor_maximum,
                      InputVal const &input,
                      const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "psar",
      .id = id,
      .options = {{"acceleration_factor_step", MetaDataOptionDefinition{acceleration_factor_step}},
                  {"acceleration_factor_maximum", MetaDataOptionDefinition{acceleration_factor_maximum}}},
      .timeframe = timeframe,
      .inputs = {{epoch_script::ARG, std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto crossany = [](std::string const &id, InputVal const &input1,
                          InputVal const &input2,
                          epoch_script::TimeFrame const &timeframe) {
  return double_operand_op("cross", "any", id, input1, input2, timeframe);
};

inline auto crossover = [](std::string const &id, InputVal const &input1,
                           InputVal const &input2,
                           epoch_script::TimeFrame const &timeframe) {
  return double_operand_op("cross", "over", id, input1, input2, timeframe);
};

inline auto crossunder = [](std::string const &id, InputVal const &input1,
                            InputVal const &input2,
                            epoch_script::TimeFrame const &timeframe) {
  return double_operand_op("cross", "under", id, input1, input2, timeframe);
};

inline auto cs_momentum = [](int64_t id, InputVal const &input,
                             epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = "cs_momentum",
      .id = std::to_string(id),
      .timeframe = timeframe,
      .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto cs_topk = [](int64_t id, InputVal const &input, int64_t k,
                         epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = "top_k",
      .id = std::to_string(id),
      .options = {{"k", MetaDataOptionDefinition{static_cast<double>(k)}}},
      .timeframe = timeframe,
      .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto cs_bottomk = [](int64_t id, InputVal const &input, int64_t k,
                            epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = "bottom_k",
      .id = std::to_string(id),
      .options = {{"k", MetaDataOptionDefinition{static_cast<double>(k)}}},
      .timeframe = timeframe,
      .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto cs_topk_percentile =
    [](int64_t id, InputVal const &input, int64_t k,
       epoch_script::TimeFrame const &timeframe) {
      TransformDefinitionData data{
          .type = "top_k_percent",
          .id = std::to_string(id),
          .options = {{"k", MetaDataOptionDefinition{static_cast<double>(k)}}},
          .timeframe = timeframe,
          .inputs = {{"SLOT", std::vector<InputVal>{input}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto cs_bottomk_percentile =
    [](int64_t id, InputVal const &input, int64_t k,
       epoch_script::TimeFrame const &timeframe) {
      TransformDefinitionData data{
          .type = "bottom_k_percent",
          .id = std::to_string(id),
          .options = {{"k", MetaDataOptionDefinition{static_cast<double>(k)}}},
          .timeframe = timeframe,
          .inputs = {{"SLOT", std::vector<InputVal>{input}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
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
                    InputVal const &input, int64_t period,
                    epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = type,
      .id = std::string(id),
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}}},
      .timeframe = timeframe,
      .inputs = {{"SLOT", std::vector{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto sma = [](auto &&...args) { return ma("sma", args...); };

// NOTE: boolean_select helper removed - use typed variants:
// boolean_select_string, boolean_select_number, boolean_select_boolean, boolean_select_timestamp

inline auto select_n = [](int64_t id, int n, InputVal const &index,
                          const std::vector<InputVal> &options,
                          const epoch_script::TimeFrame &timeframe) {
  // Updated for typed switch transforms: switch{N}_number
  epoch_script::strategy::InputMapping input_map;
  input_map["index"] = {index};
  for (int i = 0; i < n; ++i) {
    input_map["SLOT" + std::to_string(i)] = {options[i]};
  }
  TransformDefinitionData data{
      .type = "switch" + std::to_string(n) + "_number",
      .id = std::to_string(id),
      .timeframe = timeframe,
      .inputs = std::move(input_map)
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// NOTE: first_non_null helper removed - use typed variants:
// first_non_null_string, first_non_null_number, first_non_null_boolean, first_non_null_timestamp

// NOTE: conditional_select untyped helper removed - use typed variants with this helper:
// Helper for typed conditional_select variants (conditional_select_number, etc.)
// inputs: vector of input IDs in order [cond0, val0, cond1, val1, ..., optional_default]
inline auto typed_conditional_select = [](const std::string &type, int64_t id,
                                          const std::vector<InputVal> &inputs,
                                          const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = type,  // e.g., "conditional_select_number"
      .id = std::to_string(id),
      .timeframe = timeframe,
      .inputs = {{epoch_script::ARG, inputs}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Helper for typed first_non_null variants (first_non_null_number, etc.)
// inputs: vector of input IDs to check in order
inline auto typed_first_non_null = [](const std::string &type, int64_t id,
                                      const std::vector<InputVal> &inputs,
                                      const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = type,  // e.g., "first_non_null_number"
      .id = std::to_string(id),
      .timeframe = timeframe,
      .inputs = {{epoch_script::ARG, inputs}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto rolling_volatility =
    [](std::string const &id, int64_t period,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "return_vol",
          .id = id,
          .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto price_diff_volatility =
    [](std::string const &id, int64_t period,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "price_diff_vol",
          .id = id,
          .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto swing_highs_lows = [](std::string const &id, int64_t swing_length,
                                  const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "swing_highs_lows",
      .id = id,
      .options = {{"swing_length", MetaDataOptionDefinition{static_cast<double>(swing_length)}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto order_blocks =
    [](std::string const &id, InputVal const &high_low,
       bool close_mitigation, const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "order_blocks",
          .id = id,
          .options = {{"close_mitigation", MetaDataOptionDefinition{close_mitigation}}},
          .timeframe = timeframe,
          .inputs = {{"high_low", std::vector<InputVal>{high_low}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto bos_choch = [](std::string const &id, InputVal const &high_low,
                           InputVal const &level, bool close_break,
                           const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "bos_choch",
      .id = id,
      .options = {{"close_break", MetaDataOptionDefinition{close_break}}},
      .timeframe = timeframe,
      .inputs = {{"high_low", std::vector<InputVal>{high_low}},
                 {"level", std::vector<InputVal>{level}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto liquidity = [](std::string const &id, InputVal const &high_low,
                           InputVal const &level, double range_percent,
                           const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "liquidity",
      .id = id,
      .options = {{"range_percent", MetaDataOptionDefinition{range_percent}}},
      .timeframe = timeframe,
      .inputs = {{"high_low", std::vector<InputVal>{high_low}},
                 {"level", std::vector<InputVal>{level}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto retracements =
    [](std::string const &id, InputVal const &high_low,
       InputVal const &level, const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "retracements",
          .id = id,
          .timeframe = timeframe,
          .inputs = {{"high_low", std::vector<InputVal>{high_low}},
                     {"level", std::vector<InputVal>{level}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto fair_value_gap = [](std::string const &id, bool join_consecutive,
                                const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "fair_value_gap",
      .id = id,
      .options = {{"join_consecutive", MetaDataOptionDefinition{join_consecutive}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto sessions = [](std::string const &id,
                          std::string const &session_name,
                          const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "sessions",
      .id = id,
      .options = {{"session_type", MetaDataOptionDefinition{session_name}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Session Time Window - Detects proximity to session boundaries
inline auto session_time_window = [](std::string const &id,
                                      std::string const &session_type,
                                      int64_t minute_offset,
                                      std::string const &boundary_type,
                                      const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "session_time_window",
      .id = id,
      .options = {{"session_type", MetaDataOptionDefinition{session_type}},
                  {"minute_offset", MetaDataOptionDefinition{static_cast<double>(minute_offset)}},
                  {"boundary_type", MetaDataOptionDefinition{boundary_type}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto previous_high_low = [](std::string const &id, int64_t interval,
                                   std::string const &type,
                                   const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "previous_high_low",
      .id = id,
      .options = {{"interval", MetaDataOptionDefinition{static_cast<double>(interval)}},
                  {"type", MetaDataOptionDefinition{type}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// NOTE: percentile_select helper removed - use typed variants:
// percentile_select_string, percentile_select_number, percentile_select_boolean, percentile_select_timestamp

inline auto boolean_branch = [](std::string const &id,
                                InputVal const &condition,
                                const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "boolean_branch",
      .id = id,
      .timeframe = timeframe,
      .inputs = {{"condition", std::vector<InputVal>{condition}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto ratio_branch = [](std::string const &id, InputVal const &ratio,
                              double threshold_high, double threshold_low,
                              const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "ratio_branch",
      .id = id,
      .options = {{"threshold_high", MetaDataOptionDefinition{threshold_high}},
                  {"threshold_low", MetaDataOptionDefinition{threshold_low}}},
      .timeframe = timeframe,
      .inputs = {{"ratio", std::vector<InputVal>{ratio}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto previous_gt = [](std::string const &id, InputVal const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "previous_gt", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto previous_gte = [](std::string const &id, InputVal const &input,
                              int64_t periods,
                              const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "previous_gte", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto previous_lt = [](std::string const &id, InputVal const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "previous_lt", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto previous_lte = [](std::string const &id, InputVal const &input,
                              int64_t periods,
                              const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "previous_lte", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto previous_eq = [](std::string const &id, InputVal const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "previous_eq", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto previous_neq = [](std::string const &id, InputVal const &input,
                              int64_t periods,
                              const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "previous_neq", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto highest_gt = [](std::string const &id, InputVal const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "highest_gt", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto highest_gte = [](std::string const &id, InputVal const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "highest_gte", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto highest_lt = [](std::string const &id, InputVal const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "highest_lt", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto highest_lte = [](std::string const &id, InputVal const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "highest_lte", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto highest_eq = [](std::string const &id, InputVal const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "highest_eq", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto highest_neq = [](std::string const &id, InputVal const &input,
                             int64_t periods,
                             const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "highest_neq", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto lowest_gt = [](std::string const &id, InputVal const &input,
                           int64_t periods,
                           const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "lowest_gt", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto lowest_gte = [](std::string const &id, InputVal const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "lowest_gte", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto lowest_lt = [](std::string const &id, InputVal const &input,
                           int64_t periods,
                           const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "lowest_lt", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto lowest_lte = [](std::string const &id, InputVal const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "lowest_lte", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto lowest_eq = [](std::string const &id, InputVal const &input,
                           int64_t periods,
                           const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "lowest_eq", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto lowest_neq = [](std::string const &id, InputVal const &input,
                            int64_t periods,
                            const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "lowest_neq", .id = id,
      .options = {{"periods", MetaDataOptionDefinition{static_cast<double>(periods)}}},
      .timeframe = timeframe, .inputs = {{"SLOT", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Aggregate transform helpers
inline auto aggregate_transform =
    [](std::string const &agg_type, std::string const &id,
       const std::vector<InputVal> &inputs,
       epoch_script::TimeFrame const &timeframe) {
      TransformDefinitionData data{
          .type = "agg_" + agg_type,
          .id = id,
          .timeframe = timeframe,
          .inputs = {{"SLOT", inputs}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto agg_sum = [](std::string const &id,
                         const std::vector<InputVal> &inputs,
                         epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("sum", id, inputs, timeframe);
};

inline auto agg_mean = [](std::string const &id,
                          const std::vector<InputVal> &inputs,
                          epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("mean", id, inputs, timeframe);
};

inline auto agg_min = [](std::string const &id,
                         const std::vector<InputVal> &inputs,
                         epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("min", id, inputs, timeframe);
};

inline auto agg_max = [](std::string const &id,
                         const std::vector<InputVal> &inputs,
                         epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("max", id, inputs, timeframe);
};

inline auto agg_all_of = [](std::string const &id,
                            const std::vector<InputVal> &inputs,
                            epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("all_of", id, inputs, timeframe);
};

inline auto agg_any_of = [](std::string const &id,
                            const std::vector<InputVal> &inputs,
                            epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("any_of", id, inputs, timeframe);
};

inline auto agg_none_of = [](std::string const &id,
                             const std::vector<InputVal> &inputs,
                             epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("none_of", id, inputs, timeframe);
};

inline auto agg_all_equal = [](std::string const &id,
                               const std::vector<InputVal> &inputs,
                               epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("all_equal", id, inputs, timeframe);
};

inline auto agg_all_unique = [](std::string const &id,
                                const std::vector<InputVal> &inputs,
                                epoch_script::TimeFrame const &timeframe) {
  return aggregate_transform("all_unique", id, inputs, timeframe);
};

inline auto abands_cfg = [](std::string const &id, int64_t period,
                            double multiplier,
                            const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "acceleration_bands",
      .id = id,
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}},
                  {"multiplier", MetaDataOptionDefinition{multiplier}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto chande_kroll_cfg = [](std::string const &id, int64_t p_period,
                                  int64_t q_period, double multiplier,
                                  const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "chande_kroll_stop",
      .id = id,
      .options = {{"p_period", MetaDataOptionDefinition{static_cast<double>(p_period)}},
                  {"q_period", MetaDataOptionDefinition{static_cast<double>(q_period)}},
                  {"multiplier", MetaDataOptionDefinition{multiplier}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto garman_klass_cfg = [](std::string const &id, int64_t period,
                                  int64_t trading_days,
                                  const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "garman_klass",
      .id = id,
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}},
                  {"trading_days", MetaDataOptionDefinition{static_cast<double>(trading_days)}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto hodges_tompkins_cfg =
    [](std::string const &id, int64_t period, int64_t trading_periods,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "hodges_tompkins",
          .id = id,
          .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}},
                      {"trading_periods", MetaDataOptionDefinition{static_cast<double>(trading_periods)}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto keltner_channels_cfg =
    [](std::string const &id, int64_t roll_period, double band_multiplier,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "keltner_channels",
          .id = id,
          .options = {{"roll_period", MetaDataOptionDefinition{static_cast<double>(roll_period)}},
                      {"band_multiplier", MetaDataOptionDefinition{band_multiplier}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto parkinson_cfg = [](std::string const &id, int64_t period,
                               int64_t trading_days,
                               const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "parkinson",
      .id = id,
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}},
                  {"trading_periods", MetaDataOptionDefinition{static_cast<double>(trading_days)}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto ulcer_index_cfg = [](std::string const &id, int64_t period,
                                 bool use_sum,
                                 const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "ulcer_index",
      .id = id,
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}},
                  {"use_sum", MetaDataOptionDefinition{use_sum}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto yang_zhang_cfg = [](std::string const &id, int64_t period,
                                int64_t trading_days,
                                const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "yang_zhang",
      .id = id,
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}},
                  {"trading_periods", MetaDataOptionDefinition{static_cast<double>(trading_days)}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Indicators

inline auto pivot_point_sr_cfg =
    [](std::string const &id, const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "pivot_point_sr",
          .id = id,
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto hurst_exponent_cfg =
    [](std::string const &id, int64_t min_period, InputVal const &input,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "hurst_exponent",
          .id = id,
          .options = {{"min_period", MetaDataOptionDefinition{static_cast<double>(min_period)}}},
          .timeframe = timeframe,
          .inputs = {{"SLOT", std::vector<InputVal>{input}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto rolling_hurst_exponent_cfg =
    [](std::string const &id, int64_t period, InputVal const &input,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "rolling_hurst_exponent",
          .id = id,
          .options = {{"window", MetaDataOptionDefinition{static_cast<double>(period)}}},
          .timeframe = timeframe,
          .inputs = {{"SLOT", std::vector<InputVal>{input}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto elders_thermometer_cfg =
    [](std::string const &id, int64_t period, double buy_factor,
       double sell_factor, const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "elders_thermometer",
          .id = id,
          .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}},
                      {"buy_factor", MetaDataOptionDefinition{buy_factor}},
                      {"sell_factor", MetaDataOptionDefinition{sell_factor}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto peaks_and_valleys_cfg =
    [](std::string const &id, const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "peaks_and_valleys",
          .id = id,
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto price_distance_cfg =
    [](std::string const &id, const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "price_distance",
          .id = id,
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto psl_cfg = [](std::string const &id, int64_t period,
                         const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "psl",
      .id = id,
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto qqe_cfg = [](std::string const &id, int64_t avg_period,
                         int64_t smooth_period, double width_factor,
                         const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "qqe",
      .id = id,
      .options = {{"avg_period", MetaDataOptionDefinition{static_cast<double>(avg_period)}},
                  {"smooth_period", MetaDataOptionDefinition{static_cast<double>(smooth_period)}},
                  {"width_factor", MetaDataOptionDefinition{width_factor}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto vortex_cfg = [](std::string const &id, int64_t period,
                            const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "vortex",
      .id = id,
      .options = {{"period", MetaDataOptionDefinition{static_cast<double>(period)}}},
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Trade Executor helpers
inline auto trade_executor_adapter_cfg =
    [](std::string const &id, InputVal const &input,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "trade_executor_adapter",
          .id = id,
          .timeframe = timeframe,
          .inputs = {{"SLOT", std::vector<InputVal>{input}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

inline auto trade_signal_executor_cfg =
    [](std::string const &id,
       const std::unordered_map<std::string, InputVal> &inputs,
       const epoch_script::TimeFrame &timeframe) {
      strategy::InputMapping input_map;
      for (const auto &[key, value] : inputs) {
        input_map[key] = {value};
      }
      TransformDefinitionData data{
          .type = "trade_signal_executor",
          .id = id,
          .timeframe = timeframe,
          .inputs = std::move(input_map)
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };
inline auto data_source = [](std::string const &id,
                             const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "market_data_source",
      .id = id,
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto news = [](std::string const &id,
                      const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "news",
      .id = id,
      .timeframe = timeframe
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Scalar aggregation config helpers
// Generic helper for <agg_type>_scalar with common options
inline auto scalar_aggregation_cfg =
    [](std::string const &agg_type, std::string const &id,
       InputVal const &input, const epoch_script::TimeFrame &timeframe,
       epoch_script::MetaDataArgDefinitionMapping options = {}) {
      // Pass required options explicitly; do not rely on metadata defaults
      if (!options.contains("skip_nulls") && !agg_type.starts_with("count_") &&
          agg_type != "kurtosis" && agg_type != "skew") {
        options["skip_nulls"] = MetaDataOptionDefinition{false};
      }
      if (!options.contains("min_count") && !agg_type.starts_with("count") &&
          agg_type != "kurtosis" && agg_type != "skew") {
        options["min_count"] = MetaDataOptionDefinition{1.0};
      }
      if ((agg_type == "stddev" || agg_type == "variance") &&
          !options.contains("ddof")) {
        options["ddof"] = MetaDataOptionDefinition{1.0};
      }
      if ((agg_type == "quantile" || agg_type == "tdigest") &&
          !options.contains("quantile")) {
        options["quantile"] = MetaDataOptionDefinition{0.5};
      }
      TransformDefinitionData data{
          .type = "scalar_" + agg_type,
          .id = id,
          .options = std::move(options),
          .timeframe = timeframe,
          .inputs = {{"SLOT", std::vector<InputVal>{input}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// stddev(id, input) with ddof option
inline auto stddev_scalar_cfg = []<class... T>(int64_t ddof, T &&...args) {
  epoch_script::MetaDataArgDefinitionMapping options{
      {"ddof", MetaDataOptionDefinition{static_cast<double>(ddof)}}
  };
  return scalar_aggregation_cfg("stddev", std::forward<T>(args)..., std::move(options));
};

// variance(id, input) with ddof option
inline auto variance_scalar_cfg = []<class... T>(int64_t ddof, T &&...args) {
  epoch_script::MetaDataArgDefinitionMapping options{
      {"ddof", MetaDataOptionDefinition{static_cast<double>(ddof)}}
  };
  return scalar_aggregation_cfg("variance", std::forward<T>(args)..., std::move(options));
};

// quantile(id, input) with quantile option
inline auto quantile_scalar_cfg = []<class... T>(double quantile, T &&...args) {
  epoch_script::MetaDataArgDefinitionMapping options{
      {"quantile", MetaDataOptionDefinition{quantile}}
  };
  return scalar_aggregation_cfg("quantile", std::forward<T>(args)..., std::move(options));
};

inline auto tdigest_scalar_cfg = []<class... T>(double quantile, T &&...args) {
  epoch_script::MetaDataArgDefinitionMapping options{
      {"quantile", MetaDataOptionDefinition{quantile}}
  };
  return scalar_aggregation_cfg("tdigest", std::forward<T>(args)..., std::move(options));
};

// sum(id, input)
inline auto sum_scalar_cfg = []<class... T>(T &&...args) {
  return scalar_aggregation_cfg("sum", std::forward<T>(args)...);
};

// mean(id, input) with options
inline auto mean_scalar_cfg = []<class... T>(bool skip_nulls, int min_count,
                                             T &&...args) {
  epoch_script::MetaDataArgDefinitionMapping options{
      {"skip_nulls", MetaDataOptionDefinition{skip_nulls}},
      {"min_count", MetaDataOptionDefinition{static_cast<double>(min_count)}}
  };
  return scalar_aggregation_cfg("mean", std::forward<T>(args)..., std::move(options));
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
    [](std::string const &id, const std::vector<InputVal> &inputs,
       const epoch_script::TimeFrame &timeframe, int n_states = 3,
       std::size_t max_iterations = 1000, double tolerance = 1e-5,
       bool compute_zscore = true, std::size_t min_training_samples = 100,
       std::size_t lookback_window = 0) {
      TransformDefinitionData data{
          .type = "hmm_" + std::to_string(n_states),
          .id = id,
          .options = {
              {"max_iterations", MetaDataOptionDefinition{static_cast<double>(max_iterations)}},
              {"tolerance", MetaDataOptionDefinition{tolerance}},
              {"compute_zscore", MetaDataOptionDefinition{compute_zscore}},
              {"min_training_samples", MetaDataOptionDefinition{static_cast<double>(min_training_samples)}},
              {"lookback_window", MetaDataOptionDefinition{static_cast<double>(lookback_window)}}
          },
          .timeframe = timeframe,
          .inputs = {{"SLOT", inputs}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Convenience single-input HMM helper
inline auto hmm_single_cfg =
    [](std::string const &id, InputVal const &input,
       const epoch_script::TimeFrame &timeframe, int n_states = 3,
       std::size_t max_iterations = 1000, double tolerance = 1e-5,
       bool compute_zscore = true, std::size_t min_training_samples = 100,
       std::size_t lookback_window = 0) {
      return hmm_cfg(id, std::vector<InputVal>{input}, timeframe, n_states,
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
      TransformDefinitionData data{
          .type = "triangles",
          .id = id,
          .options = {{"lookback", MetaDataOptionDefinition{static_cast<double>(lookback)}},
                      {"triangle_type", MetaDataOptionDefinition{triangle_type}},
                      {"r_squared_min", MetaDataOptionDefinition{r_squared_min}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Flag - Detects bull and bear flag patterns
inline auto flag_cfg =
    [](std::string const &id, int64_t lookback, int64_t min_pivot_points,
       double r_squared_min, double slope_parallel_tolerance,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "flag",
          .id = id,
          .options = {{"lookback", MetaDataOptionDefinition{static_cast<double>(lookback)}},
                      {"min_pivot_points", MetaDataOptionDefinition{static_cast<double>(min_pivot_points)}},
                      {"r_squared_min", MetaDataOptionDefinition{r_squared_min}},
                      {"slope_parallel_tolerance", MetaDataOptionDefinition{slope_parallel_tolerance}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Pennant - Detects brief consolidation patterns
inline auto pennant_cfg =
    [](std::string const &id, int64_t lookback, int64_t min_pivot_points,
       double r_squared_min, int64_t max_duration,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "pennant",
          .id = id,
          .options = {{"lookback", MetaDataOptionDefinition{static_cast<double>(lookback)}},
                      {"min_pivot_points", MetaDataOptionDefinition{static_cast<double>(min_pivot_points)}},
                      {"r_squared_min", MetaDataOptionDefinition{r_squared_min}},
                      {"max_duration", MetaDataOptionDefinition{static_cast<double>(max_duration)}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Head and Shoulders - Detects bearish reversal pattern
inline auto head_and_shoulders_cfg =
    [](std::string const &id, int64_t lookback, double head_ratio_before,
       double head_ratio_after, double neckline_slope_max,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "head_and_shoulders",
          .id = id,
          .options = {{"lookback", MetaDataOptionDefinition{static_cast<double>(lookback)}},
                      {"head_ratio_before", MetaDataOptionDefinition{head_ratio_before}},
                      {"head_ratio_after", MetaDataOptionDefinition{head_ratio_after}},
                      {"neckline_slope_max", MetaDataOptionDefinition{neckline_slope_max}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Inverse Head and Shoulders - Detects bullish reversal pattern
inline auto inverse_head_and_shoulders_cfg =
    [](std::string const &id, int64_t lookback, double head_ratio_before,
       double head_ratio_after, double neckline_slope_max,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "inverse_head_and_shoulders",
          .id = id,
          .options = {{"lookback", MetaDataOptionDefinition{static_cast<double>(lookback)}},
                      {"head_ratio_before", MetaDataOptionDefinition{head_ratio_before}},
                      {"head_ratio_after", MetaDataOptionDefinition{head_ratio_after}},
                      {"neckline_slope_max", MetaDataOptionDefinition{neckline_slope_max}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Double Top/Bottom - Detects M/W reversal patterns
inline auto double_top_bottom_cfg =
    [](std::string const &id, int64_t lookback,
       std::string const &pattern_type, double similarity_tolerance,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "double_top_bottom",
          .id = id,
          .options = {{"lookback", MetaDataOptionDefinition{static_cast<double>(lookback)}},
                      {"pattern_type", MetaDataOptionDefinition{pattern_type}},
                      {"similarity_tolerance", MetaDataOptionDefinition{similarity_tolerance}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// ConsolidationBox - Detects horizontal consolidation boxes
inline auto consolidation_box_cfg =
    [](std::string const &id, int64_t lookback, int64_t min_pivot_points,
       double r_squared_min, double max_slope,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "consolidation_box",
          .id = id,
          .options = {{"lookback", MetaDataOptionDefinition{static_cast<double>(lookback)}},
                      {"min_pivot_points", MetaDataOptionDefinition{static_cast<double>(min_pivot_points)}},
                      {"r_squared_min", MetaDataOptionDefinition{r_squared_min}},
                      {"max_slope", MetaDataOptionDefinition{max_slope}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// =========================
// Event Marker configuration helpers
// =========================

// Event Marker with Filter - Uses boolean column to filter rows
// Accepts EventMarkerSchema object directly - NO YAML!
inline auto event_marker_cfg =
    [](std::string const &id, epoch_script::EventMarkerSchema const &schema,
       const std::vector<InputVal> &inputs,
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
                                  InputVal const &input,
                                  const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_case",
      .id = id,
      .options = {{"operation", MetaDataOptionDefinition{operation}}},
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_trim_cfg = [](std::string const &id, std::string const &operation,
                                  InputVal const &input, std::string const &trim_chars,
                                  const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_trim",
      .id = id,
      .options = {
          {"operation", MetaDataOptionDefinition{operation}},
          {"trim_chars", MetaDataOptionDefinition{trim_chars}}
      },
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_contains_cfg = [](std::string const &id, std::string const &operation,
                                      InputVal const &input, std::string const &pattern,
                                      const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_contains",
      .id = id,
      .options = {
          {"operation", MetaDataOptionDefinition{operation}},
          {"pattern", MetaDataOptionDefinition{pattern}}
      },
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_check_cfg = [](std::string const &id, std::string const &operation,
                                   InputVal const &input,
                                   const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_check",
      .id = id,
      .options = {{"operation", MetaDataOptionDefinition{operation}}},
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_replace_cfg = [](std::string const &id, InputVal const &input,
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
      .inputs = {{"input", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_length_cfg = [](std::string const &id, InputVal const &input,
                                    const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_length",
      .id = id,
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto string_reverse_cfg = [](std::string const &id, InputVal const &input,
                                     const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "string_reverse",
      .id = id,
      .timeframe = timeframe,
      .inputs = {{"input", std::vector<InputVal>{input}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// =========================
// ML/AI Transform Helpers
// =========================

// FinBERT Sentiment Analysis - AWS SageMaker sentiment analysis
inline auto finbert_sentiment_cfg = [](std::string const &id,
                                        epoch_script::strategy::InputValue const &input,
                                        const epoch_script::TimeFrame &timeframe) {
  TransformDefinitionData data{
      .type = "finbert_sentiment",
      .id = id,
      .options = {},
      .timeframe = timeframe,
      .inputs = {{ARG, std::vector<epoch_script::strategy::InputValue>{input}}}
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
      TransformDefinitionData data{
          .type = "index_datetime_extract",
          .id = id,
          .options = {{"component", MetaDataOptionDefinition{component}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Column Datetime Extract - Extract datetime component from timestamp column
inline auto column_datetime_extract_cfg =
    [](std::string const &id, InputVal const &input,
       std::string const &component, const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "column_datetime_extract",
          .id = id,
          .options = {{"component", MetaDataOptionDefinition{component}}},
          .timeframe = timeframe,
          .inputs = {{"SLOT", std::vector<InputVal>{input}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Datetime Diff - Calculate time difference between two timestamps
inline auto datetime_diff_cfg =
    [](std::string const &id, InputVal const &start_input,
       InputVal const &end_input, std::string const &unit,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "datetime_diff",
          .id = id,
          .options = {{"unit", MetaDataOptionDefinition{unit}}},
          .timeframe = timeframe,
          .inputs = {{"SLOT0", std::vector<InputVal>{start_input}},
                     {"SLOT1", std::vector<InputVal>{end_input}}}
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Timestamp Scalar - Create constant timestamp value
inline auto timestamp_scalar_cfg =
    [](std::string const &id, std::string const &value,
       const epoch_script::TimeFrame &timeframe) {
      TransformDefinitionData data{
          .type = "timestamp_scalar",
          .id = id,
          .options = {{"value", MetaDataOptionDefinition{value}}},
          .timeframe = timeframe
      };
      return TransformConfiguration{TransformDefinition{std::move(data)}};
    };

// Validation transform helpers
inline auto is_null_cfg = [](std::string const &id, InputVal const &input,
                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("is_null", id, input, timeframe);
};

inline auto is_valid_cfg = [](std::string const &id, InputVal const &input,
                               const epoch_script::TimeFrame &timeframe) {
  return single_input_op("is_valid", id, input, timeframe);
};

inline auto is_zero_cfg = [](std::string const &id, InputVal const &input,
                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("is_zero", id, input, timeframe);
};

inline auto is_one_cfg = [](std::string const &id, InputVal const &input,
                             const epoch_script::TimeFrame &timeframe) {
  return single_input_op("is_one", id, input, timeframe);
};

// Static cast transform helpers (compiler-inserted type materializers)
inline auto static_cast_to_integer_cfg = [](std::string const &id, InputVal const &input,
                                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_integer", id, input, timeframe);
};

inline auto static_cast_to_decimal_cfg = [](std::string const &id, InputVal const &input,
                                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_decimal", id, input, timeframe);
};

inline auto static_cast_to_boolean_cfg = [](std::string const &id, InputVal const &input,
                                              const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_boolean", id, input, timeframe);
};

inline auto static_cast_to_string_cfg = [](std::string const &id, InputVal const &input,
                                             const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_string", id, input, timeframe);
};

inline auto static_cast_to_timestamp_cfg = [](std::string const &id, InputVal const &input,
                                                const epoch_script::TimeFrame &timeframe) {
  return single_input_op("static_cast_to_timestamp", id, input, timeframe);
};

// GroupBy aggregate transform helpers
inline auto groupby_numeric_agg = [](std::string const &id,
                                      std::string const &agg_type,
                                      InputVal const &group_key,
                                      InputVal const &value,
                                      epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = "groupby_numeric_agg",
      .id = id,
      .options = {{"agg", MetaDataOptionDefinition{agg_type}}},
      .timeframe = timeframe,
      .inputs = {{"group_key", std::vector<InputVal>{group_key}},
                 {"value", std::vector<InputVal>{value}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto groupby_boolean_agg = [](std::string const &id,
                                      std::string const &agg_type,
                                      InputVal const &group_key,
                                      InputVal const &value,
                                      epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = "groupby_boolean_agg",
      .id = id,
      .options = {{"agg", MetaDataOptionDefinition{agg_type}}},
      .timeframe = timeframe,
      .inputs = {{"group_key", std::vector<InputVal>{group_key}},
                 {"value", std::vector<InputVal>{value}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

inline auto groupby_any_agg = [](std::string const &id,
                                  std::string const &agg_type,
                                  InputVal const &group_key,
                                  InputVal const &value,
                                  epoch_script::TimeFrame const &timeframe) {
  TransformDefinitionData data{
      .type = "groupby_any_agg",
      .id = id,
      .options = {{"agg", MetaDataOptionDefinition{agg_type}}},
      .timeframe = timeframe,
      .inputs = {{"group_key", std::vector<InputVal>{group_key}},
                 {"value", std::vector<InputVal>{value}}}
  };
  return TransformConfiguration{TransformDefinition{std::move(data)}};
};

// Convenience helpers for specific numeric aggregations
inline auto groupby_sum = [](std::string const &id,
                              InputVal const &group_key,
                              InputVal const &value,
                              epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "sum", group_key, value, timeframe);
};

inline auto groupby_mean = [](std::string const &id,
                               InputVal const &group_key,
                               InputVal const &value,
                               epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "mean", group_key, value, timeframe);
};

inline auto groupby_count = [](std::string const &id,
                                InputVal const &group_key,
                                InputVal const &value,
                                epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "count", group_key, value, timeframe);
};

inline auto groupby_first = [](std::string const &id,
                                InputVal const &group_key,
                                InputVal const &value,
                                epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "first", group_key, value, timeframe);
};

inline auto groupby_last = [](std::string const &id,
                               InputVal const &group_key,
                               InputVal const &value,
                               epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "last", group_key, value, timeframe);
};

inline auto groupby_min = [](std::string const &id,
                              InputVal const &group_key,
                              InputVal const &value,
                              epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "min", group_key, value, timeframe);
};

inline auto groupby_max = [](std::string const &id,
                              InputVal const &group_key,
                              InputVal const &value,
                              epoch_script::TimeFrame const &timeframe) {
  return groupby_numeric_agg(id, "max", group_key, value, timeframe);
};

// Convenience helpers for boolean aggregations
inline auto groupby_allof = [](std::string const &id,
                                InputVal const &group_key,
                                InputVal const &value,
                                epoch_script::TimeFrame const &timeframe) {
  return groupby_boolean_agg(id, "AllOf", group_key, value, timeframe);
};

inline auto groupby_anyof = [](std::string const &id,
                                InputVal const &group_key,
                                InputVal const &value,
                                epoch_script::TimeFrame const &timeframe) {
  return groupby_boolean_agg(id, "AnyOf", group_key, value, timeframe);
};

inline auto groupby_noneof = [](std::string const &id,
                                 InputVal const &group_key,
                                 InputVal const &value,
                                 epoch_script::TimeFrame const &timeframe) {
  return groupby_boolean_agg(id, "NoneOf", group_key, value, timeframe);
};

// Convenience helpers for any aggregations
inline auto groupby_isequal = [](std::string const &id,
                                  InputVal const &group_key,
                                  InputVal const &value,
                                  epoch_script::TimeFrame const &timeframe) {
  return groupby_any_agg(id, "IsEqual", group_key, value, timeframe);
};

inline auto groupby_isunique = [](std::string const &id,
                                   InputVal const &group_key,
                                   InputVal const &value,
                                   epoch_script::TimeFrame const &timeframe) {
  return groupby_any_agg(id, "IsUnique", group_key, value, timeframe);
};

// ===================================
// User-Defined Literals for InputValue
// ===================================

// Inline namespace to prevent UDL pollution in global namespace
inline namespace input_value_literals {

  // Create InputValue with ConstantValue from integer literal like 42_in_literal
  inline InputVal operator""_in_literal(unsigned long long value) {
    return InputVal{epoch_script::transform::ConstantValue{static_cast<int64_t>(value)}};
  }

  // Create InputValue with ConstantValue from floating-point literal like 3.14_in_literal
  inline InputVal operator""_in_literal(long double value) {
    return InputVal{epoch_script::transform::ConstantValue{static_cast<double>(value)}};
  }

  // Create InputValue with ConstantValue from string literal like "hello"_in_literal
  inline InputVal operator""_in_literal(const char* str, size_t len) {
    return InputVal{epoch_script::transform::ConstantValue{std::string(str, len)}};
  }

} // namespace input_value_literals

} // namespace epoch_script::transform