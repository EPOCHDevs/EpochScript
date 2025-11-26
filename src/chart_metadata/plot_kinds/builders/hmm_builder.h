#pragma once

#include "../complex.h"
#include <stdexcept>
#include <regex>

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for hmm PlotKind (Hidden Markov Model)
 * Outputs: state + dynamic state_*_prob outputs (state_0_prob, state_1_prob, etc.)
 *
 * The number of states is extracted from the transform type (e.g., "hmm_3" -> 3 states)
 * and all state probability outputs (state_0_prob through state_{n-1}_prob) are returned.
 */
class HMMBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    std::unordered_map<std::string, std::string> result = {
      {"index", INDEX_COLUMN},
      {"state", cfg.GetOutputId("state").GetColumnName()}
    };

    // Extract num_states from transform type (e.g., "hmm_3" -> 3)
    int num_states = ExtractNumStates(cfg.GetTransformName());

    // Add all state probability outputs
    for (int i = 0; i < num_states; ++i) {
      std::string prob_id = "state_" + std::to_string(i) + "_prob";
      result[prob_id] = cfg.GetOutputId(prob_id).GetColumnName();
    }

    return result;
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // Validate required "state" output exists
    ValidateOutput(cfg, "state", "HMM");

    // Extract num_states from transform type
    int num_states = ExtractNumStates(cfg.GetTransformName());

    // Validate all expected probability outputs exist
    const auto &outputs = cfg.GetOutputs();
    for (int i = 0; i < num_states; ++i) {
      std::string prob_id = "state_" + std::to_string(i) + "_prob";
      bool found = false;
      for (const auto &output : outputs) {
        if (output.id == prob_id) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw std::runtime_error(
          "HMM transform with " + std::to_string(num_states) +
          " states must have output '" + prob_id + "'"
        );
      }
    }
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

private:
  /**
   * @brief Extract the number of states from HMM transform type
   * @param type Transform type string (e.g., "hmm_2", "hmm_3", "hmm_4", "hmm_5")
   * @return Number of states (2-5)
   * @throws std::runtime_error if type format is invalid
   */
  static int ExtractNumStates(const std::string &type) {
    // Match "hmm_N" where N is a digit
    std::regex hmm_regex("^hmm_(\\d+)$");
    std::smatch match;

    if (std::regex_match(type, match, hmm_regex)) {
      int num_states = std::stoi(match[1].str());
      if (num_states >= 2 && num_states <= 5) {
        return num_states;
      }
      throw std::runtime_error(
        "HMM num_states must be between 2 and 5, got: " + std::to_string(num_states)
      );
    }

    throw std::runtime_error(
      "Invalid HMM transform type format: '" + type + "'. Expected 'hmm_N' where N is 2-5"
    );
  }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
