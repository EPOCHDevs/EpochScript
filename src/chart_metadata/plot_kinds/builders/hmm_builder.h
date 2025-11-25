#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for hmm PlotKind (Hidden Markov Model)
 * Outputs: state + dynamic state_*_prob outputs (state_0_prob, state_1_prob, etc.)
 */
class HMMBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"state", cfg.GetOutputId("state").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // Validate required "state" output exists
    ValidateOutput(cfg, "state", "HMM");

    // Count state probability outputs
    const auto &outputs = cfg.GetOutputs();
    int prob_count = 0;
    for (const auto &output : outputs) {
      if (output.id.find("state_") == 0 && output.id.find("_prob") != std::string::npos) {
        prob_count++;
      }
    }

    // Must have at least 2 states
    if (prob_count < 2) {
      throw std::runtime_error(
        "HMM transform must have at least 2 state probability outputs (state_0_prob, state_1_prob, ...)"
      );
    }
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
