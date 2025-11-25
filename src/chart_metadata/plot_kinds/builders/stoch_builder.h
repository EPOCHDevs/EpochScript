#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Stochastic PlotKind
 * Expects 2 outputs: stoch_k, stoch_d
 */
class StochBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"stoch_k", cfg.GetOutputId("stoch_k").GetColumnName()},
      {"stoch_d", cfg.GetOutputId("stoch_d").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "stoch_k", "Stochastic");
    ValidateOutput(cfg, "stoch_d", "Stochastic");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
