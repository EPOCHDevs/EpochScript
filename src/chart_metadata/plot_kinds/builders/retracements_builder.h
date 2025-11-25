#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Retracements PlotKind
 * Expects 3 outputs: direction, current_retracement, deepest_retracement
 */
class RetracementsBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"direction", cfg.GetOutputId("direction").GetColumnName()},
      {"current_retracement", cfg.GetOutputId("current_retracement").GetColumnName()},
      {"deepest_retracement", cfg.GetOutputId("deepest_retracement").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "direction", "Retracements");
    ValidateOutput(cfg, "current_retracement", "Retracements");
    ValidateOutput(cfg, "deepest_retracement", "Retracements");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
