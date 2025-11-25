#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Double Top/Bottom PlotKind
 * Expects 3 outputs: pattern_detected, breakout_level, target
 */
class DoubleTopBottomBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"pattern_detected", cfg.GetOutputId("pattern_detected").GetColumnName()},
      {"breakout_level", cfg.GetOutputId("breakout_level").GetColumnName()},
      {"target", cfg.GetOutputId("target").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "pattern_detected", "DoubleTopBottom");
    ValidateOutput(cfg, "breakout_level", "DoubleTopBottom");
    ValidateOutput(cfg, "target", "DoubleTopBottom");
  }

  uint8_t GetZIndex() const override { return 10; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
