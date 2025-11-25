#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Head and Shoulders PlotKind
 * Expects 3 outputs: pattern_detected, neckline_level, target
 */
class HeadAndShouldersBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"pattern_detected", cfg.GetOutputId("pattern_detected").GetColumnName()},
      {"neckline_level", cfg.GetOutputId("neckline_level").GetColumnName()},
      {"target", cfg.GetOutputId("target").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "pattern_detected", "HeadAndShoulders");
    ValidateOutput(cfg, "neckline_level", "HeadAndShoulders");
    ValidateOutput(cfg, "target", "HeadAndShoulders");
  }

  uint8_t GetZIndex() const override { return 10; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
