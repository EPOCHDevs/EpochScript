#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Consolidation Box PlotKind
 * Expects 9 outputs: box_detected, box_top, box_bottom, box_height, touch_count,
 *                    upper_slope, lower_slope, target_up, target_down
 */
class ConsolidationBoxBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    std::unordered_map<std::string, std::string> result = {{"index", INDEX_COLUMN}};
    for (const auto& name : {"box_detected", "box_top", "box_bottom", "box_height",
                             "touch_count", "upper_slope", "lower_slope", "target_up", "target_down"}) {
      result[name] = cfg.GetOutputId(name).GetColumnName();
    }
    return result;
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "box_detected", "ConsolidationBox");
    ValidateOutput(cfg, "box_top", "ConsolidationBox");
    ValidateOutput(cfg, "box_bottom", "ConsolidationBox");
    ValidateOutput(cfg, "box_height", "ConsolidationBox");
    ValidateOutput(cfg, "touch_count", "ConsolidationBox");
    ValidateOutput(cfg, "upper_slope", "ConsolidationBox");
    ValidateOutput(cfg, "lower_slope", "ConsolidationBox");
    ValidateOutput(cfg, "target_up", "ConsolidationBox");
    ValidateOutput(cfg, "target_down", "ConsolidationBox");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
