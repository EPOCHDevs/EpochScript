#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Vortex Indicator PlotKind
 * Expects 2 outputs: plus_indicator, minus_indicator
 */
class VortexBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"plus_indicator", cfg.GetOutputId("plus_indicator").GetColumnName()},
      {"minus_indicator", cfg.GetOutputId("minus_indicator").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "plus_indicator", "Vortex");
    ValidateOutput(cfg, "minus_indicator", "Vortex");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
