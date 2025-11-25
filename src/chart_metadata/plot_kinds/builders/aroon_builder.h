#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Aroon PlotKind
 * Expects 2 outputs: aroon_up, aroon_down
 */
class AroonBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"aroon_up", cfg.GetOutputId("aroon_up").GetColumnName()},
      {"aroon_down", cfg.GetOutputId("aroon_down").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "aroon_up", "Aroon");
    ValidateOutput(cfg, "aroon_down", "Aroon");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
