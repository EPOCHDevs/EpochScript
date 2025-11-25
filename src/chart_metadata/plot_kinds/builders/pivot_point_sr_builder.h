#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Pivot Point Support/Resistance PlotKind
 * Expects 7 outputs: pivot, resist_1, support_1, resist_2, support_2, resist_3, support_3
 */
class PivotPointSRBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"pivot", cfg.GetOutputId("pivot").GetColumnName()},
      {"resist_1", cfg.GetOutputId("resist_1").GetColumnName()},
      {"support_1", cfg.GetOutputId("support_1").GetColumnName()},
      {"resist_2", cfg.GetOutputId("resist_2").GetColumnName()},
      {"support_2", cfg.GetOutputId("support_2").GetColumnName()},
      {"resist_3", cfg.GetOutputId("resist_3").GetColumnName()},
      {"support_3", cfg.GetOutputId("support_3").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "pivot", "PivotPointSR");
    ValidateOutput(cfg, "resist_1", "PivotPointSR");
    ValidateOutput(cfg, "support_1", "PivotPointSR");
    ValidateOutput(cfg, "resist_2", "PivotPointSR");
    ValidateOutput(cfg, "support_2", "PivotPointSR");
    ValidateOutput(cfg, "resist_3", "PivotPointSR");
    ValidateOutput(cfg, "support_3", "PivotPointSR");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
