#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Pennant Pattern PlotKind
 * Expects 4 outputs: bull_pennant, bear_pennant, slmax, slmin
 */
class PennantPatternBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"bull_pennant", cfg.GetOutputId("bull_pennant").GetColumnName()},
      {"bear_pennant", cfg.GetOutputId("bear_pennant").GetColumnName()},
      {"slmax", cfg.GetOutputId("slmax").GetColumnName()},
      {"slmin", cfg.GetOutputId("slmin").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "bull_pennant", "PennantPattern");
    ValidateOutput(cfg, "bear_pennant", "PennantPattern");
    ValidateOutput(cfg, "slmax", "PennantPattern");
    ValidateOutput(cfg, "slmin", "PennantPattern");
  }

  uint8_t GetZIndex() const override { return 10; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
