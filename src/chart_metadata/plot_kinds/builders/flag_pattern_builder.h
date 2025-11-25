#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Flag Pattern PlotKind
 * Expects 4 outputs: bull_flag, bear_flag, slmax, slmin
 */
class FlagPatternBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"bull_flag", cfg.GetOutputId("bull_flag").GetColumnName()},
      {"bear_flag", cfg.GetOutputId("bear_flag").GetColumnName()},
      {"slmax", cfg.GetOutputId("slmax").GetColumnName()},
      {"slmin", cfg.GetOutputId("slmin").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "bull_flag", "FlagPattern");
    ValidateOutput(cfg, "bear_flag", "FlagPattern");
    ValidateOutput(cfg, "slmax", "FlagPattern");
    ValidateOutput(cfg, "slmin", "FlagPattern");
  }

  uint8_t GetZIndex() const override { return 10; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
