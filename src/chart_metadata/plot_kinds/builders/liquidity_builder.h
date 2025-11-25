#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Liquidity PlotKind
 * Expects 4 outputs: liquidity, level, end, swept
 */
class LiquidityBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"liquidity", cfg.GetOutputId("liquidity").GetColumnName()},
      {"level", cfg.GetOutputId("level").GetColumnName()},
      {"end", cfg.GetOutputId("end").GetColumnName()},
      {"swept", cfg.GetOutputId("swept").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "liquidity", "Liquidity");
    ValidateOutput(cfg, "level", "Liquidity");
    ValidateOutput(cfg, "end", "Liquidity");
    ValidateOutput(cfg, "swept", "Liquidity");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
