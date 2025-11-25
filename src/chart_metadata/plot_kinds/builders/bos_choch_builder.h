#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Break of Structure / Change of Character PlotKind
 * Expects 4 outputs: bos, choch, level, broken_index
 */
class BosChochBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"bos", cfg.GetOutputId("bos").GetColumnName()},
      {"choch", cfg.GetOutputId("choch").GetColumnName()},
      {"level", cfg.GetOutputId("level").GetColumnName()},
      {"broken_index", cfg.GetOutputId("broken_index").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "bos", "BosChoch");
    ValidateOutput(cfg, "choch", "BosChoch");
    ValidateOutput(cfg, "level", "BosChoch");
    ValidateOutput(cfg, "broken_index", "BosChoch");
  }

  uint8_t GetZIndex() const override { return 10; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
