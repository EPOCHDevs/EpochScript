#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Close Line PlotKind
 * Only maps the "c" (close) output - strict validation
 * Used for transforms that output OHLCV data but we only want to plot close
 */
class CloseLineBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"c", cfg.GetOutputId("c").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // Strict validation: must have "c" output
    ValidateOutput(cfg, "c", "CloseLine");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
