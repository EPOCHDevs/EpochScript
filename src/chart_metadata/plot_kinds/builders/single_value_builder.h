#pragma once

#include "../single_value.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Generic builder for single-value PlotKinds
 * Used by RSI, CCI, ATR, and all other indicators with single "result" output
 */
class SingleValueBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"value", cfg.GetOutputId("result").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "result", "SingleValue");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
