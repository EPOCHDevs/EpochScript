#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Previous High/Low PlotKind
 * Expects 4 outputs: previous_high, previous_low, broken_high, broken_low
 */
class PreviousHighLowBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"previous_high", cfg.GetOutputId("previous_high").GetColumnName()},
      {"previous_low", cfg.GetOutputId("previous_low").GetColumnName()},
      {"broken_high", cfg.GetOutputId("broken_high").GetColumnName()},
      {"broken_low", cfg.GetOutputId("broken_low").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "previous_high", "PreviousHighLow");
    ValidateOutput(cfg, "previous_low", "PreviousHighLow");
    ValidateOutput(cfg, "broken_high", "PreviousHighLow");
    ValidateOutput(cfg, "broken_low", "PreviousHighLow");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
