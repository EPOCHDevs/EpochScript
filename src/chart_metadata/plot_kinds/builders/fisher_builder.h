#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Fisher Transform PlotKind
 * Expects 2 outputs: fisher, fisher_signal
 */
class FisherBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"fisher", cfg.GetOutputId("fisher").GetColumnName()},
      {"fisher_signal", cfg.GetOutputId("fisher_signal").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "fisher", "Fisher");
    ValidateOutput(cfg, "fisher_signal", "Fisher");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
