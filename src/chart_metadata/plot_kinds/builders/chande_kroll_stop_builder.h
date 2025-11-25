#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Chande Kroll Stop PlotKind
 * Expects 2 outputs: long_stop, short_stop
 */
class ChandeKrollStopBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"long_stop", cfg.GetOutputId("long_stop").GetColumnName()},
      {"short_stop", cfg.GetOutputId("short_stop").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "long_stop", "ChandeKrollStop");
    ValidateOutput(cfg, "short_stop", "ChandeKrollStop");
  }

  uint8_t GetZIndex() const override { return 1; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
