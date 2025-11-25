#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Sessions PlotKind
 * Expects 5 outputs: active, high, low, closed, opened
 */
class SessionsBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"active", cfg.GetOutputId("active").GetColumnName()},
      {"high", cfg.GetOutputId("high").GetColumnName()},
      {"low", cfg.GetOutputId("low").GetColumnName()},
      {"closed", cfg.GetOutputId("closed").GetColumnName()},
      {"opened", cfg.GetOutputId("opened").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "active", "Sessions");
    ValidateOutput(cfg, "high", "Sessions");
    ValidateOutput(cfg, "low", "Sessions");
    ValidateOutput(cfg, "closed", "Sessions");
    ValidateOutput(cfg, "opened", "Sessions");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
