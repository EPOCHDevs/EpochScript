#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for QQE PlotKind
 * Expects 4 outputs: result, rsi_ma, long_line, short_line
 */
class QQEBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"result", cfg.GetOutputId("result").GetColumnName()},
      {"rsi_ma", cfg.GetOutputId("rsi_ma").GetColumnName()},
      {"long_line", cfg.GetOutputId("long_line").GetColumnName()},
      {"short_line", cfg.GetOutputId("short_line").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "result", "QQE");
    ValidateOutput(cfg, "rsi_ma", "QQE");
    ValidateOutput(cfg, "long_line", "QQE");
    ValidateOutput(cfg, "short_line", "QQE");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
