#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for MACD PlotKind
 * Expects 3 outputs: macd, macd_signal, macd_histogram
 */
class MACDBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"macd", cfg.GetOutputId("macd").GetColumnName()},
      {"macd_signal", cfg.GetOutputId("macd_signal").GetColumnName()},
      {"macd_histogram", cfg.GetOutputId("macd_histogram").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "macd", "MACD");
    ValidateOutput(cfg, "macd_signal", "MACD");
    ValidateOutput(cfg, "macd_histogram", "MACD");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
