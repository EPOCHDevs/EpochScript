#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Elder's Thermometer PlotKind
 * Expects 4 outputs: result, ema, buy_signal, sell_signal
 */
class EldersBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"result", cfg.GetOutputId("result").GetColumnName()},
      {"ema", cfg.GetOutputId("ema").GetColumnName()},
      {"buy_signal", cfg.GetOutputId("buy_signal").GetColumnName()},
      {"sell_signal", cfg.GetOutputId("sell_signal").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "result", "Elders");
    ValidateOutput(cfg, "ema", "Elders");
    ValidateOutput(cfg, "buy_signal", "Elders");
    ValidateOutput(cfg, "sell_signal", "Elders");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
