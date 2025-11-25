#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for gap PlotKind (Gap indicator)
 * Outputs: gap_filled, gap_retrace, gap_size, psc, psc_timestamp
 */
class GapBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"gap_filled", cfg.GetOutputId("gap_filled").GetColumnName()},
      {"gap_retrace", cfg.GetOutputId("gap_retrace").GetColumnName()},
      {"gap_size", cfg.GetOutputId("gap_size").GetColumnName()},
      {"psc", cfg.GetOutputId("psc").GetColumnName()},
      {"psc_timestamp", cfg.GetOutputId("psc_timestamp").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "gap_filled", "Gap");
    ValidateOutput(cfg, "gap_retrace", "Gap");
    ValidateOutput(cfg, "gap_size", "Gap");
    ValidateOutput(cfg, "psc", "Gap");
    ValidateOutput(cfg, "psc_timestamp", "Gap");
  }

  uint8_t GetZIndex() const override { return 1; }
  bool RequiresOwnAxis() const override { return false; }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
