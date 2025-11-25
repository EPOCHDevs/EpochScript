#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Order Blocks PlotKind
 * Expects 6 outputs: ob, top, bottom, ob_volume, mitigated_index, percentage
 */
class OrderBlocksBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"ob", cfg.GetOutputId("ob").GetColumnName()},
      {"top", cfg.GetOutputId("top").GetColumnName()},
      {"bottom", cfg.GetOutputId("bottom").GetColumnName()},
      {"ob_volume", cfg.GetOutputId("ob_volume").GetColumnName()},
      {"mitigated_index", cfg.GetOutputId("mitigated_index").GetColumnName()},
      {"percentage", cfg.GetOutputId("percentage").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "ob", "OrderBlocks");
    ValidateOutput(cfg, "top", "OrderBlocks");
    ValidateOutput(cfg, "bottom", "OrderBlocks");
    ValidateOutput(cfg, "ob_volume", "OrderBlocks");
    ValidateOutput(cfg, "mitigated_index", "OrderBlocks");
    ValidateOutput(cfg, "percentage", "OrderBlocks");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
