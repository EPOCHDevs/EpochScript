#pragma once

#include "../bands.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Band-style indicators (generic bands PlotKind)
 * Supports multiple transforms with different output naming:
 * - donchian_channel: bbands_upper, bbands_middle, bbands_lower
 * - acceleration_bands: upper_band, middle_band, lower_band
 * - keltner_channels: upper_band, lower_band (no middle)
 *
 * Middle band is optional (e.g., keltner_channels only has upper/lower)
 */
class BbandsBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    std::unordered_map<std::string, std::string> mapping;
    mapping["index"] = INDEX_COLUMN;

    // Try bbands_* naming first, then fall back to *_band naming
    if (cfg.ContainsOutputId("bbands_upper")) {
      mapping["bbands_upper"] = cfg.GetOutputId("bbands_upper").GetColumnName();
    } else if (cfg.ContainsOutputId("upper_band")) {
      mapping["bbands_upper"] = cfg.GetOutputId("upper_band").GetColumnName();
    }

    // Middle band is optional (e.g., keltner_channels doesn't have it)
    // Always add the key, but use empty string if not present
    if (cfg.ContainsOutputId("bbands_middle")) {
      mapping["bbands_middle"] = cfg.GetOutputId("bbands_middle").GetColumnName();
    } else if (cfg.ContainsOutputId("middle_band")) {
      mapping["bbands_middle"] = cfg.GetOutputId("middle_band").GetColumnName();
    } else {
      mapping["bbands_middle"] = "";  // Empty string if no middle band
    }

    if (cfg.ContainsOutputId("bbands_lower")) {
      mapping["bbands_lower"] = cfg.GetOutputId("bbands_lower").GetColumnName();
    } else if (cfg.ContainsOutputId("lower_band")) {
      mapping["bbands_lower"] = cfg.GetOutputId("lower_band").GetColumnName();
    }

    return mapping;
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // Check upper band (required)
    bool hasUpper = cfg.ContainsOutputId("bbands_upper") ||
                    cfg.ContainsOutputId("upper_band");
    if (!hasUpper) {
      throw std::runtime_error(
        cfg.GetTransformName() + " transform missing required output: bbands_upper or upper_band"
      );
    }

    // Check lower band (required)
    bool hasLower = cfg.ContainsOutputId("bbands_lower") ||
                    cfg.ContainsOutputId("lower_band");
    if (!hasLower) {
      throw std::runtime_error(
        cfg.GetTransformName() + " transform missing required output: bbands_lower or lower_band"
      );
    }

    // Middle band is optional - no validation needed
  }

  uint8_t GetZIndex() const override { return 1; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
