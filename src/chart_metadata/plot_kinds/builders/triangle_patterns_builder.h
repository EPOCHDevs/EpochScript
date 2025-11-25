#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Triangle Patterns PlotKind
 * Expects 4 outputs: pattern_detected, upper_slope, lower_slope, triangle_type
 */
class TrianglePatternsBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"pattern_detected", cfg.GetOutputId("pattern_detected").GetColumnName()},
      {"upper_slope", cfg.GetOutputId("upper_slope").GetColumnName()},
      {"lower_slope", cfg.GetOutputId("lower_slope").GetColumnName()},
      {"triangle_type", cfg.GetOutputId("triangle_type").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    ValidateOutput(cfg, "pattern_detected", "TrianglePatterns");
    ValidateOutput(cfg, "upper_slope", "TrianglePatterns");
    ValidateOutput(cfg, "lower_slope", "TrianglePatterns");
    ValidateOutput(cfg, "triangle_type", "TrianglePatterns");
  }

  uint8_t GetZIndex() const override { return 10; }
  bool RequiresOwnAxis() const override { return false; }

};

} // namespace epoch_script::chart_metadata::plot_kinds
