#pragma once

#include "../single_value.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for bb_percent_b PlotKind (Bollinger %B)
 */
class BBPercentBBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    // After validation, we know exactly which output exists
    std::string valueCol;
    if (cfg.ContainsOutputId("result")) {
      valueCol = cfg.GetOutputId("result").GetColumnName();
    } else if (cfg.ContainsOutputId("value")) {
      valueCol = cfg.GetOutputId("value").GetColumnName();
    } else {
      // Validation ensures at least one output exists
      valueCol = cfg.GetOutputId(cfg.GetOutputs()[0].id).GetColumnName();
    }

    return {
      {"index", INDEX_COLUMN},
      {"value", valueCol}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    const auto &outputs = cfg.GetOutputs();

    // Must have at least one output
    if (outputs.empty()) {
      throw std::runtime_error("BBPercentB transform has no outputs");
    }

    // If more than one output, must have "result" or "value"
    if (outputs.size() > 1) {
      if (!cfg.ContainsOutputId("result") && !cfg.ContainsOutputId("value")) {
        throw std::runtime_error(
          "BBPercentB transform with multiple outputs must have 'result' or 'value' output"
        );
      }
    }
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
