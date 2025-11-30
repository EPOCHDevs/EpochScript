#pragma once

#include "../complex.h"
#include <stdexcept>

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for lightgbm PlotKind (LightGBM models)
 *
 * Handles both classifier (prediction + probability) and regressor (prediction only)
 */
class LightGBMBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    std::unordered_map<std::string, std::string> result = {
      {"index", INDEX_COLUMN},
      {"prediction", cfg.GetOutputId("prediction").GetColumnName()}
    };

    // Check if this is a classifier (has probability output)
    const auto &outputs = cfg.GetOutputs();
    for (const auto &output : outputs) {
      if (output.id == "probability") {
        result["probability"] = cfg.GetOutputId("probability").GetColumnName();
        break;
      }
    }

    return result;
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // All LightGBM models must have prediction output
    ValidateOutput(cfg, "prediction", "LightGBM");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
