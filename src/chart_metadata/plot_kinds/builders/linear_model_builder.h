#pragma once

#include "../complex.h"
#include <stdexcept>

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for linear_model PlotKind (LIBLINEAR models)
 *
 * Handles both classifiers (logistic_l1, logistic_l2) and regressors (svr_l1, svr_l2)
 *
 * Classifier outputs: prediction, probability, decision_value
 * Regressor outputs: prediction only
 */
class LinearModelBuilder : public IPlotKindBuilder {
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
    bool has_probability = false;
    bool has_decision_value = false;

    for (const auto &output : outputs) {
      if (output.id == "probability") {
        has_probability = true;
      }
      if (output.id == "decision_value") {
        has_decision_value = true;
      }
    }

    // Add classifier-specific outputs if present
    if (has_probability) {
      result["probability"] = cfg.GetOutputId("probability").GetColumnName();
    }
    if (has_decision_value) {
      result["decision_value"] = cfg.GetOutputId("decision_value").GetColumnName();
    }

    return result;
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // All linear models must have prediction output
    ValidateOutput(cfg, "prediction", "LinearModel");
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return true; }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
