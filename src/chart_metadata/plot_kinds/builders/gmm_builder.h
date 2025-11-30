#pragma once

#include "../complex.h"
#include <stdexcept>
#include <regex>

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for gmm PlotKind (Gaussian Mixture Model)
 * Outputs: component + dynamic component_*_prob outputs + log_likelihood
 *
 * The number of components is extracted from the transform type (e.g., "gmm_3" -> 3 components)
 * and all component probability outputs (component_0_prob through component_{n-1}_prob) are returned.
 */
class GMMBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    std::unordered_map<std::string, std::string> result = {
      {"index", INDEX_COLUMN},
      {"component", cfg.GetOutputId("component").GetColumnName()},
      {"log_likelihood", cfg.GetOutputId("log_likelihood").GetColumnName()}
    };

    // Extract num_components from transform type (e.g., "gmm_3" -> 3)
    int num_components = ExtractNumComponents(cfg.GetTransformName());

    // Add all component probability outputs
    for (int i = 0; i < num_components; ++i) {
      std::string prob_id = "component_" + std::to_string(i) + "_prob";
      result[prob_id] = cfg.GetOutputId(prob_id).GetColumnName();
    }

    return result;
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // Validate required "component" output exists
    ValidateOutput(cfg, "component", "GMM");

    // Validate required "log_likelihood" output exists
    ValidateOutput(cfg, "log_likelihood", "GMM");

    // Extract num_components from transform type
    int num_components = ExtractNumComponents(cfg.GetTransformName());

    // Validate all expected probability outputs exist
    const auto &outputs = cfg.GetOutputs();
    for (int i = 0; i < num_components; ++i) {
      std::string prob_id = "component_" + std::to_string(i) + "_prob";
      bool found = false;
      for (const auto &output : outputs) {
        if (output.id == prob_id) {
          found = true;
          break;
        }
      }
      if (!found) {
        throw std::runtime_error(
          "GMM transform with " + std::to_string(num_components) +
          " components must have output '" + prob_id + "'"
        );
      }
    }
  }

  uint8_t GetZIndex() const override { return 5; }
  bool RequiresOwnAxis() const override { return false; }

private:
  /**
   * @brief Extract the number of components from GMM transform type
   * @param type Transform type string (e.g., "gmm_2", "gmm_3", "gmm_4", "gmm_5")
   * @return Number of components (2-5)
   * @throws std::runtime_error if type format is invalid
   */
  static int ExtractNumComponents(const std::string &type) {
    // Match "gmm_N" where N is a digit
    std::regex gmm_regex("^gmm_(\\d+)$");
    std::smatch match;

    if (std::regex_match(type, match, gmm_regex)) {
      int num_components = std::stoi(match[1].str());
      if (num_components >= 2 && num_components <= 5) {
        return num_components;
      }
      throw std::runtime_error(
        "GMM num_components must be between 2 and 5, got: " + std::to_string(num_components)
      );
    }

    throw std::runtime_error(
      "Invalid GMM transform type format: '" + type + "'. Expected 'gmm_N' where N is 2-5"
    );
  }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
