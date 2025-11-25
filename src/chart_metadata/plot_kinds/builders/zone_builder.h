#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Zone PlotKind
 * Maps a single boolean output (either "result" or "value")
 * Used for time-based highlighting like day_of_week, session_time_window, etc.
 */
class ZoneBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    // Validation ensures exactly one of "result" or "value" exists
    // Determine which one to use (not checking IF it exists, but WHICH exists)
    std::string value_id = cfg.ContainsOutputId("result")
      ? cfg.GetOutputId("result").GetColumnName()
      : cfg.GetOutputId("value").GetColumnName();

    return {
      {"index", INDEX_COLUMN},
      {"value", value_id}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    const auto &outputs = cfg.GetOutputs();

    // Zone transforms must have at least one output
    if (outputs.empty()) {
      throw std::runtime_error("Zone transform has no outputs");
    }

    // Must have exactly one of "result" or "value" output (mutually exclusive)
    bool has_result = cfg.ContainsOutputId("result");
    bool has_value = cfg.ContainsOutputId("value");

    if (!has_result && !has_value) {
      throw std::runtime_error(
        "Zone transform must have either 'result' or 'value' output"
      );
    }

    if (has_result && has_value) {
      throw std::runtime_error(
        "Zone transform cannot have both 'result' and 'value' outputs"
      );
    }
  }

  uint8_t GetZIndex() const override { return 3; }
  bool RequiresOwnAxis() const override { return false; }

  epoch_script::MetaDataArgDefinitionMapping GetDefaultConfigOptions(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    const auto &metadata = cfg.GetTransformDefinition().GetMetadata();

    // Assign unique color per zone transform
    static const std::unordered_map<std::string, epoch_core::Color> zoneColors = {
      {"turn_of_month", epoch_core::Color::Blue},
      {"day_of_week", epoch_core::Color::Green},
      {"month_of_year", epoch_core::Color::Orange},
      {"quarter", epoch_core::Color::Purple},
      {"holiday", epoch_core::Color::Red},
      {"week_of_month", epoch_core::Color::Cyan},
      {"session_time_window", epoch_core::Color::Yellow}
    };

    auto color = epoch_core::lookupDefault(zoneColors, cfg.GetId(), epoch_core::Color::Default);

    return {
      {"name", epoch_script::MetaDataOptionDefinition{metadata.name}},
      {"position", epoch_script::MetaDataOptionDefinition{std::string("center")}},
      {"color", epoch_script::MetaDataOptionDefinition{
        epoch_core::ColorWrapper::ToString(color)
      }}
    };
  }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
