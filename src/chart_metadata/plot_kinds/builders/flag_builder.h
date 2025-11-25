#pragma once

#include "../complex.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Helper to get actual column name for an output
 *
 * For DataSource transforms, looks up the prefixed column name from requiredDataSources
 * (e.g., "title" -> "N:title" for News).
 * For other transforms, uses the NodeReference format (e.g., "sma_1d#result").
 *
 * @param cfg Transform configuration
 * @param outputId The simple output ID (e.g., "title", "event_type")
 * @return The actual column name to use in dataMapping
 */
inline std::string GetActualColumnName(
    const epoch_script::transform::TransformConfiguration &cfg,
    const std::string &outputId) {
  const auto &metadata = cfg.GetTransformDefinition().GetMetadata();

  // For DataSource transforms, look up column name in requiredDataSources
  if (metadata.category == epoch_core::TransformCategory::DataSource &&
      !metadata.requiredDataSources.empty()) {
    // Find matching entry in requiredDataSources
    // Entries are like "N:title", "TE:event_type", etc.
    for (const auto &col : metadata.requiredDataSources) {
      // Check if column ends with ":" + outputId or equals outputId exactly
      if (col == outputId) {
        return col;
      }
      std::string suffix = ":" + outputId;
      if (col.size() > suffix.size() &&
          col.compare(col.size() - suffix.size(), suffix.size(), suffix) == 0) {
        return col;
      }
    }
  }

  // For non-DataSource transforms or if not found, use NodeReference format
  return cfg.GetOutputId(outputId).GetColumnName();
}

/**
 * @brief Builder for Flag PlotKind
 * Special case: maps ALL outputs dynamically for template substitution
 * Used for generic event markers like candle patterns, fundamentals, etc.
 */
class FlagBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    // Start with index column
    std::unordered_map<std::string, std::string> dataMapping = {
      {"index", INDEX_COLUMN}
    };

    // Get flagSchema to determine valueKey
    const auto &metadata = cfg.GetTransformDefinition().GetMetadata();
    if (!metadata.flagSchema.has_value()) {
      throw std::runtime_error(
        "Flag transform '" + cfg.GetId() + "' missing required flagSchema"
      );
    }
    const auto& schema = metadata.flagSchema.value();

    // Add "value" mapping for flag positioning (required by UI)
    // For DataSource transforms, use actual column names from requiredDataSources
    if (!schema.valueKey.empty()) {
      dataMapping["value"] = GetActualColumnName(cfg, schema.valueKey);
    }

    // Note: Template substitution data (e.g., {column_name}) should be handled
    // in a separate field, NOT in dataMapping

    return dataMapping;
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    const auto &outputs = cfg.GetOutputs();

    // Flag transforms must have at least one output for template substitution
    if (outputs.empty()) {
      throw std::runtime_error("Flag transform has no outputs");
    }

    // Flag PlotKind MUST have flagSchema defined
    const auto &metadata = cfg.GetTransformDefinition().GetMetadata();
    if (!metadata.flagSchema.has_value()) {
      throw std::runtime_error(
        "Flag transform '" + cfg.GetId() + "' missing required flagSchema"
      );
    }

    const auto& schema = metadata.flagSchema.value();

    // If valueKey is specified, it must reference a valid output
    if (!schema.valueKey.empty() && !cfg.ContainsOutputId(schema.valueKey)) {
      throw std::runtime_error(
        "Flag transform '" + cfg.GetId() + "' flagSchema.valueKey '" +
        schema.valueKey + "' does not match any output"
      );
    }

    // All outputs must be accessible (validation confirms structure)
    for (const auto &output : outputs) {
      if (!cfg.ContainsOutputId(output.id)) {
        throw std::runtime_error(
          "Flag transform missing output: " + output.id
        );
      }
    }
  }

  uint8_t GetZIndex() const override { return 10; }
  bool RequiresOwnAxis() const override { return false; }

  epoch_script::MetaDataArgDefinitionMapping GetDefaultConfigOptions(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    const auto &metadata = cfg.GetTransformDefinition().GetMetadata();
    epoch_script::MetaDataArgDefinitionMapping defaults;

    // Flag PlotKind MUST have flagSchema defined
    if (!metadata.flagSchema.has_value()) {
      throw std::runtime_error(
        "Flag transform '" + cfg.GetId() + "' missing required flagSchema"
      );
    }

    const auto& schema = metadata.flagSchema.value();

    // Flag title
    std::string title = schema.title.has_value()
      ? schema.title.value()
      : metadata.name;
    defaults["flagTitle"] = epoch_script::MetaDataOptionDefinition{title};

    // Flag schema fields
    defaults["flagText"] = epoch_script::MetaDataOptionDefinition{schema.text};
    defaults["flagTextIsTemplate"] = epoch_script::MetaDataOptionDefinition{schema.textIsTemplate};
    defaults["flagIcon"] = epoch_script::MetaDataOptionDefinition{
      epoch_core::IconWrapper::ToString(schema.icon)
    };
    defaults["flagColor"] = epoch_script::MetaDataOptionDefinition{
      epoch_core::ColorWrapper::ToString(schema.color)
    };

    return defaults;
  }
};

}  // namespace epoch_script::chart_metadata::plot_kinds
