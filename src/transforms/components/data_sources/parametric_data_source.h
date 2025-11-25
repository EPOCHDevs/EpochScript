#pragma once

#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/dataframe.h>
#include "metadata_helper.h"
#include <regex>

namespace epoch_script::transform {

// Unified data source transform that handles parametric placeholders
// Supports any data source with optional placeholder expansion (e.g., {category}, {ticker})
class ParametricDataSourceTransform final : public ITransform {
public:
  explicit ParametricDataSourceTransform(const TransformConfiguration &config)
      : ITransform(config) {

    // Auto-detect placeholder from requiredDataSources
    auto requiredSources = config.GetTransformDefinition().GetMetadata().requiredDataSources;
    m_placeholderName = DetectPlaceholder(requiredSources);

    // Extract placeholder value from options if placeholder detected
    if (!m_placeholderName.empty()) {
      try {
        auto optionValue = config.GetOptionValue(m_placeholderName);
        // Handle both String and SelectOption types
        if (optionValue.IsType(MetaDataOptionType::String)) {
          m_placeholderValue = optionValue.GetString();
        } else if (optionValue.IsType(MetaDataOptionType::Select)) {
          m_placeholderValue = optionValue.GetSelectOption();
        }
      } catch (...) {
        // Option not found or wrong type - leave placeholder value empty
      }
    }

    // Build column rename mapping from loader DataFrame column names to graph output IDs
    BuildReplacementsMap();
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &data) const override {
    // External loader has already fetched data from API
    // and converted to DataFrame with expanded column names.
    // We just rename columns to match the node's output IDs.
    return data.rename(m_replacements);
  }

  // Expand placeholder in requiredDataSources for data loading
  std::vector<std::string> GetRequiredDataSources() const override {
    if (m_placeholderName.empty() || m_placeholderValue.empty()) {
      return ITransform::GetRequiredDataSources();
    }
    using epoch_script::data_sources::ReplacePlaceholders;
    auto unexpanded = ITransform::GetRequiredDataSources();
    return ReplacePlaceholders(unexpanded, m_placeholderName, m_placeholderValue);
  }

private:
  std::string m_placeholderName;   // e.g., "category" or "ticker"
  std::string m_placeholderValue;  // e.g., "CPI" or "SPX"
  std::unordered_map<std::string, std::string> m_replacements;

  // Detect placeholder name from requiredDataSources (e.g., "{category}" → "category")
  std::string DetectPlaceholder(const std::vector<std::string>& sources) {
    if (sources.empty()) {
      return "";
    }

    // Look for pattern {placeholder_name} in first source
    std::regex placeholderRegex(R"(\{([a-zA-Z_][a-zA-Z0-9_]*)\})");
    std::smatch match;

    for (const auto& source : sources) {
      if (std::regex_search(source, match, placeholderRegex)) {
        return match[1].str();  // Return the placeholder name without braces
      }
    }

    return "";  // No placeholder found
  }

  // Build m_replacements map: expanded loader column name → graph output ID
  // Example: {"ECON:CPI:value" → "fred_cpi#value"}
  void BuildReplacementsMap() {
    auto requiredSources = GetConfiguration().GetTransformDefinition().GetMetadata().requiredDataSources;
    auto outputs = GetConfiguration().GetOutputs();

    // Assumption: requiredDataSources and outputs have same count and parallel structure
    // Example: requiredDataSources[0] = "ECON:{category}:value" maps to outputs[0].id = "value"
    if (requiredSources.size() != outputs.size()) {
      // Mismatch - fallback to simple 1:1 mapping
      for (const auto& output : outputs) {
        std::string outputId = GetConfiguration().GetOutputId(output.id).GetColumnName();
        m_replacements[output.id] = outputId;
      }
      return;
    }

    // Build mapping from expanded requiredDataSource to graph output ID
    using epoch_script::data_sources::ReplacePlaceholder;
    for (size_t i = 0; i < requiredSources.size(); ++i) {
      std::string loaderColumnName = requiredSources[i];

      // Expand placeholder if present
      if (!m_placeholderName.empty() && !m_placeholderValue.empty()) {
        loaderColumnName = ReplacePlaceholder(loaderColumnName, m_placeholderName, m_placeholderValue);
      }

      // Map: loader column name → graph output ID
      // Example: "ECON:CPI:value" → "fred_cpi#value"
      std::string graphOutputId = GetConfiguration().GetOutputId(outputs[i].id).GetColumnName();
      m_replacements[loaderColumnName] = graphOutputId;
    }
  }
};

// Type aliases for backward compatibility
using FREDTransform = ParametricDataSourceTransform;
using PolygonDataSourceTransform = ParametricDataSourceTransform;
using PolygonBalanceSheetTransform = ParametricDataSourceTransform;
using PolygonIncomeStatementTransform = ParametricDataSourceTransform;
using PolygonCashFlowTransform = ParametricDataSourceTransform;
using PolygonFinancialRatiosTransform = ParametricDataSourceTransform;
using PolygonCommonIndicesTransform = ParametricDataSourceTransform;
using PolygonIndicesTransform = ParametricDataSourceTransform;
using PolygonNewsTransform = ParametricDataSourceTransform;
using PolygonDividendsTransform = ParametricDataSourceTransform;
using PolygonSplitsTransform = ParametricDataSourceTransform;
using PolygonTickerEventsTransform = ParametricDataSourceTransform;
using PolygonShortInterestTransform = ParametricDataSourceTransform;
using PolygonShortVolumeTransform = ParametricDataSourceTransform;

} // namespace epoch_script::transform
