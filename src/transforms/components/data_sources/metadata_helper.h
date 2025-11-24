#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/common/metadata.hpp>
#include <vector>

namespace epoch_script::data_sources {

/**
 * Converts data_sdk::ArrowType to epoch_core::IODataType
 */
inline epoch_core::IODataType ConvertArrowTypeToIODataType(data_sdk::ArrowType arrowType) {
  switch (arrowType) {
    case data_sdk::ArrowType::STRING:
      return epoch_core::IODataType::String;
    case data_sdk::ArrowType::INT32:
    case data_sdk::ArrowType::INT64:
      return epoch_core::IODataType::Integer;
    case data_sdk::ArrowType::FLOAT32:
    case data_sdk::ArrowType::FLOAT64:
      return epoch_core::IODataType::Decimal;
    case data_sdk::ArrowType::BOOLEAN:
      return epoch_core::IODataType::Boolean;
    case data_sdk::ArrowType::TIMESTAMP_NS_UTC:
      return epoch_core::IODataType::Timestamp;
    default:
      return epoch_core::IODataType::String;
  }
}

/**
 * Builds IOMetaData outputs from SDK column metadata
 * Uses SIMPLE column IDs (no prefix) for AST compiler validation
 * @param sdkMetadata SDK metadata containing columns and category_prefix
 */
inline std::vector<epoch_script::transforms::IOMetaData>
BuildOutputsFromSDKMetadata(data_sdk::DataFrameMetadata const& sdkMetadata) {
  std::vector<epoch_script::transforms::IOMetaData> outputs;
  for (const auto& col : sdkMetadata.columns) {
    epoch_core::IODataType type = ConvertArrowTypeToIODataType(col.type);
    // Use simple column ID only (e.g., "value", "observation_date")
    // This allows AST compiler to validate handles like: fred_cpi.value
    outputs.push_back({type, col.id, col.name});
  }
  return outputs;
}

/**
 * Builds requiredDataSources from SDK column metadata
 * Prepends the category_prefix to each column ID
 * @param sdkMetadata SDK metadata containing columns and category_prefix
 * @param placeholder Optional placeholder name (e.g., "category", "ticker"). When provided,
 *                    inserts {placeholder} between prefix and id: "ECON:{category}:observation_date"
 */
inline std::vector<std::string>
BuildRequiredDataSourcesFromSDKMetadata(data_sdk::DataFrameMetadata const& sdkMetadata,
                                         std::string const& placeholder = "") {
  std::vector<std::string> requiredDataSources;
  for (const auto& col : sdkMetadata.columns) {
    std::string id = sdkMetadata.category_prefix;
    if (!placeholder.empty()) {
      id += "{" + placeholder + "}:";
    }
    id += col.id;
    requiredDataSources.push_back(id);
  }
  return requiredDataSources;
}

/**
 * Replaces a placeholder in a string with the actual value
 * @param str String containing placeholder (e.g., "ECON:{category}:value")
 * @param placeholder Placeholder name (e.g., "category")
 * @param value Actual value to substitute (e.g., "CPI")
 * @return String with placeholder replaced (e.g., "ECON:CPI:value")
 */
inline std::string ReplacePlaceholder(std::string str,
                                      std::string const& placeholder,
                                      std::string const& value) {
  std::string token = "{" + placeholder + "}";
  size_t pos = str.find(token);
  if (pos != std::string::npos) {
    str.replace(pos, token.length(), value);
  }
  return str;
}

/**
 * Replaces a placeholder in a vector of strings with the actual value
 * @param strings Vector of strings containing placeholders
 * @param placeholder Placeholder name (e.g., "category", "ticker")
 * @param value Actual value to substitute
 * @return Vector with placeholders replaced in all strings
 */
inline std::vector<std::string> ReplacePlaceholders(std::vector<std::string> const& strings,
                                                     std::string const& placeholder,
                                                     std::string const& value) {
  std::vector<std::string> result;
  result.reserve(strings.size());
  for (const auto& str : strings) {
    result.push_back(ReplacePlaceholder(str, placeholder, value));
  }
  return result;
}

} // namespace epoch_script::data_sources
