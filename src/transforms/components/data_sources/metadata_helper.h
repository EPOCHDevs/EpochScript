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
 */
inline std::vector<epoch_script::transforms::IOMetaData>
BuildOutputsFromSDKMetadata(data_sdk::DataFrameMetadata const& sdkMetadata) {
  std::vector<epoch_script::transforms::IOMetaData> outputs;
  for (const auto& col : sdkMetadata.columns) {
    epoch_core::IODataType type = ConvertArrowTypeToIODataType(col.type);
    outputs.push_back({type, col.id, col.name});
  }
  return outputs;
}

/**
 * Builds requiredDataSources from SDK column metadata
 * Prepends the category_prefix to each column ID
 */
inline std::vector<std::string>
BuildRequiredDataSourcesFromSDKMetadata(data_sdk::DataFrameMetadata const& sdkMetadata) {
  std::vector<std::string> requiredDataSources;
  for (const auto& col : sdkMetadata.columns) {
    requiredDataSources.push_back(sdkMetadata.category_prefix + col.id);
  }
  return requiredDataSources;
}

  inline std::vector<std::string>
BuildRequiredDataSourcesFromSDKMetadata(data_sdk::DataFrameMetadata const& sdkMetadata, std::string const& placeholder) {
  std::vector<std::string> requiredDataSources;
  for (const auto& col : sdkMetadata.columns) {
    requiredDataSources.push_back(sdkMetadata.category_prefix + "{" + placeholder + "}"+ col.id);
  }
  return requiredDataSources;
}

} // namespace epoch_script::data_sources
