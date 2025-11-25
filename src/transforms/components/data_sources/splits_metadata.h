#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

// Factory function to create metadata for Splits data source
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeSplitsDataSource() {
  using namespace epoch_script::polygon;
  using namespace epoch_script::data_sources;

  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry
  auto dataCategory = DataCategory::Splits;
  auto sdkMetadata = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(dataCategory);

  // Build outputs and requiredDataSources from SDK metadata
  auto outputs = BuildOutputsFromSDKMetadata(sdkMetadata);
  auto requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(sdkMetadata);

  // Splits data source
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = SPLITS,
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Stock Splits",
          .options = {},
          .isCrossSectional = false,
          .desc = sdkMetadata.description,
          .inputs = {},
          .outputs = outputs,
          .tags = {"splits", "data", "source", "polygon", "corporate-actions"},
          .requiresTimeFrame = false,
          .requiredDataSources = requiredDataSources,
          .intradayOnly = IsIntradayOnlyCategory(dataCategory),  // Auto-computed from registry
          .allowNullInputs = true,  // Splits are sparse - keep null rows for dates without splits
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Split,
              .text = "Stock Split: {split_from}:{split_to}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Warning,
              .title = std::nullopt,
              .valueKey = "split_from"
          },
          .strategyTypes = {"corporate-actions", "event-driven"},
          .assetRequirements = {"single-asset"},
          .usageContext = "Track stock split events for price adjustment awareness and corporate action strategies. Monitor split ratios and execution dates.",
          .limitations = "Split data normalized to dates (no intraday precision). Price history should already be adjusted for splits in most data feeds."
      });

  return metadataList;
}

} // namespace epoch_script::transform
