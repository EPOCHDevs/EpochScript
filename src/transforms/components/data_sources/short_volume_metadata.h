#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

// Factory function to create metadata for ShortVolume data source
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeShortVolumeDataSource() {
  using namespace epoch_script::polygon;
  using namespace epoch_script::data_sources;

  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry
  auto dataCategory = DataCategory::ShortVolume;
  auto sdkMetadata = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(dataCategory);

  // Build outputs and requiredDataSources from SDK metadata
  auto outputs = BuildOutputsFromSDKMetadata(sdkMetadata);
  auto requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(sdkMetadata);

  // ShortVolume data source
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = SHORT_VOLUME,
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Short Volume",
          .options = {},
          .isCrossSectional = false,
          .desc = sdkMetadata.description,
          .inputs = {},
          .outputs = outputs,
          .tags = {"short-volume", "data", "source", "polygon", "volume", "sentiment"},
          .requiresTimeFrame = false,
          .requiredDataSources = requiredDataSources,
          .intradayOnly = IsIntradayOnlyCategory(dataCategory),  // Auto-computed from registry
          .allowNullInputs = true,  // Data sources should preserve null rows
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Activity,
              .text = "Short Volume<br/>Volume: {short_volume}<br/>Ratio: {short_volume_ratio}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Warning
          },
          .strategyTypes = {"sentiment", "volume-analysis", "microstructure"},
          .assetRequirements = {"single-asset"},
          .usageContext = "Track daily short volume as a percentage of total volume for sentiment analysis and order flow studies. High short volume may indicate bearish sentiment or market making activity.",
          .limitations = "Short volume != short interest. Represents daily short sale volume only. Normalized to dates (no intraday precision)."
      });

  return metadataList;
}

} // namespace epoch_script::transform
