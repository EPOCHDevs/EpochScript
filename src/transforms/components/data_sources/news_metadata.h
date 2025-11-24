#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

// Factory function to create metadata for News data source
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeNewsDataSource() {
  using namespace epoch_script::polygon;
  using namespace epoch_script::data_sources;

  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry
  auto dataCategory = DataCategory::News;
  auto sdkMetadata = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(dataCategory);

  // Build outputs and requiredDataSources from SDK metadata
  auto outputs = BuildOutputsFromSDKMetadata(sdkMetadata);
  auto requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(sdkMetadata);

  // News data source
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = NEWS,
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "News",
          .options = {},
          .isCrossSectional = false,
          .desc = sdkMetadata.description,
          .inputs = {},
          .outputs = outputs,
          .tags = {"news", "data", "source", "polygon", "sentiment"},
          .requiresTimeFrame = false,  // News doesn't require explicit timeframe
          .requiredDataSources = requiredDataSources,
          .intradayOnly = IsIntradayOnlyCategory(dataCategory),  // Auto-computed from registry
          .allowNullInputs = true,  // News is sparse - keep null rows for dates without news
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Newspaper,
              .text = "{title}<br/>{description}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Info
          },
          .strategyTypes = {"event-driven", "sentiment", "news-based"},
          .assetRequirements = {"single-asset"},
          .usageContext = "Access news articles for sentiment analysis, event detection, or news-driven strategies. Use for monitoring corporate announcements, earnings, or market-moving events.",
          .limitations = "News availability depends on provider coverage. Historical news may be limited. Sentiment analysis requires additional processing."
      });

  return metadataList;
}

} // namespace epoch_script::transform
