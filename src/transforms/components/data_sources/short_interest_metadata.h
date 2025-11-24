#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

// Factory function to create metadata for ShortInterest data source
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeShortInterestDataSource() {
  using namespace epoch_script::polygon;
  using namespace epoch_script::data_sources;

  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry
  auto dataCategory = DataCategory::ShortInterest;
  auto sdkMetadata = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(dataCategory);

  // Build outputs and requiredDataSources from SDK metadata
  auto outputs = BuildOutputsFromSDKMetadata(sdkMetadata);
  auto requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(sdkMetadata);

  // ShortInterest data source
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = SHORT_INTEREST,
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Short Interest",
          .options = {},
          .isCrossSectional = false,
          .desc = sdkMetadata.description,
          .inputs = {},
          .outputs = outputs,
          .tags = {"short-interest", "data", "source", "polygon", "sentiment"},
          .requiresTimeFrame = false,
          .requiredDataSources = requiredDataSources,
          .intradayOnly = IsIntradayOnlyCategory(dataCategory),  // Auto-computed from registry
          .allowNullInputs = true,  // Short interest is reported periodically - keep null rows
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Activity,
              .text = "Short Interest: {short_interest} shares",
              .textIsTemplate = true,
              .color = epoch_core::Color::Error
          },
          .strategyTypes = {"short-squeeze", "sentiment", "contrarian"},
          .assetRequirements = {"single-asset"},
          .usageContext = "Track short interest levels for short squeeze detection, sentiment analysis, and contrarian strategies. Monitor days-to-cover ratios and short position changes.",
          .limitations = "Short interest data typically updated bi-weekly or monthly. Data lags actual short positions. Normalized to dates (no intraday precision)."
      });

  return metadataList;
}

} // namespace epoch_script::transform
