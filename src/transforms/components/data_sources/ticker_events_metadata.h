#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

// Factory function to create metadata for TickerEvents data source
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeTickerEventsDataSource() {
  using namespace epoch_script::polygon;
  using namespace epoch_script::data_sources;

  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry
  auto dataCategory = DataCategory::TickerEvents;
  auto sdkMetadata = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(dataCategory);

  // Build outputs and requiredDataSources from SDK metadata
  auto outputs = BuildOutputsFromSDKMetadata(sdkMetadata);
  auto requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(sdkMetadata);

  // TickerEvents data source
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = TICKER_EVENTS,
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Ticker Events",
          .options = {},
          .isCrossSectional = false,
          .desc = sdkMetadata.description,
          .inputs = {},
          .outputs = outputs,
          .tags = {"ticker-events", "data", "source", "polygon", "corporate-events"},
          .requiresTimeFrame = false,
          .requiredDataSources = requiredDataSources,
          .intradayOnly = IsIntradayOnlyCategory(dataCategory),  // Auto-computed from registry
          .allowNullInputs = true,  // Ticker events are sparse - keep null rows for dates without events
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Bell,
              .text = "Ticker Event: {event_type}<br/>{ticker}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Warning
          },
          .strategyTypes = {"event-driven", "corporate-actions"},
          .assetRequirements = {"single-asset"},
          .usageContext = "Access ticker-level corporate events including name changes, ticker symbol changes, delistings, and other ticker lifecycle events.",
          .limitations = "Event data normalized to dates (no intraday precision). Historical event data coverage varies by provider."
      });

  return metadataList;
}

} // namespace epoch_script::transform
