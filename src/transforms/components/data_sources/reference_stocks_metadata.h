#ifndef EPOCH_METADATA_REFERENCE_STOCKS_METADATA_H
#define EPOCH_METADATA_REFERENCE_STOCKS_METADATA_H

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeReferenceStocksDataSources() {
  using namespace epoch_script::data_sources;
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry for market bars (reference stocks use same schema)
  // Note: Reference stocks can be either DailyBars or MinuteBars depending on timeframe
  auto dailyBarsMeta = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(DataCategory::DailyBars);

  // Note: Uses DailyBars metadata by default, but actual category is determined at runtime based on timeframe
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "us_reference_stocks",
      .category = epoch_core::TransformCategory::DataSource,
      .plotKind = epoch_core::TransformPlotKind::close_line,
      .name = "US Reference Stocks",
      .options =
          {
              MetaDataOption{
                  .id = "ticker",
                  .name = "Reference Ticker",
                  .type = epoch_core::MetaDataOptionType::String,
                  .defaultValue = MetaDataOptionDefinition(std::string("SPY")),
                  .desc = "Reference stock ticker symbol (e.g., SPY, QQQ, DIA, IWM)"},
          },
      .isCrossSectional = false,
      .desc = dailyBarsMeta.description,
      .inputs = {},
      .outputs = BuildOutputsFromSDKMetadata(dailyBarsMeta),
      .atLeastOneInputRequired = false,
      .tags = {"reference", "comparison", "benchmark", "data", "source", "etf"},
      .requiresTimeFrame = true,
      .requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(dailyBarsMeta),
      .intradayOnly = false,
      .allowNullInputs = true,  // Data sources should preserve null rows
      .strategyTypes = {"pairs-trading", "relative-strength", "beta-hedging", "correlation"},
      .assetRequirements = {"multi-asset"},
      .usageContext =
          "Use this node to load reference stock data for comparison against your main asset. "
          "Date range automatically aligns with the strategy's main market_data_source. "
          "Common use cases: comparing stock performance to SPY, pairs trading, calculating beta, "
          "or building market-neutral strategies. The is_eod parameter is automatically determined "
          "from the timeframe (intraday vs daily/higher).",
      .limitations =
          "Data availability depends on Polygon.io subscription level. "
          "Date range is determined by the main market_data_source node in the strategy. "
          "External loader must handle API authentication, rate limiting, and date alignment.",
  });

  return metadataList;
}

}  // namespace epoch_script::transform

#endif  // EPOCH_METADATA_REFERENCE_STOCKS_METADATA_H
