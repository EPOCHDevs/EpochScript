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

  // Get metadata from MetadataRegistry (same OHLC schema as indices)
  auto stocksMetadata = data_sdk::dataloader::MetadataRegistry::GetIndicesMetadata(true);

  // Build outputs with simple IDs (e.g., "c", "o", "h", "l") for AST compiler validation
  auto outputs = BuildOutputsFromSDKMetadata(stocksMetadata);

  // Common Reference Stocks with SelectOption dropdown
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "common_reference_stocks",
      .category = epoch_core::TransformCategory::DataSource,
      .plotKind = epoch_core::TransformPlotKind::close_line,
      .name = "Common Reference Stocks",
      .options =
          {
              MetaDataOption{
                  .id = "ticker",
                  .name = "Reference Stock",
                  .type = epoch_core::MetaDataOptionType::Select,
                  .defaultValue = MetaDataOptionDefinition(std::string("SPY")),
                  .selectOption =
                      {
                          // Major ETFs
                          {"SPY - S&P 500 ETF", "SPY"},
                          {"QQQ - NASDAQ 100 ETF", "QQQ"},
                          {"DIA - Dow Jones ETF", "DIA"},
                          {"IWM - Russell 2000 ETF", "IWM"},
                          {"AGG - Aggregate Bond ETF", "AGG"},
                          {"VTI - Total Stock Market ETF", "VTI"},
                          {"GLD - Gold ETF", "GLD"},
                          {"TLT - 20+ Year Treasury ETF", "TLT"},
                      },
                  .desc = "Select the reference stock"},
          },
      .desc = stocksMetadata.description,
      .inputs = {},
      .outputs = outputs,
      .requiresTimeFrame = true,
      .requiredDataSources = {"STK:{ticker}:c", "STK:{ticker}:o", "STK:{ticker}:h", "STK:{ticker}:l"},
      .intradayOnly = false,
      .allowNullInputs = true,
      .strategyTypes = {"pairs-trading", "relative-strength", "beta-hedging", "correlation"},
      .assetRequirements = {"single-asset", "multi-asset"},
      .usageContext =
          "Use this node to load reference stock data for comparison against your main asset. "
          "Common use cases: comparing stock performance to SPY, pairs trading, calculating beta, "
          "or building market-neutral strategies.",
      .limitations =
          "Data availability depends on Polygon.io subscription level. "
          "External loader must handle API authentication and rate limiting.",
  });

  // Dynamic Reference Stocks with ticker parameter
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "reference_stocks",
      .category = epoch_core::TransformCategory::DataSource,
      .plotKind = epoch_core::TransformPlotKind::close_line,
      .name = "Reference Stocks",
      .options =
          {
              MetaDataOption{
                  .id = "ticker",
                  .name = "Reference Ticker",
                  .type = epoch_core::MetaDataOptionType::String,
                  .defaultValue = MetaDataOptionDefinition(std::string("SPY")),
                  .desc = "Reference stock ticker symbol (e.g., SPY, QQQ, DIA, IWM, AAPL)"},
          },
      .desc = stocksMetadata.description,
      .inputs = {},
      .outputs = outputs,
      .requiresTimeFrame = true,
      .requiredDataSources = {"STK:{ticker}:c", "STK:{ticker}:o", "STK:{ticker}:h", "STK:{ticker}:l"},
      .intradayOnly = false,
      .allowNullInputs = true,
      .strategyTypes = {"pairs-trading", "relative-strength", "beta-hedging", "correlation"},
      .assetRequirements = {"single-asset", "multi-asset"},
      .usageContext =
          "Use this node to load reference stock data for any ticker. "
          "Useful for individual stocks or ETFs not in the common list.",
      .limitations =
          "Data availability depends on Polygon.io subscription level. "
          "External loader must handle API authentication and rate limiting.",
  });

  return metadataList;
}

}  // namespace epoch_script::transform

#endif  // EPOCH_METADATA_REFERENCE_STOCKS_METADATA_H
