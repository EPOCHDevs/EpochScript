#ifndef EPOCH_METADATA_REFERENCE_FX_METADATA_H
#define EPOCH_METADATA_REFERENCE_FX_METADATA_H

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeReferenceFXDataSources() {
  using namespace epoch_script::data_sources;
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry for FX (same OHLC schema as indices)
  auto fxMetadata = data_sdk::dataloader::MetadataRegistry::GetIndicesMetadata(true);

  // Build outputs with simple IDs (e.g., "c", "o", "h", "l") for AST compiler validation
  auto outputs = BuildOutputsFromSDKMetadata(fxMetadata);

  // Common FX Pairs with SelectOption dropdown
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "common_fx_pairs",
      .category = epoch_core::TransformCategory::DataSource,
      .plotKind = epoch_core::TransformPlotKind::close_line,
      .name = "Common FX Pairs",
      .options =
          {
              MetaDataOption{
                  .id = "ticker",
                  .name = "Currency Pair",
                  .type = epoch_core::MetaDataOptionType::Select,
                  .defaultValue = MetaDataOptionDefinition(std::string("EURUSD")),
                  .selectOption =
                      {
                          // Major Pairs (G10)
                          {"EUR/USD - Euro/US Dollar", "EURUSD"},
                          {"GBP/USD - British Pound/US Dollar", "GBPUSD"},
                          {"USD/JPY - US Dollar/Japanese Yen", "USDJPY"},
                          {"USD/CHF - US Dollar/Swiss Franc", "USDCHF"},
                          {"AUD/USD - Australian Dollar/US Dollar", "AUDUSD"},
                          {"USD/CAD - US Dollar/Canadian Dollar", "USDCAD"},
                          {"NZD/USD - New Zealand Dollar/US Dollar", "NZDUSD"},
                          // Cross Rates
                          {"EUR/GBP - Euro/British Pound", "EURGBP"},
                          {"EUR/JPY - Euro/Japanese Yen", "EURJPY"},
                          {"GBP/JPY - British Pound/Japanese Yen", "GBPJPY"},
                      },
                  .desc = "Select the FX currency pair"},
          },
      .desc = fxMetadata.description,
      .inputs = {},
      .outputs = outputs,
      .requiresTimeFrame = true,
      .requiredDataSources = {"FX:{ticker}:c", "FX:{ticker}:o", "FX:{ticker}:h", "FX:{ticker}:l"},
      .intradayOnly = false,
      .allowNullInputs = true,
      .strategyTypes = {"fx-trading", "carry-trade", "correlation", "hedge"},
      .assetRequirements = {"single-asset", "multi-asset"},
      .usageContext =
          "Use this node to access historical FX data for currency trading strategies, "
          "carry trades, or cross-asset correlation studies. Select from popular currency pairs.",
      .limitations =
          "Data availability and update frequency depend on Polygon.io subscription level. "
          "External loader must handle API authentication and rate limiting.",
  });

  // Dynamic FX Pairs with ticker parameter
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "fx_pairs",
      .category = epoch_core::TransformCategory::DataSource,
      .plotKind = epoch_core::TransformPlotKind::close_line,
      .name = "FX Pairs",
      .options =
          {
              MetaDataOption{
                  .id = "ticker",
                  .name = "Currency Pair",
                  .type = epoch_core::MetaDataOptionType::String,
                  .defaultValue = MetaDataOptionDefinition(std::string("EURUSD")),
                  .desc = "FX currency pair symbol (e.g., EURUSD, GBPUSD, USDJPY, EURGBP)"},
          },
      .desc = fxMetadata.description,
      .inputs = {},
      .outputs = outputs,
      .requiresTimeFrame = true,
      .requiredDataSources = {"FX:{ticker}:c", "FX:{ticker}:o", "FX:{ticker}:h", "FX:{ticker}:l"},
      .intradayOnly = false,
      .allowNullInputs = true,
      .strategyTypes = {"fx-trading", "carry-trade", "correlation", "hedge"},
      .assetRequirements = {"single-asset", "multi-asset"},
      .usageContext =
          "Use this node to access historical data for any FX pair by specifying its ticker symbol. "
          "Useful for exotic pairs or custom currency combinations not in the common list.",
      .limitations =
          "Data availability and update frequency depend on Polygon.io subscription level. "
          "External loader must handle API authentication and rate limiting.",
  });

  return metadataList;
}

}  // namespace epoch_script::transform

#endif  // EPOCH_METADATA_REFERENCE_FX_METADATA_H
