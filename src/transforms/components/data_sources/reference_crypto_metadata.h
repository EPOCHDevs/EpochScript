#ifndef EPOCH_METADATA_REFERENCE_CRYPTO_METADATA_H
#define EPOCH_METADATA_REFERENCE_CRYPTO_METADATA_H

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeReferenceCryptoDataSources() {
  using namespace epoch_script::data_sources;
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry (same OHLC schema as indices)
  auto cryptoMetadata = data_sdk::dataloader::MetadataRegistry::GetIndicesMetadata(true);

  // Build outputs with simple IDs (e.g., "c", "o", "h", "l") for AST compiler validation
  auto outputs = BuildOutputsFromSDKMetadata(cryptoMetadata);

  // Common Crypto Pairs with SelectOption dropdown
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "common_crypto_pairs",
      .category = epoch_core::TransformCategory::DataSource,
      .plotKind = epoch_core::TransformPlotKind::close_line,
      .name = "Common Crypto Pairs",
      .options =
          {
              MetaDataOption{
                  .id = "ticker",
                  .name = "Crypto Pair",
                  .type = epoch_core::MetaDataOptionType::Select,
                  .defaultValue = MetaDataOptionDefinition(std::string("BTCUSD")),
                  .selectOption =
                      {
                          // Major Cryptos vs USD
                          {"BTC/USD - Bitcoin/US Dollar", "BTCUSD"},
                          {"ETH/USD - Ethereum/US Dollar", "ETHUSD"},
                          {"SOL/USD - Solana/US Dollar", "SOLUSD"},
                          {"XRP/USD - Ripple/US Dollar", "XRPUSD"},
                          {"DOGE/USD - Dogecoin/US Dollar", "DOGEUSD"},
                          {"ADA/USD - Cardano/US Dollar", "ADAUSD"},
                          {"MATIC/USD - Polygon/US Dollar", "MATICUSD"},
                          {"DOT/USD - Polkadot/US Dollar", "DOTUSD"},
                          {"LTC/USD - Litecoin/US Dollar", "LTCUSD"},
                          {"BNB/USD - Binance Coin/US Dollar", "BNBUSD"},
                      },
                  .desc = "Select the cryptocurrency pair"},
          },
      .desc = cryptoMetadata.description,
      .inputs = {},
      .outputs = outputs,
      .requiresTimeFrame = true,
      .requiredDataSources = {"CRYPTO:{ticker}:c", "CRYPTO:{ticker}:o", "CRYPTO:{ticker}:h", "CRYPTO:{ticker}:l"},
      .intradayOnly = false,
      .allowNullInputs = true,
      .strategyTypes = {"crypto-trading", "momentum", "correlation", "hedge"},
      .assetRequirements = {"single-asset", "multi-asset"},
      .usageContext =
          "Use this node to access historical crypto data for trading strategies or "
          "cross-asset analysis. Select from popular cryptocurrency pairs.",
      .limitations =
          "Data availability depends on Polygon.io subscription level. "
          "Crypto markets trade 24/7 unlike traditional markets.",
  });

  // Dynamic Crypto Pairs with ticker parameter
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "crypto_pairs",
      .category = epoch_core::TransformCategory::DataSource,
      .plotKind = epoch_core::TransformPlotKind::close_line,
      .name = "Crypto Pairs",
      .options =
          {
              MetaDataOption{
                  .id = "ticker",
                  .name = "Crypto Pair",
                  .type = epoch_core::MetaDataOptionType::String,
                  .defaultValue = MetaDataOptionDefinition(std::string("BTCUSD")),
                  .desc = "Cryptocurrency pair symbol (e.g., BTCUSD, ETHUSD, SOLUSD)"},
          },
      .desc = cryptoMetadata.description,
      .inputs = {},
      .outputs = outputs,
      .requiresTimeFrame = true,
      .requiredDataSources = {"CRYPTO:{ticker}:c", "CRYPTO:{ticker}:o", "CRYPTO:{ticker}:h", "CRYPTO:{ticker}:l"},
      .intradayOnly = false,
      .allowNullInputs = true,
      .strategyTypes = {"crypto-trading", "momentum", "correlation", "hedge"},
      .assetRequirements = {"single-asset", "multi-asset"},
      .usageContext =
          "Use this node to access historical data for any crypto pair by specifying its ticker. "
          "Useful for altcoins or custom trading pairs not in the common list.",
      .limitations =
          "Data availability depends on Polygon.io subscription level. "
          "Crypto markets trade 24/7 unlike traditional markets.",
  });

  return metadataList;
}

}  // namespace epoch_script::transform

#endif  // EPOCH_METADATA_REFERENCE_CRYPTO_METADATA_H
