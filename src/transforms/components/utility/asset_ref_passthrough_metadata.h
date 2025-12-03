//
// Asset Reference Passthrough Metadata
// Provides metadata for asset_ref_passthrough transforms (all datatype specializations)
//

#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transform {

// Transform IDs for asset_ref_passthrough specializations
inline constexpr auto ASSET_REF_PASSTHROUGH_ID = "asset_ref_passthrough";
inline constexpr auto ASSET_REF_PASSTHROUGH_BOOL_ID = "asset_ref_passthrough_bool";
inline constexpr auto ASSET_REF_PASSTHROUGH_STRING_ID = "asset_ref_passthrough_string";
inline constexpr auto ASSET_REF_PASSTHROUGH_TIMESTAMP_ID = "asset_ref_passthrough_timestamp";

// Transform ID for is_asset_ref (boolean asset matcher)
inline constexpr auto IS_ASSET_REF_ID = "is_asset_ref";

// Helper to check if a transform type is an asset_ref_passthrough
inline bool IsAssetRefPassthroughType(const std::string& transformType) {
  return transformType == ASSET_REF_PASSTHROUGH_ID ||
         transformType == ASSET_REF_PASSTHROUGH_BOOL_ID ||
         transformType == ASSET_REF_PASSTHROUGH_STRING_ID ||
         transformType == ASSET_REF_PASSTHROUGH_TIMESTAMP_ID;
}

// Helper to check if a transform type is is_asset_ref
inline bool IsAssetRefType(const std::string& transformType) {
  return transformType == IS_ASSET_REF_ID;
}

// Function to create metadata for all asset_ref_passthrough specializations
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeAssetRefPassthroughMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  // Common options for all specializations - all optional, evaluated at runtime using AssetSpecification
  auto commonOptions = std::vector<MetaDataOption>{
    {.id = "ticker",
     .name = "Ticker Filter",
     .type = epoch_core::MetaDataOptionType::String,
     .isRequired = false,
     .desc = "Filter by exact ticker match (e.g., 'SPY'). Empty matches all."},
    {.id = "asset_class",
     .name = "Asset Class",
     .type = epoch_core::MetaDataOptionType::Select,
     .isRequired = false,
     .selectOption = {{"Stocks", "Stocks"}, {"Crypto", "Crypto"}, {"FX", "FX"}},
     .desc = "Filter by asset class (Stocks, Crypto, FX)."},
    {.id = "sector",
     .name = "Sector",
     .type = epoch_core::MetaDataOptionType::String,
     .isRequired = false,
     .desc = "Filter by sector (e.g., 'Technology', 'Healthcare')."},
    {.id = "industry",
     .name = "Industry",
     .type = epoch_core::MetaDataOptionType::String,
     .isRequired = false,
     .desc = "Filter by industry (e.g., 'Software', 'Semiconductors')."},
    {.id = "base_currency",
     .name = "Base Currency",
     .type = epoch_core::MetaDataOptionType::String,
     .isRequired = false,
     .desc = "Filter FX pairs by base currency (e.g., 'EUR' matches EURUSD, EURGBP)."},
    {.id = "counter_currency",
     .name = "Counter Currency",
     .type = epoch_core::MetaDataOptionType::String,
     .isRequired = false,
     .desc = "Filter FX pairs by counter/quote currency (e.g., 'USD' matches EURUSD, GBPUSD)."}
  };

  // ============================================================================
  // asset_ref_passthrough (Number - default)
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = ASSET_REF_PASSTHROUGH_ID,
    .category = epoch_core::TransformCategory::Utility,
    .name = "Asset Reference Passthrough",
    .options = commonOptions,
    .isCrossSectional = false,
    .desc = "Filter numeric data by asset matching criteria. "
            "For matching assets, input is passed through unchanged. "
            "For non-matching assets, no output is stored (skipped entirely). "
            "Use for pairs trading (get specific asset's data) or universe filtering.",
    .inputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "SLOT",
       .name = "Input Series"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "result",
       .name = "Filtered Output"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"utility", "filter", "passthrough", "asset-ref", "pairs-trading"},
    .requiresTimeFrame = false,
    .allowNullInputs = false,
    .strategyTypes = {"pairs_trading", "universe_filter"},
    .relatedTransforms = {},
    .assetRequirements = {"any"},
    .usageContext = "Use to filter data to specific assets. For pairs trading, "
                    "use to get counterpart asset's data. For universe filtering, "
                    "use to restrict strategy to subset of assets.",
    .limitations = "Non-matching assets produce no output (may affect downstream transforms)."
  });

  // ============================================================================
  // asset_ref_passthrough_bool (Boolean)
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = ASSET_REF_PASSTHROUGH_BOOL_ID,
    .category = epoch_core::TransformCategory::Utility,
    .name = "Asset Reference Passthrough (Boolean)",
    .options = commonOptions,
    .isCrossSectional = false,
    .desc = "Filter boolean data by asset matching criteria.",
    .inputs = {
      {.type = epoch_core::IODataType::Boolean,
       .id = "SLOT",
       .name = "Input Series"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::Boolean,
       .id = "result",
       .name = "Filtered Output"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"utility", "filter", "passthrough", "asset-ref"},
    .requiresTimeFrame = false,
    .allowNullInputs = false
  });

  // ============================================================================
  // asset_ref_passthrough_string (String)
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = ASSET_REF_PASSTHROUGH_STRING_ID,
    .category = epoch_core::TransformCategory::Utility,
    .name = "Asset Reference Passthrough (String)",
    .options = commonOptions,
    .isCrossSectional = false,
    .desc = "Filter string data by asset matching criteria.",
    .inputs = {
      {.type = epoch_core::IODataType::String,
       .id = "SLOT",
       .name = "Input Series"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::String,
       .id = "result",
       .name = "Filtered Output"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"utility", "filter", "passthrough", "asset-ref"},
    .requiresTimeFrame = false,
    .allowNullInputs = false
  });

  // ============================================================================
  // asset_ref_passthrough_timestamp (Timestamp)
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = ASSET_REF_PASSTHROUGH_TIMESTAMP_ID,
    .category = epoch_core::TransformCategory::Utility,
    .name = "Asset Reference Passthrough (Timestamp)",
    .options = commonOptions,
    .isCrossSectional = false,
    .desc = "Filter timestamp data by asset matching criteria.",
    .inputs = {
      {.type = epoch_core::IODataType::Timestamp,
       .id = "SLOT",
       .name = "Input Series"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::Timestamp,
       .id = "result",
       .name = "Filtered Output"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"utility", "filter", "passthrough", "asset-ref"},
    .requiresTimeFrame = false,
    .allowNullInputs = false
  });

  // ============================================================================
  // is_asset_ref - Boolean asset matcher (runtime per-asset scalar)
  // Note: NOT TransformCategory::Scalar - that triggers compile-time inlining
  // This is a runtime scalar where value depends on current asset
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = IS_ASSET_REF_ID,
    .category = epoch_core::TransformCategory::Utility,
    .name = "Is Asset Reference",
    .options = commonOptions,  // Same options as asset_ref_passthrough
    .isCrossSectional = false,
    .desc = "Returns a boolean scalar indicating if current asset matches the filter criteria. "
            "For matching assets, outputs true. For non-matching assets, outputs false. "
            "This is a scalar transform - the value is known at construction time. "
            "Use for conditional logic in pairs trading (e.g., buy AAPL when condition, sell others).",
    .inputs = {},  // No inputs - this is a scalar
    .outputs = {
      {.type = epoch_core::IODataType::Boolean,
       .id = "result",
       .name = "Is Matching Asset"}
    },
    .atLeastOneInputRequired = false,
    .tags = {"utility", "filter", "asset-ref", "pairs-trading", "conditional", "scalar"},
    .requiresTimeFrame = false,
    .allowNullInputs = false,
    .strategyTypes = {"pairs_trading", "statistical_arbitrage"},
    .relatedTransforms = {"asset_ref_passthrough"},
    .assetRequirements = {"any"},
    .usageContext = "Use for conditional logic based on asset identity. "
                    "In pairs trading: use to create different signals for different assets. "
                    "e.g., is_asset_ref(ticker='AAPL')() returns true for AAPL, false for others.",
    .limitations = "Only matches by ticker. For complex asset filtering, use multiple is_asset_ref transforms."
  });

  return metadataList;
}

} // namespace epoch_script::transform
