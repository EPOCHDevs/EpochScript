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
inline constexpr auto ASSET_REF_PASSTHROUGH_DATETIME_ID = "asset_ref_passthrough_datetime";

// Helper to check if a transform type is an asset_ref_passthrough
inline bool IsAssetRefPassthroughType(const std::string& transformType) {
  return transformType == ASSET_REF_PASSTHROUGH_ID ||
         transformType == ASSET_REF_PASSTHROUGH_BOOL_ID ||
         transformType == ASSET_REF_PASSTHROUGH_STRING_ID ||
         transformType == ASSET_REF_PASSTHROUGH_DATETIME_ID;
}

// Function to create metadata for all asset_ref_passthrough specializations
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeAssetRefPassthroughMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  // Common options for all specializations
  auto commonOptions = std::vector<MetaDataOption>{
    {.id = "ticker",
     .name = "Ticker Filter",
     .type = epoch_core::MetaDataOptionType::String,
     .isRequired = false,
     .desc = "Filter by exact ticker match (e.g., 'SPY'). Empty matches all."},
    {.id = "asset_class",
     .name = "Asset Class",
     .type = epoch_core::MetaDataOptionType::String,
     .isRequired = false,
     .desc = "Filter by asset class (Stocks, Crypto, Futures, FX, Indices)."},
    {.id = "sector",
     .name = "Sector",
     .type = epoch_core::MetaDataOptionType::String,
     .isRequired = false,
     .desc = "Filter by sector."}
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
       .id = "passthrough",
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
       .id = "passthrough",
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
       .id = "passthrough",
       .name = "Filtered Output"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"utility", "filter", "passthrough", "asset-ref"},
    .requiresTimeFrame = false,
    .allowNullInputs = false
  });

  // ============================================================================
  // asset_ref_passthrough_datetime (DateTime)
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = ASSET_REF_PASSTHROUGH_DATETIME_ID,
    .category = epoch_core::TransformCategory::Utility,
    .name = "Asset Reference Passthrough (DateTime)",
    .options = commonOptions,
    .isCrossSectional = false,
    .desc = "Filter datetime data by asset matching criteria.",
    .inputs = {
      {.type = epoch_core::IODataType::DateTime,
       .id = "SLOT",
       .name = "Input Series"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::DateTime,
       .id = "passthrough",
       .name = "Filtered Output"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"utility", "filter", "passthrough", "asset-ref"},
    .requiresTimeFrame = false,
    .allowNullInputs = false
  });

  return metadataList;
}

} // namespace epoch_script::transform
