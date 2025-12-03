#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include "static_cast.h"

namespace epoch_script::transform {

// Function to create metadata for static_cast transforms
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeStaticCastMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  // StaticCastToInteger - compiler-inserted Integer type materializer
  metadataList.emplace_back(TransformsMetaData{
    .id = "static_cast_to_integer",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Static Cast To Integer",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted type materializer for Integer types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaDataConstants::INTEGER_OUTPUT_METADATA},
    .atLeastOneInputRequired = false,
    .tags = {"internal", "compiler", "type-system"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "static_cast",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler during type resolution.",
    .limitations = "Internal use only."
  });

  // StaticCastToDecimal - compiler-inserted Decimal type materializer
  metadataList.emplace_back(TransformsMetaData{
    .id = "static_cast_to_decimal",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Static Cast To Decimal",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted type materializer for Decimal types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaDataConstants::NUMBER_OUTPUT_METADATA},
    .atLeastOneInputRequired = false,
    .tags = {"internal", "compiler", "type-system"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "static_cast",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler during type resolution.",
    .limitations = "Internal use only."
  });

  // StaticCastToBoolean - compiler-inserted Boolean type materializer
  metadataList.emplace_back(TransformsMetaData{
    .id = "static_cast_to_boolean",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Static Cast To Boolean",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted type materializer for Boolean types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaDataConstants::BOOLEAN_OUTPUT_METADATA},
    .atLeastOneInputRequired = false,
    .tags = {"internal", "compiler", "type-system"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "static_cast",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler during type resolution.",
    .limitations = "Internal use only."
  });

  // StaticCastToString - compiler-inserted String type materializer
  metadataList.emplace_back(TransformsMetaData{
    .id = "static_cast_to_string",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Static Cast To String",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted type materializer for String types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaDataConstants::STRING_OUTPUT_METADATA},
    .atLeastOneInputRequired = false,
    .tags = {"internal", "compiler", "type-system"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "static_cast",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler during type resolution.",
    .limitations = "Internal use only."
  });

  // StaticCastToTimestamp - compiler-inserted Timestamp type materializer
  metadataList.emplace_back(TransformsMetaData{
    .id = "static_cast_to_timestamp",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Static Cast To Timestamp",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted type materializer for Timestamp types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaData{epoch_core::IODataType::Timestamp, "result", ""}},
    .atLeastOneInputRequired = false,
    .tags = {"internal", "compiler", "type-system"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "static_cast",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler during type resolution.",
    .limitations = "Internal use only."
  });

  return metadataList;
}

} // namespace epoch_script::transform
