#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include "alias.h"

namespace epoch_script::transform {

// Function to create metadata for typed alias transforms
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeAliasMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  // AliasDecimal - compiler-inserted identity for Decimal/Number types
  metadataList.emplace_back(TransformsMetaData{
    .id = "alias_decimal",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Alias Decimal",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted column renamer for Decimal types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaDataConstants::NUMBER_OUTPUT_METADATA},
    .atLeastOneInputRequired = true,
    .tags = {"internal", "compiler", "identity"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "alias",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler for variable assignments.",
    .limitations = "Internal use only."
  });

  // AliasBoolean - compiler-inserted identity for Boolean types
  metadataList.emplace_back(TransformsMetaData{
    .id = "alias_boolean",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Alias Boolean",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted column renamer for Boolean types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaDataConstants::BOOLEAN_OUTPUT_METADATA},
    .atLeastOneInputRequired = true,
    .tags = {"internal", "compiler", "identity"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "alias",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler for variable assignments.",
    .limitations = "Internal use only."
  });

  // AliasString - compiler-inserted identity for String types
  metadataList.emplace_back(TransformsMetaData{
    .id = "alias_string",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Alias String",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted column renamer for String types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaDataConstants::STRING_OUTPUT_METADATA},
    .atLeastOneInputRequired = true,
    .tags = {"internal", "compiler", "identity"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "alias",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler for variable assignments.",
    .limitations = "Internal use only."
  });

  // AliasInteger - compiler-inserted identity for Integer types
  metadataList.emplace_back(TransformsMetaData{
    .id = "alias_integer",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Alias Integer",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted column renamer for Integer types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaDataConstants::INTEGER_OUTPUT_METADATA},
    .atLeastOneInputRequired = true,
    .tags = {"internal", "compiler", "identity"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "alias",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler for variable assignments.",
    .limitations = "Internal use only."
  });

  // AliasTimestamp - compiler-inserted identity for Timestamp types
  metadataList.emplace_back(TransformsMetaData{
    .id = "alias_timestamp",
    .category = epoch_core::TransformCategory::Utility,
    .name = "Alias Timestamp",
    .options = {},
    .isCrossSectional = false,
    .desc = "Compiler-inserted column renamer for Timestamp types.",
    .inputs = {IOMetaDataConstants::ANY_INPUT_METADATA},
    .outputs = {IOMetaData{epoch_core::IODataType::Timestamp, "result", ""}},
    .atLeastOneInputRequired = true,
    .tags = {"internal", "compiler", "identity"},
    .requiresTimeFrame = false,
    .allowNullInputs = true,
    .internalUse = true,
    .alias = "alias",
    .strategyTypes = {},
    .relatedTransforms = {},
    .assetRequirements = {"single-asset"},
    .usageContext = "Automatically inserted by compiler for variable assignments.",
    .limitations = "Internal use only."
  });

  return metadataList;
}

} // namespace epoch_script::transform
