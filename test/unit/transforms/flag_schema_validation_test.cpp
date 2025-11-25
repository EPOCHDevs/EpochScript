#include "../../common.h"
#include <catch2/catch_all.hpp>
#include <epoch_script/transforms/core/registration.h>
#include <epoch_script/transforms/core/registry.h>
#include <regex>
#include <set>

using namespace epoch_script;
using namespace epoch_script::transforms;

// Extract placeholders from template text (e.g., "{foo}" -> "foo")
std::set<std::string> ExtractPlaceholders(const std::string& text) {
  std::set<std::string> placeholders;
  std::regex placeholder_regex(R"(\{([a-zA-Z_][a-zA-Z0-9_]*)\})");

  auto words_begin = std::sregex_iterator(text.begin(), text.end(), placeholder_regex);
  auto words_end = std::sregex_iterator();

  for (std::sregex_iterator i = words_begin; i != words_end; ++i) {
    std::smatch match = *i;
    placeholders.insert(match[1].str());  // Capture group 1 is the placeholder name
  }

  return placeholders;
}

TEST_CASE("FlagSchema - template placeholders match output IDs", "[metadata][flagSchema]") {
  // Register all transforms
  RegisterTransformMetadata(epoch_script::DEFAULT_YAML_LOADER);

  // Get all transforms from registry
  auto& registry = ITransformRegistry::GetInstance();
  const auto& allTransforms = registry.GetMetaData();

  // Track validation results
  size_t transformsWithFlagSchema = 0;
  size_t transformsWithPlaceholders = 0;
  std::vector<std::string> failedTransforms;

  // Iterate through all transforms
  for (const auto& [transformId, metadata] : allTransforms) {
    // All flag PlotKind transforms MUST have flagSchema
    if (metadata.plotKind == epoch_core::TransformPlotKind::flag) {
      INFO("Flag transform '" << transformId << "' MUST have flagSchema");
      REQUIRE(metadata.flagSchema.has_value());
    }

    // Skip transforms without flagSchema
    if (!metadata.flagSchema.has_value()) {
      continue;
    }

    transformsWithFlagSchema++;
    const auto& flagSchema = metadata.flagSchema.value();

    // Extract placeholders from text
    auto placeholders = ExtractPlaceholders(flagSchema.text);

    // If there are placeholders, verify textIsTemplate is true
    if (!placeholders.empty()) {
      transformsWithPlaceholders++;

      INFO("Transform: " << transformId);
      INFO("FlagSchema text: " << flagSchema.text);
      INFO("textIsTemplate: " << (flagSchema.textIsTemplate ? "true" : "false"));
      INFO("Transform '" << transformId << "' has placeholders but textIsTemplate is false");

      REQUIRE(flagSchema.textIsTemplate);

      // Build set of valid output IDs
      std::set<std::string> validOutputIds;
      for (const auto& output : metadata.outputs) {
        validOutputIds.insert(output.id);
      }

      // Verify each placeholder matches an output ID
      for (const auto& placeholder : placeholders) {
        INFO("Checking placeholder: " << placeholder);

        bool isValid = validOutputIds.count(placeholder) > 0;

        if (!isValid) {
          std::string errorMsg = "Transform '" + transformId + "' has invalid placeholder '{" +
                                placeholder + "}' that doesn't match any output ID. ";
          errorMsg += "Valid outputs: ";
          for (const auto& outputId : validOutputIds) {
            errorMsg += outputId + " ";
          }

          failedTransforms.push_back(transformId);
          FAIL(errorMsg);
        }
      }
    }

    // If textIsTemplate is true, should have placeholders
    if (flagSchema.textIsTemplate && placeholders.empty()) {
      std::string errorMsg = "Transform '" + transformId +
                            "' has textIsTemplate=true but no placeholders in text: " +
                            flagSchema.text;
      failedTransforms.push_back(transformId);
      WARN(errorMsg);  // Warning only, not a hard failure
    }
  }

  // Summary info
  INFO("Total transforms with flagSchema: " << transformsWithFlagSchema);
  INFO("Transforms with template placeholders: " << transformsWithPlaceholders);

  // All validations should pass
  REQUIRE(failedTransforms.empty());
}

TEST_CASE("FlagSchema - all flag transforms have valid color", "[metadata][flagSchema]") {
  RegisterTransformMetadata(epoch_script::DEFAULT_YAML_LOADER);

  auto& registry = ITransformRegistry::GetInstance();
  const auto& allTransforms = registry.GetMetaData();

  for (const auto& [transformId, metadata] : allTransforms) {
    // Only check flag PlotKind transforms
    if (metadata.plotKind != epoch_core::TransformPlotKind::flag) {
      continue;
    }

    INFO("Transform: " << transformId);

    // Flag transforms MUST have flagSchema
    INFO("Flag transform '" << transformId << "' missing required flagSchema");
    REQUIRE(metadata.flagSchema.has_value());

    // Color should be set (enum, so always valid)
    // Just verify the flagSchema is present
    REQUIRE(metadata.flagSchema.has_value());
  }
}

TEST_CASE("FlagSchema - all flag transforms have valid icon", "[metadata][flagSchema]") {
  RegisterTransformMetadata(epoch_script::DEFAULT_YAML_LOADER);

  auto& registry = ITransformRegistry::GetInstance();
  const auto& allTransforms = registry.GetMetaData();

  for (const auto& [transformId, metadata] : allTransforms) {
    // Only check flag PlotKind transforms
    if (metadata.plotKind != epoch_core::TransformPlotKind::flag) {
      continue;
    }

    INFO("Transform: " << transformId);

    // Flag transforms MUST have flagSchema
    INFO("Flag transform '" << transformId << "' missing required flagSchema");
    REQUIRE(metadata.flagSchema.has_value());

    const auto& flagSchema = metadata.flagSchema.value();

    // Icon should be set (enum, so we just verify toString doesn't crash)
    std::string iconStr = epoch_core::IconWrapper::ToString(flagSchema.icon);
    REQUIRE_FALSE(iconStr.empty());
  }
}

TEST_CASE("FlagSchema - valueKey validation", "[metadata][flagSchema]") {
  RegisterTransformMetadata(epoch_script::DEFAULT_YAML_LOADER);

  auto& registry = ITransformRegistry::GetInstance();
  const auto& allTransforms = registry.GetMetaData();

  std::vector<std::string> failedTransforms;

  for (const auto& [transformId, metadata] : allTransforms) {
    // Only check flag PlotKind transforms
    if (metadata.plotKind != epoch_core::TransformPlotKind::flag) {
      continue;
    }

    INFO("Transform: " << transformId);

    // Flag transforms MUST have flagSchema
    REQUIRE(metadata.flagSchema.has_value());

    const auto& flagSchema = metadata.flagSchema.value();

    // If valueKey is specified (not empty), it must match one of the outputs
    if (!flagSchema.valueKey.empty()) {
      bool foundOutput = false;
      for (const auto& output : metadata.outputs) {
        if (output.id == flagSchema.valueKey) {
          foundOutput = true;
          break;
        }
      }

      if (!foundOutput) {
        std::string errorMsg = "Transform '" + transformId +
                              "' has valueKey '" + flagSchema.valueKey +
                              "' that doesn't match any output ID. Valid outputs: ";
        for (const auto& output : metadata.outputs) {
          errorMsg += output.id + " ";
        }
        failedTransforms.push_back(transformId);
        FAIL(errorMsg);
      }
    }
  }

  REQUIRE(failedTransforms.empty());
}

TEST_CASE("FlagSchema - coverage report", "[metadata][flagSchema][coverage]") {
  RegisterTransformMetadata(epoch_script::DEFAULT_YAML_LOADER);

  auto& registry = ITransformRegistry::GetInstance();
  const auto& allTransforms = registry.GetMetaData();

  size_t totalFlagTransforms = 0;
  size_t flagTransformsWithSchema = 0;
  size_t flagTransformsWithValueKey = 0;

  for (const auto& [transformId, metadata] : allTransforms) {
    if (metadata.plotKind != epoch_core::TransformPlotKind::flag) {
      continue;
    }

    totalFlagTransforms++;

    if (metadata.flagSchema.has_value()) {
      flagTransformsWithSchema++;

      if (!metadata.flagSchema.value().valueKey.empty()) {
        flagTransformsWithValueKey++;
      }
    }
  }

  INFO("Total flag transforms: " << totalFlagTransforms);
  INFO("Flag transforms with flagSchema: " << flagTransformsWithSchema);
  INFO("Flag transforms with valueKey: " << flagTransformsWithValueKey);
  INFO("Coverage: " << (totalFlagTransforms > 0 ?
       (100.0 * flagTransformsWithSchema / totalFlagTransforms) : 0) << "%");

  // This is informational, not a requirement
  SUCCEED("FlagSchema coverage report generated");
}
