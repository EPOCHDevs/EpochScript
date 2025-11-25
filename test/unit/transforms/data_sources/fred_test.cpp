//
// Created by Claude Code for FRED Transform Testing
//

#include <algorithm>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/data_sources/parametric_data_source.h"
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/index_factory.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

TEST_CASE("FRED Metadata is correctly registered", "[fred][metadata]") {
  auto &registry = transforms::ITransformRegistry::GetInstance();

  SECTION("economic_indicator transform is registered") {
    REQUIRE(registry.IsValid("economic_indicator"));

    auto metadataOpt = registry.GetMetaData("economic_indicator");
    REQUIRE(metadataOpt.has_value());

    auto metadata = metadataOpt.value().get();

    SECTION("Basic metadata properties") {
      REQUIRE(metadata.id == "economic_indicator");
      REQUIRE(metadata.name == "Economic Indicator");
      REQUIRE(metadata.category == epoch_core::TransformCategory::DataSource);
      REQUIRE(metadata.plotKind == epoch_core::TransformPlotKind::flag);
      REQUIRE(metadata.isCrossSectional == false);
      REQUIRE(metadata.atLeastOneInputRequired == false);
    }

    SECTION("No date options are exposed to users") {
      // Verify that from_date, to_date, published_from options don't exist
      bool hasDateOptions = false;
      for (const auto& option : metadata.options) {
        if (option.id == "from_date" || option.id == "to_date" ||
            option.id == "published_from" || option.id == "published_to") {
          hasDateOptions = true;
          break;
        }
      }
      REQUIRE_FALSE(hasDateOptions);
    }

    SECTION("Has category SelectOption") {
      REQUIRE(metadata.options.size() == 1);
      auto categoryOption = metadata.options[0];
      REQUIRE(categoryOption.id == "category");
      REQUIRE(categoryOption.name == "Economic Indicator");
      REQUIRE(categoryOption.type == epoch_core::MetaDataOptionType::Select);
      REQUIRE(categoryOption.selectOption.size() > 0);

      // Verify some key economic indicators are present
      auto& selectOptions = categoryOption.selectOption;

      // selectOption is a vector of SelectOption structs (name=display, value=enum)
      auto hasOption = [&selectOptions](const std::string& optionValue) {
        return std::any_of(selectOptions.begin(), selectOptions.end(),
                          [&optionValue](const auto& option) { return option.value == optionValue; });
      };

      REQUIRE(hasOption("CPI"));
      REQUIRE(hasOption("FedFunds"));
      REQUIRE(hasOption("Unemployment"));
      REQUIRE(hasOption("GDP"));
    }

    SECTION("Required data sources includes ALFRED fields") {
      REQUIRE(metadata.requiresTimeFrame == true);
      // 3 requiredDataSources with ECON: prefix and {category} template pattern
      REQUIRE(metadata.requiredDataSources.size() == 3);

      // Verify the core fields are present with template pattern: ECON:{category}:field
      REQUIRE(std::find(metadata.requiredDataSources.begin(), metadata.requiredDataSources.end(), "ECON:{category}:observation_date") != metadata.requiredDataSources.end());
      REQUIRE(std::find(metadata.requiredDataSources.begin(), metadata.requiredDataSources.end(), "ECON:{category}:value") != metadata.requiredDataSources.end());
      REQUIRE(std::find(metadata.requiredDataSources.begin(), metadata.requiredDataSources.end(), "ECON:{category}:revision") != metadata.requiredDataSources.end());
    }

    SECTION("Output columns are correct") {
      REQUIRE(metadata.inputs.empty());
      // SDK returns 3 outputs: observation_date, value, revision
      REQUIRE(metadata.outputs.size() == 3);

      // Verify observation_date output
      auto observationDateOutput = metadata.outputs[0];
      REQUIRE(observationDateOutput.id == "observation_date");
      REQUIRE(observationDateOutput.name == "Observation Date");
      REQUIRE(observationDateOutput.type == epoch_core::IODataType::Timestamp);

      // Verify value output
      auto valueOutput = metadata.outputs[1];
      REQUIRE(valueOutput.id == "value");
      REQUIRE(valueOutput.name == "Value");
      REQUIRE(valueOutput.type == epoch_core::IODataType::Decimal);

      // Verify revision output exists
      auto revisionOutput = metadata.outputs[2];
      REQUIRE(revisionOutput.id == "revision");
      REQUIRE(revisionOutput.name == "Revision Number");
      REQUIRE(revisionOutput.type == epoch_core::IODataType::Integer);
    }

    SECTION("Has appropriate tags") {
      auto& tags = metadata.tags;
      REQUIRE(std::find(tags.begin(), tags.end(), "fred") != tags.end());
      REQUIRE(std::find(tags.begin(), tags.end(), "macro") != tags.end());
      REQUIRE(std::find(tags.begin(), tags.end(), "economic-indicators") != tags.end());
    }

    SECTION("Usage context describes publication events") {
      REQUIRE(metadata.usageContext.find("publication events") != std::string::npos);
      REQUIRE(metadata.usageContext.find("auto-derived") != std::string::npos);
    }

    SECTION("Limitations describe sparse data behavior") {
      REQUIRE(metadata.limitations.find("ONLY on publication dates") != std::string::npos);
      REQUIRE(metadata.limitations.find("not forward-filled") != std::string::npos);
      REQUIRE(metadata.limitations.find("ALFRED") != std::string::npos);
    }
  }
}

TEST_CASE("FREDTransform can be created", "[fred][transform]") {
  SECTION("Transform can be created with different categories") {
    std::vector<std::string> categories = {"CPI", "CorePCE", "FedFunds", "Unemployment", "GDP"};

    for (const auto& category : categories) {
      TransformConfiguration config = TransformConfiguration{
          TransformDefinition{YAML::Load(std::format(R"(
type: economic_indicator
id: {}_data
options:
  category: {}
timeframe: 1d
)", category, category))}};

      auto transformBase = MAKE_TRANSFORM(config);
      REQUIRE(transformBase != nullptr);

      auto transform = dynamic_cast<FREDTransform*>(transformBase.get());
      REQUIRE(transform != nullptr);
    }
  }
}

TEST_CASE("FREDTransform configuration", "[fred][transform]") {
  SECTION("Transform can be created with valid config") {
    TransformConfiguration config = TransformConfiguration{
        TransformDefinition{YAML::Load(R"(
type: economic_indicator
id: test_fred
options:
  category: CorePCE
timeframe: 1d
)")}};

    auto transformBase = MAKE_TRANSFORM(config);
    REQUIRE(transformBase != nullptr);

    auto transform = dynamic_cast<FREDTransform*>(transformBase.get());
    REQUIRE(transform != nullptr);
  }

  SECTION("Output IDs are correctly configured") {
    TransformConfiguration config = TransformConfiguration{
        TransformDefinition{YAML::Load(R"(
type: economic_indicator
id: fed_funds_test
options:
  category: FedFunds
timeframe: 1d
)")}};

    // Verify output IDs follow naming convention
    auto observationDateId = config.GetOutputId("observation_date").GetColumnName();
    auto valueId = config.GetOutputId("value").GetColumnName();

    REQUIRE_FALSE(observationDateId.empty());
    REQUIRE_FALSE(valueId.empty());
    REQUIRE(observationDateId != valueId);
  }
}
