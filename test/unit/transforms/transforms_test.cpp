
#include "epoch_script/strategy/registration.h"
#include <catch.hpp>
#include <epoch_script/core/constants.h>
#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/transforms/core/registration.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_script/transforms/core/config_helper.h>
#include <unordered_map>

using namespace epoch_script;
using namespace epoch_script::transform::input_value_literals;
using Catch::Approx;

TEST_CASE("Transform Definition") {

  SECTION("TransformDefinition Constructor and Basic Methods") {
    // Create metadata to avoid registry lookup
    epoch_script::transforms::TransformsMetaData metadata;
    metadata.id = "example_type";

    TransformDefinitionData data{
        .type = "example_type",
        .id = "1234",
        .options = {},
        .timeframe = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY,
        .inputs = {{"input1", {strategy::InputValue(strategy::NodeReference("src", "close"))}}},
        .metaData = metadata};

    TransformDefinition transform(data);

    SECTION("Constructor initializes correctly") {
      REQUIRE(transform.GetType() == "example_type");
      REQUIRE(transform.GetId() == "1234");
      REQUIRE(transform.GetTimeframe().ToString() == "1D");
      const auto& inputs = transform.GetInputs().at("input1");
      REQUIRE(inputs.size() == 1);
      REQUIRE(inputs[0].IsNodeReference());
      REQUIRE(inputs[0].GetNodeReference() == strategy::NodeReference("src", "close"));
    }

    SECTION("SetOption updates options correctly") {
      transform.SetOption("key1",
                          epoch_script::MetaDataOptionDefinition{3.14});
      REQUIRE(epoch_script::MetaDataOptionDefinition{
                  transform.GetOptions().at("key1")}
                  .GetDecimal() == Approx(3.14));

      transform.SetOption("key2",
                          epoch_script::MetaDataOptionDefinition{42.0});
      REQUIRE(epoch_script::MetaDataOptionDefinition{
                  transform.GetOptions().at("key2")}
                  .GetInteger() == Approx(42));
    }

    SECTION("SetPeriod and SetPeriods") {
      transform.SetPeriod(10);
      REQUIRE(epoch_script::MetaDataOptionDefinition{
                  transform.GetOptions().at("period")}
                  .GetInteger() == Approx(10));

      transform.SetPeriods(20);
      REQUIRE(epoch_script::MetaDataOptionDefinition{
                  transform.GetOptions().at("periods")}
                  .GetInteger() == Approx(20));
    }

    SECTION("SetType methods") {
      transform.SetType("new_type");
      REQUIRE(transform.GetType() == "new_type");

      TransformDefinition copy = transform.SetTypeCopy("copied_type");
      REQUIRE(copy.GetType() == "copied_type");
      REQUIRE(transform.GetType() == "new_type");

      transform.SetTypeIfEmpty("should_not_change");
      REQUIRE(transform.GetType() == "new_type");
    }

    SECTION("SetInput creates a copy with new inputs") {
      InputMapping newInputs = {{"new_input", {strategy::InputValue(strategy::NodeReference("data", "high"))}}};
      TransformDefinition copy = transform.SetInput(newInputs);
      const auto& copyInputs = copy.GetInputs().at("new_input");
      REQUIRE(copyInputs.size() == 1);
      REQUIRE(copyInputs[0].IsNodeReference());
      REQUIRE(copyInputs[0].GetNodeReference() == strategy::NodeReference("data", "high"));

      const auto& origInputs = transform.GetInputs().at("input1");
      REQUIRE(origInputs.size() == 1);
      REQUIRE(origInputs[0].IsNodeReference());
      REQUIRE(origInputs[0].GetNodeReference() == strategy::NodeReference("src", "close"));
    }

    SECTION("GetOptionAsDouble with and without fallback") {
      transform.SetOption("double_key",
                          epoch_script::MetaDataOptionDefinition{7.5});
      REQUIRE(transform.GetOptionAsDouble("double_key") == Approx(7.5));
      REQUIRE(transform.GetOptionAsDouble("missing_key", 1.5) == Approx(1.5));
    }
  }

  SECTION("TransformDefinition Constructor with Descriptor") {
    YAML::Node node;
    node["id"] = "1234";
    node["tag"] = "example_tag";
    node["type"] = "sma";
    node["timeframe"]["interval"] = 1;
    node["timeframe"]["type"] = "day";
    node["options"]["period"] = 5;

    // Use InputValue YAML format with type and value map (single input, not sequence)
    YAML::Node inputNode;
    inputNode["type"] = "ref";
    YAML::Node valueNode;
    valueNode["node_id"] = "data";
    valueNode["handle"] = "close";
    inputNode["value"] = valueNode;
    node["inputs"]["SLOT"] = inputNode;  // Single input, not push_back

    TransformDefinition transform(node);

    SECTION("Constructor initializes correctly from descriptor") {
      REQUIRE(transform.GetType() == "sma");
      REQUIRE(transform.GetId() == "1234");
      REQUIRE(transform.GetTimeframe().ToString() == "1D");
      const auto& inputs = transform.GetInputs().at("SLOT");
      REQUIRE(inputs.size() == 1);
      REQUIRE(inputs[0].IsNodeReference());
      REQUIRE(inputs[0].GetNodeReference() == strategy::NodeReference("data", "close"));
    }
  }
}
