#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <epoch_script/core/constants.h>
#include "test_helpers.h"

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;
using namespace investigation_test;

// Test to reproduce the null label issue in bar_chart_report
TEST_CASE("bar_chart_report with null labels", "[investigation][bar_chart]") {

  SECTION("Test bar_chart_report with null labels - should filter them out") {
    // Create a dataframe with null labels using helper
    auto index = make_date_range(0, 10);

    // First 5 labels are null, then alternating "HighDoC" and "LowDoC"
    std::vector<std::optional<std::string>> labels = {
      std::nullopt, std::nullopt, std::nullopt, std::nullopt, std::nullopt,
      "LowDoC", "HighDoC", "LowDoC", "HighDoC", "LowDoC"
    };
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};

    // Column names with # prefix to match GetInputId() format (node_id + "#" + handle)
    auto input = make_dataframe_with_nullable_strings(index, labels, values, "#label", "#value");

    INFO("Input dataframe created");
    INFO("Rows: " << input.size());

    // Try to create a bar_chart_report transform
    TransformConfiguration config = TransformConfiguration{
      TransformDefinition{YAML::Load(R"(
type: bar_chart_report
id: test_bar_chart
options:
  agg: count
  title: "Test Bar Chart"
  category: "Test"
  vertical: true
  x_axis_label: "Label"
  y_axis_label: "Count"
inputs:
  label: { type: ref, value: { node_id: "", handle: "label" } }
  value: { type: ref, value: { node_id: "", handle: "value" } }
outputs: []
timeframe:
  interval: 1
  type: day
)")}};

    auto transform = MAKE_TRANSFORM(config);

    // After the fix, this should now SUCCEED (null rows are dropped)
    INFO("Attempting to transform with null labels (should drop null rows)...");
    CHECK_NOTHROW(transform->TransformData(input));
    INFO("SUCCESS: bar_chart_report now handles null labels by dropping rows with nulls");
  }

  SECTION("Test bar_chart_report without nulls - should succeed") {
    // Create a dataframe WITHOUT null labels
    auto index = make_date_range(0, 10);

    // All labels are valid
    std::vector<std::optional<std::string>> labels = {
      "LowDoC", "HighDoC", "LowDoC", "HighDoC", "LowDoC",
      "LowDoC", "HighDoC", "LowDoC", "HighDoC", "LowDoC"
    };
    std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0};

    // Column names with # prefix to match GetInputId() format (node_id + "#" + handle)
    auto input = make_dataframe_with_nullable_strings(index, labels, values, "#label", "#value");

    INFO("Input dataframe created (no nulls)");

    // Create bar_chart_report transform
    TransformConfiguration config = TransformConfiguration{
      TransformDefinition{YAML::Load(R"(
type: bar_chart_report
id: test_bar_chart
options:
  agg: count
  title: "Test Bar Chart"
  category: "Test"
  vertical: true
  x_axis_label: "Label"
  y_axis_label: "Count"
inputs:
  label: { type: ref, value: { node_id: "", handle: "label" } }
  value: { type: ref, value: { node_id: "", handle: "value" } }
outputs: []
timeframe:
  interval: 1
  type: day
)")}};

    auto transform = MAKE_TRANSFORM(config);

    INFO("Attempting to transform with NO null labels...");
    // This should succeed
    CHECK_NOTHROW(transform->TransformData(input));
  }
}
