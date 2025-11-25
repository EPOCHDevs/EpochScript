#include <catch.hpp>
#include <epoch_script/transforms/components/event_markers/event_marker.h>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/registration.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/core/metadata_options.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <glaze/glaze.hpp>
#include <sstream>

#include <epoch_script/transforms/core/registry.h>

using namespace epoch_script;
using namespace epoch_script::transform;
using namespace epoch_frame;
using Catch::Approx;

// Helper to create a sample DataFrame for testing
DataFrame CreateTestDataFrame() {
  // Create a simple DataFrame with columns for testing
  // Timestamps in nanoseconds (original milliseconds * 1,000,000)
  std::vector<int64_t> timestamps = {1609459200000000000LL, 1609545600000000000LL, 1609632000000000000LL, 1609718400000000000LL};

  // Create index from timestamps
  auto index = factory::index::make_datetime_index(timestamps, "index","UTC");

  // Create columns with node#column format
  std::vector<arrow::ChunkedArrayPtr> columns = {
    factory::array::make_array(std::vector<std::string>{"BUY", "SELL", "BUY", "SELL"}),
    factory::array::make_array(std::vector<double>{10.5, -5.2, 15.3, -8.1}),
    factory::array::make_array(std::vector<bool>{true, true, false, true})
  };

  std::vector<std::string> fields = {"node#direction", "node#profit_pct", "node#is_signal"};

  return make_dataframe(index, columns, fields);
}

TEST_CASE("EventMarker - Basic Functionality", "[event_markers][event_marker]") {

  SECTION("Card selector filter metadata is correctly registered") {
    auto& registry = transforms::ITransformRegistry::GetInstance();
    auto metadataMap = registry.GetMetaData();

    REQUIRE(metadataMap.contains("event_marker"));
    auto metadata = metadataMap.at("event_marker");

    REQUIRE(metadata.id == "event_marker");
    REQUIRE(metadata.name == "Event Marker");
    REQUIRE(metadata.category == epoch_core::TransformCategory::EventMarker);
    REQUIRE(metadata.atLeastOneInputRequired == true);
    REQUIRE(metadata.outputs.empty());  // Selectors don't output to graph
  }


  SECTION("Card selector filter has required schema option") {
    auto& registry = transforms::ITransformRegistry::GetInstance();
    auto metadataMap = registry.GetMetaData();

    REQUIRE(metadataMap.contains("event_marker"));
    auto metadata = metadataMap.at("event_marker");

    bool hasCardSchemaOption = false;
    for (const auto& option : metadata.options) {
      if (option.id == "schema") {
        hasCardSchemaOption = true;
        REQUIRE(option.isRequired == true);
        REQUIRE(option.type == epoch_core::MetaDataOptionType::EventMarkerSchema);
      }
    }
    REQUIRE(hasCardSchemaOption);
  }

}

TEST_CASE("EventMarkerSchema - JSON Parsing", "[event_markers][event_marker]") {

  SECTION("Parse schema with select_key") {
    std::string schemaJson = R"({
      "title": "Trade Signals",
      "select_key": "is_signal",
      "icon": "Info",
      "schemas": [
        {
          "column_id": "direction",
          "slot": "PrimaryBadge",
          "render_type": "Badge",
          "color_map": {
            "Success": ["BUY"],
            "Error": ["SELL"]
          }
        },
        {
          "column_id": "profit_pct",
          "slot": "Hero",
          "render_type": "Decimal",
          "color_map": {}
        },
        {
          "column_id": "timestamp",
          "slot": "Footer",
          "render_type": "Timestamp",
          "color_map": {}
        }
      ]
    })";

    EventMarkerSchema schema;
    auto error = glz::read_json(schema, schemaJson);

    if (error) {
      FAIL(glz::format_error(error, schemaJson));
    }
    REQUIRE(schema.title == "Trade Signals");
    REQUIRE(schema.select_key == "is_signal");
    REQUIRE(schema.schemas.size() == 3);

    // Verify first schema (direction badge)
    REQUIRE(schema.schemas[0].column_id == "direction");
    REQUIRE(schema.schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
    REQUIRE(schema.schemas[0].render_type == epoch_core::CardRenderType::Badge);
    REQUIRE(schema.schemas[0].color_map.size() == 2);

    // Verify color mappings
    REQUIRE(schema.schemas[0].color_map.at(epoch_core::Color::Success).size() == 1);
    REQUIRE(schema.schemas[0].color_map.at(epoch_core::Color::Success)[0].get_string() == "BUY");
    REQUIRE(schema.schemas[0].color_map.at(epoch_core::Color::Error)[0].get_string() == "SELL");
  }

  SECTION("Parse schema with all render types") {
    std::string schemaJson = R"({
      "title": "All Types",
      "select_key": "filter_col",
      "schemas": [
        {"column_id": "col1", "slot": "PrimaryBadge", "render_type": "Text", "color_map": {}},
        {"column_id": "col2", "slot": "SecondaryBadge", "render_type": "Decimal", "color_map": {}},
        {"column_id": "col3", "slot": "Hero", "render_type": "Badge", "color_map": {}},
        {"column_id": "col4", "slot": "Subtitle", "render_type": "Timestamp", "color_map": {}},
        {"column_id": "col5", "slot": "Footer", "render_type": "Boolean", "color_map": {}}
      ]
    })";

    EventMarkerSchema schema;
    auto error = glz::read_json(schema, schemaJson);

    REQUIRE(!error);
    REQUIRE(schema.schemas.size() == 5);
    REQUIRE(schema.schemas[0].render_type == epoch_core::CardRenderType::Text);
    REQUIRE(schema.schemas[1].render_type == epoch_core::CardRenderType::Decimal);
    REQUIRE(schema.schemas[2].render_type == epoch_core::CardRenderType::Badge);
    REQUIRE(schema.schemas[3].render_type == epoch_core::CardRenderType::Timestamp);
    REQUIRE(schema.schemas[4].render_type == epoch_core::CardRenderType::Boolean);
  }

  SECTION("Parse schema with optional label field") {
    std::string schemaJson = R"({
      "title": "Schema with Labels",
      "select_key": "filter_col",
      "schemas": [
        {"column_id": "col1", "slot": "Hero", "render_type": "Decimal", "color_map": {}},
        {"column_id": "fill_time", "slot": "Details", "render_type": "Text", "color_map": {}, "label": "Fill Time"},
        {"column_id": "psc_timestamp", "slot": "Details", "render_type": "Timestamp", "color_map": {}, "label": "Prior Session Close"}
      ]
    })";

    EventMarkerSchema schema;
    auto error = glz::read_json(schema, schemaJson);

    if (error) {
      FAIL(glz::format_error(error, schemaJson));
    }

    REQUIRE(schema.schemas.size() == 3);

    // First schema has no label
    REQUIRE(schema.schemas[0].column_id == "col1");
    REQUIRE(!schema.schemas[0].label.has_value());

    // Second schema has label
    REQUIRE(schema.schemas[1].column_id == "fill_time");
    REQUIRE(schema.schemas[1].label.has_value());
    REQUIRE(schema.schemas[1].label.value() == "Fill Time");

    // Third schema has label
    REQUIRE(schema.schemas[2].column_id == "psc_timestamp");
    REQUIRE(schema.schemas[2].label.has_value());
    REQUIRE(schema.schemas[2].label.value() == "Prior Session Close");
  }
}

TEST_CASE("CardColumnSchema - Equality and Comparison", "[event_markers][event_marker]") {

  SECTION("CardColumnSchema equality") {
    CardColumnSchema schema1{
      .column_id = "col1",
      .slot = epoch_core::CardSlot::Hero,
      .render_type = epoch_core::CardRenderType::Decimal,
      .color_map = {},
      .label = "label1"
    };

    CardColumnSchema schema2{
      .column_id = "col1",
      .slot = epoch_core::CardSlot::Hero,
      .render_type = epoch_core::CardRenderType::Decimal,
      .color_map = {},
      .label = "label1"
    };

    REQUIRE(schema1 == schema2);
  }

  SECTION("CardColumnSchema equality with labels") {
    CardColumnSchema schema1{
      .column_id = "col1",
      .slot = epoch_core::CardSlot::Details,
      .render_type = epoch_core::CardRenderType::Text,
      .color_map = {},
      .label = "Display Label"
    };

    CardColumnSchema schema2{
      .column_id = "col1",
      .slot = epoch_core::CardSlot::Details,
      .render_type = epoch_core::CardRenderType::Text,
      .color_map = {},
      .label = "Display Label"
    };

    REQUIRE(schema1 == schema2);
  }

  SECTION("CardColumnSchema inequality with different labels") {
    CardColumnSchema schema1{
      .column_id = "col1",
      .slot = epoch_core::CardSlot::Details,
      .render_type = epoch_core::CardRenderType::Text,
      .color_map = {},
      .label = "Label 1"
    };

    CardColumnSchema schema2{
      .column_id = "col1",
      .slot = epoch_core::CardSlot::Details,
      .render_type = epoch_core::CardRenderType::Text,
      .color_map = {},
      .label = "Label 2"
    };

    REQUIRE(schema1 != schema2);
  }

  SECTION("EventMarkerSchema equality") {
    EventMarkerSchema list1{
      .title = "Test",
      .select_key = "key",
      .schemas = {}
    };

    EventMarkerSchema list2{
      .title = "Test",
      .select_key = "key",
      .schemas = {}
    };

    REQUIRE(list1 == list2);
  }

}

TEST_CASE("EventMarker - Transform Functionality", "[event_markers][event_marker]") {
  SECTION("Filter selector returns filtered DataFrame") {
    // Create test DataFrame
    auto df = CreateTestDataFrame();

    // Create EventMarkerSchema configuration
    EventMarkerSchema schema{
      .title = "Trade Signals",
      .select_key = "node#is_signal",
      .schemas = {
        CardColumnSchema{
          .column_id = "direction",
          .slot = epoch_core::CardSlot::PrimaryBadge,
          .render_type = epoch_core::CardRenderType::Badge,
          .color_map = {},
          .label = std::nullopt
        }
      }
    };

    strategy::NodeReference direction{"node", "direction"};
    strategy::NodeReference profit_pct{"node", "profit_pct"};
    strategy::NodeReference is_signal{"node", "is_signal"};

    // Create transform config using helper function - pass object directly
    auto transformConfig = epoch_script::transform::event_marker_cfg(
      "test_selector",
      schema,
      {direction, profit_pct, is_signal},
      epoch_script::TimeFrame(epoch_frame::factory::offset::days(1))
    );

    // Create selector
    EventMarker selector(transformConfig);

    // Execute transform - should return filtered DataFrame
    auto result = selector.TransformData(df);

    // Verify filtered DataFrame (only rows where is_signal is true)
    // Original has 4 rows: [true, true, false, true]
    // Filtered should have 3 rows (indices 0, 1, 3)
    REQUIRE(result.num_rows() == 3);
    REQUIRE(result.num_cols() == 4);  // direction, profit_pct, is_signal, index

    // Verify column names are preserved
    REQUIRE(result.contains(direction.GetColumnName()));
    REQUIRE(result.contains(profit_pct.GetColumnName()));
    REQUIRE(result.contains(is_signal.GetColumnName()));

    // Verify data content - should have rows 0, 1, and 3 from original
    auto direction_col = result[direction.GetColumnName()];
    REQUIRE(direction_col.iloc(0).repr() == "BUY");   // original row 0
    REQUIRE(direction_col.iloc(1).repr() == "SELL");  // original row 1
    REQUIRE(direction_col.iloc(2).repr() == "SELL");  // original row 3

    auto profit_col = result[profit_pct.GetColumnName()];
    REQUIRE(profit_col.iloc(0).as_double() == Approx(10.5));   // original row 0
    REQUIRE(profit_col.iloc(1).as_double() == Approx(-5.2));   // original row 1
    REQUIRE(profit_col.iloc(2).as_double() == Approx(-8.1));   // original row 3

    auto is_signal_col = result[is_signal.GetColumnName()];
    REQUIRE(is_signal_col.iloc(0).as_bool() == true);  // original row 0
    REQUIRE(is_signal_col.iloc(1).as_bool() == true);  // original row 1
    REQUIRE(is_signal_col.iloc(2).as_bool() == true);  // original row 3

    // Verify timestamps (index values in nanoseconds)
    REQUIRE(result["pivot"].iloc(0).timestamp().value == 1609459200000000000LL);  // original row 0
    REQUIRE(result["pivot"].iloc(1).timestamp().value == 1609545600000000000LL);  // original row 1
    REQUIRE(result["pivot"].iloc(2).timestamp().value == 1609718400000000000LL);  // original row 3
  }
}
