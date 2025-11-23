#pragma once

#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/metadata.h>
#include <epoch_script/core/metadata_options.h>
#include <epoch_frame/dataframe.h>
#include <glaze/glaze.hpp>

namespace epoch_script::transform {

// Event Marker - filters DataFrame by boolean column and displays as interactive event markers
class EventMarker : public epoch_script::transform::ITransform {
public:
  explicit EventMarker(epoch_script::transform::TransformConfiguration config)
      : ITransform(std::move(config)),
        m_schema(GetSchemaFromConfig(config)) {}

  epoch_frame::DataFrame TransformData(const epoch_frame::DataFrame &df) const override {
    // Filter by boolean column specified in select_key
    epoch_frame::DataFrame result = df.loc(df[m_schema.select_key]);

    // Reset index to create pivot column for timestamp navigation
    result = result.reset_index("pivot");

    return result;
  }

  // Override GetEventMarkers to return event marker data based on transformed DataFrame
  std::optional<EventMarkerData>
  GetEventMarkers(const epoch_frame::DataFrame &df) const override {
    return EventMarkerData(
      m_schema.title,
      m_schema.schemas,
      df,
      m_schema.schemas.size()-1,  // pivot_index points to the last schema (pivot column)
      m_schema.icon
    );
  }

  EventMarkerSchema GetSchema() const { return m_schema; }

private:
  static EventMarkerSchema GetSchemaFromConfig(const epoch_script::transform::TransformConfiguration& config) {
    EventMarkerSchema schema = config.GetOptionValue("schema").GetCardSchemaList();

    // Automatically add index column as timestamp for chart navigation
    schema.schemas.emplace_back(CardColumnSchema{
    .column_id = "pivot",
    .slot = epoch_core::CardSlot::Subtitle,
    .render_type = epoch_core::CardRenderType::Timestamp,
    .color_map = {},
    .label = std::nullopt
    });
    return schema;
  }

  const EventMarkerSchema m_schema;
};

// Metadata for EventMarker
struct EventMarkerMetadata {
  constexpr static const char *kEventMarkerId = "event_marker";

  static epoch_script::transforms::TransformsMetaData Get() {
    return {
      .id = kEventMarkerId,
      .category = epoch_core::TransformCategory::EventMarker,
      .name = "Event Marker",
      .options = {
        {.id = "schema",
         .name = "Card Schema",
         .type = epoch_core::MetaDataOptionType::EventMarkerSchema,
         .isRequired = true,
         .desc = std::string("Card layout configuration using boolean column filter. The 'select_key' field specifies a boolean column name to filter rows (only rows where the column is true are shown as event markers). For SQL filtering, use a SQL Transform node first, then pipe output to this event marker. JSON Schema:\n") + glz::write_json_schema<EventMarkerSchema>().value_or("{}")}
      },
      .isCrossSectional = false,
      .desc = "Generate an interactive event marker where each row is a clickable event marker, filtered by a boolean column. "
              "Click an event marker to navigate to that timestamp on the candlestick chart. "
              "Accepts multiple input columns via SLOT connection. "
              "For SQL-based filtering, use a SQL Transform node before this event marker.",
      .inputs = {
        {.type = epoch_core::IODataType::Any,
         .id = "SLOT",
         .name = "Columns",
         .allowMultipleConnections = true}
      },
      .outputs = {},  // EventMarker outputs via GetEventMarkerData()
      .atLeastOneInputRequired = true,
      .tags = {"event_marker", "interactive", "cards", "navigation", "timepoint", "filter"},
      .requiresTimeFrame = false,
      .allowNullInputs = false
    };
  }
};

} // namespace epoch_script::transform
