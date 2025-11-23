#include "gap_report.h"
#include <arrow/builder.h>
#include <arrow/compute/api_scalar.h>
#include <arrow/table.h>
#include <cmath>
#include <ctime>
#include <limits>
#include <algorithm>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_script/core/metadata_options.h>
#include <spdlog/spdlog.h>

// EpochDashboard builders
#include <epoch_dashboard/tearsheet/dashboard_builders.h>
#include <epoch_dashboard/tearsheet/tearsheet_builder.h>
#include <epoch_dashboard/tearsheet/table_builder.h>

namespace epoch_script::reports {

// Gap size category constants
namespace {
  const std::vector<std::string> GAP_SIZE_CATEGORIES = {
    "0-0.19%", "0.2-0.39%", "0.4-0.59%", "0.6-0.99%", "1.0-1.99%", "2.0%+"
  };

  // Gap size category thresholds (in percentage)
  const std::vector<double> GAP_SIZE_THRESHOLDS = {
    0.2, 0.4, 0.6, 1.0, 2.0
  };

  // Helper function to get gap category from size
  std::string getGapCategory(double gapSizePct) {
    if (gapSizePct < GAP_SIZE_THRESHOLDS[0]) return GAP_SIZE_CATEGORIES[0];
    if (gapSizePct < GAP_SIZE_THRESHOLDS[1]) return GAP_SIZE_CATEGORIES[1];
    if (gapSizePct < GAP_SIZE_THRESHOLDS[2]) return GAP_SIZE_CATEGORIES[2];
    if (gapSizePct < GAP_SIZE_THRESHOLDS[3]) return GAP_SIZE_CATEGORIES[3];
    if (gapSizePct < GAP_SIZE_THRESHOLDS[4]) return GAP_SIZE_CATEGORIES[4];
    return GAP_SIZE_CATEGORIES[5];
  }
}
  using namespace epoch_tearsheet;
  const auto closeLiteral = epoch_script::EpochStratifyXConstants::instance().CLOSE();

  void GapReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                     epoch_tearsheet::DashboardBuilder &dashboard) const {
    // Generate the tearsheet using the existing implementation
    dashboard = generate_impl(normalizedDf);
  }

  std::optional<epoch_script::transform::EventMarkerData>
  GapReport::GetEventMarkers(const epoch_frame::DataFrame &normalizedDf) const {
    // Build comprehensive table data to get the processed gap events
    auto table_data = build_comprehensive_table_data(normalizedDf);

    if (table_data.total_gaps == 0 || !table_data.arrow_table) {
      return std::nullopt;  // No gaps to show
    }

    // Build card schemas for event marker display
    std::vector<epoch_script::CardColumnSchema> card_schemas;

    // Primary badge: gap_type (gap up/down)
    card_schemas.push_back({
      .column_id = "gap_type",
      .slot = epoch_core::CardSlot::PrimaryBadge,
      .render_type = epoch_core::CardRenderType::Badge,
      .color_map = {
        {epoch_core::Color::Success, {"gap up"}},
        {epoch_core::Color::Error, {"gap down"}}
      },
      .label = std::nullopt
    });

    // Secondary badge: gap_filled status
    card_schemas.push_back({
      .column_id = "gap_filled",
      .slot = epoch_core::CardSlot::SecondaryBadge,
      .render_type = epoch_core::CardRenderType::Badge,
      .color_map = {
        {epoch_core::Color::Success, {"filled"}},
        {epoch_core::Color::Default, {"not filled"}}
      },
      .label = std::nullopt
    });

    // Hero: gap_size percentage
    card_schemas.push_back({
      .column_id = "gap_size",
      .slot = epoch_core::CardSlot::Hero,
      .render_type = epoch_core::CardRenderType::Percent,
      .color_map = {},
      .label = "Gap Size"
    });

    // Footer: weekday
    card_schemas.push_back({
      .column_id = "weekday",
      .slot = epoch_core::CardSlot::Footer,
      .render_type = epoch_core::CardRenderType::Text,
      .color_map = {},
      .label = "Day"
    });

    // Details: fill_time
    card_schemas.push_back({
      .column_id = "fill_time",
      .slot = epoch_core::CardSlot::Details,
      .render_type = epoch_core::CardRenderType::Text,
      .color_map = {},
      .label = "Fill Time"
    });

    // Details: performance
    card_schemas.push_back({
      .column_id = "performance",
      .slot = epoch_core::CardSlot::Details,
      .render_type = epoch_core::CardRenderType::Badge,
      .color_map = {
        {epoch_core::Color::Success, {"green"}},
        {epoch_core::Color::Error, {"red"}}
      },
      .label = "Performance"
    });

    // Add pivot_index column for timestamp navigation
    card_schemas.push_back({
      .column_id = "pivot_index",
      .slot = epoch_core::CardSlot::Subtitle,
      .render_type = epoch_core::CardRenderType::Timestamp,
      .color_map = {},
      .label = "Date"
    });

    // The pivot_index points to the timestamp schema (last element in the array)
    size_t pivot_idx = card_schemas.size() - 1;

    // Convert Arrow table back to DataFrame and reset index for event markers
    epoch_frame::DataFrame event_df = epoch_frame::DataFrame(table_data.arrow_table);
    event_df = event_df.reset_index("pivot_index");

    return epoch_script::transform::EventMarkerData(
      "Gap Events",
      card_schemas,
      event_df,
      pivot_idx,
      epoch_core::Icon::Split
    );
  }

  epoch_tearsheet::DashboardBuilder
  GapReport::generate_impl(const epoch_frame::DataFrame &df) const {
    DashboardBuilder builder;

    // Build comprehensive table first - this becomes our single source of truth
    // We'll always build it internally even if not displayed
    auto comprehensive_table_data = build_comprehensive_table_data(df);

    // 1. Summary cards - now from table data
    auto cards = compute_summary_cards(comprehensive_table_data);
    for (const auto& card : cards) {
      builder.addCard(card);
    }

    // 2. Fill rate analysis - now as stacked bar chart
    builder.addChart(create_stacked_fill_rate_chart(comprehensive_table_data));

    // 3. Gap category analysis - shows fill rates by gap size category
    if (auto chart = create_gap_category_chart(comprehensive_table_data)) {
      builder.addChart(*chart);
    }

    // 4. Day of week analysis - shows fill rates by day of week
    if (auto chart = create_weekday_chart(comprehensive_table_data)) {
      builder.addChart(*chart);
    }

    // 5. Gap size distribution histogram - from table data
    if (auto chart = create_gap_distribution(comprehensive_table_data)) {
      builder.addChart(*chart);
    }

    // 6. Fill rate tables
    auto [gapup, gap_down] = create_fill_rate_tables(comprehensive_table_data);
    builder.addTable(gapup);
    builder.addTable(gap_down);

    // 7. Comprehensive gap table - the raw data table
    auto gap_table = create_comprehensive_gap_table(comprehensive_table_data);
    builder.addTable(gap_table);

    return builder;
  }

  GapTableData GapReport::build_comprehensive_table_data(const epoch_frame::DataFrame &gaps) const {
    GapTableData data;

    auto make_gap_dataframe = [](const epoch_frame::Date& dateIndex,
                                 double gapSize,
                                 const std::string& gapType, const std::string& gapFilled,
                                 const std::string& weekDay, const std::string& fillTime,
                                 const std::string& performance) {
      auto index  = epoch_frame::factory::index::make_datetime_index(std::vector{epoch_frame::DateTime{dateIndex}});

      // Create Arrow arrays from single values
      auto gap_size_array = epoch_frame::factory::array::make_array(std::vector<double>{gapSize});
      auto gap_type_array = epoch_frame::factory::array::make_array(std::vector<std::string>{gapType});
      auto gap_filled_array = epoch_frame::factory::array::make_array(std::vector<std::string>{gapFilled});
      auto weekday_array = epoch_frame::factory::array::make_array(std::vector<std::string>{weekDay});
      auto fill_time_array = epoch_frame::factory::array::make_array(std::vector<std::string>{fillTime});
      auto performance_array = epoch_frame::factory::array::make_array(std::vector<std::string>{performance});

      std::vector<std::shared_ptr<arrow::ChunkedArray>> columnData = {
        fill_time_array, gap_filled_array, gap_size_array,
        gap_type_array, performance_array, weekday_array
      };

      std::vector<std::string> columnNames = {
        "fill_time", "gap_filled", "gap_size", "gap_type", "performance", "weekday"
      };

      return epoch_frame::make_dataframe(index, columnData, columnNames);
    };

    // Helper to create empty DataFrame with correct schema
    auto make_empty_gap_dataframe = [] {
      auto emptyIndex = epoch_frame::factory::index::make_datetime_index(std::vector<epoch_frame::DateTime>{});

      // Create empty Arrow arrays with correct types
      auto empty_gap_size_array = epoch_frame::factory::array::make_array(std::vector<double>{});
      auto empty_gap_type_array = epoch_frame::factory::array::make_array(std::vector<std::string>{});
      auto empty_gap_filled_array = epoch_frame::factory::array::make_array(std::vector<std::string>{});
      auto empty_weekday_array = epoch_frame::factory::array::make_array(std::vector<std::string>{});
      auto empty_fill_time_array = epoch_frame::factory::array::make_array(std::vector<std::string>{});
      auto empty_performance_array = epoch_frame::factory::array::make_array(std::vector<std::string>{});

      std::vector<std::shared_ptr<arrow::ChunkedArray>> emptyColumnData = {
        empty_fill_time_array, empty_gap_filled_array, empty_gap_size_array,
        empty_gap_type_array, empty_performance_array, empty_weekday_array
      };

      return epoch_frame::make_dataframe(emptyIndex, emptyColumnData,
        std::vector<std::string>{"fill_time", "gap_filled", "gap_size", "gap_type", "performance", "weekday"});
    };

    // Group by daily normalized index to get one record per day
    auto normalized = gaps.index()->normalize()->as_chunked_array();
    auto daily_df = gaps.group_by_apply(normalized, false).apply([
        &make_gap_dataframe,
        &make_empty_gap_dataframe, this](epoch_frame::DataFrame const& in) -> epoch_frame::DataFrame {
      if (in.num_rows() == 0) {
        return make_empty_gap_dataframe();
      }

      auto index = in.index()->at(0).to_date().date();

      // Handle multiple gaps in a day - take the first one and warn if multiple
      auto gapSizeSeries = in["gap_size"].drop_null();
      if (gapSizeSeries.size() == 0) {
        return epoch_frame::DataFrame{};
      }
      if (gapSizeSeries.size() > 1) {
        SPDLOG_WARN("More than 1 gap found in a day ({}). Only taking first", gapSizeSeries.size());
      }

      // Extract first gap's data
      auto gapSize = gapSizeSeries.iloc(0);

      // Find if gap was filled during the day
      bool hasGapFilled = false;

      try {
        auto gapFilledSeries = in["gap_filled"].drop_null();
        if (gapFilledSeries.size() > 0) {
          auto anyFilled = gapFilledSeries.any();
          hasGapFilled = anyFilled.as_bool();
        }
      } catch (std::exception const& e) {
          SPDLOG_WARN("Error processing gap_filled column: {}", e.what());
        hasGapFilled = false;
      }

      // Extract close and psc for performance calculation
      auto closeVal = in[closeLiteral].iloc(0).as_double();
      auto pscVal = in["psc"].iloc(0).as_double();

      // Calculate derived fields
      auto weekDay = epoch_core::EpochDayOfWeekWrapper::ToString(static_cast<epoch_core::EpochDayOfWeek>(index.weekday()));
      auto gapSizeVal = gapSize.as_double();
      auto isGapUp = gapSizeVal > 0;
      auto gapSizePct = std::abs(gapSizeVal);

      // Performance calculation: green if close > psc, red otherwise
      std::string performance = closeVal > pscVal ? "green" : "red";

      // Fill time calculation
      std::string fillTimeStr;
      if (hasGapFilled) {
        auto datetime = in.index()->at(0).to_datetime();
        auto hour = datetime.time().hour.count();

        std::string beforeLabel = "before " + std::to_string(m_pivotHour) + ":00";
        std::string afterLabel = "after " + std::to_string(m_pivotHour) + ":00";
        fillTimeStr = hour < m_pivotHour ? beforeLabel : afterLabel;
      } else {
        fillTimeStr = ""; // Empty string for unfilled gaps
      }

      // Create single-row DataFrame with all the derived columns we need
      using namespace epoch_frame;

      // Create single-element datetime index for the day

      return make_gap_dataframe(index, gapSizePct,
                               isGapUp ? "gap up" : "gap down",
                               hasGapFilled ? "filled" : "not filled",
                               weekDay, fillTimeStr, performance);
    });

    // Clean the data before analysis - but only if we have data
    if (daily_df.num_rows() > 0 && daily_df.num_cols() > 0) {
      daily_df = daily_df.loc(daily_df["gap_type"].is_valid());
    }

    // Calculate all aggregations with DataFrame operations - no loops needed!
    data.total_gaps = daily_df.num_rows();

    // Only calculate aggregations if we have data
    if (data.total_gaps > 0) {
      // One-liner aggregations using DataFrame operations
      auto gap_up_mask = daily_df["gap_type"] == epoch_frame::Scalar{"gap up"};
      auto gap_down_mask = daily_df["gap_type"] == epoch_frame::Scalar{"gap down"};
      auto filled_mask = daily_df["gap_filled"] == epoch_frame::Scalar{"filled"};

      data.gap_up_count = gap_up_mask.sum().value<size_t>().value();
      data.gap_down_count = gap_down_mask.sum().value<size_t>().value();
      data.filled_count = filled_mask.sum().value<size_t>().value();
      data.gap_up_filled = (gap_up_mask && filled_mask).sum().value<size_t>().value();
      data.gap_down_filled = (gap_down_mask && filled_mask).sum().value<size_t>().value();

      // Use DataFrame's table directly - no loops needed!
      data.arrow_table = daily_df.table();
    } else {
      // Create empty table with correct schema when no data
      data.gap_up_count = 0;
      data.gap_down_count = 0;
      data.filled_count = 0;
      data.gap_up_filled = 0;
      data.gap_down_filled = 0;
      data.arrow_table = nullptr;
    }

    // Set column names for the essential columns
    data.gap_size_col = "gap_size";
    data.gap_type_col = "gap_type";
    data.gap_filled_col = "gap_filled";
    data.weekday_col = "weekday";
    data.fill_time_col = "fill_time";
    data.performance_col = "performance";

    // Build SelectorData for card selector
    std::vector<epoch_script::CardColumnSchema> card_schemas;

    // Primary badge: gap_type (gap up/down)
    card_schemas.push_back({
      .column_id = "gap_type",
      .slot = epoch_core::CardSlot::PrimaryBadge,
      .render_type = epoch_core::CardRenderType::Badge,
      .color_map = {
        {epoch_core::Color::Success, {"gap up"}},
        {epoch_core::Color::Error, {"gap down"}}
      },
      .label = std::nullopt
    });

    // Secondary badge: gap_filled status
    card_schemas.push_back({
      .column_id = "gap_filled",
      .slot = epoch_core::CardSlot::SecondaryBadge,
      .render_type = epoch_core::CardRenderType::Badge,
      .color_map = {
        {epoch_core::Color::Success, {"filled"}},
        {epoch_core::Color::Warning, {"not filled"}}
      },
      .label = std::nullopt
    });

    // Hero: gap_size (main focus)
    card_schemas.push_back({
      .column_id = "gap_size",
      .slot = epoch_core::CardSlot::Hero,
      .render_type = epoch_core::CardRenderType::Decimal,
      .color_map = {},
      .label = std::nullopt
    });

    // Subtitle: weekday
    card_schemas.push_back({
      .column_id = "weekday",
      .slot = epoch_core::CardSlot::Details,
      .render_type = epoch_core::CardRenderType::Text,
      .color_map = {},
      .label = std::nullopt
    });

    // Footer: performance (green/red indicator)
    card_schemas.push_back({
      .column_id = "performance",
      .slot = epoch_core::CardSlot::Footer,
      .render_type = epoch_core::CardRenderType::Badge,
      .color_map = {
        {epoch_core::Color::Success, {"green"}},
        {epoch_core::Color::Error, {"red"}}
      },
      .label = std::nullopt
    });

    // Details: fill_time (only relevant for filled gaps)
    card_schemas.push_back({
      .column_id = "fill_time",
      .slot = epoch_core::CardSlot::Details,
      .render_type = epoch_core::CardRenderType::Text,
      .color_map = {},
      .label = "Fill Time"
    });

    // Navigator: pivot_index column for candlestick chart navigation
    // This will be populated from the daily_df index via reset_index
    card_schemas.push_back({
      .column_id = "pivot_index",
      .slot = epoch_core::CardSlot::Subtitle,
      .render_type = epoch_core::CardRenderType::Timestamp,
      .color_map = {},
      .label = "Date"
    });

    // The pivot_index points to the timestamp schema (last element in the array)
    // Event marker data will be generated by GetEventMarkers() override (stateless architecture)

    return data;
  }


  std::vector<epoch_proto::CardDef>
  GapReport::compute_summary_cards(const GapTableData &table) const {
    std::vector<epoch_proto::CardDef> cards;

    // Calculate percentages
    double gap_up_pct = table.total_gaps > 0 ?
        std::floor(table.gap_up_count * 100.0 / table.total_gaps) : 0;
    double gap_down_pct = table.total_gaps > 0 ?
        std::floor(table.gap_down_count * 100.0 / table.total_gaps) : 0;
    double fill_rate = table.total_gaps > 0 ?
        std::floor(table.filled_count * 100.0 / table.total_gaps) : 0;

    // Create one card with group size 4 containing all 4 metrics
    auto summary_card = CardBuilder()
        .setType(epoch_proto::WidgetCard)
        .setCategory("Reports")
        .setGroupSize(4)
        .addCardData(
            CardDataBuilder()
                .setTitle("Total Gaps")
                .setValue(ScalarFactory::fromInteger(table.total_gaps))
                .setType(epoch_proto::TypeInteger)
                .setGroup(0)
                .build()
        )
        .addCardData(
            CardDataBuilder()
                .setTitle("Gap Up %")
                .setValue(ScalarFactory::fromPercentValue(gap_up_pct))
                .setType(epoch_proto::TypePercent)
                .setGroup(1)
                .build()
        )
        .addCardData(
            CardDataBuilder()
                .setTitle("Gap Down %")
                .setValue(ScalarFactory::fromPercentValue(gap_down_pct))
                .setType(epoch_proto::TypePercent)
                .setGroup(2)
                .build()
        )
        .addCardData(
            CardDataBuilder()
                .setTitle("Fill Rate")
                .setValue(ScalarFactory::fromPercentValue(fill_rate))
                .setType(epoch_proto::TypePercent)
                .setGroup(3)
                .build()
        )
        .build();
    cards.push_back(std::move(summary_card));

    return cards;
  }

  std::pair<epoch_proto::Table, epoch_proto::Table> GapReport::create_fill_rate_tables(const GapTableData &table) const {
    // Use aggregated data from table
    auto gap_up_total = table.gap_up_count;
    auto gap_up_filled = table.gap_up_filled;
    auto gap_up_unfilled = gap_up_total - gap_up_filled;

    auto gap_down_total = table.gap_down_count;
    auto gap_down_filled = table.gap_down_filled;
    auto gap_down_unfilled = gap_down_total - gap_down_filled;

    // Calculate percentages relative to total gaps
    // Round to 2 decimal places for display
    auto total_gaps = table.total_gaps;

    double gap_up_total_pct = total_gaps > 0 ?
      std::round(static_cast<double>(gap_up_total) / total_gaps * 10000) / 100 : 0;
    double gap_up_filled_pct = gap_up_total > 0 ?
      std::round(static_cast<double>(gap_up_filled) / gap_up_total * 10000) / 100 : 0;
    double gap_up_unfilled_pct = gap_up_total > 0 ?
      std::round(static_cast<double>(gap_up_unfilled) / gap_up_total * 10000) / 100 : 0;

    double gap_down_total_pct = total_gaps > 0 ?
      std::round(static_cast<double>(gap_down_total) / total_gaps * 10000) / 100 : 0;
    double gap_down_filled_pct = gap_down_total > 0 ?
      std::round(static_cast<double>(gap_down_filled) / gap_down_total * 10000) / 100 : 0;
    double gap_down_unfilled_pct = gap_down_total > 0 ?
      std::round(static_cast<double>(gap_down_unfilled) / gap_down_total * 10000) / 100 : 0;

    // Build Gap Up table using TableBuilder only
    TableBuilder gap_up_builder;
    gap_up_builder.setTitle("Gap Up Fill Analysis")
        .setCategory("Reports")
        .setType(epoch_proto::WidgetDataTable)
        .addColumn("category", "Category", epoch_proto::TypeString)
        .addColumn("frequency", "Frequency", epoch_proto::TypeInteger)
        .addColumn("percentage", "Percentage", epoch_proto::TypePercent);

    // Add rows for Gap Up table
    epoch_proto::TableRow row1;
    row1.add_values()->set_string_value("gap up");
    row1.add_values()->set_integer_value(gap_up_total);
    row1.add_values()->set_percent_value(gap_up_total_pct);
    gap_up_builder.addRow(row1);

    epoch_proto::TableRow row2;
    row2.add_values()->set_string_value("gap up filled");
    row2.add_values()->set_integer_value(gap_up_filled);
    row2.add_values()->set_percent_value(gap_up_filled_pct);
    gap_up_builder.addRow(row2);

    epoch_proto::TableRow row3;
    row3.add_values()->set_string_value("gap up not filled");
    row3.add_values()->set_integer_value(gap_up_unfilled);
    row3.add_values()->set_percent_value(gap_up_unfilled_pct);
    gap_up_builder.addRow(row3);

    auto gap_up_table = gap_up_builder.build();

    // Build Gap Down table using TableBuilder only
    TableBuilder gap_down_builder;
    gap_down_builder.setTitle("Gap Down Fill Analysis")
        .setCategory("Reports")
        .setType(epoch_proto::WidgetDataTable)
        .addColumn("category", "Category", epoch_proto::TypeString)
        .addColumn("frequency", "Frequency", epoch_proto::TypeInteger)
        .addColumn("percentage", "Percentage", epoch_proto::TypePercent);

    // Add rows for Gap Down table
    epoch_proto::TableRow row4;
    row4.add_values()->set_string_value("gap down");
    row4.add_values()->set_integer_value(gap_down_total);
    row4.add_values()->set_percent_value(gap_down_total_pct);
    gap_down_builder.addRow(row4);

    epoch_proto::TableRow row5;
    row5.add_values()->set_string_value("gap down filled");
    row5.add_values()->set_integer_value(gap_down_filled);
    row5.add_values()->set_percent_value(gap_down_filled_pct);
    gap_down_builder.addRow(row5);

    epoch_proto::TableRow row6;
    row6.add_values()->set_string_value("gap down not filled");
    row6.add_values()->set_integer_value(gap_down_unfilled);
    row6.add_values()->set_percent_value(gap_down_unfilled_pct);
    gap_down_builder.addRow(row6);

    auto gap_down_table = gap_down_builder.build();

    return {std::move(gap_up_table), std::move(gap_down_table)};
  }

  epoch_proto::Table GapReport::create_comprehensive_gap_table(const GapTableData &data) const {
    // Build the table
    TableBuilder builder;
    builder.setTitle("Gap Analysis Details")
        .setCategory("Reports")
        .setType(epoch_proto::WidgetDataTable)
        .addColumn("gap_size", "Gap Size %", epoch_proto::TypePercent)
        .addColumn("gap_type", "Gap Type", epoch_proto::TypeString)
        .addColumn("gap_filled", "Filled Status", epoch_proto::TypeString)
        .addColumn("weekday", "Day of Week", epoch_proto::TypeString)
        .addColumn("fill_time", "Fill Time", epoch_proto::TypeString)
        .addColumn("performance", "Performance", epoch_proto::TypeString);

    // Validate table exists - if empty, we still need to ensure data field is initialized
    if (!data.arrow_table || data.arrow_table->num_rows() == 0) {
      SPDLOG_WARN("Empty or invalid arrow_table in create_comprehensive_gap_table");
      // Build the table and ensure data field exists (even with 0 rows)
      auto table = builder.build();
      // Access mutable_data to ensure the field is initialized in the protobuf
      // This is necessary for proper comparison in tests
      auto* table_data = table.mutable_data();
      (void)table_data; // Suppress unused variable warning
      return table;
    }

    // Use safe epoch_frame::Array wrappers for all columns with GetColumnByName
    auto gap_size_array = epoch_frame::Array(data.arrow_table->GetColumnByName("gap_size"));
    auto gap_type_array = epoch_frame::Array(data.arrow_table->GetColumnByName("gap_type"));
    auto gap_filled_array = epoch_frame::Array(data.arrow_table->GetColumnByName("gap_filled"));
    auto weekday_array = epoch_frame::Array(data.arrow_table->GetColumnByName("weekday"));
    auto fill_time_array = epoch_frame::Array(data.arrow_table->GetColumnByName("fill_time"));
    auto performance_array = epoch_frame::Array(data.arrow_table->GetColumnByName("performance"));

    // Get typed views for efficient access
    auto gap_size_view = gap_size_array.to_view<double>();
    auto gap_type_view = gap_type_array.to_view<std::string>();
    auto gap_filled_view = gap_filled_array.to_view<std::string>();
    auto weekday_view = weekday_array.to_view<std::string>();
    auto fill_time_view = fill_time_array.to_view<std::string>();
    auto performance_view = performance_array.to_view<std::string>();

    // Add rows from the arrow table
    for (int64_t i = 0; i < data.arrow_table->num_rows(); ++i) {
      epoch_proto::TableRow row;

      // Gap size as percentage (column is TypePercent, so use percent_value)
      row.add_values()->set_percent_value(gap_size_view->Value(i));

      // Gap type
      row.add_values()->set_string_value(gap_type_view->GetString(i));

      // Filled status
      row.add_values()->set_string_value(gap_filled_view->GetString(i));

      // Weekday
      row.add_values()->set_string_value(weekday_view->GetString(i));

      // Fill time (may be empty for unfilled gaps)
      row.add_values()->set_string_value(fill_time_view->GetString(i));

      // Performance
      row.add_values()->set_string_value(performance_view->GetString(i));

      builder.addRow(row);
    }

    return builder.build();
  }

  epoch_proto::Chart GapReport::create_stacked_fill_rate_chart(const GapTableData &data) const {

    // Prepare the data for stacked bars
    epoch_proto::BarData filled_data;
    filled_data.set_name("Filled");
    filled_data.set_stack("gap_analysis");
    filled_data.add_values(static_cast<double>(data.gap_up_filled));
    filled_data.add_values(static_cast<double>(data.gap_down_filled));
    filled_data.add_values(static_cast<double>(data.filled_count));

    epoch_proto::BarData not_filled_data;
    not_filled_data.set_name("Not Filled");
    not_filled_data.set_stack("gap_analysis");
    not_filled_data.add_values(static_cast<double>(data.gap_up_count - data.gap_up_filled));
    not_filled_data.add_values(static_cast<double>(data.gap_down_count - data.gap_down_filled));
    not_filled_data.add_values(static_cast<double>(data.total_gaps - data.filled_count));

    // Build the stacked bar chart
    return BarChartBuilder()
        .setTitle("Gap Fill Analysis")
        .setCategory("Reports")
        .setXAxisType(epoch_proto::AxisCategory)
        .setXAxisCategories({"Gap Up", "Gap Down", "Total"})
        .setXAxisLabel("Gap Type")
        .setYAxisLabel("Count")
        .setYAxisType(epoch_proto::AxisLinear)
        .addBarData(filled_data)
        .addBarData(not_filled_data)
        .setStacked(true)
        .setStackType(epoch_proto::StackTypeNormal)
        .setVertical(true)
        .build();
  }
  

  std::optional<epoch_proto::Chart> GapReport::create_gap_distribution(const GapTableData &data) const {
    // Validate table exists
    if (!data.arrow_table || data.arrow_table->num_rows() == 0) {
      SPDLOG_WARN("Invalid or empty arrow_table in create_gap_distribution");
      return std::nullopt;
    }

    // Use safe epoch_frame::Array wrapper
    epoch_frame::Array gap_size_array(data.arrow_table->GetColumnByName(data.gap_size_col));

    // Validate array has data
    if (gap_size_array->length() == 0) {
      SPDLOG_WARN("Empty gap_size array in create_gap_distribution");
      return std::nullopt;
    }

    // Get bins count from option
    uint32_t bins = static_cast<uint32_t>(m_config.GetOptionValue("histogram_bins").GetInteger());

    // Check if we have enough data points for the histogram
    if (gap_size_array->length() < static_cast<int64_t>(bins)) {
      SPDLOG_WARN("Insufficient data points ({}) for histogram bins ({})",
                  gap_size_array->length(), bins);
      return std::nullopt;
    }
    // Create series from ChunkedArray
    const auto series = epoch_frame::Series(gap_size_array.as_chunked_array(), "gap_size");

    return HistogramChartBuilder()
        .setTitle("Gap Size Distribution")
        .setCategory("Reports")
        .setXAxisLabel("Gap Size (%)")
        .setYAxisLabel("Frequency")
        .fromSeries(series, bins)
        .build();
  }

  std::optional<epoch_proto::Chart> GapReport::create_gap_category_chart(const GapTableData &data) const {
    // Validate table exists - return nullopt if no data (don't show empty charts)
    if (!data.arrow_table || data.arrow_table->num_rows() == 0) {
      SPDLOG_WARN("Invalid or empty arrow_table in create_gap_category_chart");
      return std::nullopt;
    }

    // Get arrays with proper typed views for efficient access
    auto gap_size_view = epoch_frame::Array(data.arrow_table->GetColumnByName(data.gap_size_col)).to_view<double>();
    auto gap_filled_view = epoch_frame::Array(data.arrow_table->GetColumnByName(data.gap_filled_col)).to_view<std::string>();

    // Validate arrays and check length compatibility
    if (gap_size_view->length() != gap_filled_view->length()) {
      SPDLOG_WARN("Array validation failed or length mismatch in create_gap_category_chart");
      return std::nullopt;
    }

    // Check for empty arrays
    if (gap_size_view->length() == 0) {
      SPDLOG_WARN("Empty arrays in create_gap_category_chart");
      return std::nullopt;
    }

    // Count occurrences by gap category and fill status
    std::map<std::pair<std::string, std::string>, int64_t> counts;

    for (int64_t i = 0; i < gap_size_view->length(); ++i) {
      if ((*gap_size_view)[i] && (*gap_filled_view)[i]) {
        double gap_size = (*gap_size_view)[i].value();
        std::string fill_status((*gap_filled_view)[i].value());

        // Use helper function to determine gap category
        std::string category = getGapCategory(gap_size);

        counts[{category, fill_status}]++;
      }
    }

    // Create stacked bar data for filled/not filled
    epoch_proto::BarData filled_data;
    filled_data.set_name("Filled");
    filled_data.set_stack("fill_status");

    epoch_proto::BarData not_filled_data;
    not_filled_data.set_name("Not Filled");
    not_filled_data.set_stack("fill_status");

    // Add values for each gap category
    for (const auto& category : GAP_SIZE_CATEGORIES) {
      auto filled_count = counts[{category, "filled"}];
      auto not_filled_count = counts[{category, "not filled"}];

      filled_data.add_values(static_cast<double>(filled_count));
      not_filled_data.add_values(static_cast<double>(not_filled_count));
    }

    // Build the stacked bar chart
    return BarChartBuilder()
        .setTitle("Gap Fill Rate by Size Category")
        .setCategory("Reports")
        .setXAxisType(epoch_proto::AxisCategory)
        .setXAxisCategories(GAP_SIZE_CATEGORIES)
        .setXAxisLabel("Gap Size Category")
        .setYAxisLabel("Number of Gaps")
        .setYAxisType(epoch_proto::AxisLinear)
        .addBarData(filled_data)
        .addBarData(not_filled_data)
        .setStacked(true)
        .setStackType(epoch_proto::StackTypeNormal)
        .setVertical(true)
        .build();
  }

  std::optional<epoch_proto::Chart> GapReport::create_weekday_chart(const GapTableData &data) const {
    // Validate table exists - return nullopt if no data (don't show empty charts)
    if (!data.arrow_table || data.arrow_table->num_rows() == 0) {
      SPDLOG_WARN("Invalid or empty arrow_table in create_weekday_chart");
      return std::nullopt;
    }

    // Get arrays with proper typed views for efficient access
    auto weekday_view = epoch_frame::Array(data.arrow_table->GetColumnByName(data.weekday_col)).to_view<std::string>();
    auto gap_filled_view = epoch_frame::Array(data.arrow_table->GetColumnByName(data.gap_filled_col)).to_view<std::string>();

    // Validate arrays and check length compatibility
    if (weekday_view->length() != gap_filled_view->length()) {
      SPDLOG_WARN("Array validation failed or length mismatch in create_weekday_chart");
      return std::nullopt;
    }

    // Check for empty arrays
    if (weekday_view->length() == 0) {
      SPDLOG_WARN("Empty arrays in create_weekday_chart");
      return std::nullopt;
    }

    // Weekdays
    std::vector<std::string> weekdays = {
      "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
    };

    // Count occurrences by weekday and fill status
    std::map<std::pair<std::string, std::string>, int64_t> counts;

    for (int64_t i = 0; i < weekday_view->length(); ++i) {
      if ((*weekday_view)[i] && (*gap_filled_view)[i]) {
        std::string weekday((*weekday_view)[i].value());
        std::string fill_status((*gap_filled_view)[i].value());

        counts[{weekday, fill_status}]++;
      }
    }

    // Create stacked bar data for filled/not filled
    epoch_proto::BarData filled_data;
    filled_data.set_name("Filled");
    filled_data.set_stack("fill_status");

    epoch_proto::BarData not_filled_data;
    not_filled_data.set_name("Not Filled");
    not_filled_data.set_stack("fill_status");

    // Add values for each weekday
    for (const auto& weekday : weekdays) {
      auto filled_count = counts[{weekday, "filled"}];
      auto not_filled_count = counts[{weekday, "not filled"}];

      filled_data.add_values(static_cast<double>(filled_count));
      not_filled_data.add_values(static_cast<double>(not_filled_count));
    }

    // Build the stacked bar chart
    return BarChartBuilder()
        .setTitle("Gap Fill Rate by Day of Week")
        .setCategory("Reports")
        .setXAxisType(epoch_proto::AxisCategory)
        .setXAxisCategories(weekdays)
        .setXAxisLabel("Day of Week")
        .setYAxisLabel("Number of Gaps")
        .setYAxisType(epoch_proto::AxisLinear)
        .addBarData(filled_data)
        .addBarData(not_filled_data)
        .setStacked(true)
        .setStackType(epoch_proto::StackTypeNormal)
        .setVertical(true)
        .build();
  }


} // namespace epoch_script::reports
