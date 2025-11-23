#pragma once
#include <epoch_script/transforms/components/reports/ireport.h>
#include <glaze/glaze.hpp>
#include <epoch_script/core/bar_attribute.h>

// EpochDashboard includes
#include <epoch_dashboard/tearsheet/table_builder.h>
#include <epoch_dashboard/tearsheet/card_builder.h>
#include <epoch_dashboard/tearsheet/lines_chart_builder.h>
#include <epoch_dashboard/tearsheet/bar_chart_builder.h>
#include <epoch_dashboard/tearsheet/pie_chart_builder.h>
#include <epoch_dashboard/tearsheet/histogram_chart_builder.h>
#include <epoch_dashboard/tearsheet/scalar_converter.h>
#include <epoch_dashboard/tearsheet/dataframe_converter.h>

namespace epoch_script::reports {

// Structure to hold comprehensive gap data for reuse across visualizations
struct GapTableData {
    std::shared_ptr<arrow::Table> arrow_table;

    // Cached aggregations for efficiency
    size_t total_gaps = 0;
    size_t gap_up_count = 0;
    size_t gap_down_count = 0;
    size_t filled_count = 0;
    size_t gap_up_filled = 0;
    size_t gap_down_filled = 0;

    std::string gap_size_col;
    std::string gap_type_col;
    std::string gap_filled_col;
    std::string weekday_col;
    std::string fill_time_col;
    std::string performance_col;
};

class GapReport : public IReporter {
public:
  explicit GapReport(epoch_script::transform::TransformConfiguration config)
      : IReporter(std::move(config)), m_pivotHour(m_config.GetOptionValue("fill_time_pivot_hour").GetInteger()) {}

protected:
    int64_t m_pivotHour;
  // Implementation of IReporter's virtual methods
  void generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                          epoch_tearsheet::DashboardBuilder &dashboard) const override;

  // Override to provide event markers for gap events
  std::optional<epoch_script::transform::EventMarkerData>
  GetEventMarkers(const epoch_frame::DataFrame &df) const override;

public:
  epoch_tearsheet::DashboardBuilder generate_impl(const epoch_frame::DataFrame &df) const;

  // Analysis helpers
  std::vector<epoch_proto::CardDef>
  compute_summary_cards(const GapTableData &table) const;

  std::pair<epoch_proto::Table, epoch_proto::Table> create_fill_rate_tables(
      const GapTableData &table) const;

  epoch_proto::Table create_comprehensive_gap_table(const GapTableData &data) const;

  // New methods that work with table data
  GapTableData build_comprehensive_table_data(const epoch_frame::DataFrame &gaps) const;

  epoch_proto::Chart create_stacked_fill_rate_chart(const GapTableData &data) const;

  epoch_proto::Chart create_day_of_week_chart(const GapTableData &data) const;

  std::optional<epoch_proto::Chart> create_gap_distribution(const GapTableData &data) const;

  std::optional<epoch_proto::Chart> create_gap_category_chart(const GapTableData &data) const;

  std::optional<epoch_proto::Chart> create_weekday_chart(const GapTableData &data) const;

private:
  // Helper functions
  epoch_proto::Chart create_grouped_stacked_chart(
      const GapTableData &data,
      const std::string &title,
      const std::string &x_axis_label,
      const std::vector<std::string> &categories,
      std::function<int(int64_t)> get_category_index,
      std::function<double(double)> process_value) const;

};

// Template specialization for GapReport metadata
template <> struct ReportMetadata<GapReport> {
  constexpr static const char *kReportId = "gap_report";

  static epoch_script::transforms::TransformsMetaData Get() {
   return {
    .id = kReportId,
    .category = epoch_core::TransformCategory::Reporter,
    .name = "Overnight Gap Analysis Report",
    .options = {{.id = "fill_time_pivot_hour",
          .name = "Fill Time Pivot Hour",
          .type = epoch_core::MetaDataOptionType::Integer,
          .defaultValue = epoch_script::MetaDataOptionDefinition{13.0},
          .isRequired = false,
          .min = 0,
          .max = 23,
          .desc = "The hour used to categorize gap fill times (e.g., 13 for 'before 13:00' vs 'after 13:00'). Used in fill time analysis to identify early vs late session fills."},
        {.id = "histogram_bins",
          .name = "Histogram Bins",
          .type = epoch_core::MetaDataOptionType::Integer,
          .defaultValue = epoch_script::MetaDataOptionDefinition{10.0},
          .isRequired = false,
          .min = 3,
          .max = 50,
          .desc = "Number of bins to use for the gap size distribution histogram. Controls the granularity of the size distribution visualization."}},
    .isCrossSectional = false,
    .desc = "Comprehensive overnight gap analysis report that examines price gaps "
            "between trading sessions (day boundaries). Analyzes gaps that occur when "
            "the opening price differs from the prior session's closing price. "
            "Designed to work with outputs from session_gap transform. "
            "Tracks gap direction (up/down), size distribution, fill rates during the "
            "trading session, and intraday fill timing patterns. "
            "Generates visualizations including fill rate charts by time of day, "
            "day-of-week patterns, gap size distributions, and fill time analysis "
            "(early vs late session fills based on pivot hour). "
            "Best used with intraday data (1min-1hr bars) to capture precise fill "
            "timing throughout the session. Identifies gap trading opportunities "
            "and overnight gap behavior patterns across different market conditions.",
    .inputs = {{epoch_core::IODataType::Boolean, "gap_filled", "Gap Filled"},
               {epoch_core::IODataType::Decimal, "gap_retrace", "Gap Retrace"},
               {epoch_core::IODataType::Decimal, "gap_size", "Gap Size"},
               {epoch_core::IODataType::Decimal, "psc", "Prior Session Close"},
               {epoch_core::IODataType::Timestamp, "psc_timestamp", "PSC Timestamp"}},
    .outputs = {},
    .tags = {"session_gap", "overnight", "session-gap"},
 .requiresTimeFrame = true,
    .requiredDataSources =
        {epoch_script::EpochStratifyXConstants::instance().CLOSE()},
    // intradayOnly=true because the report needs intraday bars to analyze
    // fill timing patterns throughout the trading session (early vs late fills)
    .intradayOnly=true,
.allowNullInputs=true};
  }

  // Helper to create a TransformConfiguration from a gap classifier config
  static epoch_script::transform::TransformConfiguration
  CreateConfig(const std::string &instance_id,
               const epoch_script::transform::TransformConfiguration
                   &gap_classifier_config,
               const YAML::Node &options = {}) {

    YAML::Node config;
    config["id"] = instance_id;
    config["type"] = kReportId;
    // Just pass timeframe as string
    config["timeframe"] = "1D"; // Default for now, could get from
                                // gap_classifier_config if method exists

    // Map the gap classifier's outputs to our inputs
    // The gap classifier produces columns that we need
    YAML::Node inputs;
    std::string gap_id = gap_classifier_config.GetId();

    // Map each required input to the gap classifier's output columns
    auto metadata = Get();
    for (const auto &input : metadata.inputs) {
      // Map input name to gap_classifier_id#column_name format
      inputs[input.name].push_back(gap_id + "#" + input.name);
    }

    config["inputs"] = inputs;
    // SessionRange is optional, skip it for now
    config["options"] = options;

    return epoch_script::transform::TransformConfiguration{
        epoch_script::TransformDefinition{config}};
  }

  // Simpler helper for testing without a preceding node
  static epoch_script::transform::TransformConfiguration
  CreateConfig(const std::string &instance_id,
               const std::string &timeframe = "1D",
               const YAML::Node &options = {}) {

    YAML::Node config;
    config["id"] = instance_id;
    config["type"] = kReportId;
    config["timeframe"] = timeframe;
    config["inputs"] = YAML::Node(); // Empty for testing
    config["sessionRange"] = YAML::Node();
    config["options"] = options;

    return epoch_script::transform::TransformConfiguration{
        epoch_script::TransformDefinition{config}};
  }
};

} // namespace epoch_script::reports
