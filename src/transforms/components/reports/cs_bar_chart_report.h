#pragma once

#include <epoch_script/transforms/components/reports/ireport.h>
#include <epoch_frame/dataframe.h>
#include <arrow/api.h>

CREATE_ENUM(CSBarChartAgg,
 sum,
 mean,
 count,
 first,
 last,
 min,
 max
);

namespace epoch_script::reports {

/**
 * Cross-Sectional Bar Chart Report
 *
 * Generates a single bar chart comparing ALL assets at once.
 * Each asset becomes a bar in the chart.
 *
 * Input: Multi-column DataFrame (one column per asset)
 * Output: Single bar chart with all assets side-by-side
 */
class CSBarChartReport : public IReporter {
public:
 explicit CSBarChartReport(epoch_script::transform::TransformConfiguration config)
     : IReporter(std::move(config), true),
       m_agg(m_config.GetOptionValue("agg").GetSelectOption<epoch_core::CSBarChartAgg>()),
       m_chartTitle(m_config.GetOptionValue("title").GetString()),
      m_xAxisLabel(m_config.GetOptionValue("x_axis_label").GetString()),
      m_yAxisLabel(m_config.GetOptionValue("y_axis_label").GetString()),
       m_category(m_config.GetOptionValue("category").GetString()),
       m_vertical(m_config.GetOptionValue("vertical").GetBoolean()) {}

protected:
 void generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                          epoch_tearsheet::DashboardBuilder &dashboard) const override;

 // Override TransformData to skip column selection/renaming
 // Cross-sectional execution already renamed columns to asset_ids (AAPL, XLK, etc.)
 epoch_frame::DataFrame TransformData(const epoch_frame::DataFrame &df) const override {
   // Just return df as-is - GetDashboard() will be called by execution framework
   return df;
 }

private:
 const epoch_core::CSBarChartAgg m_agg;
 const std::string m_chartTitle;
 const std::string m_xAxisLabel;
 const std::string m_yAxisLabel;
 const std::string m_category;
 const bool m_vertical;
};

template <> struct ReportMetadata<CSBarChartReport> {
  constexpr static const char *kReportId = "cs_bar_chart_report";

  static epoch_script::transforms::TransformsMetaData Get() {
    return {
      .id = kReportId,
      .category = epoch_core::TransformCategory::Reporter,
      .name = "Cross-Sectional Bar Chart Report",
      .options = {
        {.id = "agg",
         .name = "Aggregation",
         .type = epoch_core::MetaDataOptionType::Select,
         .isRequired = true,
         .selectOption = {
           {"Sum", "sum"},
           {"Mean", "mean"},
           {"Count", "count"},
           {"First", "first"},
           {"Last", "last"},
           {"Min", "min"},
           {"Max", "max"}
         },
         .desc = "Aggregation function to apply to each asset's time series"},
        {.id = "title",
         .name = "Chart Title",
         .type = epoch_core::MetaDataOptionType::String,
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("")},
         .isRequired = false,
         .desc = "Title for the generated chart"},
        {.id = "x_axis_label",
         .name = "X Axis Label",
         .type = epoch_core::MetaDataOptionType::String,
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("")},
         .isRequired = false,
         .desc = "Label for the x-axis"},
        {.id = "y_axis_label",
         .name = "Y Axis Label",
         .type = epoch_core::MetaDataOptionType::String,
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("")},
         .isRequired = false,
         .desc = "Label for the y-axis"},
        {.id = "category",
         .name = "Category",
         .type = epoch_core::MetaDataOptionType::String,
         .isRequired = true,
         .desc = "Category for grouping in dashboard"},
        {.id = "vertical",
         .name = "Vertical Bars",
         .type = epoch_core::MetaDataOptionType::Boolean,
         .defaultValue = epoch_script::MetaDataOptionDefinition{true},
         .isRequired = false,
         .desc = "Use vertical bars (true) or horizontal bars (false)"}
      },
      .isCrossSectional = true,  // KEY: This enables cross-sectional execution
      .desc = "Generates a single bar chart comparing all assets. Each asset's data is aggregated using the specified function, and all assets appear as bars in one chart.",
      .inputs = {
        {epoch_core::IODataType::Number, ARG, "Value to aggregate per asset"}
      },
      .outputs = {},
      .atLeastOneInputRequired = true,
      .tags = {"report", "chart", "bar", "visualization", "cross-sectional", "comparison"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .assetRequirements = {"multi-asset"}
    };
  }
};

} // namespace epoch_script::reports
