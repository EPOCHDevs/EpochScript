#pragma once

#include <epoch_script/transforms/components/reports/ireport.h>
#include <epoch_frame/dataframe.h>
#include <arrow/api.h>

CREATE_ENUM(BarChartAgg,
 sum,
 mean,
 count,
 first,
 last,
 min,
 max
);

namespace epoch_script::reports {

class BarChartReport : public IReporter {
public:
 explicit BarChartReport(epoch_script::transform::TransformConfiguration config)
     : IReporter(std::move(config), true),
       m_agg(m_config.GetOptionValue("agg").GetSelectOption<epoch_core::BarChartAgg>()),
       m_chartTitle(m_config.GetOptionValue("title").GetString()),
      m_xAxisLabel(m_config.GetOptionValue("x_axis_label").GetString()),
      m_yAxisLabel(m_config.GetOptionValue("y_axis_label").GetString()),
       m_category(m_config.GetOptionValue("category").GetString()),
       m_vertical(m_config.GetOptionValue("vertical").GetBoolean()) {}

protected:
 void generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                          epoch_tearsheet::DashboardBuilder &dashboard) const override;

private:
 const epoch_core::BarChartAgg m_agg;
 const std::string m_chartTitle;
 const std::string m_xAxisLabel;
 const std::string m_yAxisLabel;

 const std::string m_category;
 const bool m_vertical;
};

template <> struct ReportMetadata<BarChartReport> {
  constexpr static const char *kReportId = "bar_chart_report";

  static epoch_script::transforms::TransformsMetaData Get() {
    return {
      .id = kReportId,
      .category = epoch_core::TransformCategory::Reporter,
      .name = "Bar Chart Report",
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
         .desc = "Aggregation function to apply when grouping by label"},
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
         .desc = "For grouping in dashboard"},
        {.id = "vertical",
         .name = "Vertical Bars",
         .type = epoch_core::MetaDataOptionType::Boolean,
         .defaultValue = epoch_script::MetaDataOptionDefinition{true},
         .isRequired = false,
         .desc = "Use vertical bars (true) or horizontal bars (false)"}
      },
      .isCrossSectional = false,
      .desc = "Generates bar chart from DataFrame with aggregation. Groups by 'label' input and aggregates using specified function.",
      .inputs = {
        {epoch_core::IODataType::String, "label", "Label Column"},
        {epoch_core::IODataType::Number, "value", "Value Column"}
      },
      .outputs = {},
      .atLeastOneInputRequired = true,
      .tags = {"report", "chart", "bar", "visualization"},
      .requiresTimeFrame = false,
      .allowNullInputs = true
    };
  }
};

} // namespace epoch_script::reports