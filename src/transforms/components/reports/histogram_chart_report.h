#pragma once

#include <epoch_script/transforms/components/reports/ireport.h>
#include <epoch_frame/dataframe.h>
#include <arrow/api.h>

namespace epoch_script::reports {

class HistogramChartReport : public IReporter {
public:
  explicit HistogramChartReport(epoch_script::transform::TransformConfiguration config)
      : IReporter(std::move(config), true),
        m_chartTitle(m_config.GetOptionValue("title").GetString()),
        m_bins(static_cast<uint32_t>(m_config.GetOptionValue("bins").GetInteger())),
        m_xAxisLabel(m_config.GetOptionValue("x_axis_label").GetString()),
        m_yAxisLabel(m_config.GetOptionValue("y_axis_label").GetString()),
        m_category(m_config.GetOptionValue("category").GetString()) {
  }

protected:
  void generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                          epoch_tearsheet::DashboardBuilder &dashboard) const override;

private:
  const std::string m_chartTitle;
  const uint32_t m_bins;
  const std::string m_xAxisLabel;
  const std::string m_yAxisLabel;
  const std::string m_category;
};

template <> struct ReportMetadata<HistogramChartReport> {
  constexpr static const char *kReportId = "histogram_chart_report";

  static epoch_script::transforms::TransformsMetaData Get() {
    return {
      .id = kReportId,
      .category = epoch_core::TransformCategory::Reporter,
      .name = "Histogram Chart Report",
      .options = {
        {.id = "title",
         .name = "Chart Title",
         .type = epoch_core::MetaDataOptionType::String,
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("")},
         .isRequired = false,
         .desc = "Title for the generated chart"},
        {.id = "bins",
         .name = "Number of Bins",
         .type = epoch_core::MetaDataOptionType::Integer,
         .defaultValue = epoch_script::MetaDataOptionDefinition{30.0},
         .isRequired = false,
         .min = 1,
         .max = 100,
         .desc = "Number of bins for the histogram"},
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
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("")},
         .isRequired = true,
         .desc = "For grouping in dashboard"}
      },
      .isCrossSectional = false,
      .desc = "Generates histogram chart from timeseries data. Processes values directly without SQL aggregation.",
      .inputs = {
        {epoch_core::IODataType::Number, "value", "Value Column"}
      },
      .outputs = {},
      .atLeastOneInputRequired = true,
      .tags = {"report", "chart", "histogram", "distribution", "visualization"},
      .requiresTimeFrame = false,
      .allowNullInputs = true
    };
  }
};

} // namespace epoch_script::reports