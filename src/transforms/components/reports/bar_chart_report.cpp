#include "bar_chart_report.h"
#include "report_utils.h"
#include <epoch_dashboard/tearsheet/bar_chart_builder.h>
#include "epoch_dashboard/tearsheet/scalar_converter.h"
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include "transforms/components/operators/dataframe_utils.h"
#include <regex>
#include <set>
#include <map>

namespace epoch_script::reports {

void BarChartReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                     epoch_tearsheet::DashboardBuilder &dashboard) const {
  // Get column names from input mapping
  auto labelColumn = m_config.GetInput("label");
  auto valueColumn = m_config.GetInput("value");

  // Group by label and aggregate values - preserve original order
  auto df = normalizedDf[{labelColumn, valueColumn}];

  // TODO: Ideally we want to fill null labels with "null" string for visibility in charts
  // (Option A from original design), but this causes type casting errors downstream:
  // "Unsupported cast from string to null using function cast_null"
  // The issue is that fillNullLabels() changes the column type/schema, and Arrow's
  // type system can't reconcile this in the chart rendering pipeline.
  // For now, drop rows with null labels to avoid the error. This means charts won't show
  // bars for incomplete data (e.g., first 252 bars with percentile_select lookback=252).
  // Future fix: Either fix at source (percentile_select) or update epochframe's
  // group_by_agg to handle nulls without schema changes.
  // Drop only rows where label is null (preserving rows with null values)
  df = epoch_script::transform::utils::drop_by_key(df, labelColumn);

  auto grouped = df.group_by_agg(labelColumn)
                   .agg(epoch_core::BarChartAggWrapper::ToString(m_agg))
                   .to_series();

  // Build categories and data arrays from grouped series
  // We need to preserve the original order of appearance, not sorted order
  // Create a map to store aggregated values by label
  std::map<std::string, epoch_proto::Scalar> value_map;
  for (int64_t i = 0; i < static_cast<int64_t>(grouped.size()); ++i) {
    value_map[grouped.index()->at(i).repr()] = epoch_tearsheet::ScalarFactory::create(grouped.iloc(i));
  }

  // Iterate through original dataframe to preserve order
  std::vector<std::string> categories;
  epoch_proto::Array data;
  std::set<std::string> seen_labels;

  // Reuse the filled dataframe to get labels
  auto filled_label_series = df[labelColumn];
  for (int64_t i = 0; i < static_cast<int64_t>(filled_label_series.size()); ++i) {
    std::string label = filled_label_series.iloc(i).repr();
    if (seen_labels.find(label) == seen_labels.end()) {
      seen_labels.insert(label);
      categories.push_back(label);
      *data.add_values() = value_map[label];
    }
  }

  // Build the bar chart using BarChartBuilder
  epoch_tearsheet::BarChartBuilder chartBuilder;
  chartBuilder.setTitle(m_chartTitle)
              .setCategory(m_category)
              .setVertical(m_vertical)
              .setStacked(false)
              .setYAxisType(epoch_proto::AxisLinear)
              .setYAxisLabel(m_yAxisLabel)
              .setXAxisType(epoch_proto::AxisCategory)
              .setXAxisLabel(m_xAxisLabel)
              .setXAxisCategories(categories)
              .setData(data);

  // Add chart to dashboard
  auto chart = chartBuilder.build();
  dashboard.addChart(chart);
}





} // namespace epoch_script::reports
