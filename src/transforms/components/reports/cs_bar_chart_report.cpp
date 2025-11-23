#include "cs_bar_chart_report.h"
#include "report_utils.h"
#include <epoch_dashboard/tearsheet/bar_chart_builder.h>
#include "epoch_dashboard/tearsheet/scalar_converter.h"
#include <epoch_frame/dataframe.h>

namespace epoch_script::reports {

void CSBarChartReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                     epoch_tearsheet::DashboardBuilder &dashboard) const {
  using namespace epoch_frame;

  // Get the input column from SLOT0 (varg input)
  auto inputColumns = m_config.GetInputs();
  std::string inputCol;

  if (inputColumns.empty() || !inputColumns.contains(ARG)) {
    std::cerr << "Error: CSBarChartReport requires at least one input" << std::endl;
    return;
  }

  // For cross-sectional, we expect the input to be available across all asset columns
  // The DataFrame comes in with columns named by asset (XLK, XLF, XLE, etc.)

  std::vector<std::string> categories;
  epoch_proto::Array data;

  // Iterate through all columns in the DataFrame
  // Each column represents one asset
  for (const auto& assetColumn : normalizedDf.column_names()) {
    try {
      auto series = normalizedDf[assetColumn];

      // Apply aggregation function
      epoch_frame::Scalar aggregatedValue;
      std::string aggFunc = epoch_core::CSBarChartAggWrapper::ToString(m_agg);

      if (aggFunc == "sum") {
        aggregatedValue = series.sum();
      } else if (aggFunc == "mean") {
        aggregatedValue = series.mean();
      } else if (aggFunc == "count") {
        aggregatedValue = epoch_frame::Scalar(static_cast<int64_t>(series.size()));
      } else if (aggFunc == "first") {
        aggregatedValue = series.iloc(0);
      } else if (aggFunc == "last") {
        aggregatedValue = series.iloc(series.size() - 1);
      } else if (aggFunc == "min") {
        aggregatedValue = series.min();
      } else if (aggFunc == "max") {
        aggregatedValue = series.max();
      } else {
        // Default to last
        aggregatedValue = series.iloc(series.size() - 1);
      }

      // Add to chart data
      categories.push_back(assetColumn);
      *data.add_values() = epoch_tearsheet::ScalarFactory::create(aggregatedValue);

    } catch (const std::exception& e) {
      std::cerr << "Warning: Failed to process asset column '" << assetColumn
                << "': " << e.what() << std::endl;
      continue;
    }
  }

  if (categories.empty()) {
    std::cerr << "Error: No valid asset data to chart" << std::endl;
    return;
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
