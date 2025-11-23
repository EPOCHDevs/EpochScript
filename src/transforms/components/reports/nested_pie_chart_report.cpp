#include "nested_pie_chart_report.h"
#include "report_utils.h"
#include <epoch_dashboard/tearsheet/pie_chart_builder.h>
#include <epoch_dashboard/tearsheet/chart_types.h>
#include <epoch_frame/dataframe.h>
#include <sstream>
#include <unordered_map>

namespace epoch_script::reports {

void NestedPieChartReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                     epoch_tearsheet::DashboardBuilder &dashboard) const {
  try {
    // Get column names from input mapping
    auto innerLabelColumn = m_config.GetInput("inner_label");
    auto outerLabelColumn = m_config.GetInput("outer_label");
    auto valueColumn = m_config.GetInput("value");

    // Normalize both inner and outer series as percentages using utility function
    auto inner_data = ReportUtils::normalizeSeriesAsPercentage(
        normalizedDf, innerLabelColumn, valueColumn);
    auto outer_data = ReportUtils::normalizeSeriesAsPercentage(
        normalizedDf, outerLabelColumn, valueColumn);

    // Build nested pie chart
    epoch_tearsheet::PieChartBuilder chartBuilder;
    chartBuilder.setTitle(m_chartTitle.empty() ? "Nested Pie Chart" : m_chartTitle)
                .setCategory(m_category);

    // Convert series to pie data using utility function
    auto outer_pie_data = ReportUtils::createPieDataFromSeries(outer_data);
    chartBuilder.addSeries(outerLabelColumn, outer_pie_data, epoch_tearsheet::PieSize{80},
                    epoch_tearsheet::PieInnerSize{60});

    auto inner_pie_data = ReportUtils::createPieDataFromSeries(inner_data);
    chartBuilder.addSeries(innerLabelColumn, inner_pie_data, epoch_tearsheet::PieSize{45},
                      epoch_tearsheet::PieInnerSize{0});

    dashboard.addChart(chartBuilder.build());

  } catch (const std::exception& e) {
    std::cerr << "Error: NestedPieChartReport execution failed: " << e.what() << std::endl;
  }
}

} // namespace epoch_script::reports
