#include "cs_table_report.h"
#include "report_utils.h"
#include <epoch_dashboard/tearsheet/table_builder.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/table_factory.h>
#include <set>
#include <map>
#include <sstream>

namespace epoch_script::reports {

void CSTableReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                     epoch_tearsheet::DashboardBuilder &dashboard) const {
  using namespace epoch_frame;

  if (normalizedDf.empty() || normalizedDf.num_cols() == 0) {
    std::cerr << "Error: CSTableReport received empty DataFrame" << std::endl;
    return;
  }

  // Get all input columns
  // Expected naming: AssetName for single metric (e.g., "XLK", "XLF", "XLE")
  // For cross-sectional execution, each column represents one asset
  std::vector<std::string> assetNames;

  for (const auto& colName : normalizedDf.column_names()) {
    assetNames.push_back(colName);
  }

  if (assetNames.empty()) {
    std::cerr << "Error: No asset data found" << std::endl;
    return;
  }

  // Build table structure with assets as columns
  // Each asset becomes a column header
  std::vector<std::string> columnNames = assetNames;

  // Single row of data with aggregated values for each asset
  std::vector<std::string> dataRow;

  for (const auto& assetName : assetNames) {
    try {
      auto series = normalizedDf[assetName];

      // Apply aggregation
      epoch_frame::Scalar aggregatedValue;

      if (m_agg == "last") {
        aggregatedValue = series.iloc(series.size() - 1);
      } else if (m_agg == "first") {
        aggregatedValue = series.iloc(0);
      } else if (m_agg == "mean") {
        aggregatedValue = series.mean();
      } else if (m_agg == "sum") {
        aggregatedValue = series.sum();
      } else if (m_agg == "min") {
        aggregatedValue = series.min();
      } else if (m_agg == "max") {
        aggregatedValue = series.max();
      } else {
        // Default to last
        aggregatedValue = series.iloc(series.size() - 1);
      }

      // Convert to string for table display
      dataRow.push_back(aggregatedValue.repr());

    } catch (const std::exception& e) {
      std::cerr << "Warning: Failed to process asset column '" << assetName
                << "': " << e.what() << std::endl;
      dataRow.push_back("N/A");
    }
  }

  // Table has one row with values for all assets
  std::vector<std::vector<std::string>> tableData = {dataRow};

  // Convert to DataFrame for TableBuilder
  // Create Arrow schema
  arrow::FieldVector fields;
  for (const auto& colName : columnNames) {
    fields.push_back(arrow::field(colName, arrow::utf8()));
  }
  auto schema = arrow::schema(fields);

  // Create Arrow arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  for (size_t colIdx = 0; colIdx < columnNames.size(); ++colIdx) {
    arrow::StringBuilder builder;
    for (const auto& row : tableData) {
      if (colIdx < row.size()) {
        ARROW_UNUSED(builder.Append(row[colIdx]));
      } else {
        ARROW_UNUSED(builder.AppendNull());
      }
    }
    std::shared_ptr<arrow::Array> array;
    ARROW_UNUSED(builder.Finish(&array));
    arrays.push_back(array);
  }

  // Create Arrow table and DataFrame
  auto arrowTable = AssertTableResultIsOk(arrow::Table::Make(schema, arrays));
  auto df = epoch_frame::DataFrame(arrowTable);

  // Build protobuf Table using TableBuilder
  epoch_tearsheet::TableBuilder tableBuilder;
  tableBuilder.setTitle(m_title.empty() ? "Asset Comparison" : m_title)
              .setCategory(m_category.empty() ? "Cross-Sectional" : m_category)
              .setType(epoch_proto::WidgetDataTable)
              .fromDataFrame(df);

  // Add table to dashboard
  dashboard.addTable(tableBuilder.build());
}

} // namespace epoch_script::reports
