#include "table_report.h"
#include "report_utils.h"
#include <epoch_dashboard/tearsheet/table_builder.h>
#include <epoch_frame/dataframe.h>
#include <sstream>
#include <unordered_map>

namespace epoch_script::reports {

void TableReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                     epoch_tearsheet::DashboardBuilder &dashboard) const {
  try {
    // Filter by boolean column specified in select_key (like event_marker)
    // Note: select_key is already resolved to node_id#handle by compiler
    epoch_frame::DataFrame filteredDf = normalizedDf.loc(normalizedDf[m_schema.select_key]);

    // Select and rename columns based on schema
    std::unordered_map<std::string, std::string> columnRenameMap;
    std::vector<std::string> columnsToSelect;

    for (const auto& col : m_schema.columns) {
      columnsToSelect.push_back(col.column_id);
      columnRenameMap[col.column_id] = col.title;
    }

    // Select only the columns specified in the schema
    epoch_frame::DataFrame resultDf = filteredDf[columnsToSelect];

    // Rename columns to their display titles
    resultDf = resultDf.rename(columnRenameMap);

    // Build protobuf Table using TableBuilder
    epoch_tearsheet::TableBuilder tableBuilder;
    tableBuilder.setTitle(m_schema.title.empty() ? "Table Report" : m_schema.title)
                .setCategory("Reports")
                .setType(epoch_proto::WidgetDataTable)
                .fromDataFrame(resultDf);

    // Add table to dashboard
    dashboard.addTable(tableBuilder.build());

  } catch (const std::exception& e) {
    std::cerr << "Error: TableReport execution failed: " << e.what() << std::endl;
    return;
  }
}

TableReportSchema TableReport::GetTableReportSchema() const {
  auto options = m_config.GetOptions();
  if (options.contains("schema") && options["schema"].IsType(epoch_core::MetaDataOptionType::TableReportSchema)) {
    return options["schema"].GetTableReportSchema();
  }
  // Return empty schema if not found (will fail validation)
  throw std::runtime_error("TableReport requires 'schema' option");
}

} // namespace epoch_script::reports
