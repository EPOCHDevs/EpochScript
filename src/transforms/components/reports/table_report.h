#pragma once

#include <epoch_script/transforms/components/reports/ireport.h>
#include <epoch_script/core/metadata_options.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/scalar.h>
#include <arrow/api.h>

namespace epoch_script::reports {

class TableReport : public IReporter {
public:
  explicit TableReport(epoch_script::transform::TransformConfiguration config)
      : IReporter(std::move(config), true),
        m_schema(GetTableReportSchema()) {
  }

protected:
  void generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                          epoch_tearsheet::DashboardBuilder &dashboard) const override;


private:
  // Cached configuration values
  const TableReportSchema m_schema;

  // Configuration getters
  TableReportSchema GetTableReportSchema() const;

  // Helper methods
};

// Metadata specialization for TableReport
template <> struct ReportMetadata<TableReport> {
  constexpr static const char *kReportId = "table_report";

  static epoch_script::transforms::TransformsMetaData Get() {
    return {
      .id = kReportId,
      .category = epoch_core::TransformCategory::Reporter,
      .name = "Table Report",
      .options = {
        {.id = "schema",
         .name = "Table Schema",
         .type = epoch_core::MetaDataOptionType::TableReportSchema,
         .isRequired = true,
         .desc = "Schema defining table title, select_key for filtering, and columns to display"}
      },
      .isCrossSectional = false,
      .desc = "Filter DataFrame using a boolean column and display specified columns in a table",
      .inputs = {
        {epoch_core::IODataType::Any, epoch_script::ARG, "", true}
      },
      .outputs = {},  // Report outputs via TearSheet
      .atLeastOneInputRequired = true,
      .tags = {"report", "table", "filter"},
      .requiresTimeFrame = false,
      .allowNullInputs = true
    };
  }
};

} // namespace epoch_script::reports