#pragma once

#include <epoch_script/transforms/components/reports/ireport.h>
#include <epoch_script/core/metadata_options.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/scalar.h>
#include <arrow/api.h>

namespace epoch_script::reports {

/**
 * Cross-Sectional Table Report
 *
 * Generates a single table with assets as columns and a single metric row.
 *
 * Input: Multi-column DataFrame (one column per asset for single metric)
 * Output: Single-row table with all assets as column headers
 *
 * Example:
 *         | XLK  | XLF  | XLE  | XLV  | ...
 *   ------|------|------|------|------|----
 *   Value | 2.50 | 1.20 |-0.80 | 0.95 | ...
 */
class CSTableReport : public IReporter {
public:
  explicit CSTableReport(epoch_script::transform::TransformConfiguration config)
      : IReporter(std::move(config), true),
        m_title(m_config.GetOptionValue("title").GetString()),
        m_category(m_config.GetOptionValue("category").GetString()),
        m_agg(m_config.GetOptionValue("agg").GetString()) {}

protected:
  void generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                          epoch_tearsheet::DashboardBuilder &dashboard) const override;

  // Override TransformData to skip column selection/renaming
  // Cross-sectional execution already renamed columns to asset_ids (AAPL, XLK, etc.)
  epoch_frame::DataFrame TransformData(const epoch_frame::DataFrame &df) const override {
    // Pass DataFrame directly to generateTearsheet - columns are already asset_ids
    // Dashboard generation handled by GetDashboard() in execution framework
    return df;
  }

private:
  const std::string m_title;
  const std::string m_category;
  const std::string m_agg;  // Aggregation function: "last", "first", "mean", "sum"
};

// Metadata specialization for CSTableReport
template <> struct ReportMetadata<CSTableReport> {
  constexpr static const char *kReportId = "cs_table_report";

  static epoch_script::transforms::TransformsMetaData Get() {
    return {
      .id = kReportId,
      .category = epoch_core::TransformCategory::Reporter,
      .name = "Cross-Sectional Table Report",
      .options = {
        {.id = "title",
         .name = "Table Title",
         .type = epoch_core::MetaDataOptionType::String,
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("Asset Comparison")},
         .isRequired = false,
         .desc = "Title for the table"},
        {.id = "category",
         .name = "Category",
         .type = epoch_core::MetaDataOptionType::String,
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("Cross-Sectional")},
         .isRequired = false,
         .desc = "Category for dashboard grouping"},
        {.id = "agg",
         .name = "Aggregation",
         .type = epoch_core::MetaDataOptionType::Select,
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("last")},
         .isRequired = false,
         .selectOption = {
           {"Last", "last"},
           {"First", "first"},
           {"Mean", "mean"},
           {"Sum", "sum"},
           {"Min", "min"},
           {"Max", "max"}
         },
         .desc = "Aggregation function to apply to each asset's time series"}
      },
      .isCrossSectional = true,  // KEY: This enables cross-sectional execution
      .desc = "Display assets as columns with a single metric row. Each asset appears as a column header with its aggregated value in the row.",
      .inputs = {
        transforms::IOMetaData{epoch_core::IODataType::Any, ARG, "Metric to display"}
      },
      .outputs = {},  // Report outputs via TearSheet
      .atLeastOneInputRequired = true,
      .tags = {"report", "table", "cross-sectional", "comparison"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .assetRequirements = {"multi-asset"}
    };
  }
};

} // namespace epoch_script::reports
