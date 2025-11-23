#pragma once

#include <epoch_script/transforms/components/reports/ireport.h>
#include "epoch_core/enum_wrapper.h"

// Cross-Sectional Numeric aggregate functions
CREATE_ENUM(CSNumericArrowAggregateFunction,
  approximate_median,
  count_all,
  count_distinct,
  first,
  kurtosis,
  last,
  max,
  mean,
  min,
  product,
  skew,
  stddev,
  sum,
  tdigest,
  variance
);

namespace epoch_script::reports {

/**
 * Cross-Sectional Numeric Card Report
 *
 * Generates a card group showing one metric for each asset.
 * All assets appear as cards in the same group.
 *
 * Input: Multi-column DataFrame (one column per asset)
 * Output: Card group with one card per asset
 *
 * Example: Sector returns
 *   [XLK: 2.50%] [XLF: 1.20%] [XLE: -0.80%] ...
 */
class CSNumericCardReport : public IReporter {
public:
  explicit CSNumericCardReport(epoch_script::transform::TransformConfiguration config)
      : IReporter(std::move(config), true),
        m_agg(config.GetOptionValue("agg").GetSelectOption<epoch_core::CSNumericArrowAggregateFunction>()),
        m_category(config.GetOptionValue("category").GetString()),
        m_title(config.GetOptionValue("title").GetString()) {}

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
  const epoch_core::CSNumericArrowAggregateFunction m_agg;
  const std::string m_category;
  const std::string m_title;

  std::string GetAggregation() const;
};

template <> struct ReportMetadata<CSNumericCardReport> {
  constexpr static const char *kReportId = "cs_numeric_cards_report";

  static epoch_script::transforms::TransformsMetaData Get() {
    return {
      .id = kReportId,
      .category = epoch_core::TransformCategory::Reporter,
      .name = "Cross-Sectional Numeric Cards Report",
      .options = {
        {.id = "agg",
         .name = "Aggregation",
         .type = epoch_core::MetaDataOptionType::Select,
         .defaultValue = epoch_script::MetaDataOptionDefinition{"last"},
         .isRequired = false,
         .selectOption = {
           {"Approximate Median", "approximate_median"},
           {"Count All", "count_all"},
           {"Count Distinct", "count_distinct"},
           {"First", "first"},
           {"Kurtosis", "kurtosis"},
           {"Last", "last"},
           {"Max", "max"},
           {"Mean", "mean"},
           {"Min", "min"},
           {"Product", "product"},
           {"Skew", "skew"},
           {"StdDev", "stddev"},
           {"Sum", "sum"},
           {"TDigest", "tdigest"},
           {"Variance", "variance"}
         },
         .desc = "Numeric aggregate function to apply to each asset's time series"},
        {.id = "category",
         .name = "Category",
         .type = epoch_core::MetaDataOptionType::String,
         .isRequired = true,
         .desc = "Category name for the card group"},
        {.id = "title",
         .name = "Card Title Pattern",
         .type = epoch_core::MetaDataOptionType::String,
         .defaultValue = epoch_script::MetaDataOptionDefinition{std::string("")},
         .isRequired = false,
         .desc = "Title pattern for cards (empty = asset name)"}
      },
      .isCrossSectional = true,  // KEY: This enables cross-sectional execution
      .desc = "Generate a card group by aggregating each asset's time series. All assets appear as cards in the same group for comparison.",
      .inputs = {
        {epoch_core::IODataType::Number, ARG, "Numeric value to aggregate per asset"}
      },
      .outputs = {},  // Report outputs via TearSheet
      .atLeastOneInputRequired = true,
      .tags = {"report", "cards", "aggregation", "summary", "numeric", "cross-sectional", "comparison"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .assetRequirements = {"multi-asset"}
    };
  }
};

} // namespace epoch_script::reports
