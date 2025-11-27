#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"
#include "parametric_data_source.h"

namespace epoch_script::transform {

// Factory function to create metadata for all Polygon data source transforms
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakePolygonDataSources() {
  using namespace epoch_script::data_sources;
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry for financial statements
  auto balanceSheetMeta = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(DataCategory::BalanceSheets);
  auto incomeStatementMeta = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(DataCategory::IncomeStatements);
  auto cashFlowMeta = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(DataCategory::CashFlowStatements);
  auto ratiosMeta = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(DataCategory::Ratios);

  // 1. Balance Sheet Data
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = "balance_sheet",
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Balance Sheet",
          .options = {
              MetaDataOption{
                  .id = "period",  // NOT "timeframe" - reserved for orchestrator resampling
                  .name = "Reporting Period",
                  .type = epoch_core::MetaDataOptionType::Select,
                  .defaultValue = std::nullopt,  // Required - no default
                  .isRequired = true,
                  .selectOption = {
                      {"Quarterly (10-Q)", "quarterly"},
                      {"Annual (10-K)", "annual"},
                  },
                  .desc = "Select financial statement reporting period"},
          },
          .isCrossSectional = false,
          .desc = "Load balance sheet fundamental data. "
                  "Provides assets, liabilities, equity, and other balance "
                  "sheet metrics over time.",
          .inputs = {},
          .outputs = BuildOutputsFromSDKMetadata(balanceSheetMeta),
          .atLeastOneInputRequired = false,
          .tags = {"fundamentals", "balance-sheet", "financial-statements"},
          .requiresTimeFrame = true,
          .requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(balanceSheetMeta),
          .intradayOnly = IsIntradayOnlyCategory(DataCategory::BalanceSheets),
          .allowNullInputs = true,  // Data sources should preserve null rows
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::FileText,
              .text = "Q{fiscal_quarter} {fiscal_year} Balance Sheet<br/>Cash: ${cash}<br/>Debt: ${lt_debt}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Info,
              .title = std::nullopt,
              .valueKey = "cash"  // UI will check if valid to determine when to show flag
          },
          .strategyTypes = {"fundamental-analysis", "value-investing"},
          .assetRequirements = {"single-asset"},
          .usageContext =
              "Access balance sheet data for fundamental analysis. Use to "
              "evaluate company financial health, leverage, liquidity. "
              "Combine with price data for value strategies. Data is "
              "quarterly/annual based on company filings.",
          .limitations =
              "Data availability depends on company filing schedules. "
              "Quarterly data has reporting lag. Only available for US "
              "equities with SEC filings. Requires external data loader.",
      });

  // 2. Income Statement Data
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = "income_statement",
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Income Statement",
          .options = {
              MetaDataOption{
                  .id = "period",  // NOT "timeframe" - reserved for orchestrator resampling
                  .name = "Reporting Period",
                  .type = epoch_core::MetaDataOptionType::Select,
                  .defaultValue = std::nullopt,  // Required - no default
                  .isRequired = true,
                  .selectOption = {
                      {"Quarterly (10-Q)", "quarterly"},
                      {"Annual (10-K)", "annual"},
                      {"Trailing Twelve Months (TTM)", "trailing_twelve_months"},
                  },
                  .desc = "Select financial statement reporting period"},
          },
          .isCrossSectional = false,
          .desc = "Load income statement fundamental data . "
                  "Provides revenue, expenses, earnings, and profitability "
                  "metrics over time.",
          .inputs = {},
          .outputs = BuildOutputsFromSDKMetadata(incomeStatementMeta),
          .atLeastOneInputRequired = false,
          .tags = { "fundamentals", "income-statement", "earnings",
                   "financial-statements"},
          .requiresTimeFrame = true,
          .requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(incomeStatementMeta),
          .intradayOnly = IsIntradayOnlyCategory(DataCategory::IncomeStatements),
          .allowNullInputs = true,  // Data sources should preserve null rows
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Receipt,
              .text = "Q{fiscal_quarter} {fiscal_year} Earnings<br/>Revenue: ${revenue}<br/>EPS: ${diluted_eps}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Info,
              .title = std::nullopt,
              .valueKey = "revenue"  // UI will check if valid to determine when to show flag
          },
          .strategyTypes = {"fundamental-analysis", "growth-investing",
                            "earnings-momentum"},
          .assetRequirements = {"single-asset"},
          .usageContext =
              "Access income statement data for profitability analysis. "
              "Track revenue growth, margin expansion, earnings quality. "
              "Essential for growth and earnings-based strategies. Compare "
              "quarter-over-quarter and year-over-year trends.",
          .limitations =
              "Data availability depends on company filing schedules. "
              "Quarterly data has reporting lag (typically 45+ days after "
              "quarter end). Only available for US equities with SEC filings. "
              "Requires external data loader.",
      });

  // 3. Cash Flow Statement Data
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = "cash_flow",
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Cash Flow",
          .options = {
              MetaDataOption{
                  .id = "period",  // NOT "timeframe" - reserved for orchestrator resampling
                  .name = "Reporting Period",
                  .type = epoch_core::MetaDataOptionType::Select,
                  .defaultValue = std::nullopt,  // Required - no default
                  .isRequired = true,
                  .selectOption = {
                      {"Quarterly (10-Q)", "quarterly"},
                      {"Annual (10-K)", "annual"},
                      {"Trailing Twelve Months (TTM)", "trailing_twelve_months"},
                  },
                  .desc = "Select financial statement reporting period"},
          },
          .isCrossSectional = false,
          .desc = "Load cash flow statement fundamental data . "
                  "Provides operating, investing, and financing cash flows to "
                  "analyze liquidity and capital allocation.",
          .inputs = {},
          .outputs = BuildOutputsFromSDKMetadata(cashFlowMeta),
          .atLeastOneInputRequired = false,
          .tags = { "fundamentals", "cash-flow", "financial-statements"},
          .requiresTimeFrame = true,
          .requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(cashFlowMeta),
          .intradayOnly = IsIntradayOnlyCategory(DataCategory::CashFlowStatements),
          .allowNullInputs = true,  // Data sources should preserve null rows
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Wallet,
              .text = "Q{fiscal_quarter} {fiscal_year} Cash Flow<br/>Operating CF: ${cfo}<br/>CapEx: ${capex}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Info,
              .title = std::nullopt,
              .valueKey = "cfo"  // UI will check if valid to determine when to show flag
          },
          .strategyTypes = {"fundamental-analysis", "cash-flow-analysis",
                            "quality-investing"},
          .assetRequirements = {"single-asset"},
          .usageContext =
              "Access cash flow data to assess company liquidity, capital "
              "allocation efficiency, and financial flexibility. Free cash "
              "flow (Operating CF - CapEx) is key metric. Essential for "
              "quality-focused fundamental strategies.",
          .limitations =
              "Data availability depends on company filing schedules. "
              "Quarterly data has reporting lag. Only available for US "
              "equities with SEC filings. Requires external data loader.",
      });

  // 4. Financial Ratios Data
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = "financial_ratios",
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Financial Ratios",
          .options = {},
          .isCrossSectional = false,
          .desc = "Load financial ratios and valuation metrics . "
                  "Provides P/E, P/B, P/S, EV/EBITDA, and other key ratios for "
                  "fundamental screening and valuation analysis.",
          .inputs = {},
          .outputs = BuildOutputsFromSDKMetadata(ratiosMeta),
          .atLeastOneInputRequired = false,
          .tags = { "fundamentals", "ratios", "valuation", "screening"},
          .requiresTimeFrame = true,
          .requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(ratiosMeta),
          .intradayOnly = IsIntradayOnlyCategory(DataCategory::Ratios),
          .allowNullInputs = true,  // Data sources should preserve null rows
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Calculator,
              .text = "Valuation Ratios<br/>P/E: {price_to_earnings}<br/>ROE: {return_on_equity}%",
              .textIsTemplate = true,
              .color = epoch_core::Color::Info,
              .title = std::nullopt,
              .valueKey = "price_to_earnings"  // UI will check if valid to determine when to show flag
          },
          .strategyTypes = {"fundamental-analysis", "value-investing",
                            "screening", "factor-investing"},
          .assetRequirements = {"single-asset"},
          .usageContext =
              "Access pre-calculated financial ratios for valuation analysis. "
              "Use for fundamental screening (low P/E, high ROE), factor "
              "strategies, and relative value comparisons. Combine with price "
              "momentum for quality-value hybrids.",
          .limitations =
              "Ratios are calculated by Polygon based on most recent filings. "
              "Update frequency matches filing schedule (quarterly/annual). "
              "Only available for US equities. Cross-sectional comparisons "
              "require multiple node instances. Requires external data loader.",
      });

  // NOTE: Quotes and Trades transforms are not yet fully implemented
  // Backend data loading infrastructure (DataCategory, MetadataRegistry, clients)
  // needs to be completed before these can be enabled.
  // See: EpochDataSDK/include/epoch_data_sdk/common/enums.hpp (TickData category)

  return metadataList;
}

} // namespace epoch_script::transform
