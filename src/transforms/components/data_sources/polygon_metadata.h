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
          .options = {},
          .isCrossSectional = false,
          .desc = "Load balance sheet fundamental data. "
                  "Provides assets, liabilities, equity, and other balance "
                  "sheet metrics over time.",
          .inputs = {},
          .outputs =
              {
                  {epoch_core::IODataType::Decimal, "accounts_payable",
                   "Accounts Payable", true},
                  {epoch_core::IODataType::Decimal, "accrued_liabilities",
                   "Accrued & Other Current Liabilities", true},
                  {epoch_core::IODataType::Decimal, "aoci",
                   "Accumulated Other Comprehensive Income", true},
                  {epoch_core::IODataType::Decimal, "cash",
                   "Cash and Equivalents", true},
                  {epoch_core::IODataType::String, "cik", "CIK", true},
                  {epoch_core::IODataType::Decimal, "current_debt",
                   "Current Debt", true},
                  {epoch_core::IODataType::Decimal, "deferred_revenue",
                   "Deferred Revenue (Current)", true},
                  {epoch_core::IODataType::Integer, "fiscal_quarter",
                   "Fiscal Quarter", true},
                  {epoch_core::IODataType::Integer, "fiscal_year",
                   "Fiscal Year", true},
                  {epoch_core::IODataType::Decimal, "inventories", "Inventories",
                   true},
                  {epoch_core::IODataType::Decimal, "long_term_debt",
                   "Long Term Debt & Capital Lease", true},
                  {epoch_core::IODataType::Decimal, "other_current_assets",
                   "Other Current Assets", true},
                  {epoch_core::IODataType::Decimal, "other_ltl",
                   "Other Non-Current Liabilities", true},
                  {epoch_core::IODataType::Timestamp, "period_end", "Period End",
                   true},
                  {epoch_core::IODataType::Decimal, "ppe_net",
                   "Property Plant Equipment Net", true},
                  {epoch_core::IODataType::Decimal, "receivables", "Receivables",
                   true},
                  {epoch_core::IODataType::Decimal, "retained_earnings",
                   "Retained Earnings (Deficit)", true},
                  {epoch_core::IODataType::String, "timeframe", "Timeframe",
                   true},
              },
          .atLeastOneInputRequired = false,
          .tags = {"fundamentals", "balance-sheet", "financial-statements"},
          .requiresTimeFrame = true,
          .requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(balanceSheetMeta),
          .intradayOnly = IsIntradayOnlyCategory(DataCategory::BalanceSheets),
          .allowNullInputs = true,  // Data sources should preserve null rows
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::FileText,
              .text = "Q{fiscal_quarter} {fiscal_year} Balance Sheet<br/>Cash: ${cash}<br/>Debt: ${long_term_debt}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Info
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
          .options = {},
          .isCrossSectional = false,
          .desc = "Load income statement fundamental data . "
                  "Provides revenue, expenses, earnings, and profitability "
                  "metrics over time.",
          .inputs = {},
          .outputs =
              {
                  {epoch_core::IODataType::Decimal, "basic_eps",
                   "Basic EPS", true},
                  {epoch_core::IODataType::Decimal, "basic_shares",
                   "Basic Shares Outstanding", true},
                  {epoch_core::IODataType::String, "cik", "CIK", true},
                  {epoch_core::IODataType::Decimal, "net_income",
                   "Consolidated Net Income", true},
                  {epoch_core::IODataType::Decimal, "cogs",
                   "Cost of Revenue", true},
                  {epoch_core::IODataType::Decimal, "diluted_eps",
                   "Diluted EPS", true},
                  {epoch_core::IODataType::Decimal, "diluted_shares",
                   "Diluted Shares Outstanding", true},
                  {epoch_core::IODataType::Integer, "fiscal_quarter",
                   "Fiscal Quarter", true},
                  {epoch_core::IODataType::Integer, "fiscal_year",
                   "Fiscal Year", true},
                  {epoch_core::IODataType::Decimal, "gross_profit",
                   "Gross Profit", true},
                  {epoch_core::IODataType::Decimal, "ebt",
                   "Income Before Taxes", true},
                  {epoch_core::IODataType::Decimal, "income_tax",
                   "Income Taxes", true},
                  {epoch_core::IODataType::Decimal, "ni_common",
                   "Net Income (Common Shareholders)", true},
                  {epoch_core::IODataType::Decimal, "operating_income",
                   "Operating Income", true},
                  {epoch_core::IODataType::Decimal, "other_income",
                   "Other Income/Expenses", true},
                  {epoch_core::IODataType::Decimal, "other_opex",
                   "Other Operating Expenses", true},
                  {epoch_core::IODataType::Timestamp, "period_end", "Period End",
                   true},
                  {epoch_core::IODataType::Decimal, "rnd",
                   "R&D Expenses", true},
                  {epoch_core::IODataType::Decimal, "revenue", "Revenue", true},
                  {epoch_core::IODataType::Decimal, "sga",
                   "SG&A Expenses", true},
                  {epoch_core::IODataType::String, "timeframe", "Timeframe",
                   true},
              },
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
              .color = epoch_core::Color::Info
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
          .options = {},
          .isCrossSectional = false,
          .desc = "Load cash flow statement fundamental data . "
                  "Provides operating, investing, and financing cash flows to "
                  "analyze liquidity and capital allocation.",
          .inputs = {},
          .outputs =
              {
                  {epoch_core::IODataType::Decimal, "cfo_cont",
                   "Operating Cash Flow", true},
                  {epoch_core::IODataType::Decimal, "change_in_cash",
                   "Change in Cash & Equivalents", true},
                  {epoch_core::IODataType::Decimal, "change_in_wc",
                   "Change in Other Operating Assets/Liabilities", true},
                  {epoch_core::IODataType::String, "cik", "CIK", true},
                  {epoch_core::IODataType::Decimal, "dna",
                   "D&A", true},
                  {epoch_core::IODataType::Decimal, "dividends", "Dividends",
                   true},
                  {epoch_core::IODataType::Integer, "fiscal_quarter",
                   "Fiscal Quarter", true},
                  {epoch_core::IODataType::Integer, "fiscal_year",
                   "Fiscal Year", true},
                  {epoch_core::IODataType::Decimal, "lt_debt_net",
                   "Long Term Debt Issuances/Repayments", true},
                  {epoch_core::IODataType::Decimal, "cff",
                   "Net Cash from Financing", true},
                  {epoch_core::IODataType::Decimal, "cff_cont",
                   "Net Cash from Financing (Continuing)", true},
                  {epoch_core::IODataType::Decimal, "cfi",
                   "Net Cash from Investing", true},
                  {epoch_core::IODataType::Decimal, "cfi_cont",
                   "Net Cash from Investing (Continuing)", true},
                  {epoch_core::IODataType::Decimal, "cfo",
                   "Net Cash from Operating", true},
                  {epoch_core::IODataType::Decimal, "net_income",
                   "Net Income", true},
                  {epoch_core::IODataType::Decimal, "other_cff",
                   "Other Financing Activities", true},
                  {epoch_core::IODataType::Decimal, "other_cfi",
                   "Other Investing Activities", true},
                  {epoch_core::IODataType::Decimal, "other_cfo",
                   "Other Operating Activities", true},
                  {epoch_core::IODataType::Timestamp, "period_end", "Period End",
                   true},
                  {epoch_core::IODataType::Decimal, "capex", "CapEx", true},
                  {epoch_core::IODataType::Decimal, "st_debt_net",
                   "Short Term Debt Issuances/Repayments", true},
                  {epoch_core::IODataType::String, "timeframe", "Timeframe",
                   true},
              },
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
              .color = epoch_core::Color::Info
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
          .outputs =
              {
                  {epoch_core::IODataType::Decimal, "avg_volume",
                   "Average Volume", true},
                  {epoch_core::IODataType::Decimal, "cash", "Cash", true},
                  {epoch_core::IODataType::String, "cik", "CIK", true},
                  {epoch_core::IODataType::Decimal, "current_ratio",
                   "Current Ratio", true},
                  {epoch_core::IODataType::String, "date", "Date", true},
                  {epoch_core::IODataType::Decimal, "debt_equity",
                   "Debt to Equity", true},
                  {epoch_core::IODataType::Decimal, "div_yield",
                   "Dividend Yield", true},
                  {epoch_core::IODataType::Decimal, "eps", "EPS", true},
                  {epoch_core::IODataType::Decimal, "ev",
                   "Enterprise Value", true},
                  {epoch_core::IODataType::Decimal, "ev_ebitda",
                   "EV/EBITDA", true},
                  {epoch_core::IODataType::Decimal, "ev_sales", "EV/Sales",
                   true},
                  {epoch_core::IODataType::Decimal, "fcf",
                   "Free Cash Flow", true},
                  {epoch_core::IODataType::Decimal, "market_cap", "Market Cap",
                   true},
                  {epoch_core::IODataType::Decimal, "price", "Price", true},
                  {epoch_core::IODataType::Decimal, "pb",
                   "Price to Book", true},
                  {epoch_core::IODataType::Decimal, "pcf",
                   "Price to Cash Flow", true},
                  {epoch_core::IODataType::Decimal, "pe",
                   "Price to Earnings", true},
                  {epoch_core::IODataType::Decimal, "pfcf",
                   "Price to Free Cash Flow", true},
                  {epoch_core::IODataType::Decimal, "ps",
                   "Price to Sales", true},
                  {epoch_core::IODataType::Decimal, "quick_ratio",
                   "Quick Ratio", true},
                  {epoch_core::IODataType::Decimal, "roa", "ROA", true},
                  {epoch_core::IODataType::Decimal, "roe", "ROE", true},
                  {epoch_core::IODataType::String, "ticker", "Ticker", true},
              },
          .atLeastOneInputRequired = false,
          .tags = { "fundamentals", "ratios", "valuation", "screening"},
          .requiresTimeFrame = true,
          .requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(ratiosMeta),
          .intradayOnly = IsIntradayOnlyCategory(DataCategory::Ratios),
          .allowNullInputs = true,  // Data sources should preserve null rows
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::Calculator,
              .text = "Valuation Ratios<br/>P/E: {pe}<br/>ROE: {roe}%",
              .textIsTemplate = true,
              .color = epoch_core::Color::Info
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
