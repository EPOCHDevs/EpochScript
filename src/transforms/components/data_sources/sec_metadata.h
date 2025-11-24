#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

// Factory function to create metadata for all SEC data source transforms
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeSECDataSources() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // 1. Form 13F Institutional Holdings
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = "form13f_holdings",
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Form 13F Holdings",
          .options =
              {
                  MetaDataOption{
                      .id = "filing_type",
                      .name = "Filing Type",
                      .type = epoch_core::MetaDataOptionType::Select,
                      .selectOption =
                          {
                              {"13F-HR (Institutional Holdings)", "13F-HR"},
                              {"10-K (Annual Report)", "10-K"},
                              {"10-Q (Quarterly Report)", "10-Q"},
                              {"8-K (Current Report)", "8-K"},
                          },
                      .desc = "Filter by SEC filing type. 13F-HR is the standard institutional holdings report. "
                              "Other forms may contain ownership information in exhibits."},
                  MetaDataOption{
                      .id = "min_value",
                      .name = "Minimum Position Value",
                      .type = epoch_core::MetaDataOptionType::Decimal,
                      .defaultValue = MetaDataOptionDefinition(0.0),
                      .desc = "Minimum position value in USD to filter holdings. "
                              "Use to focus on large institutional positions (e.g., $10M+)"},
                  MetaDataOption{
                      .id = "institution_cik",
                      .name = "Institution CIK",
                      .type = epoch_core::MetaDataOptionType::String,
                      .desc = "Filter by specific institution's CIK (Central Index Key). "
                              "Example: 1067983 = Berkshire Hathaway, 1324404 = Citadel Advisors"},
              },
          .isCrossSectional = false,
          .desc = "Load SEC Form 13F institutional holdings data. "
                  "Track holdings reported by investment managers with $100M+ AUM. "
                  "Form 13F-HR is filed quarterly (45 days after quarter end) and "
                  "discloses long positions in US equities and convertible debt. "
                  "Data is automatically aggregated based on timeframe (EOD vs intraday).",
          .inputs = {},
          .outputs =
              {
                  {epoch_core::IODataType::Decimal, "shares",
                   "Number of Shares Held", true},
                  {epoch_core::IODataType::Decimal, "value",
                   "Position Value (USD)", true},
                  {epoch_core::IODataType::String, "security_type",
                   "Security Type (SH/PRN)", true},
                  {epoch_core::IODataType::String, "investment_discretion",
                   "Investment Discretion (SOLE/SHARED/DFND)", true},
                  {epoch_core::IODataType::String, "institution_name",
                   "Institution Name", true},
                  {epoch_core::IODataType::Timestamp, "period_end",
                   "Reporting Period End (Quarter End Date)", true},
              },
          .atLeastOneInputRequired = false,
          .tags = {"sec", "13f", "institutional", "holdings", "smart-money",
                   "fundamentals"},
          .requiresTimeFrame = true,
          .requiredDataSources = {"shares", "value", "security_type", "investment_discretion", "institution_name", "period_end"},
          .intradayOnly = false,
          .allowNullInputs = true,  // SEC filings are sparse (quarterly/annually) - keep null rows
          .strategyTypes = {"fundamental-analysis", "follow-smart-money",
                            "institutional-flow", "ownership-analysis"},
          .assetRequirements = {"single-asset"},
          .usageContext =
              "Track institutional ownership changes for follow-the-smart-money "
              "strategies. Monitor hedge fund and institutional portfolio changes "
              "quarterly. Identify concentrated ownership positions and sector "
              "crowding. Use to detect institutional accumulation/distribution "
              "patterns. Combine with price data for ownership-momentum strategies. "
              "Timeframe determines data aggregation: EOD timeframes aggregate to daily, "
              "intraday timeframes preserve second-level timestamps.",
          .limitations =
              "Quarterly filing frequency only (Q1-Q4). 45-day reporting lag "
              "after quarter end means holdings data is stale. Only long positions "
              "disclosed - short positions and derivatives not included. $100M+ AUM "
              "threshold excludes smaller managers. Position changes may be "
              "partially attributed to price movements vs. actual buying/selling. "
              "Requires external SEC-API data loader with API key.",
      });

  // 2. Insider Trading
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = "insider_trading",
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Insider Trading",
          .options =
              {
                  MetaDataOption{
                      .id = "filing_type",
                      .name = "Filing Type",
                      .type = epoch_core::MetaDataOptionType::Select,
                      .defaultValue = MetaDataOptionDefinition(std::string("All")),
                      .selectOption =
                          {
                              {"Form 3 (Initial Ownership)", "3"},
                              {"Form 4 (Transaction Report)", "4"},
                              {"Form 5 (Annual Summary)", "5"},
                              {"Form 144 (Restricted Sale)", "144"},
                          },
                      .desc = "Filter by SEC insider trading form type. Form 4 is most common (filed within 2 business days). "
                              "Form 3 = Initial beneficial ownership, Form 5 = Annual summary, Form 144 = Restricted securities sale."},
                  MetaDataOption{
                      .id = "transaction_code",
                      .name = "Transaction Type",
                      .type = epoch_core::MetaDataOptionType::Select,
                      .selectOption =
                          {
                              {"P - Open Market Purchase", "P"},
                              {"S - Open Market Sale", "S"},
                              {"A - Award/Grant (Rule 16b-3)", "A"},
                              {"M - Exercise/Conversion", "M"},
                              {"D - Disposition to Issuer", "D"},
                              {"F - Payment of Tax Liability", "F"},
                              {"I - Discretionary Transaction", "I"},
                              {"C - Conversion of Derivative", "C"},
                              {"G - Gift", "G"},
                              {"J - Other (See Description)", "J"},
                              {"V - Voluntary Early Report", "V"},
                          },
                      .desc = "Filter by transaction code. P=Open market purchase (bullish signal), "
                              "S=Sale (bearish, but may be for diversification), A=Award/Grant, M=Exercise. "
                              "Focus on P (purchases) for smart-money strategies."},
                  MetaDataOption{
                      .id = "min_value",
                      .name = "Minimum Transaction Value",
                      .type = epoch_core::MetaDataOptionType::Decimal,
                      .defaultValue = MetaDataOptionDefinition(0.0),
                      .desc = "Minimum transaction value in USD to filter trades. "
                              "Use to focus on significant insider purchases (e.g., $100K+). "
                              "Calculated as shares * price."},
                  MetaDataOption{
                      .id = "owner_name",
                      .name = "Insider Name",
                      .type = epoch_core::MetaDataOptionType::String,
                      .defaultValue = MetaDataOptionDefinition(std::string("")),
                      .desc = "Filter by specific insider's name (officer, director, or 10%+ owner). "
                              "Use to track transactions by key executives or major shareholders."},
              },
          .isCrossSectional = false,
          .desc = "Load SEC insider trading data from Forms 3, 4, 5, and 144. "
                  "Track transactions made by company insiders (officers, directors, "
                  "10%+ owners). Form 4 filed within 2 business days of transaction. "
                  "Use for insider sentiment and smart-money signals. "
                  "Data is automatically aggregated based on timeframe (EOD vs intraday).",
          .inputs = {},
          .outputs =
              {
                  {epoch_core::IODataType::Timestamp, "transaction_date",
                   "Transaction Date (When Trade Occurred)", true},
                  {epoch_core::IODataType::String, "owner_name",
                   "Insider Name", true},
                  {epoch_core::IODataType::String, "transaction_code",
                   "Transaction Code (P/S/A/M)", true},
                  {epoch_core::IODataType::Decimal, "shares",
                   "Number of Shares", true},
                  {epoch_core::IODataType::Decimal, "price",
                   "Price Per Share", true},
                  {epoch_core::IODataType::Decimal, "ownership_after",
                   "Ownership After Transaction", true},
              },
          .atLeastOneInputRequired = false,
          .tags = {"sec", "insider", "trading", "form-4", "smart-money",
                   "sentiment"},
          .requiresTimeFrame = true,
          .requiredDataSources = {"transaction_date", "owner_name", "transaction_code", "shares", "price", "ownership_after"},
          .strategyTypes = {"insider-sentiment", "smart-money",
                            "signal-generation", "event-driven"},
          .assetRequirements = {"single-asset"},
          .usageContext =
              "Track insider buying/selling for sentiment signals. Insider "
              "purchases are generally bullish signals (insiders buying on private "
              "information or confidence). Cluster of insider buys can signal "
              "undervaluation. Focus on open-market purchases (code P) vs. automatic "
              "sales (10b5-1 plans). Large purchases or director/CEO buys carry more "
              "weight. Aggregate multiple insider transactions for stronger signals. "
              "Combine with price momentum for confirmation. "
              "Timeframe determines data aggregation: EOD timeframes aggregate to daily, "
              "intraday timeframes preserve second-level timestamps.",
          .limitations =
              "2-day reporting lag for Form 4 means some timing delay. Doesn't "
              "capture all insider activity - derivatives and indirect holdings may "
              "be excluded. Pre-arranged trading plans (Rule 10b5-1) dilute signal "
              "quality as sales may be scheduled regardless of outlook. Sales can be "
              "for tax/diversification reasons, not bearish views. Transaction codes "
              "are complex - not all transactions are open-market buys/sells. "
              "Requires external SEC-API data loader with API key.",
      });

  return metadataList;
}

} // namespace epoch_script::transform
