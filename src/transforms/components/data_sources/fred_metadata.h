#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include "../data_source.h"
#include "metadata_helper.h"
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>

namespace epoch_script::transform {

// Factory function to create metadata for FRED economic data source
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeFREDDataSource() {
  using namespace epoch_script::data_sources;
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry for ALFRED (FRED with revision tracking)
  auto sdkMetadata = data_sdk::dataloader::MetadataRegistry::GetAlfredMetadata();

  // Build outputs from SDK metadata
  auto outputs = BuildOutputsFromSDKMetadata(sdkMetadata);

  // Build required data sources from SDK metadata with template placeholder
  // This creates strings like "ECON:{category}:observation_date"
  auto requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(sdkMetadata, "category");

  // Single FRED transform with category SelectOption
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = epoch_script::fred::ECONOMIC_INDICATOR,
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Economic Indicator",
          .options =
              {
                  MetaDataOption{
                      .id = "category",
                      .name = "Economic Indicator",
                      .type = epoch_core::MetaDataOptionType::Select,
                      .defaultValue =
                          MetaDataOptionDefinition(std::string("CPI")),
                      .selectOption =
                          {
                              // Inflation Indicators
                              {"Consumer Price Index (CPI-U)", "CPI"},
                              {"Core CPI (ex Food & Energy)", "CoreCPI"},
                              {"Personal Consumption Expenditures Price Index",
                               "PCE"},
                              {"Core PCE (Fed's Preferred Measure)", "CorePCE"},

                              // Interest Rates & Monetary Policy
                              {"Federal Funds Effective Rate", "FedFunds"},
                              {"3-Month Treasury Bill Rate", "Treasury3M"},
                              {"2-Year Treasury Rate", "Treasury2Y"},
                              {"5-Year Treasury Rate", "Treasury5Y"},
                              {"10-Year Treasury Rate", "Treasury10Y"},
                              {"30-Year Treasury Rate", "Treasury30Y"},

                              // Employment & Labor Market
                              {"Unemployment Rate", "Unemployment"},
                              {"Nonfarm Payrolls", "NonfarmPayrolls"},
                              {"Initial Jobless Claims (Weekly)", "InitialClaims"},

                              // Economic Growth & Production
                              {"Real Gross Domestic Product", "GDP"},
                              {"Industrial Production Index",
                               "IndustrialProduction"},
                              {"Retail Sales", "RetailSales"},
                              {"Housing Starts", "HousingStarts"},

                              // Market Sentiment & Money Supply
                              {"Consumer Sentiment (University of Michigan)",
                               "ConsumerSentiment"},
                              {"M2 Money Supply", "M2"}
                          },
                      .desc = "Select the economic indicator series to load"},
              },
          .isCrossSectional = false,
          .desc =
              "Load Federal Reserve Economic Data (FRED) for macro analysis. "
              "Provides economic indicators like inflation, interest rates, "
              "GDP, employment data, and market indices. Non-asset-specific - "
              "applies globally to strategy.",
          .inputs = {},
          .outputs = outputs,
          .atLeastOneInputRequired = false,
          .tags = {"fred", "macro", "economic-indicators", "inflation",
                   "interest-rates", "gdp", "employment"},
          .requiresTimeFrame = true,
          .requiredDataSources = requiredDataSources,
          .intradayOnly = false,
          .allowNullInputs = false,
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::LineChart,
              .text = "Economic Indicator<br/>Value: {value}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Info
          },
          .strategyTypes = {"macro-analysis", "regime-detection",
                            "economic-calendar", "risk-on-risk-off"},
          .assetRequirements = {},
          .usageContext =
              "Access Federal Reserve economic data for macro-driven "
              "strategies. Date range auto-derived from connected market data. "
              "Returns publication events with revision tracking - includes "
              "published_at timestamp to avoid look-ahead bias. Each observation "
              "date may have multiple rows showing how data was revised over time. "
              "Use for economic cycle identification, monetary policy regime "
              "detection, and risk-on/risk-off switching. Combine inflation + "
              "rates for policy stance, unemployment + GDP for cycle phase. "
              "Requires connection to market data source.",
          .limitations =
              "Publication frequency varies: daily (rates/VIX), weekly (claims), "
              "monthly (CPI/employment), quarterly (GDP). Significant lag between "
              "period end and publication (weeks to months). Values appear ONLY "
              "on publication dates (not forward-filled). FRED data is US-centric. "
              "Uses ALFRED API for point-in-time data with revision tracking - "
              "each observation_date may have multiple revisions over time. "
              "Requires external FRED data loader with API key.",
      });

  return metadataList;
}

} // namespace epoch_script::transform
