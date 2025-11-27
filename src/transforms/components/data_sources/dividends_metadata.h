#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <epoch_data_sdk/dataloader/metadata_registry.hpp>
#include "data_category_mapper.h"
#include "metadata_helper.h"

namespace epoch_script::transform {

// Factory function to create metadata for Dividends data source
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeDividendsDataSource() {
  using namespace epoch_script::polygon;
  using namespace epoch_script::data_sources;

  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Get metadata from MetadataRegistry
  auto dataCategory = DataCategory::Dividends;
  auto sdkMetadata = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(dataCategory);

  // Build outputs and requiredDataSources from SDK metadata
  auto outputs = BuildOutputsFromSDKMetadata(sdkMetadata);
  auto requiredDataSources = BuildRequiredDataSourcesFromSDKMetadata(sdkMetadata);
  // Dividends data source
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = DIVIDENDS,
          .category = epoch_core::TransformCategory::DataSource,
          .plotKind = epoch_core::TransformPlotKind::flag,
          .name = "Dividends",
          .options = {
              MetaDataOption{
                  .id = "dividend_type",
                  .name = "Dividend Type",
                  .type = epoch_core::MetaDataOptionType::Select,
                  .defaultValue = MetaDataOptionDefinition(std::string("")),  // Empty = all types
                  .selectOption = {
                      {"All Types", ""},
                      {"Cash Dividend (CD)", "CD"},
                      {"Stock/Special Cash (SC)", "SC"},
                      {"Long-Term Capital Gain (LT)", "LT"},
                      {"Short-Term Capital Gain (ST)", "ST"},
                  },
                  .desc = "Filter by dividend type. Leave empty for all types."},
          },
          .isCrossSectional = false,
          .desc = sdkMetadata.description,
          .inputs = {},
          .outputs = outputs,
          .tags = {"dividends", "data", "source", "polygon", "corporate-actions"},
          .requiresTimeFrame = false,
          .requiredDataSources = requiredDataSources,
          .intradayOnly = IsIntradayOnlyCategory(dataCategory),  // Auto-computed from registry
          .allowNullInputs = true,  // Dividends are sparse - keep null rows for dates without dividends
          .flagSchema = epoch_script::transforms::FlagSchema{
              .icon = epoch_core::Icon::DollarSign,
              .text = "Dividend: ${cash_amount}<br/>Declared: {declaration_date}<br/>Pay Date: {pay_date}",
              .textIsTemplate = true,
              .color = epoch_core::Color::Success,
              .title = std::nullopt,
              .valueKey = "cash_amount"  // UI will check if valid to determine when to show flag
          },
          .strategyTypes = {"dividend-capture", "income", "fundamental"},
          .assetRequirements = {"single-asset"},
          .usageContext = "Access dividend distribution records for dividend capture strategies, income investing, or fundamental analysis. Track ex-dividend dates, payment dates, and dividend amounts.",
          .limitations = "Dividend data normalized to dates (no intraday precision). Historical dividend data may have adjustments or corrections."
      });

  return metadataList;
}

} // namespace epoch_script::transform
