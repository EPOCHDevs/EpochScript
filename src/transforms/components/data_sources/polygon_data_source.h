#pragma once

#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/dataframe.h>

namespace epoch_script::transform {

// Polygon data source transform
// Handles all Polygon data types (balance_sheet, income_statement, cash_flow, etc.)
// The specific data type is determined by the transform ID in the configuration
class PolygonDataSourceTransform final : public ITransform {
public:
  explicit PolygonDataSourceTransform(const TransformConfiguration &config)
      : ITransform(config) {
    // Build column rename mapping from Polygon API field names to output IDs
    for (auto const &outputMetaData : config.GetOutputs()) {
      m_replacements[outputMetaData.id] = config.GetOutputId(outputMetaData.id);
    }

    // Extract ticker for indices transforms (for early validation)
    auto transformName = config.GetTransformName();
    if (transformName == epoch_script::polygon::COMMON_INDICES) {
      m_ticker = config.GetOptionValue("ticker").GetSelectOption();
    } else if (transformName == epoch_script::polygon::INDICES) {
      m_ticker = config.GetOptionValue("ticker").GetString();
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &data) const override {
    // External loader has already fetched data from Polygon API
    // and converted to DataFrame with expected column names.
    // We just rename columns to match the node's output IDs.
    return data.rename(m_replacements);
  }

  // Override to expand {ticker} placeholder for indices transforms
  std::vector<std::string> GetRequiredDataSources() const override {
    auto requiredDataSources = ITransform::GetRequiredDataSources();

    // If ticker is set (indices transforms), replace placeholder
    if (!m_ticker.empty()) {
      for (auto& dataSource : requiredDataSources) {
        size_t pos = dataSource.find("{ticker}");
        if (pos != std::string::npos) {
          dataSource.replace(pos, 8, m_ticker);  // 8 = length of "{ticker}"
        }
      }
    }

    return requiredDataSources;
  }

private:
  std::string m_ticker;
  std::unordered_map<std::string, std::string> m_replacements;
};

// Type aliases for each Polygon data source (for backward compatibility and clarity)
using PolygonBalanceSheetTransform = PolygonDataSourceTransform;
using PolygonIncomeStatementTransform = PolygonDataSourceTransform;
using PolygonCashFlowTransform = PolygonDataSourceTransform;
using PolygonFinancialRatiosTransform = PolygonDataSourceTransform;
// NOTE: Quotes and Trades not yet fully implemented - backend data loading disabled
// using PolygonQuotesTransform = PolygonDataSourceTransform;
// using PolygonTradesTransform = PolygonDataSourceTransform;
using PolygonCommonIndicesTransform = PolygonDataSourceTransform;
using PolygonIndicesTransform = PolygonDataSourceTransform;

// New data source transforms (using MetadataRegistry)
using PolygonNewsTransform = PolygonDataSourceTransform;
using PolygonDividendsTransform = PolygonDataSourceTransform;
using PolygonSplitsTransform = PolygonDataSourceTransform;
using PolygonTickerEventsTransform = PolygonDataSourceTransform;
using PolygonShortInterestTransform = PolygonDataSourceTransform;
using PolygonShortVolumeTransform = PolygonDataSourceTransform;

} // namespace epoch_script::transform
