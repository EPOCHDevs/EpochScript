#pragma once

#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/dataframe.h>
#include <unordered_map>

namespace epoch_script::transform {

// Transform for FRED economic indicators
// Cross-sectional transform: receives data for all assets, returns single-column broadcast
// External loader fetches FRED data based on date range from input data
class FREDTransform final : public ITransform {
public:
  explicit FREDTransform(const TransformConfiguration &config)
      : ITransform(config),
        m_category(config.GetOptionValue("category").GetSelectOption()) {
    // Build column rename mapping from FRED API field names to output IDs
    for (auto const &outputMetaData : config.GetOutputs()) {
      m_replacements[outputMetaData.id] = config.GetOutputId(outputMetaData.id);
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &fred_data) const override {
    // External loader provides FRED data already indexed and formatted
    // Just rename columns to match the node's output IDs
    return fred_data.rename(m_replacements);
  }

  // Override to expand {category} placeholder in requiredDataSources
  std::vector<std::string> GetRequiredDataSources() const override {
    auto requiredDataSources = ITransform::GetRequiredDataSources();

    // Replace {category} with actual category value
    for (auto& dataSource : requiredDataSources) {
      size_t pos = dataSource.find("{category}");
      if (pos != std::string::npos) {
        dataSource.replace(pos, 10, m_category);  // 10 = length of "{category}"
      }
    }

    return requiredDataSources;
  }

private:
  std::string m_category;
  std::unordered_map<std::string, std::string> m_replacements;
};

// Category to FRED Series ID mapping for external loader reference
// External loader uses this to map user's category selection to FRED API series_id
inline const std::unordered_map<std::string, std::string> FRED_SERIES_MAP{
    // Inflation Indicators
    {"CPI", "CPIAUCSL"},
    {"CoreCPI", "CPILFESL"},
    {"PCE", "PCEPI"},
    {"CorePCE", "PCEPILFE"},

    // Interest Rates & Monetary Policy
    {"FedFunds", "DFF"},
    {"Treasury3M", "DTB3"},
    {"Treasury2Y", "DGS2"},
    {"Treasury5Y", "DGS5"},
    {"Treasury10Y", "DGS10"},
    {"Treasury30Y", "DGS30"},

    // Employment & Labor Market
    {"Unemployment", "UNRATE"},
    {"NonfarmPayrolls", "PAYEMS"},
    {"InitialClaims", "ICSA"},

    // Economic Growth & Production
    {"GDP", "GDPC1"},
    {"IndustrialProduction", "INDPRO"},
    {"RetailSales", "RSXFS"},
    {"HousingStarts", "HOUST"},

    // Market Sentiment & Money Supply
    {"ConsumerSentiment", "UMCSENT"},
    {"M2", "M2SL"},
    {"SP500", "SP500"},
    {"VIX", "VIXCLS"},
};

} // namespace epoch_script::transform
