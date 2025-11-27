//
// Data Category Mapper Implementation
//

#include "data_category_mapper.h"
#include "epoch_script/core/constants.h"

// Include headers for transform ID constants
#include "news_metadata.h"
#include "dividends_metadata.h"
#include "splits_metadata.h"
#include "ticker_events_metadata.h"
#include "short_interest_metadata.h"
#include "short_volume_metadata.h"

#include <unordered_map>

namespace epoch_script::data_sources {

std::optional<DataCategory> GetDataCategoryForTransform(
    std::string const& transformId) {

  using namespace epoch_script::polygon;
  using namespace epoch_script::fred;

  // NOTE: Time-series transforms (market_data_source, vwap, trade_count, indices, etc.)
  // are NOT mapped here. They represent the PRIMARY category (MinuteBars/DailyBars)
  // which is determined by IsIntradayCampaign() in the strategy analysis.
  // This function only maps AUXILIARY data categories.

  // Polygon Financials - map to granular categories
  if (transformId == BALANCE_SHEET) {
    return DataCategory::BalanceSheets;
  }
  if (transformId == INCOME_STATEMENT) {
    return DataCategory::IncomeStatements;
  }
  if (transformId == CASH_FLOW) {
    return DataCategory::CashFlowStatements;
  }
  if (transformId == FINANCIAL_RATIOS) {
    return DataCategory::Ratios;
  }

  // Corporate Actions & Events
  if (transformId == polygon::NEWS) {
    return DataCategory::News;
  }
  if (transformId == polygon::DIVIDENDS) {
    return DataCategory::Dividends;
  }
  if (transformId == polygon::SPLITS) {
    return DataCategory::Splits;
  }
  if (transformId == polygon::TICKER_EVENTS) {
    return DataCategory::TickerEvents;
  }

  // Short Interest & Volume
  if (transformId == polygon::SHORT_INTEREST) {
    return DataCategory::ShortInterest;
  }
  if (transformId == polygon::SHORT_VOLUME) {
    return DataCategory::ShortVolume;
  }

  // ReferenceAgg transforms (Indices, Stocks, FX, Crypto)
  if (transformId == polygon::COMMON_INDICES ||
      transformId == polygon::INDICES ||
      transformId == polygon::COMMON_REFERENCE_STOCKS ||
      transformId == polygon::REFERENCE_STOCKS ||
      transformId == polygon::COMMON_FX_PAIRS ||
      transformId == polygon::FX_PAIRS ||
      transformId == polygon::COMMON_CRYPTO_PAIRS ||
      transformId == polygon::CRYPTO_PAIRS) {
    return DataCategory::ReferenceAgg;
  }

  return std::nullopt;
}

bool IsIntradayOnlyCategory(DataCategory category) {
  // Get metadata from the registry
  auto metadata = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(category);

  // intradayOnly = !index_normalized
  // If index is NOT normalized (has time-of-day), it's intraday-only
  return !metadata.index_normalized;
}

std::string GetCategoryPrefix(DataCategory category) {
  // Get metadata from the registry
  auto metadata = data_sdk::dataloader::MetadataRegistry::GetMetadataForCategory(category);

  return metadata.category_prefix;
}

} // namespace epoch_script::data_sources
