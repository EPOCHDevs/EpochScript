//
// Asset Reference Passthrough Transform
//
// Filters data by asset matching criteria and passes through for matching assets.
// Non-matching assets are skipped entirely (no output stored).
//
// This transform is handled specially by the execution layer:
// - For matching assets: input is passed through with column renamed to output ID
// - For non-matching assets: nothing is stored
//

#pragma once

#include <epoch_script/transforms/core/itransform.h>
#include <epoch_data_sdk/model/asset/asset_database.hpp>
#include <string>
#include <algorithm>
#include <cctype>

namespace epoch_script::transform {

/**
 * Asset filter options - all optional, evaluated at runtime
 */
struct AssetFilterOptions {
  std::string ticker{};          // Filter by ticker (e.g., "SPY")
  std::string asset_class{};     // Filter by asset class (Stocks, Crypto, FX)
  std::string sector{};          // Filter by sector (e.g., "Technology")
  std::string industry{};        // Filter by industry (e.g., "Software")
  std::string base_currency{};   // Filter FX by base currency (e.g., "EUR")
  std::string counter_currency{}; // Filter FX by counter currency (e.g., "USD")
};

/**
 * Case-insensitive string comparison
 */
inline bool CaseInsensitiveEquals(const std::string &a, const std::string &b) {
  if (a.size() != b.size()) return false;
  return std::equal(a.begin(), a.end(), b.begin(),
                    [](char ca, char cb) {
                      return std::toupper(static_cast<unsigned char>(ca)) ==
                             std::toupper(static_cast<unsigned char>(cb));
                    });
}

/**
 * Evaluate if asset matches ticker filter (case-insensitive)
 *
 * @param assetId The asset ID to check (e.g., "AAPL", "SPY")
 * @param tickerFilter Single ticker to match (e.g., "SPY")
 * @return true if assetId matches tickerFilter
 */
inline bool EvaluateAssetRefTicker(const std::string &assetId,
                                   const std::string &tickerFilter) {
  if (tickerFilter.empty()) {
    return true; // Empty filter matches all
  }

  if (assetId.empty()) {
    return false;
  }

  // Case-insensitive match - supports both exact match and prefix match
  std::string filterUpper = tickerFilter;
  std::string assetUpper = assetId;
  std::transform(filterUpper.begin(), filterUpper.end(), filterUpper.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  std::transform(assetUpper.begin(), assetUpper.end(), assetUpper.begin(),
                 [](unsigned char c) { return std::toupper(c); });

  // Exact match
  if (filterUpper == assetUpper) {
    return true;
  }

  // Prefix match: "AAPL" matches "AAPL-Stock", "AAPL-Crypto", etc.
  if (assetUpper.starts_with(filterUpper + "-")) {
    return true;
  }

  return false;
}

/**
 * Evaluate all asset filters against an AssetSpecification
 *
 * Uses AssetSpecificationDatabase to look up asset metadata for filtering.
 * This is the central filter function - add new filters here.
 *
 * @param assetId The asset ID to check
 * @param filters Filter options to apply
 * @return true if asset matches ALL specified filters
 */
inline bool EvaluateAssetFilters(const std::string &assetId,
                                 const AssetFilterOptions &filters) {
  // Ticker filter is evaluated directly (case-insensitive string match)
  if (!EvaluateAssetRefTicker(assetId, filters.ticker)) {
    return false;
  }

  // If no database filters are specified, we're done
  if (filters.asset_class.empty() && filters.sector.empty() &&
      filters.industry.empty() && filters.base_currency.empty() &&
      filters.counter_currency.empty()) {
    return true;
  }

  // Look up asset specification from database
  try {
    const auto &db = data_sdk::asset::AssetSpecificationDatabase::GetInstance();
    auto spec = db.GetAssetSpecification(data_sdk::Symbol{assetId});

    // Asset class filter
    if (!filters.asset_class.empty()) {
      auto assetClass = spec.GetAssetClass();
      auto assetClassStr = epoch_core::AssetClassWrapper::ToLongFormString(assetClass);
      if (!CaseInsensitiveEquals(assetClassStr, filters.asset_class)) {
        return false;
      }
    }

    // Sector filter
    if (!filters.sector.empty()) {
      if (!CaseInsensitiveEquals(spec.GetSector(), filters.sector)) {
        return false;
      }
    }

    // Industry filter
    if (!filters.industry.empty()) {
      if (!CaseInsensitiveEquals(spec.GetIndustry(), filters.industry)) {
        return false;
      }
    }

    // Currency pair filters (for FX assets)
    if (!filters.base_currency.empty() || !filters.counter_currency.empty()) {
      auto currencyPair = spec.GetCurrencyPair();
      if (!currencyPair.has_value()) {
        // Asset doesn't have a currency pair, fail currency filters
        return false;
      }

      // Base currency is index 0
      if (!filters.base_currency.empty()) {
        auto baseCurrency = (*currencyPair)[0].ToString();
        if (!CaseInsensitiveEquals(baseCurrency, filters.base_currency)) {
          return false;
        }
      }

      // Counter currency is index 1
      if (!filters.counter_currency.empty()) {
        auto counterCurrency = (*currencyPair)[1].ToString();
        if (!CaseInsensitiveEquals(counterCurrency, filters.counter_currency)) {
          return false;
        }
      }
    }

    return true;
  } catch (...) {
    // Asset not found in database - only match if ticker filter matched
    // (already checked above) and no other filters specified
    return filters.asset_class.empty() && filters.sector.empty() &&
           filters.industry.empty() && filters.base_currency.empty() &&
           filters.counter_currency.empty();
  }
}

/**
 * Legacy function for backwards compatibility
 * @deprecated Use EvaluateAssetFilters with AssetFilterOptions instead
 */
inline bool EvaluateAssetRefClass(const std::string &assetId,
                                  const std::string &assetClassFilter) {
  AssetFilterOptions opts;
  opts.asset_class = assetClassFilter;
  return EvaluateAssetFilters(assetId, opts);
}

/**
 * Asset Reference Passthrough Transform
 *
 * A specialized transform that filters data based on asset matching criteria.
 * Unlike regular transforms, this is handled specially by the execution layer:
 *
 * - For matching assets: input data is passed through with column renamed to output ID
 * - For non-matching assets: nothing is stored (skipped entirely)
 *
 * Options:
 *   - ticker: Single ticker to match (e.g., "SPY")
 *   - asset_class: Filter by asset class (Stocks, Crypto, FX)
 *   - sector: Filter by sector
 *   - industry: Filter by industry
 *   - base_currency: Filter FX by base currency
 *   - counter_currency: Filter FX by counter currency
 *
 * Inputs:
 *   - SLOT: Input series to filter (any datatype)
 *
 * Outputs:
 *   - result: Filtered output (only for matching assets)
 *
 * This transform is detected and routed specially in the execution layer.
 * TransformData should never be called directly.
 */
template<epoch_core::IODataType DataType>
class AssetRefPassthrough final : public ITransform {
public:
  explicit AssetRefPassthrough(const TransformConfiguration &config)
      : ITransform(config) {
    // Options are read by the execution layer
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const & /*df*/) const override {
    // This should never be called - handled by ApplyAssetRefPassthroughTransform
    throw std::runtime_error(
        "AssetRefPassthrough::TransformData should not be called directly. "
        "asset_ref_passthrough transforms are handled in the execution layer.");
  }
};

// Type aliases for each datatype specialization
using AssetRefPassthroughNumber = AssetRefPassthrough<epoch_core::IODataType::Number>;
using AssetRefPassthroughBoolean = AssetRefPassthrough<epoch_core::IODataType::Boolean>;
using AssetRefPassthroughString = AssetRefPassthrough<epoch_core::IODataType::String>;
using AssetRefPassthroughTimestamp = AssetRefPassthrough<epoch_core::IODataType::Timestamp>;

/**
 * Is Asset Reference Transform
 *
 * Returns a boolean series indicating if the current asset matches the filter criteria.
 * Unlike AssetRefPassthrough, this outputs for ALL assets:
 * - Matching assets: outputs all true
 * - Non-matching assets: outputs all false
 *
 * This is a scalar-optimized transform - the boolean value is constant for all rows.
 * Handled specially by the execution layer which has access to the current asset ID.
 *
 * Options:
 *   - ticker: Ticker to match
 *   - asset_class: Filter by asset class (Stocks, Crypto, FX)
 *   - sector: Filter by sector
 *   - industry: Filter by industry
 *   - base_currency: Filter FX by base currency
 *   - counter_currency: Filter FX by counter currency
 *
 * Inputs:
 *   - (none - this is a scalar)
 *
 * Outputs:
 *   - result: Boolean series (true if asset matches, false otherwise)
 */
class IsAssetRef final : public ITransform {
public:
  explicit IsAssetRef(const TransformConfiguration &config)
      : ITransform(config) {
    // Options are read by the execution layer
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const & /*df*/) const override {
    // This should never be called - handled by execution layer
    throw std::runtime_error(
        "IsAssetRef::TransformData should not be called directly. "
        "is_asset_ref transforms are handled in the execution layer.");
  }
};

} // namespace epoch_script::transform
