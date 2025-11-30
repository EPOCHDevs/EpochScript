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
#include <string>
#include <algorithm>
#include <cctype>

namespace epoch_script::transform {

/**
 * Evaluate if asset matches ticker filter
 *
 * @param assetId The asset ID to check (e.g., "AAPL", "SPY")
 * @param tickerFilter Single ticker to match (e.g., "SPY")
 * @return true if assetId matches tickerFilter (case-insensitive)
 */
inline bool EvaluateAssetRefTicker(const std::string &assetId,
                                    const std::string &tickerFilter) {
  if (tickerFilter.empty()) {
    return true; // Empty filter matches all
  }

  if (assetId.empty()) {
    return false;
  }

  // Case-insensitive exact match
  std::string filterUpper = tickerFilter;
  std::string assetUpper = assetId;
  std::transform(filterUpper.begin(), filterUpper.end(), filterUpper.begin(),
                 [](unsigned char c) { return std::toupper(c); });
  std::transform(assetUpper.begin(), assetUpper.end(), assetUpper.begin(),
                 [](unsigned char c) { return std::toupper(c); });

  return filterUpper == assetUpper;
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
 *   - asset_class: Filter by asset class (Stocks, Crypto, Futures, FX, Indices)
 *   - sector: Filter by sector
 *
 * Inputs:
 *   - SLOT: Input series to filter (any datatype)
 *
 * Outputs:
 *   - passthrough: Filtered output (only for matching assets)
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
using AssetRefPassthroughDateTime = AssetRefPassthrough<epoch_core::IODataType::DateTime>;

} // namespace epoch_script::transform
