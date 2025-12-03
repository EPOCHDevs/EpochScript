#pragma once
// Utility transforms registration
// Provides asset filtering and reference transforms
//
// Categories:
// 1. Asset Reference - Filter data by asset characteristics
//    - asset_ref_passthrough: Pass data only for matching assets
//    - is_asset_ref: Boolean check if current asset matches criteria
//
// These transforms are handled specially by the execution layer.
// They filter/flag data based on asset metadata (ticker, sector, asset class, etc.)

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "asset_ref_passthrough.h"
#include "asset_ref_passthrough_metadata.h"

namespace epoch_script::transform::utility {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all utility transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // ASSET REFERENCE - Data Filtering by Asset
    // =========================================================================
    // Filter or flag data based on asset characteristics.
    // Useful for creating asset-specific logic in multi-asset strategies.

    // asset_ref_passthrough: Pass data only for matching assets (numeric)
    // Input: Numeric series
    // Options: ticker, asset_class, sector, industry, base_currency, counter_currency
    // Outputs: Same series, but only for matching assets (others get no output)
    // Use for: Isolating specific asset data in multi-asset context,
    //          creating asset-specific signals or filters
    // Note: Handled specially in execution layer - TransformData never called
    epoch_script::transform::Register<AssetRefPassthroughNumber>("asset_ref_passthrough");

    // asset_ref_passthrough_bool: Pass data only for matching assets (boolean)
    // Input: Boolean series
    // Options: ticker, asset_class, sector, industry
    // Outputs: Same series for matching assets only
    epoch_script::transform::Register<AssetRefPassthroughBoolean>("asset_ref_passthrough_bool");

    // asset_ref_passthrough_string: Pass data only for matching assets (string)
    // Input: String series
    // Options: ticker, asset_class, sector, industry
    // Outputs: Same series for matching assets only
    epoch_script::transform::Register<AssetRefPassthroughString>("asset_ref_passthrough_string");

    // asset_ref_passthrough_timestamp: Pass data only for matching assets (timestamp)
    // Input: Timestamp series
    // Options: ticker, asset_class, sector, industry
    // Outputs: Same series for matching assets only
    epoch_script::transform::Register<AssetRefPassthroughTimestamp>("asset_ref_passthrough_timestamp");

    // is_asset_ref: Boolean flag indicating if current asset matches criteria
    // Input: None (scalar transform - operates on asset metadata)
    // Options: ticker, asset_class, sector, industry, base_currency, counter_currency
    // Outputs: Boolean series (all true if match, all false if not)
    // Use for: Conditional logic based on asset identity,
    //          creating asset-specific branches in strategy code
    // Example: is_spy = is_asset_ref(ticker="SPY")
    //          signal = spy_signal if is_spy else default_signal
    // Note: Handled specially in execution layer - TransformData never called
    epoch_script::transform::Register<IsAssetRef>("is_asset_ref");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // Asset reference passthrough and is_asset_ref metadata
    for (const auto& metadata : MakeAssetRefPassthroughMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::utility
