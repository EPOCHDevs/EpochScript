#pragma once
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/scalar.h>
#include <string>
#include <tbb/concurrent_unordered_map.h>
#include <unordered_map>

namespace epoch_script::runtime {
using TransformType = epoch_script::transform::ITransform::Ptr;
using AssetID = std::string;
using AssetDataFrameMap = std::unordered_map<AssetID, epoch_frame::DataFrame>;
using TimeFrameAssetDataFrameMap =
    std::unordered_map<std::string, AssetDataFrameMap>;

// Store complete DataFrames per transform, not individual series
using TransformCache =
    std::unordered_map<std::string,
                       epoch_frame::Series>; // outputId -> Series

// Asset-level cache (using AssetID = std::string)
using AssetCache = std::unordered_map<AssetID, TransformCache>;

// Timeframe-level cache
using TimeFrameCache = tbb::concurrent_unordered_map<std::string, AssetCache>;

// Scalar cache: stores constant values globally (no timeframe/asset dimensions)
// Scalars are timeframe-agnostic and asset-independent, so we store them once
using ScalarCache = std::unordered_map<std::string, epoch_frame::Scalar>;

// AssetScalar cache: stores per-asset scalar values (e.g., asset_ref filter results)
// These are timeframe-agnostic but asset-dependent
// Structure: outputId -> (assetId -> scalar)
using AssetScalarCache = std::unordered_map<std::string, std::unordered_map<AssetID, epoch_frame::Scalar>>;

} // namespace epoch_script::runtime
