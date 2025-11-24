#include "intermediate_storage.h"
#include "common/asserts.h"
#include "epoch_frame/common.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include <epoch_script/transforms/core/metadata.h>
#include "storage_types.h"
#include <algorithm>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <ranges>
#include <vector>
#include <numeric>

namespace epoch_script::runtime {

// Helper function to broadcast a scalar value to a target index size (DRY principle)
// SRP: Responsible only for creating a broadcasted array from a scalar
static arrow::ChunkedArrayPtr BroadcastScalar(const epoch_frame::Scalar& scalar, size_t target_size) {
    auto broadcastedArray = arrow::MakeArrayFromScalar(*scalar.value(), target_size).ValueOrDie();
    return std::make_shared<arrow::ChunkedArray>(broadcastedArray);
}

epoch_frame::DataFrame IntermediateResultStorage::GatherInputs(
    const AssetID &asset_id,
    const epoch_script::transform::ITransformBase &transformer) const {
  const auto targetTimeframe = transformer.GetTimeframe().ToString();
  const auto dataSources = transformer.GetRequiredDataSources();  // Use interface method for template expansion

  const auto transformInputs = transformer.GetInputIds();

#ifndef NDEBUG
  // DEBUG: Log what data sources this transform needs
  SPDLOG_DEBUG("Transform {} requesting {} data sources for asset {}",
               transformer.GetId(), dataSources.size(), asset_id);
  for (const auto& ds [[maybe_unused]] : dataSources) {
    SPDLOG_DEBUG("  - Requested data source: {}", ds);
  }
#endif

  if (transformInputs.empty()) {
    SPDLOG_DEBUG(
        "Gathering base data for asset: {}, timeframe {}, transform: {}.",
        asset_id, targetTimeframe, transformer.GetId());
    std::shared_lock lock(m_baseDataMutex);
    auto result = epoch_core::lookup(epoch_core::lookup(m_baseData, targetTimeframe), asset_id);
    // If requiredDataSources is specified, filter to only those columns
    if (!dataSources.empty()) {
      std::vector<std::string> availableCols;
      for (const auto& col : dataSources) {
        if (result.contains(col)) {
          availableCols.push_back(col);
        }
      }
      if (!availableCols.empty()) {
        result = result[availableCols];
      }
    }

    return result;
  }

  // Acquire read locks for all relevant caches
  std::shared_lock cacheLock(m_cacheMutex);
  std::shared_lock baseDataLock(m_baseDataMutex);
  std::shared_lock transformMapLock(m_transformMapMutex);
  std::shared_lock scalarLock(m_scalarCacheMutex);  // Also lock scalar cache for reading

  auto targetIndex =
      epoch_core::lookup(epoch_core::lookup(m_baseData, targetTimeframe,
                    "Failed to find target timeframe in basedata"),
             asset_id, "failed to find asset for target timeframe")
          .index();

  std::vector<std::string> columns;
  std::vector<arrow::ChunkedArrayPtr> arrayList;

  std::unordered_set<std::string> columIdSet;
  for (const auto &inputId : transformInputs) {
    if (columIdSet.contains(inputId)) {
      continue;
    }

    // Check if this input is a scalar (SRP: decision point for scalar vs regular path)
    if (m_scalarOutputs.contains(inputId)) {
      // Scalar path: Broadcast from global scalar cache
      auto scalarIt = m_scalarCache.find(inputId);
      if (scalarIt == m_scalarCache.end()) {
        throw std::runtime_error(
            "Scalar cache missing entry for '" + inputId +
            "'. Asset: " + asset_id + ", Timeframe: " + targetTimeframe +
            ". This indicates the scalar was registered but never populated.");
      }
      const auto& scalarValue = scalarIt->second;
      arrayList.emplace_back(BroadcastScalar(scalarValue, targetIndex->size()));
      columns.emplace_back(inputId);
      columIdSet.emplace(inputId);
      SPDLOG_DEBUG("Broadcasting scalar {} to {} rows for asset: {}, timeframe {}",
                   inputId, targetIndex->size(), asset_id, targetTimeframe);
      continue;
    }

    // Regular (non-scalar) path: Retrieve from timeframe-specific cache
    auto transform = m_ioIdToTransform.find(inputId);
    if (transform == m_ioIdToTransform.end()) {
      throw std::runtime_error("Cannot find transform for input: " +
                               inputId);
    }
    auto tf = transform->second->GetTimeframe().ToString();
    SPDLOG_DEBUG(
        "Gathering input {} for transform {}, asset: {}, timeframe {}. from {}",
        inputId, transform->second->GetId(), asset_id, tf,
        transformer.GetId());

    // Defensive cache access with clear error messages
    auto tfIt = m_cache.find(tf);
    if (tfIt == m_cache.end()) {
      throw std::runtime_error(
          "Cache missing timeframe '" + tf + "' for input '" + inputId +
          "'. Asset: " + asset_id);
    }
    auto assetIt = tfIt->second.find(asset_id);
    if (assetIt == tfIt->second.end()) {
      throw std::runtime_error(
          "Cache missing asset '" + asset_id + "' for input '" + inputId +
          "'. Timeframe: " + tf);
    }
    auto inputIt = assetIt->second.find(inputId);
    if (inputIt == assetIt->second.end()) {
      throw std::runtime_error(
          "Cache missing input '" + inputId + "' for asset '" + asset_id +
          "'. Timeframe: " + tf);
    }
    auto result = inputIt->second;
    arrayList.emplace_back(tf == targetTimeframe
                               ? result.array()
                               : result.reindex(targetIndex).array());
    columns.emplace_back(inputId);
    columIdSet.emplace(inputId);
  }

  for (const auto &dataSource : dataSources) {
    if (columIdSet.contains(dataSource)) {
      continue;
    }

    // Defensive base data access with clear error messages
    auto tfIt = m_baseData.find(targetTimeframe);
    if (tfIt == m_baseData.end()) {
      throw std::runtime_error(
          "Base data missing timeframe '" + targetTimeframe +
          "' for data source '" + dataSource + "'. Asset: " + asset_id);
    }
    auto assetIt = tfIt->second.find(asset_id);
    if (assetIt == tfIt->second.end()) {
      throw std::runtime_error(
          "Base data missing asset '" + asset_id +
          "' for data source '" + dataSource + "'. Timeframe: " + targetTimeframe);
    }
    auto& assetData = assetIt->second;

    if (!assetData.contains(dataSource)) {
      // DEBUG: Log what columns ARE available for this asset
      auto availableCols = assetData.column_names();
      SPDLOG_INFO("Transform {} looking for '{}' in asset {} - NOT FOUND",
                  transformer.GetId(), dataSource, asset_id);
      SPDLOG_INFO("  Available columns in base data ({} total):", availableCols.size());
      for (size_t i = 0; i < std::min(availableCols.size(), size_t(10)); ++i) {
        SPDLOG_INFO("    - {}", availableCols[i]);
      }
      if (availableCols.size() > 10) {
        SPDLOG_INFO("    ... and {} more", availableCols.size() - 10);
      }
      continue; // Skip missing columns entirely - don't waste space with full null columns
    }

    auto column = assetData[dataSource];
    arrayList.emplace_back(column.array());
    columns.emplace_back(dataSource);
  }

  return epoch_frame::make_dataframe(targetIndex, arrayList, columns);
}

bool IntermediateResultStorage::ValidateInputsAvailable(
    const AssetID &asset_id,
    const epoch_script::transform::ITransformBase &transformer) const {
  const auto targetTimeframe = transformer.GetTimeframe().ToString();
  const auto dataSources = transformer.GetRequiredDataSources();  // Use interface method for template expansion
  const auto transformInputs = transformer.GetInputIds();

  // If no inputs required, validation passes
  if (transformInputs.empty() && dataSources.empty()) {
    return true;
  }

  // Acquire read locks
  std::shared_lock cacheLock(m_cacheMutex);
  std::shared_lock baseDataLock(m_baseDataMutex);
  std::shared_lock transformMapLock(m_transformMapMutex);
  std::shared_lock scalarLock(m_scalarCacheMutex);

  // Check if base data exists for this asset/timeframe (needed for target index)
  if (!transformInputs.empty()) {
    auto baseDataTfIt = m_baseData.find(targetTimeframe);
    if (baseDataTfIt == m_baseData.end()) {
      SPDLOG_DEBUG("Validation failed: base data missing timeframe '{}' for asset '{}'",
                   targetTimeframe, asset_id);
      return false;
    }
    auto baseDataAssetIt = baseDataTfIt->second.find(asset_id);
    if (baseDataAssetIt == baseDataTfIt->second.end()) {
      SPDLOG_DEBUG("Validation failed: base data missing asset '{}' for timeframe '{}'",
                   asset_id, targetTimeframe);
      return false;
    }
  }

  // Validate all transform inputs are available
  for (const auto &inputId : transformInputs) {
    // Check if this is a scalar (always available globally)
    if (m_scalarOutputs.contains(inputId)) {
      auto scalarIt = m_scalarCache.find(inputId);
      if (scalarIt == m_scalarCache.end()) {
        SPDLOG_DEBUG("Validation failed: scalar cache missing '{}' for asset '{}'",
                     inputId, asset_id);
        return false;
      }
      continue;
    }

    // Check regular (non-scalar) input
    auto transform = m_ioIdToTransform.find(inputId);
    if (transform == m_ioIdToTransform.end()) {
      SPDLOG_DEBUG("Validation failed: cannot find transform for input '{}', asset '{}'",
                   inputId, asset_id);
      return false;
    }

    auto tf = transform->second->GetTimeframe().ToString();
    auto tfIt = m_cache.find(tf);
    if (tfIt == m_cache.end()) {
      SPDLOG_DEBUG("Validation failed: cache missing timeframe '{}' for input '{}', asset '{}'",
                   tf, inputId, asset_id);
      return false;
    }

    auto assetIt = tfIt->second.find(asset_id);
    if (assetIt == tfIt->second.end()) {
      SPDLOG_DEBUG("Validation failed: cache missing asset '{}' for input '{}', timeframe '{}'",
                   asset_id, inputId, tf);
      return false;
    }

    auto inputIt = assetIt->second.find(inputId);
    if (inputIt == assetIt->second.end()) {
      SPDLOG_DEBUG("Validation failed: cache missing input '{}' for asset '{}', timeframe '{}'",
                   inputId, asset_id, tf);
      return false;
    }
  }

  // Validate all required data sources are available
  for (const auto &dataSource : dataSources) {
    auto tfIt = m_baseData.find(targetTimeframe);
    if (tfIt == m_baseData.end()) {
      SPDLOG_DEBUG("Validation failed: base data missing timeframe '{}' for data source '{}', asset '{}'",
                   targetTimeframe, dataSource, asset_id);
      return false;
    }

    auto assetIt = tfIt->second.find(asset_id);
    if (assetIt == tfIt->second.end()) {
      SPDLOG_DEBUG("Validation failed: base data missing asset '{}' for data source '{}', timeframe '{}'",
                   asset_id, dataSource, targetTimeframe);
      return false;
    }

    auto& assetData = assetIt->second;
    if (!assetData.contains(dataSource)) {
#ifndef NDEBUG
      // DEBUG: Show what columns ARE available
      auto availableCols = assetData.column_names();
      SPDLOG_DEBUG("Validation failed: base data missing column '{}' for asset '{}', timeframe '{}'",
                   dataSource, asset_id, targetTimeframe);
      SPDLOG_DEBUG("  Available columns ({} total):", availableCols.size());
      for (size_t i = 0; i < std::min(availableCols.size(), size_t(20)); ++i) {
        SPDLOG_DEBUG("    - {}", availableCols[i]);
      }
      if (availableCols.size() > 20) {
        SPDLOG_DEBUG("    ... and {} more", availableCols.size() - 20);
      }
#endif
      return false;
    }
  }

  // All inputs are available
  return true;
}

void IntermediateResultStorage::InitializeBaseData(
    TimeFrameAssetDataFrameMap data, const std::unordered_set<AssetID> &allowed_asset_ids) {
  // Store base data with empty transformepoch_script::ID
  // Acquire exclusive locks for initialization
  std::unique_lock baseDataLock(m_baseDataMutex);
  std::unique_lock cacheLock(m_cacheMutex);
  std::unique_lock assetsLock(m_assetIDsMutex);

  m_baseData = std::move(data);
  std::unordered_set<AssetID> asset_id_set;

  // Update metadata for base data columns
  for (const auto &[timeframe, assetMap] : m_baseData) {
    for (const auto &[asset_id, dataFrame] : assetMap) {
      asset_id_set.insert(asset_id);

      if (!allowed_asset_ids.contains(asset_id)) {
        SPDLOG_DEBUG("Asset {} not found in required assets list", asset_id);
        continue;
      }
      SPDLOG_DEBUG("Initializing base data for asset: {}, timeframe {}",
                   asset_id, timeframe);

      // Debug: Check for duplicates in base data index
      if (asset_id == "AAPL-Stocks" && timeframe == "1D") {
        auto index = dataFrame.index();
        if (index) {
          auto index_array = index->as_chunked_array();
          auto vc_result = arrow::compute::ValueCounts(index_array);
          if (vc_result.ok()) {
            auto vc_struct = vc_result.ValueOrDie();
            auto counts = std::static_pointer_cast<arrow::Int64Array>(
                vc_struct->GetFieldByName("counts"));

            int64_t dup_count = 0;
            for (int64_t i = 0; i < counts->length(); i++) {
              if (counts->Value(i) > 1) {
                dup_count++;
              }
            }

            if (dup_count > 0) {
              SPDLOG_WARN("DUPLICATE_FOUND: InitializeBaseData: base data index has {} duplicate timestamps for asset: {}, timeframe: {}",
                           dup_count, asset_id, timeframe);
              auto col_names = dataFrame.column_names();
              std::string cols_str = "";
              for (size_t i = 0; i < col_names.size(); i++) {
                if (i > 0) cols_str += ", ";
                cols_str += col_names[i];
              }
              SPDLOG_WARN("Base data columns: {}", cols_str);
              SPDLOG_WARN("Base data index:\n{}", index->repr());
            } else {
              SPDLOG_DEBUG("NO_DUPLICATES: InitializeBaseData: base data index has NO duplicates for asset: {}, timeframe: {} ({} rows)",
                          asset_id, timeframe, index->size());
            }
          }
        }
      }

      for (const auto &colName : dataFrame.column_names()) {
        m_cache[timeframe][asset_id][colName] = dataFrame[colName];
      }
    }
  }
  m_asset_ids.assign(asset_id_set.begin(), asset_id_set.end());
}

void IntermediateResultStorage::RegisterTransform(
    const epoch_script::transform::ITransformBase &transform) {
  std::unique_lock lock(m_transformMapMutex);

  // Register each output of this transform
  for (const auto& output : transform.GetOutputMetaData()) {
    const std::string outputId = transform.GetOutputId(output.id);
    m_ioIdToTransform.insert_or_assign(outputId, &transform);
  }
}

TimeFrameAssetDataFrameMap IntermediateResultStorage::BuildFinalOutput() {
  // Acquire read locks to safely copy data
  std::shared_lock cacheLock(m_cacheMutex);
  std::shared_lock baseDataLock(m_baseDataMutex);
  std::shared_lock transformMapLock(m_transformMapMutex);
  std::shared_lock assetsLock(m_assetIDsMutex);
  std::shared_lock scalarLock(m_scalarCacheMutex);  // Also lock scalar cache

  TimeFrameAssetDataFrameMap result = m_baseData; // Copy instead of move for thread safety

  std::unordered_map<
      std::string, std::unordered_map<AssetID, std::vector<epoch_frame::FrameOrSeries>>>
      concatFrames;

  // Process regular (non-scalar) transforms
  for (const auto &asset_id : m_asset_ids) {
    for (const auto &[ioId, transform] : m_ioIdToTransform) {
      if (transform->GetConfiguration().GetTransformDefinition().GetMetadata().category ==
          epoch_core::TransformCategory::DataSource) {
        continue;
      }
      const auto targetTimeframe = transform->GetTimeframe().ToString();
      const auto &assetBucket = m_cache.at(targetTimeframe);
      auto bucket = assetBucket.find(asset_id);
      if (bucket == assetBucket.end()) {
        continue;
      }
      // we only export cseries with plot kinds
      auto target = bucket->second.find(ioId);
      if (target == bucket->second.end()) {
        continue;
      }

      concatFrames[targetTimeframe][asset_id].emplace_back(target->second);
    }
  }

  // Release locks before expensive concat operations
  cacheLock.unlock();
  baseDataLock.unlock();
  transformMapLock.unlock();
  assetsLock.unlock();
  scalarLock.unlock();

  // First, concatenate regular data
  for (const auto &[timeframe, assetMap] : result) {
    for (const auto &[asset_id, dataFrame] : assetMap) {
      auto &entry = concatFrames[timeframe][asset_id];
      if (entry.empty()) {
        continue;
      }
      entry.emplace_back(dataFrame);
      auto concated =
          epoch_frame::concat({.frames = entry,
                               .joinType = epoch_frame::JoinType::Outer,
                               .axis = epoch_frame::AxisType::Column});
      result[timeframe][asset_id] = std::move(concated);
    }
  }

  // Second, broadcast scalars to all (timeframe, asset) combinations
  // SRP: Broadcasting scalars is a separate concern from regular data concatenation
  if (!m_scalarOutputs.empty()) {
    std::shared_lock scalarReadLock(m_scalarCacheMutex);  // Re-acquire for scalar broadcasting

    for (auto &[timeframe, assetMap] : result) {
      for (auto &[asset_id, dataFrame] : assetMap) {
        auto index = dataFrame.index();
        std::vector<epoch_frame::FrameOrSeries> scalarSeries;

        // Broadcast each scalar to this (timeframe, asset) combination
        for (const auto &scalarOutputId : m_scalarOutputs) {
          // Defensive scalar cache access
          auto scalarIt = m_scalarCache.find(scalarOutputId);
          if (scalarIt == m_scalarCache.end()) {
            throw std::runtime_error(
                "Scalar cache missing entry for '" + scalarOutputId +
                "' during final output build. This indicates the scalar was registered but never populated.");
          }
          const auto& scalarValue = scalarIt->second;
          auto broadcastedArray = BroadcastScalar(scalarValue, index->size());

          epoch_frame::Series series(index, broadcastedArray, scalarOutputId);
          scalarSeries.emplace_back(std::move(series));
        }

        // Concatenate scalars with existing data
        if (!scalarSeries.empty()) {
          scalarSeries.emplace_back(dataFrame);
          result[timeframe][asset_id] = epoch_frame::concat({
              .frames = scalarSeries,
              .joinType = epoch_frame::JoinType::Outer,
              .axis = epoch_frame::AxisType::Column
          });
          SPDLOG_DEBUG("Broadcasted {} scalars to asset: {}, timeframe {}",
                       m_scalarOutputs.size(), asset_id, timeframe);
        }
      }
    }
  }

  return result;
}

std::shared_ptr<arrow::DataType>
GetArrowTypeFromIODataType(epoch_core::IODataType dataType) {
  using epoch_core::IODataType;
  switch (dataType) {
  case IODataType::Integer:
    return arrow::int64();
  case IODataType::Boolean:
    return arrow::boolean();
  case IODataType::Decimal:
  case IODataType::Number:
    return arrow::float64();
  case IODataType::String:
    return arrow::utf8();
  case IODataType::Timestamp:
    return arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
  case IODataType::Any:
    // Any type typically appears for polymorphic outputs (e.g., percentile_select labels)
    // Default to nullable utf8 string since most Any-typed outputs are label columns
    // Note: utf8 is used instead of binary because it's a valid Index type in epochframe
    SPDLOG_WARN("IODataType::Any encountered - defaulting to nullable utf8 (string) type");
    return arrow::utf8();
  default:
    break;
  }
  SPDLOG_WARN("Unknown IODataType: {}. defaulting to nullable utf8 (string) type",
              epoch_core::IODataTypeWrapper::ToString(dataType));
  return arrow::utf8();
}

void IntermediateResultStorage::StoreTransformOutput(
    const AssetID &asset_id,
    const epoch_script::transform::ITransformBase &transformer,
    const epoch_frame::DataFrame &data) {
  const auto timeframe = transformer.GetTimeframe().ToString();

  // Check if this is a scalar transform
  const auto metadata = transformer.GetConfiguration().GetTransformDefinition().GetMetadata();
  const bool isScalar = (metadata.category == epoch_core::TransformCategory::Scalar);

  if (isScalar) {
    // Scalar optimization: Store once globally, not per (timeframe, asset)
    std::unique_lock scalarLock(m_scalarCacheMutex);

    for (const auto &outputMetaData : transformer.GetOutputMetaData()) {
      auto outputId = transformer.GetOutputId(outputMetaData.id);

      // Only store if not already cached (scalars are executed once)
      if (!m_scalarCache.contains(outputId)) {
        if (data.contains(outputId) && data[outputId].size() > 0) {
          // Extract scalar value from first element of the Series
          m_scalarCache[outputId] = epoch_frame::Scalar(data[outputId].array()->GetScalar(0).ValueOrDie());
          SPDLOG_DEBUG("Stored scalar {} globally (single copy, no timeframe/asset)", outputId);
        } else {
          // Store null scalar
          m_scalarCache[outputId] = epoch_frame::Scalar(
              arrow::MakeNullScalar(GetArrowTypeFromIODataType(outputMetaData.type)));
          SPDLOG_DEBUG("Stored NULL scalar {} globally", outputId);
        }
        m_scalarOutputs.insert(outputId);
      }
    }
    return; // Early exit - scalars don't use the regular cache
  }

  // Regular (non-scalar) storage path
  // Read lock for baseData, write lock for cache (this is the hot path)
  std::shared_lock baseDataLock(m_baseDataMutex);
  std::unique_lock cacheLock(m_cacheMutex);

  // Check if base data exists for this timeframe and asset to get the index
  // If not available (e.g., in tests), use the data's own index
  epoch_frame::IndexPtr targetIndex;
  auto tfIt = m_baseData.find(timeframe);
  if (tfIt != m_baseData.end()) {
    auto assetIt = tfIt->second.find(asset_id);
    if (assetIt != tfIt->second.end()) {
      targetIndex = assetIt->second.index();

      // Debug: Check for duplicates in targetIndex
      if (asset_id == "AAPL-Stocks" && timeframe == "1D") {
        auto index_array = targetIndex->as_chunked_array();
        auto vc_result = arrow::compute::ValueCounts(index_array);
        if (vc_result.ok()) {
          auto vc_struct = vc_result.ValueOrDie();
          auto counts = std::static_pointer_cast<arrow::Int64Array>(
              vc_struct->GetFieldByName("counts"));

          int64_t dup_count = 0;
          for (int64_t i = 0; i < counts->length(); i++) {
            if (counts->Value(i) > 1) {
              dup_count++;
            }
          }

          if (dup_count > 0) {
            SPDLOG_WARN("DUPLICATE_FOUND: targetIndex has {} duplicate timestamps for asset: {}, timeframe: {}, transform: {}",
                         dup_count, asset_id, timeframe, transformer.GetId());
            SPDLOG_WARN("targetIndex details:\n{}", targetIndex->repr());
          } else {
            SPDLOG_DEBUG("NO_DUPLICATES: targetIndex has NO duplicates for asset: {}, timeframe: {}, transform: {}",
                        asset_id, timeframe, transformer.GetId());
          }
        }
      }
    }
  }

  // If we don't have base data index, use the data's index (or empty index for empty data)
  if (!targetIndex) {
    if (!data.empty()) {
      targetIndex = data.index();
    } else {
      // Create empty index for completely empty data
      targetIndex = epoch_frame::factory::index::make_datetime_index(
          std::vector<epoch_frame::DateTime>{}, "", "UTC");
    }
    SPDLOG_DEBUG("No base data for transform {} asset {} timeframe {} - using data's own index",
                 transformer.GetId(), asset_id, timeframe);
  }

  for (const auto &outputMetaData : transformer.GetOutputMetaData()) {
    auto outputId = transformer.GetOutputId(outputMetaData.id);

    if (data.contains(outputId)) {
      SPDLOG_DEBUG("Storing output {} for asset: {}, timeframe {}", outputId,
                   asset_id, timeframe);
      // TODO: Save guide for duplicate index(Futures)
      if (outputId == "ret_d#result" && asset_id == "AAPL-Stocks") {
        SPDLOG_INFO("data[outputId]:\n{}", data[outputId].repr());
        SPDLOG_INFO("targetIndex:\n{}", targetIndex->repr());
      }

      m_cache[timeframe][asset_id][outputId] = data[outputId].reindex(targetIndex);
      continue;
    }
    SPDLOG_DEBUG("Storing NULL   output {} for asset: {}, timeframe {}",
                 outputId, asset_id, timeframe);

    size_t index_size = targetIndex->size();
    auto null_array_result = arrow::MakeArrayOfNull(GetArrowTypeFromIODataType(outputMetaData.type), index_size);
    auto null_array = null_array_result.ValueOrDie();
    m_cache[timeframe][asset_id][outputId] = epoch_frame::Series(
        targetIndex,
        std::make_shared<arrow::ChunkedArray>(null_array),
        std::optional<std::string>(outputId));
  }
}
} // namespace epoch_script::runtime
