//
// Created by adesola on 12/28/24.
//
#include "execution_node.h"
#include "epoch_core/macros.h"
#include "epoch_frame/aliases.h"
#include <tbb/parallel_for_each.h>
#include <tbb/concurrent_vector.h>
#include <arrow/type_fwd.h>
#include <epoch_frame/common.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/index.h>
#include <epoch_script/core/time_frame.h>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/sessions_utils.h>
#include <unordered_map>
#include <vector>

#include "epoch_frame/factory/index_factory.h"

// TODO: Watch out for throwing excePtion in these functions -> causes deadlock
namespace epoch_script::runtime {
// Best-effort intraday detection from timeframe string (e.g., 1Min, 5Min, 1H)
static inline bool IsIntradayString(std::string const &tf) {
  if (tf.size() < 2)
    return false;
  if (tf.ends_with("Min"))
    return true;
  if (!tf.empty() && tf.back() == 'H')
    return true;
  return false;
}

// Create an empty DataFrame with proper column schema from transform outputs
static inline epoch_frame::DataFrame CreateEmptyOutputDataFrame(
    const epoch_script::transform::ITransformBase& transformer) {
  const auto& outputs = transformer.GetOutputMetaData();

  if (outputs.empty()) {
    return epoch_frame::DataFrame{};
  }

  // Create empty index
  auto empty_index = epoch_frame::factory::index::make_datetime_index(
      std::vector<epoch_frame::DateTime>{}, "", "UTC");

  // Create empty arrays for each output column
  std::vector<arrow::ChunkedArrayPtr> empty_columns;
  std::vector<std::string> fields;
  for (const auto& output : outputs) {
    std::string column_name = transformer.GetOutputId(output.id);
    fields.emplace_back(column_name);

    // Create empty array based on output type
    std::shared_ptr<arrow::ChunkedArray> empty_array;
    switch (output.type) {
      case epoch_core::IODataType::Decimal:
        empty_array = epoch_frame::factory::array::make_array(std::vector<double>{});
        break;
      case epoch_core::IODataType::Integer:
        empty_array = epoch_frame::factory::array::make_array(std::vector<int64_t>{});
        break;
      case epoch_core::IODataType::Boolean:
        empty_array = epoch_frame::factory::array::make_array(std::vector<bool>{});
        break;
      case epoch_core::IODataType::String:
        empty_array = epoch_frame::factory::array::make_array(std::vector<std::string>{});
        break;
      case epoch_core::IODataType::Timestamp:
        empty_array = epoch_frame::factory::array::make_array(std::vector<epoch_frame::DateTime>{});
        break;
      case epoch_core::IODataType::Any:
      default:
        // For Any type or unknown types, use null array
        auto null_array = arrow::MakeArrayOfNull(arrow::null(), 0).ValueOrDie();
        empty_array = std::make_shared<arrow::ChunkedArray>(null_array);
        break;
    }

    empty_columns.emplace_back(std::move(empty_array));
  }


  return epoch_frame::make_dataframe(empty_index, empty_columns, fields);
}

// Delegate to shared utils (UTC-aware)
static inline epoch_frame::DataFrame
SliceBySession(epoch_frame::DataFrame const &df,
               epoch_frame::SessionRange const &range) {
  return epoch_script::transform::sessions_utils::SliceBySessionUTC(df,
                                                                      range);
}
void ApplyDefaultTransform(
    const epoch_script::transform::ITransformBase &transformer,
    ExecutionContext &msg) {
  auto timeframe = transformer.GetTimeframe().ToString();
  auto name = transformer.GetName() + " " + transformer.GetId();

  // Enforce intradayOnly if metadata requests it
  try {
    const auto meta =
        transformer.GetConfiguration().GetTransformDefinition().GetMetadata();
    if (meta.intradayOnly && !IsIntradayString(timeframe)) {
      SPDLOG_WARN("Transform {} marked intradayOnly but timeframe {} is not "
                  "intraday. Skipping.",
                  name, timeframe);
      for (auto const &asset_id : msg.cache->GetAssetIDs()) {
        try {
          msg.cache->StoreTransformOutput(asset_id, transformer,
                                          CreateEmptyOutputDataFrame(transformer));
        } catch (std::exception const &exp) {
          msg.logger->log(std::format(
              "Asset: {}, Transform: {}, Error: {}.", asset_id,
              transformer.GetConfiguration().GetId(), exp.what()));
        }
      }
      return;
    }
  } catch (...) {
    // If metadata structure doesn't contain the flag yet, ignore
  }

  // Lambda for processing a single asset
  auto processAsset = [&](auto const &asset_id) {
    try {
      // Validate inputs before gathering - if any input is missing, return empty DataFrame
      if (!msg.cache->ValidateInputsAvailable(asset_id, transformer)) {
        SPDLOG_WARN(
            "Asset({}): Inputs not available for {}. Returning empty DataFrame with correct schema.",
            asset_id, name);
        auto empty_result = CreateEmptyOutputDataFrame(transformer);
        msg.cache->StoreTransformOutput(asset_id, transformer, empty_result);
        return;
      }

      auto result = msg.cache->GatherInputs(asset_id, transformer);
      result = transformer.GetConfiguration()
                       .GetTransformDefinition()
                       .GetMetadata()
                       .allowNullInputs
                   ? result
                   : result.drop_null();

      // Apply session slicing if required by metadata and session is resolvable
      bool requiresSession = false;
      std::optional<epoch_frame::SessionRange> sessionRange =
          transformer.GetConfiguration().GetSessionRange();
      // Heuristic: if an explicit session range isn't set, consider option
      // presence
      if (!sessionRange) {
        try {
          requiresSession =
              transformer.GetConfiguration().GetOptions().contains("session");
        } catch (...) {
          requiresSession = false;
        }
      } else {
        requiresSession = true;
      }
      if (requiresSession) {
        if (sessionRange) {
          result = SliceBySession(result, *sessionRange);
        } else {
          SPDLOG_WARN(
              "Transform {} requiresSession but no session range was resolved.",
              name);
        }
      }

      if (!result.empty()) {
        result = transformer.TransformData(result);

        // Capture reports from reporter transforms
        const auto meta = transformer.GetConfiguration().GetTransformDefinition().GetMetadata();
        if (meta.category == epoch_core::TransformCategory::Reporter) {
          try {
            auto dashboardOpt = transformer.GetDashboard(result);
            if (dashboardOpt.has_value()) {
              auto tearsheet = dashboardOpt->build();
              if (tearsheet.ByteSizeLong() > 0) {
                msg.cache->StoreReport(asset_id, tearsheet);
                SPDLOG_DEBUG("Captured report from {} for asset {} ({} bytes)",
                             transformer.GetId(), asset_id, tearsheet.ByteSizeLong());
              }
            }
          } catch (const std::exception& e) {
            SPDLOG_WARN("Failed to capture report from {} for asset {}: {}",
                        transformer.GetId(), asset_id, e.what());
          }
        }

        // Capture event markers from event_marker transforms
        if (meta.category == epoch_core::TransformCategory::EventMarker) {
          try {
            auto eventMarkerOpt = transformer.GetEventMarkers(result);
            if (eventMarkerOpt.has_value()) {
              msg.cache->StoreEventMarker(asset_id, *eventMarkerOpt);
              SPDLOG_DEBUG("Captured event marker from {} for asset {}",
                           transformer.GetId(), asset_id);
            }
          } catch (const std::exception& e) {
            SPDLOG_WARN("Failed to capture event marker from {} for asset {}: {}",
                        transformer.GetId(), asset_id, e.what());
          }
        }
      } else {
        SPDLOG_WARN(
            "Asset({}): Empty DataFrame provided to {}. Skipping transform",
            asset_id, name);
        // Create empty DataFrame with proper column schema from output metadata
        // This ensures cached entries exist even when transforms are skipped
        result = CreateEmptyOutputDataFrame(transformer);
      }

      msg.cache->StoreTransformOutput(asset_id, transformer, result);
    } catch (std::exception const &exp) {
      const auto error =
          std::format("Asset: {}, Transform: {}, Error: {}.", asset_id,
                      transformer.GetConfiguration().GetId(), exp.what());
      msg.logger->log(error);
    }
  };

  // Parallel per-asset processing using TBB
  const auto& asset_ids = msg.cache->GetAssetIDs();
  tbb::parallel_for_each(asset_ids.begin(), asset_ids.end(), processAsset);
}

// Distribute cross-sectional results to individual assets
// Handles both single-column broadcast and per-asset column extraction
static void DistributeCrossSectionalOutputs(
    const epoch_script::transform::ITransformBase &transformer,
    const epoch_frame::DataFrame &crossResult,
    const std::vector<std::string> &asset_ids,
    ExecutionContext &msg) {
  auto outputId = transformer.GetOutputId();

  SPDLOG_DEBUG("CROSS-SECTIONAL DEBUG - Transform: {}, Output ID: {}",
               transformer.GetId(), outputId);
  SPDLOG_DEBUG("CROSS-SECTIONAL DEBUG - crossResult num_cols: {}, num_rows: {}",
               crossResult.num_cols(), crossResult.num_rows());
  SPDLOG_DEBUG("CROSS-SECTIONAL DEBUG - crossResult contains(outputId={}): {}",
               outputId, crossResult.contains(outputId));
  if (!crossResult.empty()) {
    std::string colNames;
    for (const auto& col : crossResult.column_names()) {
      if (!colNames.empty()) colNames += ", ";
      colNames += col;
    }
    SPDLOG_DEBUG("CROSS-SECTIONAL DEBUG - crossResult column names: {}", colNames);
  }

  if (crossResult.num_cols() == 1 && crossResult.contains(outputId)) {
    // broadcast single column cross-sectional result across all assets
    SPDLOG_DEBUG("CROSS-SECTIONAL DEBUG - Broadcasting single column {} to all {} assets",
                 outputId, asset_ids.size());
    for (auto &asset_id : asset_ids) {
      msg.cache->StoreTransformOutput(asset_id, transformer, crossResult);
    }
  } else {
    SPDLOG_DEBUG("CROSS-SECTIONAL DEBUG - Distributing multi-column result by asset ID");
    for (auto const &asset_id : asset_ids) {
      epoch_frame::DataFrame assetResult;
      if (crossResult.contains(asset_id)) {
        SPDLOG_DEBUG("CROSS-SECTIONAL DEBUG - Asset {} found in crossResult, extracting column", asset_id);
        assetResult = crossResult[asset_id].to_frame(outputId);
      } else {
        SPDLOG_DEBUG("CROSS-SECTIONAL DEBUG - Asset {} NOT found in crossResult (empty result)", asset_id);
      }
      msg.cache->StoreTransformOutput(asset_id, transformer, assetResult);
    }
  }
}

void ApplyCrossSectionTransform(
    const epoch_script::transform::ITransformBase &transformer,
    ExecutionContext &msg) {
  // Build input list across all symbols in timeframe
  auto timeframe = transformer.GetTimeframe().ToString();
  auto inputId = transformer.GetInputId();
  const auto &asset_ids = msg.cache->GetAssetIDs();

  // Enforce intradayOnly if metadata requests it
  try {
    const auto meta =
        transformer.GetConfiguration().GetTransformDefinition().GetMetadata();
    if (meta.intradayOnly && !IsIntradayString(timeframe)) {
      SPDLOG_WARN("Cross-sectional transform {} marked intradayOnly but "
                  "timeframe {} is not intraday. Skipping.",
                  transformer.GetConfiguration().GetId(), timeframe);
      for (auto const &asset_id : asset_ids) {
        try {
          msg.cache->StoreTransformOutput(asset_id, transformer,
                                          epoch_frame::DataFrame{});
        } catch (std::exception const &exp) {
          msg.logger->log(std::format(
              "Asset: {}, Transform: {}, Error: {}.", asset_id,
              transformer.GetConfiguration().GetId(), exp.what()));
        }
      }
      return;
    }
  } catch (...) {
  }

  std::vector<epoch_frame::FrameOrSeries> inputPerAsset;
  inputPerAsset.reserve(asset_ids.size());

  // Single transform call on vector of DataFrames
  try {
    // Parallel input gathering with thread-safe vector
    tbb::concurrent_vector<epoch_frame::FrameOrSeries> concurrentInputs;

    tbb::parallel_for_each(asset_ids.begin(), asset_ids.end(), [&](auto const &asset_id) {
      // Validate inputs before gathering - skip asset if inputs not available
      if (!msg.cache->ValidateInputsAvailable(asset_id, transformer)) {
        SPDLOG_WARN(
            "Asset({}): Inputs not available for cross-sectional transform {}. Skipping asset.",
            asset_id, transformer.GetConfiguration().GetId());
        return;  // Skip this asset, don't add to concurrentInputs
      }

      auto assetDataFrame =
          msg.cache->GatherInputs(asset_id, transformer).drop_null();
      // Apply session slicing if required
      bool requiresSession = false;
      std::optional<epoch_frame::SessionRange> sessionRange =
          transformer.GetConfiguration().GetSessionRange();
      if (!sessionRange) {
        try {
          requiresSession =
              transformer.GetConfiguration().GetOptions().contains("session");
        } catch (...) {
          requiresSession = false;
        }
      } else {
        requiresSession = true;
      }
      if (requiresSession) {
        if (sessionRange) {
          assetDataFrame = SliceBySession(assetDataFrame, *sessionRange);
        } else {
          SPDLOG_WARN("Cross-sectional transform {} requiresSession but no "
                      "session range was resolved.",
                      transformer.GetConfiguration().GetId());
        }
      }
      auto inputSeries = assetDataFrame[inputId].rename(asset_id);
      concurrentInputs.push_back(inputSeries);
    });

    // Copy to regular vector for concat
    inputPerAsset.assign(concurrentInputs.begin(), concurrentInputs.end());

    auto inputDataFrame =
        epoch_frame::concat(
            epoch_frame::ConcatOptions{.frames = inputPerAsset,
                                       .joinType = epoch_frame::JoinType::Outer,
                                       .axis = epoch_frame::AxisType::Column})
            .drop_null();

    epoch_frame::DataFrame crossResult =
        inputDataFrame.empty() ? epoch_frame::DataFrame{}
                               // Empty result, cache manager will handle
                               : transformer.TransformData(inputDataFrame);

    // Check if this is a reporter/sink transform (no outputs to distribute)
    const auto meta =
        transformer.GetConfiguration().GetTransformDefinition().GetMetadata();
    if (meta.category == epoch_core::TransformCategory::Reporter) {
      // Cross-sectional reporters generate a single report for all assets
      // Capture the report under GROUP_KEY before returning
      try {
        auto dashboardOpt = transformer.GetDashboard(crossResult);
        if (dashboardOpt.has_value()) {
          auto tearsheet = dashboardOpt->build();
          if (tearsheet.ByteSizeLong() > 0) {
            msg.cache->StoreReport(epoch_script::GROUP_KEY, tearsheet);
            SPDLOG_DEBUG("Captured cross-sectional report from {} under GROUP_KEY ({} bytes)",
                         transformer.GetConfiguration().GetId(), tearsheet.ByteSizeLong());
          }
        }
      } catch (const std::exception& e) {
        SPDLOG_WARN("Failed to capture cross-sectional report from {}: {}",
                    transformer.GetConfiguration().GetId(), e.what());
      }

      SPDLOG_DEBUG("Cross-sectional reporter {} - skipping output distribution",
                   transformer.GetConfiguration().GetId());
      return;
    }

    // For non-reporter transforms, distribute outputs to individual assets
    DistributeCrossSectionalOutputs(transformer, crossResult, asset_ids, msg);

  } catch (std::exception const &exp) {
    auto error =
        std::format("Transform : {}", transformer.GetConfiguration().GetId());
    const auto exception = std::format("{}\n{}", exp.what(), error);
    msg.logger->log(exception);
  }
}
} // namespace epoch_script::runtime
