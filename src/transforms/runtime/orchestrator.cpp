//
// Created by adesola on 12/28/24.
//
#include "orchestrator.h"
#include "execution/intermediate_storage.h"
#include <boost/container_hash/hash.hpp>
#include <epoch_script/transforms/core/registration.h>
#include <epoch_script/core/constants.h>
#include "../components/utility/asset_ref.h"
#include <format>
#include <spdlog/spdlog.h>

#include <epoch_script/transforms/core/transform_registry.h>
#include "epoch_protos/tearsheet.pb.h"

#include <tbb/parallel_for_each.h>

#include "transform_manager/transform_manager.h"

namespace {
  // Helper to check if transform metadata indicates it's a reporter
  [[maybe_unused]] bool IsReporterTransform(const epoch_script::transform::ITransformBase& transform) {
    const auto metadata = transform.GetConfiguration().GetTransformDefinition().GetMetadata();
    return metadata.category == epoch_core::TransformCategory::Reporter;
  }

}

namespace epoch_script::runtime {

std::unique_ptr<IDataFlowOrchestrator> CreateDataFlowRuntimeOrchestrator(
  const std::set<std::string>& assetIdList,
  ITransformManagerPtr transformManager) {
    return std::make_unique<DataFlowRuntimeOrchestrator>(
        std::vector<std::string>(assetIdList.begin(), assetIdList.end()),
        std::move(transformManager), nullptr, nullptr);
}

DataFlowRuntimeOrchestrator::DataFlowRuntimeOrchestrator(
    std::vector<std::string> asset_ids,
    ITransformManagerPtr transformManager,
    IIntermediateStoragePtr cacheManager, ILoggerPtr logger)
    : m_asset_ids(std::move(asset_ids))
    , m_eventDispatcher(events::MakeEventDispatcher())
    , m_cancellationToken(events::MakeCancellationToken()) {

  if (cacheManager) {
    m_executionContext.cache = std::move(cacheManager);
  } else {
    m_executionContext.cache = std::make_unique<IntermediateResultStorage>();
  }

  if (logger) {
    m_executionContext.logger = std::move(logger);
  } else {
    m_executionContext.logger = std::make_unique<Logger>();
  }

  // Initialize event context
  m_executionContext.eventDispatcher = m_eventDispatcher;
  m_executionContext.cancellationToken = m_cancellationToken;
  m_executionContext.nodesCompleted = &m_nodesCompleted;
  m_executionContext.nodesFailed = &m_nodesFailed;
  m_executionContext.nodesSkipped = &m_nodesSkipped;
  m_executionContext.onNodeStarted = [this](const std::string& nodeId) {
    MarkNodeStarted(nodeId);
  };
  m_executionContext.onNodeCompleted = [this](const std::string& nodeId) {
    MarkNodeCompleted(nodeId);
  };

  // Build transform instances from configurations (validates ordering)
  auto transforms = transformManager->BuildTransforms();
  SPDLOG_DEBUG("BuildTransforms returned {} transforms", transforms.size());

  // Track unique IDs to prevent actual duplicates
  std::unordered_set<std::string> usedIds;

  for (auto& transform : transforms) {
    SPDLOG_DEBUG("Processing transform {}", (transform ? "non-null" : "NULL"));
    const std::string uniqueId = transform->GetId();
    SPDLOG_DEBUG("Transform ID = '{}'", uniqueId);
    if (usedIds.contains(uniqueId)) {
      throw std::runtime_error(
          std::format("Duplicate transform id: {}", uniqueId));
    }
    usedIds.insert(uniqueId);

    // Handle asset_ref (AssetScalar): pre-compute for all assets, store in cache
    const auto& transformType = transform->GetConfiguration().GetTransformDefinition().GetType();
    if (transformType == epoch_script::transforms::ASSET_REF_ID) {
      SPDLOG_DEBUG("Processing AssetScalar transform: {}", uniqueId);

      // Get the ticker filter option
      std::string tickerFilter;
      try {
        tickerFilter = transform->GetOption("ticker").GetString();
      } catch (...) {
        tickerFilter = "";
      }

      // Get output ID for this transform
      const auto outputId = transform->GetOutputId("match");

      // Pre-compute for each asset and store in cache
      for (const auto& asset_id : m_asset_ids) {
        bool matches = epoch_script::transform::EvaluateAssetRefTicker(asset_id, tickerFilter);

        // Store as boolean scalar in per-asset cache
        // We need to create a DataFrame with a single boolean value
        auto boolScalar = arrow::MakeScalar(matches);
        m_executionContext.cache->StoreAssetScalar(asset_id, outputId,
            epoch_frame::Scalar(boolScalar));

        SPDLOG_DEBUG("AssetScalar {}: asset={}, ticker_filter={}, matches={}",
                     uniqueId, asset_id, tickerFilter, matches);
      }

      // Don't register as execution node - already computed
      continue;
    }

    SPDLOG_DEBUG("Registering Transform {} ({}), Unique ID: {}",
                 transform->GetName(), transform->GetId(), uniqueId);
    RegisterTransform(std::move(transform));
  }
}

std::vector<DataFlowRuntimeOrchestrator::TransformExecutionNode *>
DataFlowRuntimeOrchestrator::ResolveInputDependencies(
    const epoch_script::strategy::InputMapping &inputs) const {
  std::vector<TransformExecutionNode *> result;
  std::ranges::for_each(inputs | std::views::values,
                        [&](const auto &inputList) {
                          for (const auto &input_value : inputList) {
                            // Skip literal values - they don't have dependencies
                            if (!input_value.IsNodeReference()) {
                              continue;
                            }
                            const std::string s = input_value.GetNodeReference().GetRef();
                            auto it = m_outputHandleToNode.find(s);
                            if (it == m_outputHandleToNode.end()) {
                              throw std::runtime_error(std::format(
                                  "Handle {} was not previously hashed.", s));
                            }
                            result.emplace_back(it->second);
                          }
                        });
  return result;
}

void DataFlowRuntimeOrchestrator::RegisterTransform(
    std::unique_ptr<epoch_script::transform::ITransformBase> transform) {
  auto& transformRef = *transform;

  // Create progress emitter for this transform
  auto progressEmitter = events::MakeProgressEmitter(
      m_eventDispatcher,
      m_cancellationToken,
      transformRef.GetId(),
      transformRef.GetName()
  );
  transformRef.SetProgressEmitter(progressEmitter);

  auto node = CreateTransformNode(transformRef);

  // Store the transform before creating node
  m_transforms.push_back(std::move(transform));

  // Resolve input dependencies using InputMapping (skips literals)
  const auto& input_mapping = transformRef.GetConfiguration().GetInputs();
  auto handles = ResolveInputDependencies(input_mapping);

  if (handles.empty()) {
    m_independentNodes.emplace_back(std::move(node));
    return;
  }

  for (auto &handle : handles) {
    make_edge(*handle, *node);
  }
  m_dependentNodes.emplace_back(std::move(node));
}

TimeFrameAssetDataFrameMap
DataFlowRuntimeOrchestrator::ExecutePipeline(TimeFrameAssetDataFrameMap data) {
  // Record start time
  auto startTime = events::Now();

  // Reset counters
  m_nodesCompleted.store(0);
  m_nodesFailed.store(0);
  m_nodesSkipped.store(0);
  m_cancellationToken->Reset();

  // Update execution context with total nodes
  m_executionContext.totalNodes = m_transforms.size();

  // Initialize cache with input data
  m_executionContext.cache->InitializeBaseData(std::move(data),
                                         {m_asset_ids.begin(), m_asset_ids.end()});
  // Set up shared data
  m_executionContext.logger->clear();

  // Emit pipeline started event
  m_eventDispatcher->Emit(events::PipelineStartedEvent{
      .timestamp = startTime,
      .total_nodes = m_transforms.size(),
      .total_assets = m_asset_ids.size(),
      .node_ids = GetAllNodeIds()
  });

  m_isExecuting.store(true);

  // Start progress summary thread if enabled
  if (m_summaryEnabled) {
    StartProgressSummaryThread();
  }

  // Use TBB flow graph for parallel execution
  SPDLOG_DEBUG("Executing transform graph ({} transforms)", m_transforms.size());

  try {
    // Trigger independent nodes (nodes with no dependencies)
    for (const auto& node : m_independentNodes) {
      node->try_put(tbb::flow::continue_msg());
    }

    // Wait for all nodes to complete
    m_graph.wait_for_all();
  } catch (const events::OperationCancelledException&) {
    // Stop progress summary thread
    StopProgressSummaryThread();
    m_isExecuting.store(false);

    // Emit cancelled event
    auto elapsed = events::ToMillis(events::Now() - startTime);
    m_eventDispatcher->Emit(events::PipelineCancelledEvent{
        .timestamp = events::Now(),
        .elapsed = elapsed,
        .nodes_completed = m_nodesCompleted.load(),
        .nodes_total = m_transforms.size()
    });

    throw;
  }

  // Stop progress summary thread
  StopProgressSummaryThread();
  m_isExecuting.store(false);

  // Check for errors after execution
  const auto error = m_executionContext.logger->str();
  if (!error.empty()) {
    SPDLOG_ERROR("Transform pipeline failed with errors: {}", error);

    // Emit failed event
    auto elapsed = events::ToMillis(events::Now() - startTime);
    m_eventDispatcher->Emit(events::PipelineFailedEvent{
        .timestamp = events::Now(),
        .elapsed = elapsed,
        .error_message = error
    });

    throw std::runtime_error(std::format("Transform pipeline failed: {}", error));
  }

  // Emit completed event
  auto duration = events::ToMillis(events::Now() - startTime);
  m_eventDispatcher->Emit(events::PipelineCompletedEvent{
      .timestamp = events::Now(),
      .duration = duration,
      .nodes_succeeded = m_nodesCompleted.load(),
      .nodes_failed = m_nodesFailed.load(),
      .nodes_skipped = m_nodesSkipped.load()
  });

  // Transfer cached reports from storage to orchestrator's report cache
  // Reports are captured in execution nodes during TransformData() calls
  auto cachedReports = m_executionContext.cache->GetCachedReports();
  {
    std::lock_guard<std::mutex> lock(m_reportCacheMutex);
    for (const auto& [key, report] : cachedReports) {
      if (m_reportCache.contains(key)) {
        MergeReportInPlace(m_reportCache[key], report, "FinalTransfer");
      } else {
        m_reportCache[key] = report;
      }
    }
  }
  SPDLOG_DEBUG("Transferred {} reports from storage to orchestrator cache", cachedReports.size());

  // Transfer cached event markers from storage to orchestrator's event marker cache
  auto cachedEventMarkers = m_executionContext.cache->GetCachedEventMarkers();
  {
    std::lock_guard<std::mutex> lock(m_eventMarkerCacheMutex);
    for (const auto& [key, markers] : cachedEventMarkers) {
      if (m_eventMarkerCache.contains(key)) {
        m_eventMarkerCache[key].insert(
            m_eventMarkerCache[key].end(), markers.begin(), markers.end());
      } else {
        m_eventMarkerCache[key] = markers;
      }
    }
  }
  SPDLOG_DEBUG("Transferred {} event marker entries from storage to orchestrator cache", cachedEventMarkers.size());

  SPDLOG_DEBUG("Transform pipeline completed successfully");

  // Build final output from cache
  auto result = m_executionContext.cache->BuildFinalOutput();

#ifndef NDEBUG
  // Log final output sizes for alignment debugging
  spdlog::debug("FLOW DEBUG - Transform pipeline completed with {} timeframes", result.size());
  for (const auto& [timeframe, assetMap] : result) {
    for (const auto& [asset_id, dataframe] : assetMap) {
      spdlog::debug("FLOW DEBUG - Output data: {} {} has {} rows",
                   timeframe, asset_id, dataframe.num_rows());
    }
  }
#endif

  // Clean up shared data
  m_executionContext.logger->clear();

  return result;
}

std::function<void(execution_context_t)> DataFlowRuntimeOrchestrator::CreateExecutionFunction(
    const epoch_script::transform::ITransformBase &transform) {
  // Check if this transform is cross-sectional from its metadata
  bool isCrossSectional = transform.GetConfiguration().IsCrossSectional();

  if (isCrossSectional) {
    SPDLOG_DEBUG("Creating cross-sectional execution node for transform '{}'", transform.GetId());
    return MakeExecutionNode<true>(transform, m_executionContext);
  } else {
    return MakeExecutionNode<false>(transform, m_executionContext);
  }
}

DataFlowRuntimeOrchestrator::TransformNodePtr DataFlowRuntimeOrchestrator::CreateTransformNode(
    epoch_script::transform::ITransformBase& transform) {
  auto body = CreateExecutionFunction(transform);
  m_executionFunctions.push_back(body);

  const std::string transformId = transform.GetId();

  // Unlimited concurrency - TBB enforces dependencies through graph edges
  auto node = std::make_unique<TransformExecutionNode>(m_graph, tbb::flow::unlimited, body);
  SPDLOG_DEBUG("Created transform node '{}' (dependencies enforced by TBB graph)", transformId);

  // Register transform with cache (stores metadata for later queries)
  m_executionContext.cache->RegisterTransform(transform);

  auto outputs = transform.GetOutputMetaData();
  SPDLOG_DEBUG("Transform {} has {} output(s)", transformId, outputs.size());
  for (auto const &outputMetadata : outputs) {
    // safer to use transform interface to get output id due to overrides
    auto outputId = transform.GetOutputId(outputMetadata.id);
    SPDLOG_DEBUG("Registering output {} for transform {} (metadata.id={})",
                 outputId, transformId, outputMetadata.id);
    m_outputHandleToNode.insert_or_assign(outputId, node.get());
  }
  SPDLOG_DEBUG("Total handles registered so far: {}", m_outputHandleToNode.size());

  return node;
}


void DataFlowRuntimeOrchestrator::CacheReportFromTransform(
    const epoch_script::transform::ITransformBase& transform) const {
  const std::string transformId = transform.GetId();

  // TODO: Implement stateless dashboard caching from TransformResult
  // This method needs to be called from execution nodes, not here
  // For now, stubbed out to allow compilation
  (void)transformId;  // Suppress unused variable warning
  return;

  /* OLD STATEFUL IMPLEMENTATION - TO BE REPLACED
  try {
    auto report = transform.GetTearSheet();

    // Validate report before caching
    if (report.ByteSizeLong() == 0) {
      SPDLOG_WARN("Transform {} produced empty report", transformId);
      return;
    }

    // Check if this is a cross-sectional reporter
    bool isCrossSectional = transform.GetConfiguration().IsCrossSectional();

    if (isCrossSectional) {
      // Cross-sectional reporters generate a single report for all assets
      // Store under GROUP_KEY instead of per-asset
      std::lock_guard<std::mutex> lock(m_reportCacheMutex);

      if (m_reportCache.contains(epoch_script::GROUP_KEY)) {
        SPDLOG_DEBUG("Merging cross-sectional report from transform {} with existing GROUP report",
                     transformId);

        auto& existingReport = m_reportCache[epoch_script::GROUP_KEY];
        MergeReportInPlace(existingReport, report, transformId);

        SPDLOG_DEBUG("Successfully merged cross-sectional report from transform {} (final size: {} bytes)",
                     transformId, existingReport.ByteSizeLong());
      } else {
        SPDLOG_DEBUG("Cached first cross-sectional report from transform {} under GROUP_KEY ({} bytes)",
               transformId, report.ByteSizeLong());
        m_reportCache.emplace(epoch_script::GROUP_KEY, report);
      }
    } else if (asset_id.has_value()) {
      // Per-asset transform: cache report only for the specific asset
      std::lock_guard<std::mutex> lock(m_reportCacheMutex);

      // Check if we already have a report for this asset
      if (m_reportCache.contains(*asset_id)) {
        SPDLOG_DEBUG("Merging per-asset report from transform {} with existing report for asset {}",
                     transformId, *asset_id);

        // Merge the new report with the existing one
        auto& existingReport = m_reportCache[*asset_id];
        MergeReportInPlace(existingReport, report, transformId);

        SPDLOG_DEBUG("Successfully merged per-asset report from transform {} into existing report for asset {} (final size: {} bytes)",
                     transformId, *asset_id, existingReport.ByteSizeLong());
      } else {
        SPDLOG_DEBUG("Cached first per-asset report from transform {} for asset {} ({} bytes)",
               transformId, *asset_id, report.ByteSizeLong());
        m_reportCache.emplace(*asset_id, report);
      }
    } else {
      // Legacy fallback: no asset_id provided for non-cross-sectional transform
      // This shouldn't happen with the new architecture, but keep it for safety
      SPDLOG_WARN("Non-cross-sectional transform {} has no asset_id - this indicates a bug", transformId);
    }

  } catch (const std::exception& e) {
    SPDLOG_WARN("Failed to cache report from transform {}: {}", transformId, e.what());
  }
  */
}

void DataFlowRuntimeOrchestrator::MergeReportInPlace(
    epoch_proto::TearSheet& existing,
    const epoch_proto::TearSheet& newReport,
    const std::string& sourceTransformId) {

  try {
    // Use protobuf's built-in MergeFrom method for efficient merging
    // This will:
    // - Merge repeated fields (cards, charts, tables) by appending
    // - Merge singular fields by overwriting with new values
    // - Handle all nested message merging automatically
    size_t originalSize [[maybe_unused]] = existing.ByteSizeLong();
    size_t newSize [[maybe_unused]] = newReport.ByteSizeLong();

    existing.MergeFrom(newReport);

    size_t mergedSize [[maybe_unused]] = existing.ByteSizeLong();
    SPDLOG_DEBUG("Report merge completed: {} + {} = {} bytes (from transform {})",
                 originalSize, newSize, mergedSize, sourceTransformId);

    // Log details about what was merged
    if (newReport.has_cards() && newReport.cards().cards_size() > 0) {
      SPDLOG_DEBUG("Merged {} cards from transform {}",
                   newReport.cards().cards_size(), sourceTransformId);
    }
    if (newReport.has_charts() && newReport.charts().charts_size() > 0) {
      SPDLOG_DEBUG("Merged {} charts from transform {}",
                   newReport.charts().charts_size(), sourceTransformId);
    }
    if (newReport.has_tables() && newReport.tables().tables_size() > 0) {
      SPDLOG_DEBUG("Merged {} tables from transform {}",
                   newReport.tables().tables_size(), sourceTransformId);
    }

  } catch (const std::exception& e) {
    SPDLOG_ERROR("Failed to merge report from transform {}: {}", sourceTransformId, e.what());
    throw;
  }
}

void DataFlowRuntimeOrchestrator::AssignCardGroupsAndSizes(epoch_proto::TearSheet& tearsheet) {
  if (!tearsheet.has_cards()) {
    return;
  }

  auto* cards = tearsheet.mutable_cards()->mutable_cards();

  // Group cards by category
  std::map<std::string, std::vector<epoch_proto::CardDef*>> categorized;
  for (auto& card : *cards) {
    categorized[card.category()].push_back(&card);
  }

  // For each category: sort by title, assign positions
  for (auto& [category, card_list] : categorized) {
    // Sort alphabetically by first data item's title
    std::sort(card_list.begin(), card_list.end(),
      [](const epoch_proto::CardDef* a, const epoch_proto::CardDef* b) {
        std::string aTitle = a->data_size() > 0 ? a->data(0).title() : "";
        std::string bTitle = b->data_size() > 0 ? b->data(0).title() : "";
        return aTitle < bTitle;
      });

    uint64_t size = card_list.size();
    for (size_t i = 0; i < card_list.size(); ++i) {
      auto* card = card_list[i];
      card->set_group_size(size);

      // Assign group to each CardData within this CardDef
      for (auto& data : *card->mutable_data()) {
        data.set_group(i);  // position = group
      }
    }
  }
}

AssetReportMap DataFlowRuntimeOrchestrator::GetGeneratedReports() const {
  // Get raw reports from cache
  AssetReportMap result = m_reportCache;

  // Post-process each tearsheet to assign group and group_size
  for (auto& [asset_id, tearsheet] : result) {
    AssignCardGroupsAndSizes(tearsheet);
  }

  return result;
}

void DataFlowRuntimeOrchestrator::CacheEventMarkerFromTransform(
    const epoch_script::transform::ITransformBase& transform) const {
  const std::string transformId = transform.GetId();

  // TODO: Implement stateless event marker caching from TransformResult
  // This method needs to be called from execution nodes, not here
  // For now, stubbed out to allow compilation
  (void)transformId;  // Suppress unused variable warning
  return;

  /* OLD STATEFUL IMPLEMENTATION - TO BE REPLACED
  try {
    auto eventMarkerData = transform.GetEventMarkerData();

    // Validate event marker data before caching
    if (eventMarkerData.title.empty() || eventMarkerData.schemas.empty()) {
      return;
    }

    // For multi-asset scenarios, cache the event marker for each asset
    // EventMarker transforms typically generate UI metadata that applies to all assets
    // Parallel event marker caching with mutex protection
    tbb::parallel_for_each(m_asset_ids.begin(), m_asset_ids.end(), [&](const auto& asset) {
      std::lock_guard<std::mutex> lock(m_eventMarkerCacheMutex);

      // Check if we already have event markers for this asset
      if (m_eventMarkerCache.contains(asset)) {
        // Append to existing vector of event markers
        m_eventMarkerCache[asset].push_back(eventMarkerData);
        SPDLOG_DEBUG("Appended event marker from transform {} for asset {} (title: '{}', {} schemas, total event markers: {})",
               transformId, asset, eventMarkerData.title,
               eventMarkerData.schemas.size(), m_eventMarkerCache[asset].size());
      } else {
        // Create new vector with this event marker
        std::vector<epoch_script::transform::EventMarkerData> newVector;
        newVector.push_back(eventMarkerData);
        m_eventMarkerCache.emplace(asset, std::move(newVector));
        SPDLOG_DEBUG("Cached first event marker from transform {} for asset {} (title: '{}', {} schemas)",
               transformId, asset, eventMarkerData.title,
               eventMarkerData.schemas.size());
      }
    });

  } catch (const std::exception& e) {
    SPDLOG_WARN("Failed to cache event marker from transform {}: {}", transformId, e.what());
  }
  */
}

AssetEventMarkerMap DataFlowRuntimeOrchestrator::GetGeneratedEventMarkers() const {
  return m_eventMarkerCache;
}

// ============================================================================
// Event Subscription API Implementation
// ============================================================================

boost::signals2::connection DataFlowRuntimeOrchestrator::OnEvent(
    events::OrchestratorEventSlot handler,
    events::EventFilter filter) {
  return m_eventDispatcher->Subscribe(std::move(handler), std::move(filter));
}

events::IEventDispatcherPtr DataFlowRuntimeOrchestrator::GetEventDispatcher() const {
  return m_eventDispatcher;
}

// ============================================================================
// Cancellation API Implementation
// ============================================================================

void DataFlowRuntimeOrchestrator::Cancel() {
  if (m_cancellationToken) {
    m_cancellationToken->Cancel();
  }
}

bool DataFlowRuntimeOrchestrator::IsCancellationRequested() const {
  return m_cancellationToken && m_cancellationToken->IsCancelled();
}

void DataFlowRuntimeOrchestrator::ResetCancellation() {
  if (m_cancellationToken) {
    m_cancellationToken->Reset();
  }
}

// ============================================================================
// Progress Configuration Implementation
// ============================================================================

void DataFlowRuntimeOrchestrator::SetProgressSummaryInterval(std::chrono::milliseconds interval) {
  m_summaryInterval = interval;
}

void DataFlowRuntimeOrchestrator::SetProgressSummaryEnabled(bool enabled) {
  m_summaryEnabled = enabled;
}

// ============================================================================
// Progress Summary Thread Implementation
// ============================================================================

void DataFlowRuntimeOrchestrator::StartProgressSummaryThread() {
  m_summaryRunning.store(true);
  m_summaryThread = std::thread([this]() {
    while (m_summaryRunning.load()) {
      {
        std::unique_lock<std::mutex> lock(m_summaryMutex);
        m_summaryCv.wait_for(lock, m_summaryInterval, [this]() {
          return !m_summaryRunning.load();
        });
      }

      if (!m_summaryRunning.load()) break;

      EmitProgressSummary();
    }
  });
}

void DataFlowRuntimeOrchestrator::StopProgressSummaryThread() {
  m_summaryRunning.store(false);
  m_summaryCv.notify_all();

  if (m_summaryThread.joinable()) {
    m_summaryThread.join();
  }
}

void DataFlowRuntimeOrchestrator::EmitProgressSummary() {
  m_eventDispatcher->Emit(events::ProgressSummaryEvent{
      .timestamp = events::Now(),
      .overall_progress_percent = CalculateOverallProgress(),
      .nodes_completed = m_nodesCompleted.load(),
      .nodes_total = m_transforms.size(),
      .currently_running = GetRunningNodeIds(),
      .estimated_remaining = std::nullopt  // TODO: Could estimate based on average node time
  });
}

double DataFlowRuntimeOrchestrator::CalculateOverallProgress() const {
  size_t completed = m_nodesCompleted.load();
  size_t failed = m_nodesFailed.load();
  size_t skipped = m_nodesSkipped.load();
  size_t total = m_transforms.size();

  if (total == 0) return 100.0;

  double progress = (static_cast<double>(completed + failed + skipped) / total) * 100.0;
  return std::min(progress, 100.0);
}

std::vector<std::string> DataFlowRuntimeOrchestrator::GetRunningNodeIds() const {
  std::lock_guard<std::mutex> lock(m_runningNodesMutex);
  return {m_runningNodes.begin(), m_runningNodes.end()};
}

std::vector<std::string> DataFlowRuntimeOrchestrator::GetAllNodeIds() const {
  std::vector<std::string> ids;
  ids.reserve(m_transforms.size());
  for (const auto& transform : m_transforms) {
    ids.push_back(transform->GetId());
  }
  return ids;
}

void DataFlowRuntimeOrchestrator::MarkNodeStarted(const std::string& nodeId) {
  std::lock_guard<std::mutex> lock(m_runningNodesMutex);
  m_runningNodes.insert(nodeId);
}

void DataFlowRuntimeOrchestrator::MarkNodeCompleted(const std::string& nodeId) {
  std::lock_guard<std::mutex> lock(m_runningNodesMutex);
  m_runningNodes.erase(nodeId);
}

} // namespace epoch_script::runtime
