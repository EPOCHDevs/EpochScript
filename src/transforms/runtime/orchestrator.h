//
// Created by adesola on 12/28/24.
//

#pragma once
#include <epoch_script/transforms/runtime/iorchestrator.h>
#include "execution/execution_node.h"
#include "execution/execution_context.h"
#include "events/event_dispatcher.h"
#include "events/cancellation_token.h"
#include "events/transform_progress_emitter.h"
#include <epoch_script/transforms/runtime/transform_manager/itransform_manager.h>
#include <epoch_script/transforms/core/registry.h>
#include <tbb/flow_graph.h>
#include <thread>
#include <condition_variable>
#include <set>

namespace epoch_script::runtime {
    // TODO: Provide Stream Interface to Live trading
    class DataFlowRuntimeOrchestrator final : public IDataFlowOrchestrator {

    public:
        using HandleType = std::string;

        // Concurrency policy: serial for deterministic debugging, unlimited for performance
        // Controlled by EPOCH_ENABLE_PARALLEL_EXECUTION compile flag
        using TransformExecutionNode = tbb::flow::continue_node<tbb::flow::continue_msg>;
        using TransformNodePtr = std::unique_ptr<TransformExecutionNode>;

        DataFlowRuntimeOrchestrator(
            std::vector<std::string> asset_ids,
            ITransformManagerPtr transformManager,
            IIntermediateStoragePtr cacheManager = nullptr, ILoggerPtr logger = nullptr);

        void
        RegisterTransform(std::unique_ptr<epoch_script::transform::ITransformBase> transform);

        /**
         * @brief Execute the flow graph.
         *        Typically you'd push some initial ExecutionContext into "root" nodes.
         */
        TimeFrameAssetDataFrameMap ExecutePipeline(TimeFrameAssetDataFrameMap) override;

        AssetReportMap GetGeneratedReports() const override;

        AssetEventMarkerMap GetGeneratedEventMarkers() const override;

        // Public for testing
        static void MergeReportInPlace(epoch_proto::TearSheet& existing,
                                         const epoch_proto::TearSheet& newReport,
                                         const std::string& sourceTransformId);

        // ====================================================================
        // Event Subscription API
        // ====================================================================

        // Subscribe to orchestrator events with optional filter
        boost::signals2::connection OnEvent(
            events::OrchestratorEventSlot handler,
            events::EventFilter filter = events::EventFilter::All());

        // Get the event dispatcher for advanced use
        events::IEventDispatcherPtr GetEventDispatcher() const;

        // ====================================================================
        // Cancellation API
        // ====================================================================

        // Request cancellation of the running pipeline
        void Cancel();

        // Check if cancellation has been requested
        [[nodiscard]] bool IsCancellationRequested() const;

        // Reset cancellation state (for reuse)
        void ResetCancellation();

        // ====================================================================
        // Progress Configuration
        // ====================================================================

        // Set the interval for automatic progress summary events (default: 100ms)
        void SetProgressSummaryInterval(std::chrono::milliseconds interval);

        // Enable/disable automatic progress summary events
        void SetProgressSummaryEnabled(bool enabled);

    private:
        std::vector<std::string> m_asset_ids;
        tbb::flow::graph m_graph{};
        std::unordered_map<std::string, TransformExecutionNode *> m_outputHandleToNode;
        std::vector<TransformNodePtr> m_independentNodes;
        std::vector<TransformNodePtr> m_dependentNodes;
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>>
            m_transforms;
        std::vector<std::function<void(execution_context_t)>> m_executionFunctions; // temporary
        ExecutionContext m_executionContext;

        // Report cache for reporter transforms (thread-safe with mutex)
        mutable AssetReportMap m_reportCache;
        mutable std::mutex m_reportCacheMutex;

        // EventMarker cache for event_marker transforms (thread-safe with mutex)
        mutable AssetEventMarkerMap m_eventMarkerCache;
        mutable std::mutex m_eventMarkerCacheMutex;

        // ====================================================================
        // Event System Members
        // ====================================================================
        events::IEventDispatcherPtr m_eventDispatcher;
        events::CancellationTokenPtr m_cancellationToken;

        // Progress tracking
        std::atomic<size_t> m_nodesCompleted{0};
        std::atomic<size_t> m_nodesFailed{0};
        std::atomic<size_t> m_nodesSkipped{0};
        std::atomic<bool> m_isExecuting{false};

        // Progress summary thread
        std::thread m_summaryThread;
        std::atomic<bool> m_summaryRunning{false};
        std::chrono::milliseconds m_summaryInterval{100};
        bool m_summaryEnabled{true};
        std::mutex m_summaryMutex;
        std::condition_variable m_summaryCv;

        // Currently running nodes tracking
        mutable std::mutex m_runningNodesMutex;
        std::set<std::string> m_runningNodes;

        // Progress summary helpers
        void StartProgressSummaryThread();
        void StopProgressSummaryThread();
        void EmitProgressSummary();
        double CalculateOverallProgress() const;
        std::vector<std::string> GetRunningNodeIds() const;
        std::vector<std::string> GetAllNodeIds() const;

        // Node tracking helpers
        void MarkNodeStarted(const std::string& nodeId);
        void MarkNodeCompleted(const std::string& nodeId);

        std::function<void(execution_context_t)> CreateExecutionFunction(
            const epoch_script::transform::ITransformBase &transform);

        TransformNodePtr
        CreateTransformNode(epoch_script::transform::ITransformBase& transform);

        std::vector<DataFlowRuntimeOrchestrator::TransformExecutionNode *>
        ResolveInputDependencies(const epoch_script::strategy::InputMapping &inputs) const;

        // Helper to cache reports from reporter transforms
        void CacheReportFromTransform(const epoch_script::transform::ITransformBase& transform) const;

        // Helper to cache event markers from event_marker transforms
        void CacheEventMarkerFromTransform(const epoch_script::transform::ITransformBase& transform) const;

        // Helper to assign group and group_size to cards based on category
        static void AssignCardGroupsAndSizes(epoch_proto::TearSheet& tearsheet);
    };

    using DataFlowOrchestratorPtr = std::unique_ptr<DataFlowRuntimeOrchestrator>;
} // namespace epoch_script::runtime