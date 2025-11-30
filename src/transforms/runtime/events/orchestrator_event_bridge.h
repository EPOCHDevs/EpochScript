#pragma once
//
// Bridge between EpochScript's OrchestratorEvents and EpochDataSDK's GenericEvents
// Allows orchestrator-specific events to be exposed via the domain-agnostic event system
//

#include "orchestrator_events.h"
#include "event_dispatcher.h"
#include <epoch_data_sdk/common/generic_event_types.h>
#include <epoch_data_sdk/common/generic_event_dispatcher.h>
#include <epoch_data_sdk/common/event_path.h>

namespace epoch_script::runtime::events {

// Bring in SDK types
using data_sdk::events::GenericEvent;
using data_sdk::events::LifecycleEvent;
using data_sdk::events::ProgressEvent;
using data_sdk::events::MetricEvent;
using data_sdk::events::SummaryEvent;
using data_sdk::events::LogEvent;
using data_sdk::events::OperationStatus;
using data_sdk::events::EventPath;

/**
 * Convert OrchestratorEvent to GenericEvent
 *
 * Mapping:
 *   PipelineStarted/Completed/Failed/Cancelled -> LifecycleEvent
 *   NodeStarted/Completed/Failed/Skipped -> LifecycleEvent
 *   TransformProgressEvent -> ProgressEvent (or MetricEvent for ML metrics)
 *   ProgressSummaryEvent -> SummaryEvent
 */
inline GenericEvent ToGenericEvent(const OrchestratorEvent& event, const std::string& job_id = "") {
    return std::visit([&job_id](const auto& e) -> GenericEvent {
        using T = std::decay_t<decltype(e)>;

        // Pipeline lifecycle events
        if constexpr (std::is_same_v<T, PipelineStartedEvent>) {
            LifecycleEvent le;
            le.timestamp = e.timestamp;
            le.path = data_sdk::events::MakeJobPath(job_id);
            le.status = OperationStatus::Started;
            le.operation_type = "pipeline";
            le.operation_name = "Pipeline Execution";
            le.items_total = e.total_nodes;
            le.context["total_assets"] = static_cast<double>(e.total_assets);
            return le;
        }
        else if constexpr (std::is_same_v<T, PipelineCompletedEvent>) {
            LifecycleEvent le;
            le.timestamp = e.timestamp;
            le.path = data_sdk::events::MakeJobPath(job_id);
            le.status = OperationStatus::Completed;
            le.operation_type = "pipeline";
            le.operation_name = "Pipeline Execution";
            le.duration = e.duration;
            le.items_succeeded = e.nodes_succeeded;
            le.items_failed = e.nodes_failed;
            le.items_skipped = e.nodes_skipped;
            return le;
        }
        else if constexpr (std::is_same_v<T, PipelineFailedEvent>) {
            LifecycleEvent le;
            le.timestamp = e.timestamp;
            le.path = data_sdk::events::MakeJobPath(job_id);
            le.status = OperationStatus::Failed;
            le.operation_type = "pipeline";
            le.operation_name = "Pipeline Execution";
            le.duration = e.elapsed;
            le.error_message = e.error_message;
            return le;
        }
        else if constexpr (std::is_same_v<T, PipelineCancelledEvent>) {
            LifecycleEvent le;
            le.timestamp = e.timestamp;
            le.path = data_sdk::events::MakeJobPath(job_id);
            le.status = OperationStatus::Cancelled;
            le.operation_type = "pipeline";
            le.operation_name = "Pipeline Execution";
            le.duration = e.elapsed;
            le.items_succeeded = e.nodes_completed;
            le.items_total = e.nodes_total;
            return le;
        }
        // Node lifecycle events
        else if constexpr (std::is_same_v<T, NodeStartedEvent>) {
            LifecycleEvent le;
            le.timestamp = e.timestamp;
            le.path = data_sdk::events::MakeStagePath(job_id, "nodes").Child("node", e.node_id);
            le.status = OperationStatus::Started;
            le.operation_type = "node";
            le.operation_name = e.transform_name;
            le.items_total = e.asset_count;
            le.context["node_index"] = static_cast<double>(e.node_index);
            le.context["total_nodes"] = static_cast<double>(e.total_nodes);
            le.context["is_cross_sectional"] = e.is_cross_sectional;
            return le;
        }
        else if constexpr (std::is_same_v<T, NodeCompletedEvent>) {
            LifecycleEvent le;
            le.timestamp = e.timestamp;
            le.path = data_sdk::events::MakeStagePath(job_id, "nodes").Child("node", e.node_id);
            le.status = OperationStatus::Completed;
            le.operation_type = "node";
            le.operation_name = e.transform_name;
            le.duration = e.duration;
            le.items_succeeded = e.assets_processed;
            le.items_failed = e.assets_failed;
            return le;
        }
        else if constexpr (std::is_same_v<T, NodeFailedEvent>) {
            LifecycleEvent le;
            le.timestamp = e.timestamp;
            le.path = data_sdk::events::MakeStagePath(job_id, "nodes").Child("node", e.node_id);
            le.status = OperationStatus::Failed;
            le.operation_type = "node";
            le.operation_name = e.transform_name;
            le.error_message = e.error_message;
            if (e.asset_id.has_value()) {
                le.context["asset_id"] = *e.asset_id;
            }
            return le;
        }
        else if constexpr (std::is_same_v<T, NodeSkippedEvent>) {
            LifecycleEvent le;
            le.timestamp = e.timestamp;
            le.path = data_sdk::events::MakeStagePath(job_id, "nodes").Child("node", e.node_id);
            le.status = OperationStatus::Skipped;
            le.operation_type = "node";
            le.operation_name = e.transform_name;
            le.context["reason"] = e.reason;
            return le;
        }
        // Transform progress -> ProgressEvent
        else if constexpr (std::is_same_v<T, TransformProgressEvent>) {
            ProgressEvent pe;
            pe.timestamp = e.timestamp;

            // Build path based on asset_id presence
            if (e.asset_id.has_value()) {
                pe.path = data_sdk::events::MakeNodePath(job_id, "nodes", "node", e.node_id)
                    .Child("asset", *e.asset_id);
            } else {
                pe.path = data_sdk::events::MakeStagePath(job_id, "nodes").Child("node", e.node_id);
            }

            pe.current = e.current_step;
            pe.total = e.total_steps;
            pe.progress_percent = e.progress_percent;
            pe.message = e.message;

            // Add unit if available (e.g., "epochs" for ML training)
            if (e.current_step.has_value()) {
                pe.unit = "steps";
            }

            // Copy ML metrics to context
            if (e.loss.has_value()) {
                pe.context["loss"] = *e.loss;
            }
            if (e.accuracy.has_value()) {
                pe.context["accuracy"] = *e.accuracy;
            }
            if (e.learning_rate.has_value()) {
                pe.context["learning_rate"] = *e.learning_rate;
            }
            if (e.iteration.has_value()) {
                pe.context["iteration"] = static_cast<double>(*e.iteration);
            }

            // Copy all metadata
            for (const auto& [key, value] : e.metadata) {
                pe.context[key] = value;
            }

            return pe;
        }
        // Progress summary -> SummaryEvent
        else if constexpr (std::is_same_v<T, ProgressSummaryEvent>) {
            SummaryEvent se;
            se.timestamp = e.timestamp;
            se.path = data_sdk::events::MakeJobPath(job_id);
            se.overall_progress_percent = e.overall_progress_percent;
            se.operations_completed = e.nodes_completed;
            se.operations_total = e.nodes_total;
            se.currently_running = e.currently_running;
            se.estimated_remaining = e.estimated_remaining;
            return se;
        }
        else {
            // Fallback - should never happen
            static_assert(sizeof(T) == 0, "Unhandled event type in ToGenericEvent");
        }
    }, event);
}

/**
 * BridgingEventDispatcher - Wraps an OrchestratorEvent dispatcher and
 * also emits to a GenericEvent dispatcher
 */
class BridgingEventDispatcher : public IEventDispatcher {
public:
    BridgingEventDispatcher(
        IEventDispatcherPtr orchestrator_dispatcher,
        data_sdk::events::IGenericEventDispatcherPtr generic_dispatcher,
        std::string job_id)
        : m_orchestratorDispatcher(std::move(orchestrator_dispatcher))
        , m_genericDispatcher(std::move(generic_dispatcher))
        , m_jobId(std::move(job_id))
    {}

    void Emit(OrchestratorEvent event) override {
        // Emit to orchestrator-specific subscribers
        m_orchestratorDispatcher->Emit(event);

        // Convert and emit to generic subscribers
        if (m_genericDispatcher) {
            m_genericDispatcher->Emit(ToGenericEvent(event, m_jobId));
        }
    }

    boost::signals2::connection Subscribe(
        OrchestratorEventSlot handler,
        EventFilter filter = EventFilter::All()) override {
        return m_orchestratorDispatcher->Subscribe(std::move(handler), filter);
    }

private:
    IEventDispatcherPtr m_orchestratorDispatcher;
    data_sdk::events::IGenericEventDispatcherPtr m_genericDispatcher;
    std::string m_jobId;
};

/**
 * Factory function to create a bridging dispatcher
 */
inline IEventDispatcherPtr MakeBridgingEventDispatcher(
    data_sdk::events::IGenericEventDispatcherPtr generic_dispatcher,
    const std::string& job_id) {
    return std::make_shared<BridgingEventDispatcher>(
        MakeEventDispatcher(),
        std::move(generic_dispatcher),
        job_id
    );
}

} // namespace epoch_script::runtime::events
