#pragma once
//
// Orchestrator Event Types for Progress Tracking
// Uses std::variant for type-safe event handling with boost::signals2
//

#include <boost/signals2/signal.hpp>
#include <chrono>
#include <glaze/glaze.hpp>
#include <map>
#include <optional>
#include <string>
#include <variant>
#include <vector>

namespace epoch_script::runtime::events {

using Timestamp = std::chrono::steady_clock::time_point;
// Use glaze's generic type for dynamic JSON metadata
using JsonMetadata = std::map<std::string, glz::generic>;

// ============================================================================
// Pipeline Events
// ============================================================================

struct PipelineStartedEvent {
    Timestamp timestamp;
    size_t total_nodes;
    size_t total_assets;
    std::vector<std::string> node_ids;
};

struct PipelineCompletedEvent {
    Timestamp timestamp;
    std::chrono::milliseconds duration;
    size_t nodes_succeeded;
    size_t nodes_failed;
    size_t nodes_skipped;
};

struct PipelineFailedEvent {
    Timestamp timestamp;
    std::chrono::milliseconds elapsed;
    std::string error_message;
};

struct PipelineCancelledEvent {
    Timestamp timestamp;
    std::chrono::milliseconds elapsed;
    size_t nodes_completed;
    size_t nodes_total;
};

// ============================================================================
// Node (Transform) Events
// ============================================================================

struct NodeStartedEvent {
    Timestamp timestamp;
    std::string node_id;
    std::string transform_name;
    bool is_cross_sectional;
    size_t node_index;
    size_t total_nodes;
    size_t asset_count;
};

struct NodeCompletedEvent {
    Timestamp timestamp;
    std::string node_id;
    std::string transform_name;
    std::chrono::milliseconds duration;
    size_t assets_processed;
    size_t assets_failed;
};

struct NodeFailedEvent {
    Timestamp timestamp;
    std::string node_id;
    std::string transform_name;
    std::string error_message;
    std::optional<std::string> asset_id;  // If per-asset failure
};

struct NodeSkippedEvent {
    Timestamp timestamp;
    std::string node_id;
    std::string transform_name;
    std::string reason;
};

// ============================================================================
// Internal Transform Progress (for ML, HMM, PCA, etc.)
// ============================================================================

struct TransformProgressEvent {
    Timestamp timestamp;
    std::string node_id;
    std::string transform_name;
    std::optional<std::string> asset_id;  // If per-asset progress

    // Structured progress fields (all optional)
    std::optional<size_t> current_step;      // e.g., epoch number
    std::optional<size_t> total_steps;       // e.g., total epochs
    std::optional<double> progress_percent;  // 0-100

    // ML-specific structured fields
    std::optional<double> loss;
    std::optional<double> accuracy;
    std::optional<double> learning_rate;
    std::optional<size_t> iteration;

    // Free-form metadata for transform-specific data
    JsonMetadata metadata;  // Additional key-value pairs

    // Human-readable message
    std::string message;
};

// ============================================================================
// Progress Summary (aggregated, emitted periodically)
// ============================================================================

struct ProgressSummaryEvent {
    Timestamp timestamp;
    double overall_progress_percent;
    size_t nodes_completed;
    size_t nodes_total;
    std::vector<std::string> currently_running;
    std::optional<std::chrono::milliseconds> estimated_remaining;
};

// ============================================================================
// Unified Event Variant
// ============================================================================

using OrchestratorEvent = std::variant<
    PipelineStartedEvent,
    PipelineCompletedEvent,
    PipelineFailedEvent,
    PipelineCancelledEvent,
    NodeStartedEvent,
    NodeCompletedEvent,
    NodeFailedEvent,
    NodeSkippedEvent,
    TransformProgressEvent,
    ProgressSummaryEvent
>;

// ============================================================================
// Event Type Enumeration (for filtering)
// ============================================================================

enum class EventType : uint8_t {
    PipelineStarted,
    PipelineCompleted,
    PipelineFailed,
    PipelineCancelled,
    NodeStarted,
    NodeCompleted,
    NodeFailed,
    NodeSkipped,
    TransformProgress,
    ProgressSummary
};

// ============================================================================
// Type Traits for Event Type Detection
// ============================================================================

template<typename T>
struct EventTypeFor;

template<> struct EventTypeFor<PipelineStartedEvent> {
    static constexpr EventType value = EventType::PipelineStarted;
};
template<> struct EventTypeFor<PipelineCompletedEvent> {
    static constexpr EventType value = EventType::PipelineCompleted;
};
template<> struct EventTypeFor<PipelineFailedEvent> {
    static constexpr EventType value = EventType::PipelineFailed;
};
template<> struct EventTypeFor<PipelineCancelledEvent> {
    static constexpr EventType value = EventType::PipelineCancelled;
};
template<> struct EventTypeFor<NodeStartedEvent> {
    static constexpr EventType value = EventType::NodeStarted;
};
template<> struct EventTypeFor<NodeCompletedEvent> {
    static constexpr EventType value = EventType::NodeCompleted;
};
template<> struct EventTypeFor<NodeFailedEvent> {
    static constexpr EventType value = EventType::NodeFailed;
};
template<> struct EventTypeFor<NodeSkippedEvent> {
    static constexpr EventType value = EventType::NodeSkipped;
};
template<> struct EventTypeFor<TransformProgressEvent> {
    static constexpr EventType value = EventType::TransformProgress;
};
template<> struct EventTypeFor<ProgressSummaryEvent> {
    static constexpr EventType value = EventType::ProgressSummary;
};

// Helper to get EventType from variant
inline EventType GetEventType(const OrchestratorEvent& event) {
    return std::visit([](const auto& e) -> EventType {
        return EventTypeFor<std::decay_t<decltype(e)>>::value;
    }, event);
}

// ============================================================================
// Signal Types
// ============================================================================

using OrchestratorEventSignal = boost::signals2::signal<void(const OrchestratorEvent&)>;
using OrchestratorEventSlot = OrchestratorEventSignal::slot_type;

// ============================================================================
// Helper Functions for Event Creation
// ============================================================================

inline Timestamp Now() {
    return std::chrono::steady_clock::now();
}

template<typename Duration>
inline std::chrono::milliseconds ToMillis(Duration d) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(d);
}

} // namespace epoch_script::runtime::events
