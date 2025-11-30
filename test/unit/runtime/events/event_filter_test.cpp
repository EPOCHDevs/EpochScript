//
// Unit tests for EventFilter
//

#include <catch2/catch_test_macros.hpp>

#include "events/event_dispatcher.h"
#include "events/orchestrator_events.h"

using namespace epoch_script::runtime::events;

TEST_CASE("EventFilter factory methods", "[events][filter]") {

    SECTION("All() accepts every event type") {
        auto filter = EventFilter::All();

        REQUIRE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE(filter.Accepts(EventType::PipelineCompleted));
        REQUIRE(filter.Accepts(EventType::PipelineFailed));
        REQUIRE(filter.Accepts(EventType::PipelineCancelled));
        REQUIRE(filter.Accepts(EventType::NodeStarted));
        REQUIRE(filter.Accepts(EventType::NodeCompleted));
        REQUIRE(filter.Accepts(EventType::NodeFailed));
        REQUIRE(filter.Accepts(EventType::NodeSkipped));
        REQUIRE(filter.Accepts(EventType::TransformProgress));
        REQUIRE(filter.Accepts(EventType::ProgressSummary));
    }

    SECTION("None() rejects every event type") {
        auto filter = EventFilter::None();

        REQUIRE_FALSE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineCompleted));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineFailed));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineCancelled));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeCompleted));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeFailed));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeSkipped));
        REQUIRE_FALSE(filter.Accepts(EventType::TransformProgress));
        REQUIRE_FALSE(filter.Accepts(EventType::ProgressSummary));
    }

    SECTION("Only() accepts listed types only") {
        auto filter = EventFilter::Only({
            EventType::PipelineStarted,
            EventType::PipelineCompleted
        });

        REQUIRE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE(filter.Accepts(EventType::PipelineCompleted));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineFailed));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::TransformProgress));
    }

    SECTION("Only() with single type") {
        auto filter = EventFilter::Only({EventType::TransformProgress});

        REQUIRE(filter.Accepts(EventType::TransformProgress));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeStarted));
    }

    SECTION("Except() rejects listed types only") {
        auto filter = EventFilter::Except({
            EventType::TransformProgress,
            EventType::ProgressSummary
        });

        REQUIRE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE(filter.Accepts(EventType::PipelineCompleted));
        REQUIRE(filter.Accepts(EventType::NodeStarted));
        REQUIRE(filter.Accepts(EventType::NodeCompleted));
        REQUIRE_FALSE(filter.Accepts(EventType::TransformProgress));
        REQUIRE_FALSE(filter.Accepts(EventType::ProgressSummary));
    }
}

TEST_CASE("EventFilter preset filters", "[events][filter]") {

    SECTION("PipelineOnly() accepts only pipeline events") {
        auto filter = EventFilter::PipelineOnly();

        // Should accept
        REQUIRE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE(filter.Accepts(EventType::PipelineCompleted));
        REQUIRE(filter.Accepts(EventType::PipelineFailed));
        REQUIRE(filter.Accepts(EventType::PipelineCancelled));

        // Should reject
        REQUIRE_FALSE(filter.Accepts(EventType::NodeStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeCompleted));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeFailed));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeSkipped));
        REQUIRE_FALSE(filter.Accepts(EventType::TransformProgress));
        REQUIRE_FALSE(filter.Accepts(EventType::ProgressSummary));
    }

    SECTION("NodesOnly() accepts only node events") {
        auto filter = EventFilter::NodesOnly();

        // Should accept
        REQUIRE(filter.Accepts(EventType::NodeStarted));
        REQUIRE(filter.Accepts(EventType::NodeCompleted));
        REQUIRE(filter.Accepts(EventType::NodeFailed));
        REQUIRE(filter.Accepts(EventType::NodeSkipped));

        // Should reject
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineCompleted));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineFailed));
        REQUIRE_FALSE(filter.Accepts(EventType::TransformProgress));
        REQUIRE_FALSE(filter.Accepts(EventType::ProgressSummary));
    }

    SECTION("ProgressOnly() accepts progress and summary events") {
        auto filter = EventFilter::ProgressOnly();

        // Should accept
        REQUIRE(filter.Accepts(EventType::TransformProgress));
        REQUIRE(filter.Accepts(EventType::ProgressSummary));

        // Should reject
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineCompleted));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeCompleted));
    }

    SECTION("TransformProgressOnly() accepts only transform progress") {
        auto filter = EventFilter::TransformProgressOnly();

        REQUIRE(filter.Accepts(EventType::TransformProgress));
        REQUIRE_FALSE(filter.Accepts(EventType::ProgressSummary));
        REQUIRE_FALSE(filter.Accepts(EventType::PipelineStarted));
        REQUIRE_FALSE(filter.Accepts(EventType::NodeStarted));
    }
}

TEST_CASE("EventFilter with OrchestratorEvent variants", "[events][filter]") {

    SECTION("Accepts() works with event variants") {
        auto filter = EventFilter::Only({EventType::NodeCompleted});

        NodeCompletedEvent completed{};
        NodeStartedEvent started{};
        PipelineStartedEvent pipelineStarted{};

        REQUIRE(filter.Accepts(OrchestratorEvent{completed}));
        REQUIRE_FALSE(filter.Accepts(OrchestratorEvent{started}));
        REQUIRE_FALSE(filter.Accepts(OrchestratorEvent{pipelineStarted}));
    }

    SECTION("PipelineOnly filter with variants") {
        auto filter = EventFilter::PipelineOnly();

        PipelineStartedEvent started{};
        PipelineCompletedEvent completed{};
        NodeStartedEvent nodeStarted{};

        REQUIRE(filter.Accepts(OrchestratorEvent{started}));
        REQUIRE(filter.Accepts(OrchestratorEvent{completed}));
        REQUIRE_FALSE(filter.Accepts(OrchestratorEvent{nodeStarted}));
    }

    SECTION("TransformProgress events filtered correctly") {
        auto progressFilter = EventFilter::TransformProgressOnly();
        auto noProgressFilter = EventFilter::Except({EventType::TransformProgress});

        TransformProgressEvent progress{};
        NodeCompletedEvent nodeCompleted{};

        REQUIRE(progressFilter.Accepts(OrchestratorEvent{progress}));
        REQUIRE_FALSE(progressFilter.Accepts(OrchestratorEvent{nodeCompleted}));

        REQUIRE_FALSE(noProgressFilter.Accepts(OrchestratorEvent{progress}));
        REQUIRE(noProgressFilter.Accepts(OrchestratorEvent{nodeCompleted}));
    }
}

TEST_CASE("EventFilter set operations", "[events][filter]") {

    SECTION("Union operator (|) combines whitelists") {
        auto f1 = EventFilter::Only({EventType::PipelineStarted});
        auto f2 = EventFilter::Only({EventType::PipelineCompleted});
        auto combined = f1 | f2;

        REQUIRE(combined.Accepts(EventType::PipelineStarted));
        REQUIRE(combined.Accepts(EventType::PipelineCompleted));
        REQUIRE_FALSE(combined.Accepts(EventType::NodeStarted));
        REQUIRE_FALSE(combined.Accepts(EventType::TransformProgress));
    }

    SECTION("Union of disjoint whitelists") {
        auto nodes = EventFilter::NodesOnly();
        auto pipeline = EventFilter::PipelineOnly();
        auto combined = nodes | pipeline;

        // Should accept both node and pipeline events
        REQUIRE(combined.Accepts(EventType::NodeStarted));
        REQUIRE(combined.Accepts(EventType::NodeCompleted));
        REQUIRE(combined.Accepts(EventType::PipelineStarted));
        REQUIRE(combined.Accepts(EventType::PipelineCompleted));
        // But not progress events
        REQUIRE_FALSE(combined.Accepts(EventType::TransformProgress));
    }

    SECTION("Intersection operator (&) intersects whitelists") {
        auto f1 = EventFilter::Only({
            EventType::PipelineStarted,
            EventType::PipelineCompleted,
            EventType::NodeStarted
        });
        auto f2 = EventFilter::Only({
            EventType::PipelineCompleted,
            EventType::NodeStarted,
            EventType::NodeCompleted
        });
        auto combined = f1 & f2;

        // Only events in both filters
        REQUIRE_FALSE(combined.Accepts(EventType::PipelineStarted));  // Only in f1
        REQUIRE(combined.Accepts(EventType::PipelineCompleted));       // In both
        REQUIRE(combined.Accepts(EventType::NodeStarted));             // In both
        REQUIRE_FALSE(combined.Accepts(EventType::NodeCompleted));     // Only in f2
    }

    SECTION("Union with All() - mixed whitelist/blacklist") {
        auto specific = EventFilter::Only({EventType::NodeStarted});
        auto all = EventFilter::All();
        auto combined = specific | all;

        // Mixed union: whitelist takes precedence, blacklist (empty) is subtracted
        // Result is the whitelist items that are NOT in the blacklist
        // Since blacklist is empty, result = whitelist = {NodeStarted}
        REQUIRE(combined.Accepts(EventType::NodeStarted));
        // Other event types are NOT in the resulting whitelist
        REQUIRE_FALSE(combined.Accepts(EventType::PipelineStarted));
        REQUIRE_FALSE(combined.Accepts(EventType::TransformProgress));
    }

    SECTION("Intersection with All() returns original") {
        auto specific = EventFilter::Only({EventType::NodeStarted, EventType::NodeCompleted});
        auto all = EventFilter::All();
        auto combined = specific & all;

        REQUIRE(combined.Accepts(EventType::NodeStarted));
        REQUIRE(combined.Accepts(EventType::NodeCompleted));
        REQUIRE_FALSE(combined.Accepts(EventType::PipelineStarted));
    }

    SECTION("Union with None() returns original") {
        auto specific = EventFilter::Only({EventType::NodeStarted});
        auto none = EventFilter::None();
        auto combined = specific | none;

        REQUIRE(combined.Accepts(EventType::NodeStarted));
        REQUIRE_FALSE(combined.Accepts(EventType::PipelineStarted));
    }

    SECTION("Intersection with None() returns None()") {
        auto specific = EventFilter::Only({EventType::NodeStarted});
        auto none = EventFilter::None();
        auto combined = specific & none;

        REQUIRE_FALSE(combined.Accepts(EventType::NodeStarted));
        REQUIRE_FALSE(combined.Accepts(EventType::PipelineStarted));
    }

    SECTION("Chained operations") {
        auto pipeline = EventFilter::PipelineOnly();
        auto nodes = EventFilter::NodesOnly();
        auto progress = EventFilter::ProgressOnly();

        auto combined = (pipeline | nodes) & EventFilter::Except({EventType::NodeFailed});

        REQUIRE(combined.Accepts(EventType::PipelineStarted));
        REQUIRE(combined.Accepts(EventType::NodeStarted));
        REQUIRE(combined.Accepts(EventType::NodeCompleted));
        REQUIRE_FALSE(combined.Accepts(EventType::NodeFailed));  // Excluded
        REQUIRE_FALSE(combined.Accepts(EventType::TransformProgress));  // Not in pipeline|nodes
    }
}

TEST_CASE("GetEventType helper", "[events][filter]") {

    SECTION("Returns correct type for each event") {
        REQUIRE(GetEventType(OrchestratorEvent{PipelineStartedEvent{}}) == EventType::PipelineStarted);
        REQUIRE(GetEventType(OrchestratorEvent{PipelineCompletedEvent{}}) == EventType::PipelineCompleted);
        REQUIRE(GetEventType(OrchestratorEvent{PipelineFailedEvent{}}) == EventType::PipelineFailed);
        REQUIRE(GetEventType(OrchestratorEvent{PipelineCancelledEvent{}}) == EventType::PipelineCancelled);
        REQUIRE(GetEventType(OrchestratorEvent{NodeStartedEvent{}}) == EventType::NodeStarted);
        REQUIRE(GetEventType(OrchestratorEvent{NodeCompletedEvent{}}) == EventType::NodeCompleted);
        REQUIRE(GetEventType(OrchestratorEvent{NodeFailedEvent{}}) == EventType::NodeFailed);
        REQUIRE(GetEventType(OrchestratorEvent{NodeSkippedEvent{}}) == EventType::NodeSkipped);
        REQUIRE(GetEventType(OrchestratorEvent{TransformProgressEvent{}}) == EventType::TransformProgress);
        REQUIRE(GetEventType(OrchestratorEvent{ProgressSummaryEvent{}}) == EventType::ProgressSummary);
    }
}
