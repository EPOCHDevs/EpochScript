#include <catch2/catch_test_macros.hpp>
#include <transforms/runtime/events/orchestrator_event_bridge.h>
// SDK types (LifecycleEvent, etc.) are already exposed via orchestrator_event_bridge.h

using namespace epoch_script::runtime::events;
// Use qualified names for data_sdk types to avoid Now() ambiguity

TEST_CASE("ToGenericEvent converts PipelineStartedEvent", "[events][bridge]") {
    PipelineStartedEvent event;
    event.timestamp = Now();
    event.total_nodes = 5;
    event.total_assets = 100;
    event.node_ids = {"SMA", "EMA", "RSI"};

    auto generic = ToGenericEvent(event, "job123");

    REQUIRE(std::holds_alternative<LifecycleEvent>(generic));

    auto& le = std::get<LifecycleEvent>(generic);
    REQUIRE(le.status == OperationStatus::Started);
    REQUIRE(le.operation_type == "pipeline");
    REQUIRE(le.items_total.value() == 5);
    REQUIRE(le.path.ToString() == "Job:job123");
}

TEST_CASE("ToGenericEvent converts PipelineCompletedEvent", "[events][bridge]") {
    PipelineCompletedEvent event;
    event.timestamp = Now();
    event.duration = std::chrono::milliseconds{5000};
    event.nodes_succeeded = 4;
    event.nodes_failed = 1;
    event.nodes_skipped = 0;

    auto generic = ToGenericEvent(event, "job456");

    REQUIRE(std::holds_alternative<LifecycleEvent>(generic));

    auto& le = std::get<LifecycleEvent>(generic);
    REQUIRE(le.status == OperationStatus::Completed);
    REQUIRE(le.operation_type == "pipeline");
    REQUIRE(le.duration.value() == std::chrono::milliseconds{5000});
    REQUIRE(le.items_succeeded.value() == 4);
    REQUIRE(le.items_failed.value() == 1);
}

TEST_CASE("ToGenericEvent converts PipelineFailedEvent", "[events][bridge]") {
    PipelineFailedEvent event;
    event.timestamp = Now();
    event.elapsed = std::chrono::milliseconds{1000};
    event.error_message = "Out of memory";

    auto generic = ToGenericEvent(event, "job789");

    REQUIRE(std::holds_alternative<LifecycleEvent>(generic));

    auto& le = std::get<LifecycleEvent>(generic);
    REQUIRE(le.status == OperationStatus::Failed);
    REQUIRE(le.error_message.value() == "Out of memory");
}

TEST_CASE("ToGenericEvent converts PipelineCancelledEvent", "[events][bridge]") {
    PipelineCancelledEvent event;
    event.timestamp = Now();
    event.elapsed = std::chrono::milliseconds{2000};
    event.nodes_completed = 3;
    event.nodes_total = 10;

    auto generic = ToGenericEvent(event, "jobXYZ");

    REQUIRE(std::holds_alternative<LifecycleEvent>(generic));

    auto& le = std::get<LifecycleEvent>(generic);
    REQUIRE(le.status == OperationStatus::Cancelled);
    REQUIRE(le.items_succeeded.value() == 3);
    REQUIRE(le.items_total.value() == 10);
}

TEST_CASE("ToGenericEvent converts NodeStartedEvent", "[events][bridge]") {
    NodeStartedEvent event;
    event.timestamp = Now();
    event.node_id = "node_sma";
    event.transform_name = "SMA_20";
    event.is_cross_sectional = false;
    event.node_index = 0;
    event.total_nodes = 5;
    event.asset_count = 50;

    auto generic = ToGenericEvent(event, "job123");

    REQUIRE(std::holds_alternative<LifecycleEvent>(generic));

    auto& le = std::get<LifecycleEvent>(generic);
    REQUIRE(le.status == OperationStatus::Started);
    REQUIRE(le.operation_type == "node");
    REQUIRE(le.operation_name == "SMA_20");
    REQUIRE(le.items_total.value() == 50);
    // Path should be Job:job123/Stage:nodes/Node:node_sma
    REQUIRE(le.path.ToString().find("Node:node_sma") != std::string::npos);
}

TEST_CASE("ToGenericEvent converts NodeCompletedEvent", "[events][bridge]") {
    NodeCompletedEvent event;
    event.timestamp = Now();
    event.node_id = "node_ema";
    event.transform_name = "EMA_50";
    event.duration = std::chrono::milliseconds{1500};
    event.assets_processed = 48;
    event.assets_failed = 2;

    auto generic = ToGenericEvent(event, "job123");

    REQUIRE(std::holds_alternative<LifecycleEvent>(generic));

    auto& le = std::get<LifecycleEvent>(generic);
    REQUIRE(le.status == OperationStatus::Completed);
    REQUIRE(le.operation_type == "node");
    REQUIRE(le.duration.value() == std::chrono::milliseconds{1500});
    REQUIRE(le.items_succeeded.value() == 48);
    REQUIRE(le.items_failed.value() == 2);
}

TEST_CASE("ToGenericEvent converts NodeFailedEvent", "[events][bridge]") {
    NodeFailedEvent event;
    event.timestamp = Now();
    event.node_id = "node_rsi";
    event.transform_name = "RSI_14";
    event.error_message = "Division by zero";
    event.asset_id = "AAPL";

    auto generic = ToGenericEvent(event, "job123");

    REQUIRE(std::holds_alternative<LifecycleEvent>(generic));

    auto& le = std::get<LifecycleEvent>(generic);
    REQUIRE(le.status == OperationStatus::Failed);
    REQUIRE(le.error_message.value() == "Division by zero");
    REQUIRE(le.context.count("asset_id") == 1);
}

TEST_CASE("ToGenericEvent converts NodeSkippedEvent", "[events][bridge]") {
    NodeSkippedEvent event;
    event.timestamp = Now();
    event.node_id = "node_cached";
    event.transform_name = "CachedTransform";
    event.reason = "Cache hit";

    auto generic = ToGenericEvent(event, "job123");

    REQUIRE(std::holds_alternative<LifecycleEvent>(generic));

    auto& le = std::get<LifecycleEvent>(generic);
    REQUIRE(le.status == OperationStatus::Skipped);
    REQUIRE(le.context.count("reason") == 1);
}

TEST_CASE("ToGenericEvent converts TransformProgressEvent", "[events][bridge]") {
    TransformProgressEvent event;
    event.timestamp = Now();
    event.node_id = "node_ml";
    event.transform_name = "LightGBM";
    event.current_step = 50;
    event.total_steps = 100;
    event.progress_percent = 50.0;
    event.loss = 0.123;
    event.accuracy = 0.95;
    event.learning_rate = 0.01;
    event.message = "Training in progress";

    auto generic = ToGenericEvent(event, "job123");

    REQUIRE(std::holds_alternative<ProgressEvent>(generic));

    auto& pe = std::get<ProgressEvent>(generic);
    REQUIRE(pe.current.value() == 50);
    REQUIRE(pe.total.value() == 100);
    REQUIRE(pe.progress_percent.value() == 50.0);
    REQUIRE(pe.message == "Training in progress");
    REQUIRE(pe.context.count("loss") == 1);
    REQUIRE(pe.context.count("accuracy") == 1);
    REQUIRE(pe.context.count("learning_rate") == 1);
}

TEST_CASE("ToGenericEvent converts TransformProgressEvent with asset", "[events][bridge]") {
    TransformProgressEvent event;
    event.timestamp = Now();
    event.node_id = "node_ml";
    event.transform_name = "LightGBM";
    event.asset_id = "AAPL";
    event.current_step = 25;
    event.total_steps = 100;

    auto generic = ToGenericEvent(event, "job123");

    REQUIRE(std::holds_alternative<ProgressEvent>(generic));

    auto& pe = std::get<ProgressEvent>(generic);
    // Path should include asset
    REQUIRE(pe.path.ToString().find("Asset:AAPL") != std::string::npos);
}

TEST_CASE("ToGenericEvent converts ProgressSummaryEvent", "[events][bridge]") {
    ProgressSummaryEvent event;
    event.timestamp = Now();
    event.overall_progress_percent = 67.5;
    event.nodes_completed = 5;
    event.nodes_total = 10;
    event.currently_running = {"SMA_20", "EMA_50"};
    event.estimated_remaining = std::chrono::milliseconds{30000};

    auto generic = ToGenericEvent(event, "job123");

    REQUIRE(std::holds_alternative<SummaryEvent>(generic));

    auto& se = std::get<SummaryEvent>(generic);
    REQUIRE(se.overall_progress_percent == 67.5);
    REQUIRE(se.operations_completed == 5);
    REQUIRE(se.operations_total == 10);
    REQUIRE(se.currently_running.size() == 2);
    REQUIRE(se.estimated_remaining.value() == std::chrono::milliseconds{30000});
}

TEST_CASE("BridgingEventDispatcher emits to both dispatchers", "[events][bridge]") {
    auto genericDispatcher = std::make_shared<data_sdk::events::ThreadSafeGenericEventDispatcher>();

    auto bridging = MakeBridgingEventDispatcher(genericDispatcher, "job123");

    // Track received events
    bool orchestratorReceived = false;
    bool genericReceived = false;

    genericDispatcher->Subscribe([&](const data_sdk::events::GenericEvent& e) {
        genericReceived = true;
        REQUIRE(std::holds_alternative<LifecycleEvent>(e));
    });

    // Subscribe to the bridging dispatcher for orchestrator events
    bridging->Subscribe([&orchestratorReceived](const OrchestratorEvent&) {
        orchestratorReceived = true;
    });

    // Emit an event through the bridging dispatcher
    PipelineStartedEvent event;
    event.timestamp = epoch_script::runtime::events::Now();
    event.total_nodes = 5;
    event.total_assets = 100;

    bridging->Emit(event);

    REQUIRE(orchestratorReceived);
    REQUIRE(genericReceived);
}
