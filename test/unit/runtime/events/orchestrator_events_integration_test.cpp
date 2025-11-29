//
// Integration tests for Orchestrator Event System
//

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_vector.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "events/event_dispatcher.h"
#include "events/orchestrator_events.h"
#include "events/cancellation_token.h"
#include "events/transform_progress_emitter.h"

#include "fake_data_sources.h"
#include "test_constants.h"
#include "integration/mocks/mock_transform.h"
#include "integration/mocks/mock_transform_manager.h"

#include <epoch_script/transforms/runtime/iorchestrator.h>
#include <epoch_script/transforms/core/constant_value.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_protos/tearsheet.pb.h>

#include <atomic>
#include <chrono>
#include <thread>
#include <vector>

#include "orchestrator.h"

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::events;
using namespace epoch_script::runtime::test;

namespace {
epoch_frame::DataFrame MakeSingleColumnInput() {
    auto index = epoch_frame::factory::index::make_datetime_index(
        std::vector<int64_t>{1577836800000000000LL, 1577923200000000000LL},
        "index",
        "UTC");
    auto values = epoch_frame::factory::array::make_array(std::vector<double>{1.0, 2.0});
    return epoch_frame::make_dataframe(index, {values}, {"input"});
}
}

TEST_CASE("Orchestrator event emission", "[orchestrator][events][integration]") {

    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Pipeline emits started event at beginning") {
        std::vector<EventType> receivedTypes;

        auto mock = CreateSimpleMockTransform("t1", dailyTF);

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            receivedTypes.push_back(GetEventType(e));
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 101.0, 102.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE_FALSE(receivedTypes.empty());
        REQUIRE(receivedTypes.front() == EventType::PipelineStarted);
    }

    SECTION("Pipeline emits completed event at end") {
        std::vector<EventType> receivedTypes;

        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            receivedTypes.push_back(GetEventType(e));
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 101.0, 102.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE_FALSE(receivedTypes.empty());
        REQUIRE(receivedTypes.back() == EventType::PipelineCompleted);
    }

    SECTION("PipelineStartedEvent contains correct metadata") {
        PipelineStartedEvent startedEvent{};

        auto mock1 = CreateSimpleMockTransform("transform_a", dailyTF);
        auto mock2 = CreateSimpleMockTransform("transform_b", dailyTF);

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock1));
        transforms.push_back(std::move(mock2));

        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl, TestAssetConstants::MSFT}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<PipelineStartedEvent>(&e)) {
                startedEvent = *p;
            }
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});
        input[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE(startedEvent.total_nodes == 2);
        REQUIRE(startedEvent.total_assets == 2);
        REQUIRE(startedEvent.node_ids.size() == 2);
    }

    SECTION("PipelineCompletedEvent contains duration") {
        PipelineCompletedEvent completedEvent{};

        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<PipelineCompletedEvent>(&e)) {
                completedEvent = *p;
            }
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE(completedEvent.duration.count() >= 0);
    }
}

TEST_CASE("Orchestrator node events", "[orchestrator][events][integration]") {

    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("NodeStartedEvent emitted for each transform") {
        std::vector<std::string> startedNodes;

        auto mock1 = CreateSimpleMockTransform("transform_A", dailyTF);
        auto mock2 = CreateSimpleMockTransform("transform_B", dailyTF);

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock1));
        transforms.push_back(std::move(mock2));

        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<NodeStartedEvent>(&e)) {
                startedNodes.push_back(p->node_id);
            }
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE(startedNodes.size() == 2);
        REQUIRE_THAT(startedNodes, Catch::Matchers::VectorContains(std::string("transform_A")));
        REQUIRE_THAT(startedNodes, Catch::Matchers::VectorContains(std::string("transform_B")));
    }

    SECTION("NodeCompletedEvent emitted for each transform") {
        std::vector<std::string> completedNodes;

        auto mock1 = CreateSimpleMockTransform("transform_A", dailyTF);
        auto mock2 = CreateSimpleMockTransform("transform_B", dailyTF);

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock1));
        transforms.push_back(std::move(mock2));

        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<NodeCompletedEvent>(&e)) {
                completedNodes.push_back(p->node_id);
            }
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE(completedNodes.size() == 2);
    }

    SECTION("NodeCompletedEvent includes duration") {
        NodeCompletedEvent completedEvent{};

        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<NodeCompletedEvent>(&e)) {
                completedEvent = *p;
            }
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE(completedEvent.duration.count() >= 0);
    }

    SECTION("NodeStartedEvent includes asset count") {
        NodeStartedEvent startedEvent{};

        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl, TestAssetConstants::MSFT}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<NodeStartedEvent>(&e)) {
                startedEvent = *p;
            }
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});
        input[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE(startedEvent.asset_count == 2);
    }
}

TEST_CASE("Orchestrator event filtering", "[orchestrator][events][integration]") {

    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Filtered subscription only receives matching events") {
        int pipelineEventCount = 0;
        int nodeEventCount = 0;
        int allEventCount = 0;

        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const auto&) { allEventCount++; });
        orch.OnEvent([&](const auto&) { pipelineEventCount++; }, EventFilter::PipelineOnly());
        orch.OnEvent([&](const auto&) { nodeEventCount++; }, EventFilter::NodesOnly());

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE(allEventCount > 0);
        REQUIRE(pipelineEventCount > 0);
        REQUIRE(nodeEventCount > 0);
        REQUIRE(allEventCount >= pipelineEventCount + nodeEventCount);
    }
}

TEST_CASE("Orchestrator event dispatcher access", "[orchestrator][events][integration]") {

    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("GetEventDispatcher returns non-null") {
        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        REQUIRE(orch.GetEventDispatcher() != nullptr);
    }

    SECTION("GetEventDispatcher returns same instance") {
        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        auto d1 = orch.GetEventDispatcher();
        auto d2 = orch.GetEventDispatcher();

        REQUIRE(d1.get() == d2.get());
    }

    SECTION("Direct subscription via dispatcher works") {
        int count = 0;

        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        // Subscribe via dispatcher directly
        orch.GetEventDispatcher()->Subscribe([&](const auto&) { count++; });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        REQUIRE(count > 0);
    }
}

TEST_CASE("Orchestrator cancellation", "[orchestrator][events][integration]") {

    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("IsCancellationRequested initially false") {
        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        REQUIRE_FALSE(orch.IsCancellationRequested());
    }

    SECTION("Cancel sets cancellation flag") {
        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.Cancel();

        REQUIRE(orch.IsCancellationRequested());
    }

    SECTION("ResetCancellation clears flag") {
        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.Cancel();
        REQUIRE(orch.IsCancellationRequested());

        orch.ResetCancellation();
        REQUIRE_FALSE(orch.IsCancellationRequested());
    }
}

TEST_CASE("Orchestrator progress emitter integration", "[orchestrator][events][integration]") {

    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Transforms receive progress emitter") {
        TransformProgressEmitterPtr receivedEmitter;

        auto mock = CreateFullyMockedTransform("t1", dailyTF);

        ALLOW_CALL(*mock, SetProgressEmitter(trompeloeil::_))
            .LR_SIDE_EFFECT(receivedEmitter = _1);
        ALLOW_CALL(*mock, GetProgressEmitter())
            .LR_RETURN(receivedEmitter);
        ALLOW_CALL(*mock, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame{});
        ALLOW_CALL(*mock, TransformDataWithMetadata(trompeloeil::_))
            .RETURN(epoch_script::runtime::TransformResult{});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        // Progress emitter should be set during orchestrator construction
        REQUIRE(receivedEmitter != nullptr);
    }

    SECTION("Progress emitter has correct node info") {
        TransformProgressEmitterPtr receivedEmitter;

        auto mock = CreateFullyMockedTransform("my_transform_id", dailyTF);
        mock->m_name = "MyTransformName";

        ALLOW_CALL(*mock, SetProgressEmitter(trompeloeil::_))
            .LR_SIDE_EFFECT(receivedEmitter = _1);
        ALLOW_CALL(*mock, GetProgressEmitter())
            .LR_RETURN(receivedEmitter);
        ALLOW_CALL(*mock, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame{});
        ALLOW_CALL(*mock, TransformDataWithMetadata(trompeloeil::_))
            .RETURN(epoch_script::runtime::TransformResult{});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        REQUIRE(receivedEmitter != nullptr);
        REQUIRE(receivedEmitter->GetNodeId() == "my_transform_id");
    }

    SECTION("Transform progress events flow through dispatcher") {
        TransformProgressEmitterPtr emitter;
        std::vector<TransformProgressEvent> progressEvents;

        auto mock = CreateFullyMockedTransform("t1", dailyTF);

        ALLOW_CALL(*mock, SetProgressEmitter(trompeloeil::_))
            .LR_SIDE_EFFECT(emitter = _1);
        ALLOW_CALL(*mock, GetProgressEmitter())
            .LR_RETURN(emitter);
        ALLOW_CALL(*mock, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT({
                // Emit progress during transform execution
                if (emitter) {
                    emitter->EmitProgress(50, 100, "Halfway done");
                }
            })
            .RETURN(epoch_frame::DataFrame{});
        ALLOW_CALL(*mock, TransformDataWithMetadata(trompeloeil::_))
            .LR_SIDE_EFFECT({
                if (emitter) {
                    emitter->EmitProgress(50, 100, "Halfway done");
                }
            })
            .RETURN(epoch_script::runtime::TransformResult{});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<TransformProgressEvent>(&e)) {
                progressEvents.push_back(*p);
            }
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        // Should have received progress events from the transform
        REQUIRE_FALSE(progressEvents.empty());
        REQUIRE(progressEvents[0].current_step == 50);
        REQUIRE(progressEvents[0].total_steps == 100);
    }
}

TEST_CASE("Event order consistency", "[orchestrator][events][integration]") {

    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Pipeline started comes before node events") {
        std::vector<EventType> eventOrder;

        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            eventOrder.push_back(GetEventType(e));
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        // Find positions
        auto pipelineStartedPos = std::find(eventOrder.begin(), eventOrder.end(), EventType::PipelineStarted);
        auto nodeStartedPos = std::find(eventOrder.begin(), eventOrder.end(), EventType::NodeStarted);

        REQUIRE(pipelineStartedPos != eventOrder.end());
        REQUIRE(nodeStartedPos != eventOrder.end());
        REQUIRE(pipelineStartedPos < nodeStartedPos);
    }

    SECTION("Node events come before pipeline completed") {
        std::vector<EventType> eventOrder;

        auto mock = CreateSimpleMockTransform("t1", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            eventOrder.push_back(GetEventType(e));
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        auto nodeCompletedPos = std::find(eventOrder.begin(), eventOrder.end(), EventType::NodeCompleted);
        auto pipelineCompletedPos = std::find(eventOrder.begin(), eventOrder.end(), EventType::PipelineCompleted);

        REQUIRE(nodeCompletedPos != eventOrder.end());
        REQUIRE(pipelineCompletedPos != eventOrder.end());
        REQUIRE(nodeCompletedPos < pipelineCompletedPos);
    }

    SECTION("Node started comes before node completed for same node") {
        std::vector<std::pair<EventType, std::string>> eventOrder;

        auto mock = CreateSimpleMockTransform("test_node", dailyTF);
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.emplace_back(std::move(mock));
        auto manager = CreateMockTransformManager(std::move(transforms));

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        orch.OnEvent([&](const OrchestratorEvent& e) {
            std::visit([&](const auto& ev) {
                using T = std::decay_t<decltype(ev)>;
                if constexpr (std::is_same_v<T, NodeStartedEvent>) {
                    eventOrder.emplace_back(EventType::NodeStarted, ev.node_id);
                } else if constexpr (std::is_same_v<T, NodeCompletedEvent>) {
                    eventOrder.emplace_back(EventType::NodeCompleted, ev.node_id);
                }
            }, e);
        });

        TimeFrameAssetDataFrameMap input;
        input[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        orch.ExecutePipeline(std::move(input));

        // Find the events for test_node
        size_t startedIndex = SIZE_MAX, completedIndex = SIZE_MAX;
        for (size_t i = 0; i < eventOrder.size(); ++i) {
            if (eventOrder[i].second == "test_node") {
                if (eventOrder[i].first == EventType::NodeStarted) {
                    startedIndex = i;
                } else if (eventOrder[i].first == EventType::NodeCompleted) {
                    completedIndex = i;
                }
            }
        }

        REQUIRE(startedIndex != SIZE_MAX);
        REQUIRE(completedIndex != SIZE_MAX);
        REQUIRE(startedIndex < completedIndex);
    }
}

TEST_CASE("Cross-sectional reporter emits completion and success counts", "[orchestrator][events][integration][reporter]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    auto reporter = CreateFullyMockedTransform("reporter_node", dailyTF, {"num_1"}, {}, true);
    reporter->m_outputMetadata.clear(); // reporters have no outputs

    epoch_script::strategy::InputMapping inputs{
        {"SLOT0", {epoch_script::strategy::InputValue{
            epoch_script::transform::ConstantValue{1.0}}}}
    };

    epoch_script::transforms::TransformsMetaData meta{
        .id = "mock_reporter",
        .category = epoch_core::TransformCategory::Reporter,
        .name = "Mock Reporter",
        .isCrossSectional = true,
        .inputs = {epoch_script::transforms::IOMetaData{
            epoch_core::IODataType::Decimal, "SLOT0", "slot0", false, false}},
        .outputs = {},
        .allowNullInputs = true
    };

    epoch_script::TransformDefinitionData data{
        .type = "mock_reporter",
        .id = reporter->m_id,
        .options = {},
        .timeframe = dailyTF,
        .inputs = inputs,
        .metaData = meta
    };
    reporter->m_cachedConfig = std::make_unique<epoch_script::transform::TransformConfiguration>(
        epoch_script::TransformDefinition{std::move(data)});

    int transformCalls = 0;
    int dashboardCalls = 0;

    ALLOW_CALL(*reporter, SetProgressEmitter(trompeloeil::_));
    ALLOW_CALL(*reporter, GetProgressEmitter())
        .RETURN(nullptr);

    epoch_tearsheet::DashboardBuilder builder;
    builder.addCard(epoch_proto::CardDef{});

    ALLOW_CALL(*reporter, TransformData(trompeloeil::_))
        .LR_SIDE_EFFECT(++transformCalls)
        .RETURN(epoch_frame::DataFrame{});
    ALLOW_CALL(*reporter, GetDashboard(trompeloeil::_))
        .LR_SIDE_EFFECT(++dashboardCalls)
        .LR_RETURN(std::optional{builder});

    std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
    transforms.emplace_back(std::move(reporter));
    auto manager = CreateMockTransformManager(std::move(transforms));

    DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

    std::vector<NodeCompletedEvent> nodeCompleted;
    PipelineCompletedEvent pipelineCompleted{};
    orch.OnEvent([&](const OrchestratorEvent& e) {
        if (auto* p = std::get_if<NodeCompletedEvent>(&e)) {
            nodeCompleted.push_back(*p);
        } else if (auto* p = std::get_if<PipelineCompletedEvent>(&e)) {
            pipelineCompleted = *p;
        }
    });

    TimeFrameAssetDataFrameMap input;
    input[dailyTF.ToString()][aapl] = MakeSingleColumnInput();

    orch.ExecutePipeline(std::move(input));

    REQUIRE(transformCalls == 1);
    REQUIRE(dashboardCalls == 1);
    REQUIRE(nodeCompleted.size() == 1);
    REQUIRE(nodeCompleted[0].node_id == "reporter_node");
    REQUIRE(pipelineCompleted.nodes_succeeded == 1);
    REQUIRE(pipelineCompleted.nodes_skipped == 0);
}

TEST_CASE("Skipped nodes update progress counters", "[orchestrator][events][integration][skip]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    auto skipper = CreateFullyMockedTransform("skip_node", dailyTF);

    ALLOW_CALL(*skipper, SetProgressEmitter(trompeloeil::_));
    ALLOW_CALL(*skipper, GetProgressEmitter())
        .RETURN(nullptr);

    epoch_script::transforms::TransformsMetaData meta{
        .id = "skip_meta",
        .category = epoch_core::TransformCategory::Math,
        .name = "Skip Meta",
        .isCrossSectional = false,
        .intradayOnly = true,
        .allowNullInputs = true
    };

    epoch_script::TransformDefinitionData data{
        .type = "skip_meta",
        .id = skipper->m_id,
        .options = {},
        .timeframe = dailyTF,
        .inputs = {},
        .metaData = meta
    };
    skipper->m_cachedConfig = std::make_unique<epoch_script::transform::TransformConfiguration>(
        epoch_script::TransformDefinition{std::move(data)});

    std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
    transforms.emplace_back(std::move(skipper));
    auto manager = CreateMockTransformManager(std::move(transforms));

    DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

    int skippedCount = 0;
    PipelineCompletedEvent pipelineCompleted{};
    orch.OnEvent([&](const OrchestratorEvent& e) {
        if (std::holds_alternative<NodeSkippedEvent>(e)) {
            skippedCount++;
        } else if (auto* p = std::get_if<PipelineCompletedEvent>(&e)) {
            pipelineCompleted = *p;
        }
    });

    TimeFrameAssetDataFrameMap input;
    input[dailyTF.ToString()][aapl] = MakeSingleColumnInput();

    orch.ExecutePipeline(std::move(input));

    REQUIRE(skippedCount == 1);
    REQUIRE(pipelineCompleted.nodes_succeeded == 0);
    REQUIRE(pipelineCompleted.nodes_skipped == 1);
}

TEST_CASE("Progress summary reports running nodes", "[orchestrator][events][integration][progress]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    auto mock = CreateSimpleMockTransform("long_node", dailyTF);

    ALLOW_CALL(*mock, SetProgressEmitter(trompeloeil::_));
    ALLOW_CALL(*mock, GetProgressEmitter())
        .RETURN(nullptr);

    // Slow down execution to ensure summary thread captures running state
    ALLOW_CALL(*mock, TransformData(trompeloeil::_))
        .SIDE_EFFECT(std::this_thread::sleep_for(std::chrono::milliseconds(20)))
        .RETURN(epoch_frame::DataFrame{});

    std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
    transforms.emplace_back(std::move(mock));
    auto manager = CreateMockTransformManager(std::move(transforms));

    DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));
    orch.SetProgressSummaryInterval(std::chrono::milliseconds(1));
    orch.SetProgressSummaryEnabled(true);

    std::vector<ProgressSummaryEvent> summaries;
    orch.OnEvent([&](const OrchestratorEvent& e) {
        if (auto* p = std::get_if<ProgressSummaryEvent>(&e)) {
            summaries.push_back(*p);
        }
    });

    TimeFrameAssetDataFrameMap input;
    input[dailyTF.ToString()][aapl] = MakeSingleColumnInput();

    orch.ExecutePipeline(std::move(input));

    REQUIRE_FALSE(summaries.empty());
    bool sawRunning = false;
    for (const auto& summary : summaries) {
        if (!summary.currently_running.empty() &&
            summary.currently_running[0] == "long_node") {
            sawRunning = true;
        }
    }
    REQUIRE(sawRunning);
}
