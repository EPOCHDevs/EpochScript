/**
 * @file orchestrator_parallel_execution_test.cpp
 * @brief Comprehensive tests for parallel execution in DataFlowRuntimeOrchestrator
 *
 * These tests require EPOCH_ENABLE_PARALLEL_EXECUTION to be defined.
 * They test:
 * - Concurrent execution of independent transforms
 * - Race conditions in report caching (line 290-291 with mutex)
 * - Exception handling in parallel mode (line 130-131) - CRITICAL
 * - Execution order with dependencies
 * - Thread safety of shared data structures
 * - Timing and performance characteristics
 *
 * NOTE: Some tests use delays to expose race conditions that might otherwise
 * be missed due to timing.
 */

#include "transforms/runtime/orchestrator.h"
#include "test_constants.h"
#include "../../integration/mocks/mock_transform.h"
#include "../../integration/mocks/mock_transform_manager.h"
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <trompeloeil.hpp>
#include <chrono>
#include <thread>
#include <atomic>

using namespace epoch_script::runtime;
using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;
using namespace std::chrono_literals;

TEST_CASE("DataFlowRuntimeOrchestrator - Parallel Execution", "[.orchestrator][parallel][critical]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string googl = TestAssetConstants::GOOG;

    SECTION("Independent transforms execute concurrently") {
        // Create 3 independent transforms with delays
        // If truly parallel, total time should be ~delay, not 3*delay
        auto mock1 = CreateSimpleMockTransform("fast1", dailyTF);
        auto mock2 = CreateSimpleMockTransform("fast2", dailyTF);
        auto mock3 = CreateSimpleMockTransform("fast3", dailyTF);

        std::atomic<int> executionCount{0};

        // Each transform increments counter and introduces small delay
        REQUIRE_CALL(*mock1, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                executionCount++;
                std::this_thread::sleep_for(50ms);
            )
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mock2, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                executionCount++;
                std::this_thread::sleep_for(50ms);
            )
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mock3, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                executionCount++;
                std::this_thread::sleep_for(50ms);
            )
            .RETURN(epoch_frame::DataFrame());

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock1));
        transforms.push_back(std::move(mock2));
        transforms.push_back(std::move(mock3));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        auto start = std::chrono::steady_clock::now();
        orch.ExecutePipeline(std::move(inputData));
        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        // All 3 should have executed
        REQUIRE(executionCount == 3);

        // If truly parallel, should take ~50-100ms, not 150ms+
        REQUIRE(duration.count() < 120);  // Allow some overhead
    }

    SECTION("Race condition in report caching with mutex protection") {
        // Multiple reporters, multiple assets, all trying to cache simultaneously
        // This stress-tests the mutex protection (line 291 in dataflow_orchestrator.cpp)

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;

        for (int i = 0; i < 10; ++i) {
            auto mock = CreateSimpleMockTransform("reporter_" + std::to_string(i), dailyTF);

            ALLOW_CALL(*mock, TransformData(trompeloeil::_))
                .LR_SIDE_EFFECT(std::this_thread::sleep_for(10ms))  // Introduce timing variance
                .RETURN(epoch_frame::DataFrame());

            // Return nullopt since this test focuses on parallel execution, not dashboard content
            ALLOW_CALL(*mock, GetDashboard(trompeloeil::_))
                .LR_RETURN(std::nullopt);

            transforms.push_back(std::move(mock));
        }

        DataFlowRuntimeOrchestrator orch(
            {aapl, msft, googl,
             TestAssetConstants::TSLA,
             TestAssetConstants::AMZN},
            CreateMockTransformManager(std::move(transforms))
        );

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][msft] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][googl] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][TestAssetConstants::TSLA] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][TestAssetConstants::AMZN] = epoch_frame::DataFrame();

        // This should not crash or produce corrupted data despite parallel access
        REQUIRE_NOTHROW(orch.ExecutePipeline(std::move(inputData)));

        // Verify all reports merged correctly (10 reporters * 1 card each = 10 per asset)
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.size() == 5);
        for (const auto& [asset, report] : reports) {
            REQUIRE(report.cards().cards_size() == 10);
        }
    }

    SECTION("Exception in one parallel transform stops pipeline - CRITICAL") {
        // Line 130-131 in dataflow_orchestrator.cpp
        // Exception in parallel mode should be caught and propagated

        auto mock1 = CreateSimpleMockTransform("good1", dailyTF);
        auto mock2 = CreateSimpleMockTransform("failing", dailyTF);
        auto mock3 = CreateSimpleMockTransform("good2", dailyTF);

        REQUIRE_CALL(*mock1, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(std::this_thread::sleep_for(30ms))
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mock2, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(std::this_thread::sleep_for(10ms))
            .THROW(std::runtime_error("Parallel execution failure"));

        // mock3 may or may not be called depending on when mock2 fails
        ALLOW_CALL(*mock3, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(std::this_thread::sleep_for(20ms))
            .RETURN(epoch_frame::DataFrame());

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock1));
        transforms.push_back(std::move(mock2));
        transforms.push_back(std::move(mock3));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        // Should throw with the error message
        REQUIRE_THROWS_WITH(
            orch.ExecutePipeline(std::move(inputData)),
            Catch::Matchers::ContainsSubstring("Parallel execution failure")
        );
    }

    SECTION("Execution order respects dependencies in parallel mode") {
        // A and B run in parallel, C waits for both
        auto mockA = CreateSimpleMockTransform("A", dailyTF, {}, {"result"});
        auto mockB = CreateSimpleMockTransform("B", dailyTF, {}, {"result"});
        auto mockC = CreateSimpleMockTransform("C", dailyTF, {"A#result", "B#result"}, {"result"});

        std::atomic<int> aFinished{0};
        std::atomic<int> bFinished{0};
        bool cStartedTooEarly = false;

        REQUIRE_CALL(*mockA, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                std::this_thread::sleep_for(50ms);
                aFinished = 1;
            )
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mockB, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                std::this_thread::sleep_for(50ms);
                bFinished = 1;
            )
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mockC, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                // C should only start after A and B are done
                if (aFinished == 0 || bFinished == 0) {
                    cStartedTooEarly = true;
                }
            )
            .RETURN(epoch_frame::DataFrame());

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mockA));
        transforms.push_back(std::move(mockB));
        transforms.push_back(std::move(mockC));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        // Verify dependency order was respected
        REQUIRE_FALSE(cStartedTooEarly);
    }

    SECTION("Multiple assets processed in parallel per transform") {
        // One transform, multiple assets - should process assets concurrently
        auto mock = CreateSimpleMockTransform("multi_asset", dailyTF);

        std::atomic<int> concurrentCount{0};
        std::atomic<int> maxConcurrent{0};

        REQUIRE_CALL(*mock, TransformData(trompeloeil::_))
            .TIMES(5)
            .LR_SIDE_EFFECT(
                int current = ++concurrentCount;
                if (current > maxConcurrent) {
                    maxConcurrent = current;
                }
                std::this_thread::sleep_for(50ms);
                --concurrentCount;
            )
            .RETURN(epoch_frame::DataFrame());

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock));

        DataFlowRuntimeOrchestrator orch(
            {aapl, msft, googl,
             TestAssetConstants::TSLA,
             TestAssetConstants::AMZN},
            CreateMockTransformManager(std::move(transforms))
        );

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][msft] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][googl] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][TestAssetConstants::TSLA] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][TestAssetConstants::AMZN] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        // At least 2 assets should have been processed concurrently
        REQUIRE(maxConcurrent >= 2);
    }

    SECTION("Complex parallel pipeline with mixed dependencies") {
        // A, B (parallel) -> C, D (parallel, depend on A,B) -> E (depends on C,D)
        auto mockA = CreateSimpleMockTransform("A", dailyTF);
        auto mockB = CreateSimpleMockTransform("B", dailyTF);
        auto mockC = CreateSimpleMockTransform("C", dailyTF, {"A#result"});
        auto mockD = CreateSimpleMockTransform("D", dailyTF, {"B#result"});
        auto mockE = CreateSimpleMockTransform("E", dailyTF, {"C#result", "D#result"});

        std::atomic<int> executionOrder{0};
        int aOrder = 0, bOrder = 0, cOrder = 0, dOrder = 0, eOrder = 0;

        REQUIRE_CALL(*mockA, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                aOrder = ++executionOrder;
                std::this_thread::sleep_for(20ms);
            )
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mockB, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                bOrder = ++executionOrder;
                std::this_thread::sleep_for(20ms);
            )
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mockC, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                cOrder = ++executionOrder;
                std::this_thread::sleep_for(20ms);
            )
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mockD, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                dOrder = ++executionOrder;
                std::this_thread::sleep_for(20ms);
            )
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mockE, TransformData(trompeloeil::_))
            .LR_SIDE_EFFECT(
                eOrder = ++executionOrder;
            )
            .RETURN(epoch_frame::DataFrame());

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mockA));
        transforms.push_back(std::move(mockB));
        transforms.push_back(std::move(mockC));
        transforms.push_back(std::move(mockD));
        transforms.push_back(std::move(mockE));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        // Verify dependency constraints:
        // A and B can be parallel (order can vary)
        // C must come after A
        // D must come after B
        // E must come after C and D
        REQUIRE(cOrder > aOrder);
        REQUIRE(dOrder > bOrder);
        REQUIRE(eOrder > cOrder);
        REQUIRE(eOrder > dOrder);
    }

    SECTION("Stress test - many parallel transforms") {
        // 50 independent transforms executing in parallel
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;

        for (int i = 0; i < 50; ++i) {
            auto mock = CreateSimpleMockTransform("stress_" + std::to_string(i), dailyTF);

            ALLOW_CALL(*mock, TransformData(trompeloeil::_))
                .LR_SIDE_EFFECT(std::this_thread::sleep_for(5ms))
                .RETURN(epoch_frame::DataFrame());

            transforms.push_back(std::move(mock));
        }

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        auto start = std::chrono::steady_clock::now();
        REQUIRE_NOTHROW(orch.ExecutePipeline(std::move(inputData)));
        auto end = std::chrono::steady_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);

        // If serial: 50 * 5ms = 250ms
        // If parallel: should be much less
        REQUIRE(duration.count() < 200);  // Allow overhead
    }
}
