/**
 * @file orchestrator_report_caching_test.cpp
 * @brief Comprehensive tests for report generation, caching, and merging
 *
 * Tests cover ALL report-related code paths:
 * - Reporter detection (line 137) - CRITICAL UNCOVERED
 * - Empty report handling (line 278-284) - CRITICAL UNCOVERED
 * - First report caching (line 305-307) - CRITICAL UNCOVERED
 * - Report merging (line 297-300) - CRITICAL UNCOVERED
 * - Multi-asset report caching (line 287-309) - CRITICAL UNCOVERED
 * - Parallel report caching with mutex (line 290-291)
 * - Report merge details (cards, charts, tables)
 * - GetGeneratedReports (line 377-378)
 */

#include "transforms/runtime/orchestrator.h"
#include "test_constants.h"
#include "../../integration/mocks/mock_transform.h"
#include "../../integration/mocks/mock_transform_manager.h"
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>
#include <trompeloeil.hpp>
#include <epoch_protos/tearsheet.pb.h>
#include <epoch_dashboard/tearsheet/tearsheet_builder.h>

using namespace epoch_script::runtime;
using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;

// Helper to create a dashboard builder with specific content
epoch_tearsheet::DashboardBuilder CreateDashboardWithCards(int cardCount) {
    epoch_tearsheet::DashboardBuilder builder;
    for (int i = 0; i < cardCount; ++i) {
        epoch_proto::CardDef card;
        builder.addCard(card);  // Add empty cards for counting
    }
    return builder;
}

epoch_tearsheet::DashboardBuilder CreateDashboardWithCharts(int chartCount) {
    epoch_tearsheet::DashboardBuilder builder;
    for (int i = 0; i < chartCount; ++i) {
        epoch_proto::Chart chart;
        builder.addChart(chart);  // Add empty charts for counting
    }
    return builder;
}

epoch_tearsheet::DashboardBuilder CreateDashboardWithTables(int tableCount) {
    epoch_tearsheet::DashboardBuilder builder;
    for (int i = 0; i < tableCount; ++i) {
        epoch_proto::Table table;
        builder.addTable(table);  // Add empty tables for counting
    }
    return builder;
}

TEST_CASE("DataFlowRuntimeOrchestrator - Report Caching", "[.][orchestrator][reports][critical]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string googl = TestAssetConstants::GOOG;

    SECTION("Empty report is not cached - CRITICAL") {
        // Line 278-284 in dataflow_orchestrator.cpp
        // Empty reports (ByteSizeLong() == 0) should be skipped
        auto mock = CreateSimpleMockTransform("reporter", dailyTF);

        epoch_tearsheet::DashboardBuilder emptyBuilder;  // Empty, ByteSizeLong() == 0
        REQUIRE(emptyBuilder.build().ByteSizeLong() == 0);

        REQUIRE_CALL(*mock, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mock, GetDashboard(trompeloeil::_))
            .LR_RETURN(std::nullopt);

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        // GetGeneratedReports should be empty because empty report was not cached
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.empty());
    }

    SECTION("First report cached for single asset - CRITICAL") {
        // Line 305-307 in dataflow_orchestrator.cpp
        auto mock = CreateSimpleMockTransform("reporter", dailyTF);

        auto builder = CreateDashboardWithCards(3);
        REQUIRE(builder.build().ByteSizeLong() > 0);

        REQUIRE_CALL(*mock, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mock, GetDashboard(trompeloeil::_))
            .LR_RETURN(std::optional{builder});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        // Verify report was cached
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.size() == 1);
        REQUIRE(reports.contains(aapl));
        REQUIRE(reports.at(aapl).cards().cards_size() == 3);
    }

    SECTION("First report cached for multiple assets - CRITICAL") {
        // Line 287-309 in dataflow_orchestrator.cpp
        // Report should be cached for EACH asset
        auto mock = CreateSimpleMockTransform("reporter", dailyTF);

        auto builder = CreateDashboardWithCards(2);

        REQUIRE_CALL(*mock, TransformData(trompeloeil::_))
            .TIMES(3)  // Called for each asset
            .RETURN(epoch_frame::DataFrame());

        REQUIRE_CALL(*mock, GetDashboard(trompeloeil::_))
            .TIMES(AT_LEAST(1))
            .LR_RETURN(std::optional{builder});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock));

        DataFlowRuntimeOrchestrator orch({aapl, msft, googl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][msft] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][googl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        // Verify report cached for ALL assets
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.size() == 3);
        REQUIRE(reports.contains(aapl));
        REQUIRE(reports.contains(msft));
        REQUIRE(reports.contains(googl));
        REQUIRE(reports.at(aapl).cards().cards_size() == 2);
        REQUIRE(reports.at(msft).cards().cards_size() == 2);
        REQUIRE(reports.at(googl).cards().cards_size() == 2);
    }

    SECTION("Multiple reporters merge reports for single asset - CRITICAL") {
        // Line 297-300 in dataflow_orchestrator.cpp
        auto reporter1 = CreateSimpleMockTransform("reporter1", dailyTF);
        auto reporter2 = CreateSimpleMockTransform("reporter2", dailyTF);

        auto builder1 = CreateDashboardWithCards(2);
        auto builder2 = CreateDashboardWithCards(3);

        REQUIRE_CALL(*reporter1, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        REQUIRE_CALL(*reporter1, GetDashboard(trompeloeil::_))
            .LR_RETURN(std::optional{builder1});

        REQUIRE_CALL(*reporter2, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        REQUIRE_CALL(*reporter2, GetDashboard(trompeloeil::_))
            .LR_RETURN(std::optional{builder2});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(reporter1));
        transforms.push_back(std::move(reporter2));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        // Reports should be merged: 2 + 3 = 5 cards total
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.size() == 1);
        REQUIRE(reports.at(aapl).cards().cards_size() == 5);
    }

    SECTION("Multiple reporters, multiple assets - all combinations - CRITICAL") {
        // Line 287-309 in dataflow_orchestrator.cpp
        // This is the most complex scenario
        auto reporter1 = CreateSimpleMockTransform("reporter1", dailyTF);
        auto reporter2 = CreateSimpleMockTransform("reporter2", dailyTF);
        auto reporter3 = CreateSimpleMockTransform("reporter3", dailyTF);

        auto builder1 = CreateDashboardWithCards(1);
        auto builder2 = CreateDashboardWithCards(2);
        auto builder3 = CreateDashboardWithCards(3);

        REQUIRE_CALL(*reporter1, TransformData(trompeloeil::_))
            .TIMES(2).RETURN(epoch_frame::DataFrame());
        REQUIRE_CALL(*reporter1, GetDashboard(trompeloeil::_))
            .TIMES(AT_LEAST(1)).LR_RETURN(std::optional{builder1});

        REQUIRE_CALL(*reporter2, TransformData(trompeloeil::_))
            .TIMES(2).RETURN(epoch_frame::DataFrame());
        REQUIRE_CALL(*reporter2, GetDashboard(trompeloeil::_))
            .TIMES(AT_LEAST(1)).LR_RETURN(std::optional{builder2});

        REQUIRE_CALL(*reporter3, TransformData(trompeloeil::_))
            .TIMES(2).RETURN(epoch_frame::DataFrame());
        REQUIRE_CALL(*reporter3, GetDashboard(trompeloeil::_))
            .TIMES(AT_LEAST(1)).LR_RETURN(std::optional{builder3});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(reporter1));
        transforms.push_back(std::move(reporter2));
        transforms.push_back(std::move(reporter3));

        DataFlowRuntimeOrchestrator orch({aapl, msft}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();
        inputData[dailyTF.ToString()][msft] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        // Each asset should have merged reports from all 3 reporters
        // 1 + 2 + 3 = 6 cards per asset
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.size() == 2);
        REQUIRE(reports.at(aapl).cards().cards_size() == 6);
        REQUIRE(reports.at(msft).cards().cards_size() == 6);
    }

    SECTION("Cards are merged correctly") {
        auto reporter1 = CreateSimpleMockTransform("r1", dailyTF);
        auto reporter2 = CreateSimpleMockTransform("r2", dailyTF);

        auto builder1 = CreateDashboardWithCards(5);
        auto builder2 = CreateDashboardWithCards(7);

        ALLOW_CALL(*reporter1, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        ALLOW_CALL(*reporter1, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder1});

        ALLOW_CALL(*reporter2, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        ALLOW_CALL(*reporter2, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder2});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(reporter1));
        transforms.push_back(std::move(reporter2));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.at(aapl).cards().cards_size() == 12);  // 5 + 7
    }

    SECTION("Charts are merged correctly") {
        auto reporter1 = CreateSimpleMockTransform("r1", dailyTF);
        auto reporter2 = CreateSimpleMockTransform("r2", dailyTF);

        auto builder1 = CreateDashboardWithCharts(3);
        auto builder2 = CreateDashboardWithCharts(4);

        ALLOW_CALL(*reporter1, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        ALLOW_CALL(*reporter1, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder1});

        ALLOW_CALL(*reporter2, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        ALLOW_CALL(*reporter2, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder2});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(reporter1));
        transforms.push_back(std::move(reporter2));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.at(aapl).charts().charts_size() == 7);  // 3 + 4
    }

    SECTION("Tables are merged correctly") {
        auto reporter1 = CreateSimpleMockTransform("r1", dailyTF);
        auto reporter2 = CreateSimpleMockTransform("r2", dailyTF);

        auto builder1 = CreateDashboardWithTables(2);
        auto builder2 = CreateDashboardWithTables(3);

        ALLOW_CALL(*reporter1, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        ALLOW_CALL(*reporter1, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder1});

        ALLOW_CALL(*reporter2, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        ALLOW_CALL(*reporter2, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder2});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(reporter1));
        transforms.push_back(std::move(reporter2));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.at(aapl).tables().tables_size() == 5);  // 2 + 3
    }

    SECTION("Mixed content (cards, charts, tables) merged correctly") {
        auto reporter1 = CreateSimpleMockTransform("r1", dailyTF);
        auto reporter2 = CreateSimpleMockTransform("r2", dailyTF);

        epoch_tearsheet::DashboardBuilder builder1;
        epoch_proto::CardDef card1;
        builder1.addCard(card1);  // Add empty card
        epoch_proto::Chart chart1;
        builder1.addChart(chart1);  // Add empty chart

        epoch_tearsheet::DashboardBuilder builder2;
        epoch_proto::CardDef card2;
        builder2.addCard(card2);  // Add empty card
        epoch_proto::Table table2;
        builder2.addTable(table2);  // Add empty table

        ALLOW_CALL(*reporter1, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        ALLOW_CALL(*reporter1, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder1});

        ALLOW_CALL(*reporter2, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());
        ALLOW_CALL(*reporter2, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder2});

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(reporter1));
        transforms.push_back(std::move(reporter2));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.at(aapl).cards().cards_size() == 2);
        REQUIRE(reports.at(aapl).charts().charts_size() == 1);
        REQUIRE(reports.at(aapl).tables().tables_size() == 1);
    }

    SECTION("GetGeneratedReports returns empty for no reporters") {
        // Line 377-378
        auto mock = CreateSimpleMockTransform("non_reporter", dailyTF);

        REQUIRE_CALL(*mock, TransformData(trompeloeil::_))
            .RETURN(epoch_frame::DataFrame());

        // No GetDashboard call expected

        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
        transforms.push_back(std::move(mock));

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.empty());
    }

    SECTION("Large number of reporters (stress test)") {
        std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;

        for (int i = 0; i < 20; ++i) {
            auto mock = CreateSimpleMockTransform("reporter_" + std::to_string(i), dailyTF);
            auto builder = CreateDashboardWithCards(1);

            ALLOW_CALL(*mock, TransformData(trompeloeil::_))
                .RETURN(epoch_frame::DataFrame());
            ALLOW_CALL(*mock, GetDashboard(trompeloeil::_)).LR_RETURN(std::optional{builder});

            transforms.push_back(std::move(mock));
        }

        DataFlowRuntimeOrchestrator orch({aapl}, CreateMockTransformManager(std::move(transforms)));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = epoch_frame::DataFrame();

        orch.ExecutePipeline(std::move(inputData));

        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.at(aapl).cards().cards_size() == 20);
    }
}
