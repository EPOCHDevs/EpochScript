/**
 * @file orchestrator_event_marker_caching_test.cpp
 * @brief Realistic tests for event marker generation and caching in orchestrator
 *
 * Tests event marker functionality with:
 * - Compiled Python/epoch source code
 * - Single and multiple assets
 * - Node references and literal inputs
 * - Concrete value verification
 */

#include "transforms/runtime/orchestrator.h"
#include "transform_manager/transform_manager.h"
#include "fake_data_sources.h"
#include "../common/test_constants.h"
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/core/constants.h>
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;

namespace {

// Helper to compile source and build TransformManager
std::unique_ptr<TransformManager> CompileSource(const std::string& sourceCode) {
    auto pythonSource = epoch_script::strategy::PythonSource(sourceCode);
    return std::make_unique<TransformManager>(pythonSource);
}

} // anonymous namespace

TEST_CASE("EventMarker - Realistic Caching Tests", "[orchestrator][event_markers]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string googl = TestAssetConstants::GOOGL;

    SECTION("Basic event marker with single asset") {
        // Source: compare close price > 100, create event marker
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
event_marker(
    schema=EventMarkerSchema(
        title="Price Above 100",
        select_key="SLOT0",
        schemas=[CardColumnSchema(
            column_id="SLOT0",
            slot="PrimaryBadge",
            render_type="Badge",
            color_map={}
        )]
    )
)(signal)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        // Input: close prices [90, 110, 95, 120] - 2 values > 100
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({90.0, 110.0, 95.0, 120.0});

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        // Verify event markers exist for single asset
        REQUIRE(eventMarkers.size() == 1);
        REQUIRE(eventMarkers.contains(aapl));

        auto& aaplMarkers = eventMarkers.at(aapl);
        REQUIRE(aaplMarkers.size() == 1);

        // Verify title
        REQUIRE(aaplMarkers[0].title == "Price Above 100");

        // Verify schema (EventMarker automatically adds pivot column, so 1 input + 1 pivot = 2)
        REQUIRE(aaplMarkers[0].schemas.size() == 2);
        REQUIRE(aaplMarkers[0].schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
        REQUIRE(aaplMarkers[0].schemas[0].render_type == epoch_core::CardRenderType::Badge);

        // Verify filtered data: only rows where c > 100 (indices 1 and 3)
        REQUIRE(aaplMarkers[0].data.num_rows() == 2);

        // Verify pivot column exists for chart navigation
        REQUIRE(aaplMarkers[0].data.contains("pivot"));

        // Verify column_id mapping - SLOT0 resolves to comparison result
        REQUIRE(aaplMarkers[0].schemas[0].column_id == "gt_0#result");

        // Verify pivot schema was added correctly
        REQUIRE(aaplMarkers[0].schemas[1].column_id == "pivot");
        REQUIRE(aaplMarkers[0].schemas[1].render_type == epoch_core::CardRenderType::Timestamp);
    }

    SECTION("Event marker with multiple assets") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
event_marker(
    schema=EventMarkerSchema(
        title="Multi-Asset Signal",
        select_key="SLOT0",
        schemas=[CardColumnSchema(
            column_id="SLOT0",
            slot="PrimaryBadge",
            render_type="Badge",
            color_map={}
        )]
    )
)(signal)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl, msft, googl}, std::move(transformManager));

        // Same data for all assets
        auto ohlcv = CreateOHLCVData({90.0, 110.0, 95.0, 120.0});
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = ohlcv;
        inputData[dailyTF.ToString()][msft] = ohlcv;
        inputData[dailyTF.ToString()][googl] = ohlcv;

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        // Verify all 3 assets have event markers
        REQUIRE(eventMarkers.size() == 3);
        REQUIRE(eventMarkers.contains(aapl));
        REQUIRE(eventMarkers.contains(msft));
        REQUIRE(eventMarkers.contains(googl));

        // Verify each asset has same structure with concrete values
        for (const auto& asset : {aapl, msft, googl}) {
            INFO("Checking asset: " << asset);
            auto& markers = eventMarkers.at(asset);
            REQUIRE(markers.size() == 1);
            REQUIRE(markers[0].title == "Multi-Asset Signal");
            REQUIRE(markers[0].schemas.size() == 2);  // 1 input + pivot
            REQUIRE(markers[0].data.num_rows() == 2);

            // Verify schema column IDs - SLOT0 resolves to actual column name
            REQUIRE(markers[0].schemas[0].column_id == "gt_0#result");  // signal: src.c > 100
            REQUIRE(markers[0].schemas[1].column_id == "pivot");

            // Verify pivot column exists in data
            REQUIRE(markers[0].data.contains("pivot"));

            // Verify pivot render type
            REQUIRE(markers[0].schemas[1].render_type == epoch_core::CardRenderType::Timestamp);
        }
    }

    SECTION("Event marker with moving average crossover") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
fast = sma(period=2)(src.c)
slow = sma(period=3)(src.c)
signal = crossover()(fast, slow)
event_marker(
    schema=EventMarkerSchema(
        title="MA Crossover",
        select_key="SLOT0",
        schemas=[CardColumnSchema(
            column_id="SLOT0",
            slot="PrimaryBadge",
            render_type="Badge",
            color_map={}
        )]
    )
)(signal)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        // Create data with a clear crossover pattern
        // Prices: 100, 95, 90, 95, 100, 105 (dip then rise = crossover opportunity)
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 95.0, 90.0, 95.0, 100.0, 105.0});

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        REQUIRE(eventMarkers.size() == 1);
        REQUIRE(eventMarkers.contains(aapl));

        auto& markers = eventMarkers.at(aapl);
        REQUIRE(markers.size() == 1);
        REQUIRE(markers[0].title == "MA Crossover");

        // Verify schema structure
        REQUIRE(markers[0].schemas.size() == 2);  // 1 input + pivot
        REQUIRE(markers[0].schemas[0].column_id == "signal#result");  // crossover uses Python variable name
        REQUIRE(markers[0].schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
        REQUIRE(markers[0].schemas[1].column_id == "pivot");
        REQUIRE(markers[0].schemas[1].render_type == epoch_core::CardRenderType::Timestamp);

        // Crossover events depend on data - verify structure is correct
        REQUIRE(markers[0].data.contains("pivot"));
    }

    SECTION("Multiple event markers in pipeline") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
high_signal = src.c > 110
low_signal = src.c < 95

event_marker(
    schema=EventMarkerSchema(
        title="Price High",
        select_key="SLOT0",
        schemas=[CardColumnSchema(column_id="SLOT0", slot="PrimaryBadge", render_type="Badge", color_map={})]
    )
)(high_signal)

event_marker(
    schema=EventMarkerSchema(
        title="Price Low",
        select_key="SLOT0",
        schemas=[CardColumnSchema(column_id="SLOT0", slot="SecondaryBadge", render_type="Badge", color_map={})]
    )
)(low_signal)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        // Data: 90, 110, 95, 120, 80
        // high_signal: [F, F, F, T, F] -> 1 row (120)
        // low_signal: [T, F, F, F, T] -> 2 rows (90, 80)
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({90.0, 110.0, 95.0, 120.0, 80.0});

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        REQUIRE(eventMarkers.size() == 1);
        REQUIRE(eventMarkers.contains(aapl));

        auto& markers = eventMarkers.at(aapl);
        REQUIRE(markers.size() == 2);

        // Find markers by title
        const transform::EventMarkerData* highMarker = nullptr;
        const transform::EventMarkerData* lowMarker = nullptr;
        for (const auto& m : markers) {
            if (m.title == "Price High") highMarker = &m;
            if (m.title == "Price Low") lowMarker = &m;
        }

        REQUIRE(highMarker != nullptr);
        REQUIRE(lowMarker != nullptr);

        // Verify high marker - concrete values
        REQUIRE(highMarker->schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
        REQUIRE(highMarker->schemas[0].column_id == "gt_0#result");  // src.c > 110
        REQUIRE(highMarker->data.num_rows() == 1);  // Only 120 > 110
        REQUIRE(highMarker->data.contains("pivot"));

        // Verify low marker - concrete values
        REQUIRE(lowMarker->schemas[0].slot == epoch_core::CardSlot::SecondaryBadge);
        REQUIRE(lowMarker->schemas[0].column_id == "lt_0#result");  // src.c < 95
        REQUIRE(lowMarker->data.num_rows() == 2);  // 90 and 80 < 95
        REQUIRE(lowMarker->data.contains("pivot"));
    }

    SECTION("Event marker with multiple columns") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
event_marker(
    schema=EventMarkerSchema(
        title="Breakout with Context",
        select_key="SLOT0",
        schemas=[
            CardColumnSchema(column_id="SLOT0", slot="PrimaryBadge", render_type="Badge", color_map={}),
            CardColumnSchema(column_id="SLOT1", slot="Hero", render_type="Decimal", color_map={})
        ]
    )
)(signal, src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({90.0, 110.0, 95.0, 120.0});

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        REQUIRE(eventMarkers.contains(aapl));
        auto& markers = eventMarkers.at(aapl);
        REQUIRE(markers.size() == 1);

        // Verify multiple schemas (2 input + pivot = 3)
        REQUIRE(markers[0].schemas.size() == 3);
        REQUIRE(markers[0].schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
        REQUIRE(markers[0].schemas[0].column_id == "gt_0#result");  // signal: src.c > 100
        REQUIRE(markers[0].schemas[1].slot == epoch_core::CardSlot::Hero);
        REQUIRE(markers[0].schemas[1].column_id == "src#c");  // src.c -> market data close
        REQUIRE(markers[0].schemas[1].render_type == epoch_core::CardRenderType::Decimal);
        REQUIRE(markers[0].schemas[2].column_id == "pivot");
        REQUIRE(markers[0].schemas[2].render_type == epoch_core::CardRenderType::Timestamp);

        // Verify filtered rows
        REQUIRE(markers[0].data.num_rows() == 2);
        REQUIRE(markers[0].data.contains("pivot"));
    }

    SECTION("Event marker with literal SLOT input (constant number)") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
event_marker(
    schema=EventMarkerSchema(
        title="Signal with Constant",
        select_key="SLOT0",
        schemas=[
            CardColumnSchema(column_id="SLOT0", slot="PrimaryBadge", render_type="Badge", color_map={}),
            CardColumnSchema(column_id="SLOT1", slot="Hero", render_type="Decimal", color_map={})
        ]
    )
)(signal, 42.5)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({90.0, 110.0, 95.0, 120.0});

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        REQUIRE(eventMarkers.contains(aapl));
        auto& markers = eventMarkers.at(aapl);
        REQUIRE(markers.size() == 1);
        REQUIRE(markers[0].title == "Signal with Constant");

        // Verify 3 schemas (signal + constant + pivot)
        REQUIRE(markers[0].schemas.size() == 3);
        REQUIRE(markers[0].schemas[0].column_id == "gt_0#result");  // Boolean comparison result
        REQUIRE(markers[0].schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
        REQUIRE(markers[0].schemas[1].column_id == "number_1#result");  // Constant 42.5
        REQUIRE(markers[0].schemas[1].slot == epoch_core::CardSlot::Hero);
        REQUIRE(markers[0].schemas[2].column_id == "pivot");

        // Verify filtered rows (2 rows where c > 100)
        REQUIRE(markers[0].data.num_rows() == 2);
        REQUIRE(markers[0].data.contains("pivot"));
    }

    SECTION("Event marker with mixed literals and node references") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
event_marker(
    schema=EventMarkerSchema(
        title="Mixed Inputs",
        select_key="SLOT0",
        schemas=[
            CardColumnSchema(column_id="SLOT0", slot="PrimaryBadge", render_type="Badge", color_map={}),
            CardColumnSchema(column_id="SLOT1", slot="Hero", render_type="Decimal", color_map={}),
            CardColumnSchema(column_id="SLOT2", slot="Subtitle", render_type="Integer", color_map={}),
            CardColumnSchema(column_id="SLOT3", slot="Footer", render_type="Text", color_map={})
        ]
    )
)(signal, src.c, 100, "breakout")
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({90.0, 110.0, 95.0, 120.0});

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        REQUIRE(eventMarkers.contains(aapl));
        auto& markers = eventMarkers.at(aapl);
        REQUIRE(markers.size() == 1);
        REQUIRE(markers[0].title == "Mixed Inputs");

        // Verify all 5 schemas (4 input + pivot)
        REQUIRE(markers[0].schemas.size() == 5);

        // Verify column IDs (SLOT refs resolve to actual column names)
        REQUIRE(markers[0].schemas[0].column_id == "gt_0#result");  // signal: src.c > 100
        REQUIRE(markers[0].schemas[1].column_id == "src#c");  // src.c -> market data close
        REQUIRE(markers[0].schemas[2].column_id == "number_0#result");  // 100 -> literal number
        REQUIRE(markers[0].schemas[3].column_id == "text_0#result");  // "breakout" -> literal string
        REQUIRE(markers[0].schemas[4].column_id == "pivot");

        // Verify slots
        REQUIRE(markers[0].schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
        REQUIRE(markers[0].schemas[1].slot == epoch_core::CardSlot::Hero);
        REQUIRE(markers[0].schemas[2].slot == epoch_core::CardSlot::Subtitle);
        REQUIRE(markers[0].schemas[3].slot == epoch_core::CardSlot::Footer);

        // Verify render types
        REQUIRE(markers[0].schemas[0].render_type == epoch_core::CardRenderType::Badge);
        REQUIRE(markers[0].schemas[1].render_type == epoch_core::CardRenderType::Decimal);
        REQUIRE(markers[0].schemas[2].render_type == epoch_core::CardRenderType::Integer);
        REQUIRE(markers[0].schemas[3].render_type == epoch_core::CardRenderType::Text);
        REQUIRE(markers[0].schemas[4].render_type == epoch_core::CardRenderType::Timestamp);

        // Verify filtered rows and pivot column
        REQUIRE(markers[0].data.num_rows() == 2);
        REQUIRE(markers[0].data.contains("pivot"));
    }

    SECTION("Event marker with only literal inputs (except filter)") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
event_marker(
    schema=EventMarkerSchema(
        title="All Literals",
        select_key="SLOT0",
        schemas=[
            CardColumnSchema(column_id="SLOT0", slot="PrimaryBadge", render_type="Badge", color_map={}),
            CardColumnSchema(column_id="SLOT1", slot="Hero", render_type="Decimal", color_map={}),
            CardColumnSchema(column_id="SLOT2", slot="Subtitle", render_type="Text", color_map={})
        ]
    )
)(signal, 99.99, "BUY")
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({90.0, 110.0, 95.0, 120.0});

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        REQUIRE(eventMarkers.contains(aapl));
        auto& markers = eventMarkers.at(aapl);
        REQUIRE(markers.size() == 1);
        REQUIRE(markers[0].title == "All Literals");

        // Verify 4 schemas (3 input + pivot)
        REQUIRE(markers[0].schemas.size() == 4);

        // Verify column IDs (SLOT refs resolve to actual column names)
        REQUIRE(markers[0].schemas[0].column_id == "gt_0#result");  // signal: src.c > 100
        REQUIRE(markers[0].schemas[1].column_id == "number_1#result");  // 99.99 -> literal number
        REQUIRE(markers[0].schemas[2].column_id == "text_0#result");  // "BUY" -> literal string
        REQUIRE(markers[0].schemas[3].column_id == "pivot");

        // Verify slots
        REQUIRE(markers[0].schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
        REQUIRE(markers[0].schemas[1].slot == epoch_core::CardSlot::Hero);
        REQUIRE(markers[0].schemas[2].slot == epoch_core::CardSlot::Subtitle);

        // Verify render types
        REQUIRE(markers[0].schemas[0].render_type == epoch_core::CardRenderType::Badge);
        REQUIRE(markers[0].schemas[1].render_type == epoch_core::CardRenderType::Decimal);
        REQUIRE(markers[0].schemas[2].render_type == epoch_core::CardRenderType::Text);
        REQUIRE(markers[0].schemas[3].render_type == epoch_core::CardRenderType::Timestamp);

        // Verify filtered rows (literals broadcast to match signal rows)
        REQUIRE(markers[0].data.num_rows() == 2);
        REQUIRE(markers[0].data.contains("pivot"));
    }

    SECTION("Event marker with empty filter result") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 200
event_marker(
    schema=EventMarkerSchema(
        title="No Matches",
        select_key="SLOT0",
        schemas=[CardColumnSchema(
            column_id="SLOT0",
            slot="PrimaryBadge",
            render_type="Badge",
            color_map={}
        )]
    )
)(signal)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        // All values < 200, so signal is all false
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({90.0, 110.0, 95.0, 120.0});

        orch.ExecutePipeline(std::move(inputData));

        auto eventMarkers = orch.GetGeneratedEventMarkers();

        REQUIRE(eventMarkers.contains(aapl));
        auto& markers = eventMarkers.at(aapl);
        REQUIRE(markers.size() == 1);
        REQUIRE(markers[0].title == "No Matches");

        // Verify schema is still complete even with empty data
        REQUIRE(markers[0].schemas.size() == 2);  // 1 input + pivot
        REQUIRE(markers[0].schemas[0].column_id == "gt_0#result");  // signal: src.c > 200
        REQUIRE(markers[0].schemas[0].slot == epoch_core::CardSlot::PrimaryBadge);
        REQUIRE(markers[0].schemas[1].column_id == "pivot");
        REQUIRE(markers[0].schemas[1].render_type == epoch_core::CardRenderType::Timestamp);

        // Empty result - no rows pass the filter
        REQUIRE(markers[0].data.num_rows() == 0);
    }
}
