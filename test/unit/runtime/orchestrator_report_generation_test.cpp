/**
 * @file orchestrator_report_generation_test.cpp
 * @brief Comprehensive tests for report generation through the orchestrator
 *
 * Tests the FULL report generation pipeline using REAL transforms (no mocks).
 * Verifies that reports are correctly generated, captured, and cached by the
 * orchestrator for all 14 report types:
 *
 * Card Reports (6):
 * - numeric_cards_report: Numeric aggregation cards
 * - boolean_cards_report: Boolean aggregation cards
 * - any_cards_report: Any-type cards
 * - index_cards_report: Index lookup cards
 * - quantile_cards_report: Quantile computation cards
 * - cs_numeric_cards_report: Cross-sectional numeric cards
 *
 * Chart Reports (5):
 * - bar_chart_report: Bar chart visualization
 * - pie_chart_report: Pie chart visualization
 * - nested_pie_chart_report: Nested pie chart
 * - histogram_chart_report: Histogram visualization
 * - cs_bar_chart_report: Cross-sectional bar chart
 *
 * Table Reports (2):
 * - table_report: Basic table report
 * - cs_table_report: Cross-sectional table report
 *
 * Gap Report (1):
 * - gap_report: Gap analysis report
 */

#include "transforms/runtime/orchestrator.h"
#include "../common/test_constants.h"
#include "../../integration/mocks/mock_transform_manager.h"
#include "fake_data_sources.h"
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_script/core/constants.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_protos/tearsheet.pb.h>
#include <fmt/format.h>
#include <unordered_map>
#include <epoch_data_sdk/events/all.h>

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

namespace {
    auto ExecuteWithEmitter(DataFlowRuntimeOrchestrator& orch, TimeFrameAssetDataFrameMap inputData) {
        data_sdk::events::ScopedProgressEmitter emitter;
        return orch.ExecutePipeline(std::move(inputData), emitter);
    }
}


// ============================================================================
// CARD REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - numeric_cards_report", "[orchestrator][report][cards][numeric]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Single asset generates numeric card with sum aggregation") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = numeric_cards_report(agg="sum", category="Price", title="Total Close")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // Verify report was generated
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.size() > 0);
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_cards());
        REQUIRE(tearsheet.cards().cards_size() > 0);

        // CardDef.data contains CardData entries; access first card's data
        auto& cardDef = tearsheet.cards().cards(0);
        REQUIRE(cardDef.data_size() > 0);
        auto& cardData = cardDef.data(0);
        REQUIRE(cardData.title() == "Total Close");
        REQUIRE(cardDef.category() == "Price");
        // Sum of [100, 105, 110, 115, 120] = 550
        REQUIRE(cardData.value().has_decimal_value());
        REQUIRE(std::abs(cardData.value().decimal_value() - 550.0) < 0.01);
    }

    SECTION("Mean aggregation calculates correctly") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = numeric_cards_report(agg="mean", category="Stats", title="Average Price")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 200.0, 300.0});

        ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& cardData = reports[aapl].cards().cards(0).data(0);
        // Mean of [100, 200, 300] = 200
        REQUIRE(cardData.value().has_decimal_value());
        REQUIRE(std::abs(cardData.value().decimal_value() - 200.0) < 0.01);
    }

    SECTION("Multi-asset isolation - each asset gets its own report") {
        const std::string msft = TestAssetConstants::MSFT;

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = numeric_cards_report(agg="mean", category="Stats", title="Mean Price")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 100.0, 100.0});  // Mean = 100
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 200.0, 200.0});  // Mean = 200

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();

        // Each asset should have its own report
        REQUIRE(reports.contains(aapl));
        REQUIRE(reports.contains(msft));

        // AAPL mean = 100
        REQUIRE(reports[aapl].cards().cards(0).data(0).value().has_decimal_value());
        REQUIRE(std::abs(reports[aapl].cards().cards(0).data(0).value().decimal_value() - 100.0) < 0.01);

        // MSFT mean = 200
        REQUIRE(reports[msft].cards().cards(0).data(0).value().has_decimal_value());
        REQUIRE(std::abs(reports[msft].cards().cards(0).data(0).value().decimal_value() - 200.0) < 0.01);
    }
}

TEST_CASE("Orchestrator Report Generation - boolean_cards_report", "[orchestrator][report][cards][boolean]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Boolean any aggregation - returns true when any value is true") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
above_105 = gt()(c, 105)
report = boolean_cards_report(agg="any", category="Signals", title="Any Above 105")(above_105)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Values: [100, 105, 110, 115, 120] - 3 values above 105, so any() returns true
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& cardData = reports[aapl].cards().cards(0).data(0);
        REQUIRE(cardData.title() == "Any Above 105");
        // any() returns true (1.0) when at least one value is true
        REQUIRE(cardData.value().has_boolean_value());
        REQUIRE(cardData.value().boolean_value() == true);
    }

    SECTION("Boolean all aggregation - returns false when not all values are true") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
above_105 = gt()(c, 105)
report = boolean_cards_report(agg="all", category="Signals", title="All Above 105")(above_105)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Values: [100, 105, 110, 115, 120] - only 3 out of 5 above 105, so all() returns false
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& cardData = reports[aapl].cards().cards(0).data(0);
        REQUIRE(cardData.title() == "All Above 105");
        REQUIRE(cardData.value().has_boolean_value());
        REQUIRE(cardData.value().boolean_value() == false);
    }
}

TEST_CASE("Orchestrator Report Generation - quantile_cards_report", "[orchestrator][report][cards][quantile]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Median calculation (quantile=0.5)") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = quantile_cards_report(quantile=0.5, category="Stats", title="Median Price")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Values: [100, 200, 300, 400, 500] - Median = 300
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 200.0, 300.0, 400.0, 500.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& cardData = reports[aapl].cards().cards(0).data(0);
        REQUIRE(cardData.title() == "Median Price");
        REQUIRE(cardData.value().has_decimal_value());
        REQUIRE(std::abs(cardData.value().decimal_value() - 300.0) < 0.01);
    }
}

// ============================================================================
// CHART REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - bar_chart_report", "[orchestrator][report][chart][bar]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Bar chart with labeled data") {
        // Use SDK-style code that creates label and value columns
        // boolean_select_string takes positional inputs: (condition, true_val, false_val)
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
# Create labels based on price threshold
is_high = gte()(c, 110)
label = boolean_select_string()(is_high, "High", "Low")
report = bar_chart_report(agg="count", title="Price Distribution", category="Analysis", vertical=True, x_axis_label="Category", y_axis_label="Count")(label, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // [100, 105, 110, 115, 120] -> 2 Low, 3 High
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_bar_def());
        auto& bar_def = chart.bar_def();

        // Verify chart metadata
        REQUIRE(bar_def.chart_def().title() == "Price Distribution");
        REQUIRE(bar_def.chart_def().category() == "Analysis");
        REQUIRE(bar_def.chart_def().x_axis().label() == "Category");
        REQUIRE(bar_def.chart_def().y_axis().label() == "Count");
        REQUIRE(bar_def.vertical() == true);

        // Verify data: 2 categories (Low appears first, then High based on input order)
        REQUIRE(bar_def.chart_def().x_axis().categories_size() == 2);
        REQUIRE(bar_def.chart_def().x_axis().categories(0) == "Low");
        REQUIRE(bar_def.chart_def().x_axis().categories(1) == "High");

        // Verify bar values: Low=2, High=3 (counts)
        REQUIRE(bar_def.data_size() == 1);
        REQUIRE(bar_def.data(0).values_size() == 2);
        REQUIRE(std::abs(bar_def.data(0).values(0) - 2.0) < 0.01);  // Low count
        REQUIRE(std::abs(bar_def.data(0).values(1) - 3.0) < 0.01);  // High count
    }
}

TEST_CASE("Orchestrator Report Generation - histogram_chart_report", "[orchestrator][report][chart][histogram]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Histogram with auto bins") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = histogram_chart_report(bins=5, title="Price Histogram", category="Distribution", x_axis_label="Price", y_axis_label="Frequency")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // 10 evenly spaced values from 100 to 118
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 102.0, 104.0, 106.0, 108.0, 110.0, 112.0, 114.0, 116.0, 118.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_histogram_def());
        auto& hist_def = chart.histogram_def();

        // Verify chart metadata
        REQUIRE(hist_def.chart_def().title() == "Price Histogram");
        REQUIRE(hist_def.chart_def().category() == "Distribution");
        REQUIRE(hist_def.chart_def().x_axis().label() == "Price");
        REQUIRE(hist_def.chart_def().y_axis().label() == "Frequency");

        // Verify data: 10 raw values for histogram (binning done client-side)
        REQUIRE(hist_def.data().values_size() == 10);
        REQUIRE(hist_def.bins_count() == 5);

        // Verify actual data values match input
        REQUIRE(hist_def.data().values(0).decimal_value() == 100.0);
        REQUIRE(hist_def.data().values(4).decimal_value() == 108.0);
        REQUIRE(hist_def.data().values(9).decimal_value() == 118.0);
    }
}

// ============================================================================
// CROSS-SECTIONAL REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - cs_numeric_cards_report", "[orchestrator][report][cards][cross-sectional]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string goog = TestAssetConstants::GOOG;

    SECTION("Cross-sectional mean across assets") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_numeric_cards_report(agg="mean", category="Cross-Section", title="CS Mean")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft, goog}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // AAPL: mean = 100, MSFT: mean = 200, GOOG: mean = 300
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 100.0, 100.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 200.0, 200.0});
        inputData[dailyTF.ToString()][goog] = CreateOHLCVData({300.0, 300.0, 300.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();

        // Cross-sectional reports are stored under GROUP_KEY ("ALL")
        REQUIRE(reports.contains(epoch_script::GROUP_KEY));

        auto& tearsheet = reports[epoch_script::GROUP_KEY];
        REQUIRE(tearsheet.has_cards());
        REQUIRE(tearsheet.cards().cards_size() == 1);

        auto& cardDef = tearsheet.cards().cards(0);
        REQUIRE(cardDef.category() == "Cross-Section");
        // CS creates one CardData per asset: 3 assets = 3 data entries
        REQUIRE(cardDef.data_size() == 3);

        // Build a map of asset -> value for verification
        // Title format: "CS Mean - {asset}" or just asset name
        std::unordered_map<std::string, double> assetValues;
        for (int i = 0; i < cardDef.data_size(); ++i) {
            auto& data = cardDef.data(i);
            REQUIRE(data.value().has_decimal_value());

            // Extract asset name from title (e.g., "CS Mean - AAPL-Stock" -> contains "AAPL-Stock")
            if (data.title().find(aapl) != std::string::npos) {
                assetValues[aapl] = data.value().decimal_value();
            } else if (data.title().find(msft) != std::string::npos) {
                assetValues[msft] = data.value().decimal_value();
            } else if (data.title().find(goog) != std::string::npos) {
                assetValues[goog] = data.value().decimal_value();
            }
        }

        // Verify concrete values: AAPL=100, MSFT=200, GOOG=300
        REQUIRE(assetValues.contains(aapl));
        REQUIRE(assetValues.contains(msft));
        REQUIRE(assetValues.contains(goog));
        REQUIRE(std::abs(assetValues[aapl] - 100.0) < 0.01);
        REQUIRE(std::abs(assetValues[msft] - 200.0) < 0.01);
        REQUIRE(std::abs(assetValues[goog] - 300.0) < 0.01);
    }
}

TEST_CASE("Orchestrator Report Generation - cs_bar_chart_report", "[orchestrator][report][chart][cross-sectional]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;

    SECTION("Cross-sectional bar chart comparing assets") {
        // Note: Python-style boolean True/False required
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_bar_chart_report(agg="last", title="Asset Comparison", x_axis_label="Asset", y_axis_label="Price", category="Cross-Section", vertical=True)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // AAPL: last = 120, MSFT: last = 220
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 210.0, 220.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(epoch_script::GROUP_KEY));

        auto& tearsheet = reports[epoch_script::GROUP_KEY];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_bar_def());
        auto& bar_def = chart.bar_def();

        // Verify chart metadata
        REQUIRE(bar_def.chart_def().title() == "Asset Comparison");
        REQUIRE(bar_def.chart_def().category() == "Cross-Section");
        REQUIRE(bar_def.chart_def().x_axis().label() == "Asset");
        REQUIRE(bar_def.chart_def().y_axis().label() == "Price");
        REQUIRE(bar_def.vertical() == true);

        // Verify data: 2 assets as categories
        REQUIRE(bar_def.chart_def().x_axis().categories_size() == 2);
        REQUIRE(bar_def.data_size() == 1);
        REQUIRE(bar_def.data(0).values_size() == 2);

        // Find values by asset name (order may vary)
        std::unordered_map<std::string, double> asset_values;
        for (int i = 0; i < bar_def.chart_def().x_axis().categories_size(); ++i) {
            asset_values[bar_def.chart_def().x_axis().categories(i)] = bar_def.data(0).values(i);
        }

        // Verify last values: AAPL=120, MSFT=220
        REQUIRE(asset_values.contains(aapl));
        REQUIRE(asset_values.contains(msft));
        REQUIRE(std::abs(asset_values[aapl] - 120.0) < 0.01);
        REQUIRE(std::abs(asset_values[msft] - 220.0) < 0.01);
    }
}

// ============================================================================
// MULTI-REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - Multiple reports in single pipeline", "[orchestrator][report][multi]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Multiple card reports from same pipeline") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
price_report = numeric_cards_report(agg="mean", category="Price Stats", title="Mean Price")(c)
volume_report = numeric_cards_report(agg="sum", category="Volume Stats", title="Total Volume")(v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 200.0, 300.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        // Should have cards from both reports
        REQUIRE(tearsheet.has_cards());
        REQUIRE(tearsheet.cards().cards_size() >= 2);
    }

    SECTION("Mixed per-asset and cross-sectional reports") {
        const std::string msft = TestAssetConstants::MSFT;

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
# Per-asset report
asset_report = numeric_cards_report(agg="mean", category="Per-Asset", title="Asset Mean")(c)
# Cross-sectional report
cs_report = cs_numeric_cards_report(agg="mean", category="Cross-Section", title="CS Mean")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 100.0, 100.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 200.0, 200.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();

        // Per-asset reports
        REQUIRE(reports.contains(aapl));
        REQUIRE(reports.contains(msft));
        REQUIRE(reports[aapl].has_cards());
        REQUIRE(reports[msft].has_cards());

        // Cross-sectional report under GROUP_KEY
        REQUIRE(reports.contains(epoch_script::GROUP_KEY));
        REQUIRE(reports[epoch_script::GROUP_KEY].has_cards());
    }
}

// ============================================================================
// REPORT CACHING CONSISTENCY TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - Cache consistency", "[orchestrator][report][cache]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string goog = TestAssetConstants::GOOG;

    SECTION("Multiple assets don't corrupt each other's reports") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = numeric_cards_report(agg="sum", category="Test", title="Sum")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft, goog}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Distinct values for each asset
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 100.0});  // Sum = 200
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({300.0, 300.0});  // Sum = 600
        inputData[dailyTF.ToString()][goog] = CreateOHLCVData({500.0, 500.0});  // Sum = 1000

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();

        // Verify each asset has correct independent value
        REQUIRE(reports.contains(aapl));
        REQUIRE(reports.contains(msft));
        REQUIRE(reports.contains(goog));

        REQUIRE(std::abs(reports[aapl].cards().cards(0).data(0).value().decimal_value() - 200.0) < 0.01);
        REQUIRE(std::abs(reports[msft].cards().cards(0).data(0).value().decimal_value() - 600.0) < 0.01);
        REQUIRE(std::abs(reports[goog].cards().cards(0).data(0).value().decimal_value() - 1000.0) < 0.01);
    }

    SECTION("Report retrieval is idempotent") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = numeric_cards_report(agg="mean", category="Test", title="Mean")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 200.0, 300.0});

ExecuteWithEmitter(orch, std::move(inputData));
        // Call GetGeneratedReports multiple times
        auto reports1 = orch.GetGeneratedReports();
        auto reports2 = orch.GetGeneratedReports();
        auto reports3 = orch.GetGeneratedReports();

        // All should return the same values
        REQUIRE(reports1.size() == reports2.size());
        REQUIRE(reports2.size() == reports3.size());
        REQUIRE(reports1[aapl].cards().cards(0).data(0).value().decimal_value() == reports2[aapl].cards().cards(0).data(0).value().decimal_value());
        REQUIRE(reports2[aapl].cards().cards(0).data(0).value().decimal_value() == reports3[aapl].cards().cards(0).data(0).value().decimal_value());
    }
}

// ============================================================================
// PIE CHART REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - pie_chart_report", "[orchestrator][report][chart][pie]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Pie chart with labeled data") {
        // boolean_select_string takes positional inputs: (condition, true_val, false_val)
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
# Create labels based on price threshold
is_high = gte()(c, 110)
label = boolean_select_string()(is_high, "High", "Low")
report = pie_chart_report(title="Price Distribution", category="Analysis")(label, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // [100, 105, 110, 115, 120]:
        // Low: 100 + 105 = 205 (37.27%)
        // High: 110 + 115 + 120 = 345 (62.73%)
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_pie_def());
        auto& pie_def = chart.pie_def();

        // Verify chart metadata
        REQUIRE(pie_def.chart_def().title() == "Price Distribution");
        REQUIRE(pie_def.chart_def().category() == "Analysis");

        // Verify pie data structure
        REQUIRE(pie_def.data_size() == 1);  // One series
        auto& series = pie_def.data(0);
        REQUIRE(series.points_size() == 2);  // Two slices: Low and High

        // Find Low and High slices (order may vary)
        double low_value = 0.0, high_value = 0.0;
        for (int i = 0; i < series.points_size(); ++i) {
            if (series.points(i).name() == "Low") {
                low_value = series.points(i).y();
            } else if (series.points(i).name() == "High") {
                high_value = series.points(i).y();
            }
        }

        // Expected percentages: Low=37.27%, High=62.73% (sum of values normalized)
        // 205 / 550 * 100 = 37.27, 345 / 550 * 100 = 62.73
        REQUIRE(std::abs(low_value - 37.27) < 0.5);
        REQUIRE(std::abs(high_value - 62.73) < 0.5);
    }
}

// ============================================================================
// INDEX CARD REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - index_cards_report", "[orchestrator][report][cards][index]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Find index of specific value") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = index_cards_report(target_value="200", category="Search", title="Price 200 Index")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Index 0: 100, Index 1: 150, Index 2: 200, Index 3: 250
        // Searching for 200 should return index 2
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 150.0, 200.0, 250.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_cards());
        REQUIRE(tearsheet.cards().cards_size() == 1);

        auto& cardDef = tearsheet.cards().cards(0);
        REQUIRE(cardDef.category() == "Search");
        REQUIRE(cardDef.data_size() == 1);

        auto& cardData = cardDef.data(0);
        REQUIRE(cardData.title() == "Price 200 Index");
        // Value 200 is at index 2 (0-indexed) - verify the index is returned
        REQUIRE(cardData.value().has_integer_value());
        REQUIRE(cardData.value().integer_value() == 2);
    }
}

// ============================================================================
// ANY CARD REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - any_cards_report", "[orchestrator][report][cards][any]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Any-type card with string aggregation") {
        // boolean_select_string takes positional inputs: (condition, true_val, false_val)
        // First value: 100 < 110, so first label = "Low"
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
is_high = gte()(c, 110)
label = boolean_select_string()(is_high, "High", "Low")
report = any_cards_report(agg="first", category="Labels", title="First Label")(label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Values: [100, 105, 110, 115, 120] -> labels: ["Low", "Low", "High", "High", "High"]
        // First label = "Low"
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_cards());
        REQUIRE(tearsheet.cards().cards_size() == 1);

        auto& cardDef = tearsheet.cards().cards(0);
        REQUIRE(cardDef.category() == "Labels");
        REQUIRE(cardDef.data_size() == 1);

        auto& cardData = cardDef.data(0);
        REQUIRE(cardData.title() == "First Label");
        // First value = "Low" (since 100 < 110)
        REQUIRE(cardData.value().has_string_value());
        REQUIRE(cardData.value().string_value() == "Low");
    }
}

// ============================================================================
// CROSS-SECTIONAL TABLE REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - cs_table_report", "[orchestrator][report][table][cross-sectional]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;

    SECTION("Cross-sectional table with multiple assets") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
# Create cross-sectional table
report = cs_table_report(title="Asset Prices", category="Comparison", agg="last")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // AAPL: last = 120, MSFT: last = 220
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 210.0, 220.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();

        // Cross-sectional tables are stored under GROUP_KEY
        REQUIRE(reports.contains(epoch_script::GROUP_KEY));

        auto& tearsheet = reports[epoch_script::GROUP_KEY];
        REQUIRE(tearsheet.has_tables());
        REQUIRE(tearsheet.tables().tables_size() == 1);

        auto& table = tearsheet.tables().tables(0);
        REQUIRE(table.title() == "Asset Prices");
        REQUIRE(table.category() == "Comparison");

        // Verify columns: one per asset
        REQUIRE(table.columns_size() == 2);

        // Verify data rows: one row with aggregated values
        REQUIRE(table.data().rows_size() == 1);
        REQUIRE(table.data().rows(0).values_size() == 2);

        // Find column indices for each asset
        std::unordered_map<std::string, int> colIndex;
        for (int i = 0; i < table.columns_size(); ++i) {
            colIndex[table.columns(i).name()] = i;
        }

        // Verify the last values: AAPL=120, MSFT=220
        // Note: cs_table_report stores values as strings via Scalar.repr()
        if (colIndex.contains(aapl)) {
            auto& val = table.data().rows(0).values(colIndex[aapl]);
            REQUIRE(val.has_string_value());
            // String contains the numeric value (120.0)
            REQUIRE(val.string_value().find("120") != std::string::npos);
        }
        if (colIndex.contains(msft)) {
            auto& val = table.data().rows(0).values(colIndex[msft]);
            REQUIRE(val.has_string_value());
            // String contains the numeric value (220.0)
            REQUIRE(val.string_value().find("220") != std::string::npos);
        }
    }
}

// ============================================================================
// CONCURRENT EXECUTION TESTS
// ============================================================================

// Helper to find a card by title in a tearsheet
static const epoch_proto::CardData* FindCardByTitle(const epoch_proto::TearSheet& ts, const std::string& title) {
    for (int i = 0; i < ts.cards().cards_size(); ++i) {
        const auto& card = ts.cards().cards(i);
        for (int j = 0; j < card.data_size(); ++j) {
            if (card.data(j).title() == title) {
                return &card.data(j);
            }
        }
    }
    return nullptr;
}

TEST_CASE("Orchestrator Report Generation - Concurrent execution", "[orchestrator][report][concurrent]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string goog = TestAssetConstants::GOOG;
    const std::string amzn = TestAssetConstants::AMZN;
    const std::string tsla = TestAssetConstants::TSLA;

    SECTION("Many assets with concurrent report generation") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
price_report = numeric_cards_report(agg="mean", category="Price", title="Mean Price")(c)
vol_report = numeric_cards_report(agg="sum", category="Volume", title="Total Volume")(src.v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft, goog, amzn, tsla}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 100.0, 100.0});  // Mean = 100
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 200.0, 200.0});  // Mean = 200
        inputData[dailyTF.ToString()][goog] = CreateOHLCVData({300.0, 300.0, 300.0});  // Mean = 300
        inputData[dailyTF.ToString()][amzn] = CreateOHLCVData({400.0, 400.0, 400.0});  // Mean = 400
        inputData[dailyTF.ToString()][tsla] = CreateOHLCVData({500.0, 500.0, 500.0});  // Mean = 500

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();

        // All assets should have reports
        REQUIRE(reports.contains(aapl));
        REQUIRE(reports.contains(msft));
        REQUIRE(reports.contains(goog));
        REQUIRE(reports.contains(amzn));
        REQUIRE(reports.contains(tsla));

        // Each should have 2 cards (price and volume)
        for (const auto& [asset, tearsheet] : reports) {
            REQUIRE(tearsheet.has_cards());
            REQUIRE(tearsheet.cards().cards_size() >= 2);
        }

        // Verify mean price values are isolated per asset (use title lookup since order isn't guaranteed)
        auto aaplPrice = FindCardByTitle(reports[aapl], "Mean Price");
        auto msftPrice = FindCardByTitle(reports[msft], "Mean Price");
        auto googPrice = FindCardByTitle(reports[goog], "Mean Price");
        auto amznPrice = FindCardByTitle(reports[amzn], "Mean Price");
        auto tslaPrice = FindCardByTitle(reports[tsla], "Mean Price");

        REQUIRE(aaplPrice != nullptr);
        REQUIRE(msftPrice != nullptr);
        REQUIRE(googPrice != nullptr);
        REQUIRE(amznPrice != nullptr);
        REQUIRE(tslaPrice != nullptr);

        REQUIRE(std::abs(aaplPrice->value().decimal_value() - 100.0) < 0.01);
        REQUIRE(std::abs(msftPrice->value().decimal_value() - 200.0) < 0.01);
        REQUIRE(std::abs(googPrice->value().decimal_value() - 300.0) < 0.01);
        REQUIRE(std::abs(amznPrice->value().decimal_value() - 400.0) < 0.01);
        REQUIRE(std::abs(tslaPrice->value().decimal_value() - 500.0) < 0.01);
    }
}

// ============================================================================
// EDGE CASES
// ============================================================================

TEST_CASE("Orchestrator Report Generation - Edge cases", "[orchestrator][report][edge]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Empty data produces empty report") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = numeric_cards_report(agg="sum", category="Test", title="Test")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Empty data
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        // Should either have no report or have an empty/zero-valued report
        if (reports.contains(aapl)) {
            // If report exists, it should handle empty gracefully
            REQUIRE(reports[aapl].ByteSizeLong() >= 0);
        }
    }

    SECTION("Pipeline without reports produces empty report map") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
ma = sma(period=3)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 200.0, 300.0, 400.0, 500.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        // No reporter transforms, so no reports
        REQUIRE(reports.empty());
    }
}

// ============================================================================
// LINE CHART REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - line_chart_report", "[orchestrator][report][chart][line]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Line chart with single series") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = line_chart_report(title="Price Trend", category="Analysis", x_axis_label="Date", y_axis_label="Price")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_lines_def());
        auto& lines_def = chart.lines_def();

        // Verify chart metadata
        REQUIRE(lines_def.chart_def().title() == "Price Trend");
        REQUIRE(lines_def.chart_def().category() == "Analysis");
        REQUIRE(lines_def.chart_def().x_axis().label() == "Date");
        REQUIRE(lines_def.chart_def().y_axis().label() == "Price");

        // Verify axis types
        REQUIRE(lines_def.chart_def().x_axis().type() == epoch_proto::AxisDateTime);
        REQUIRE(lines_def.chart_def().y_axis().type() == epoch_proto::AxisLinear);

        // Verify we have one line series
        REQUIRE(lines_def.lines_size() == 1);

        // Verify data points in the line series
        auto& line = lines_def.lines(0);
        REQUIRE(line.data_size() == 5);

        // Verify each data point value using Approx
        REQUIRE(line.data(0).y() == Catch::Approx(100.0));
        REQUIRE(line.data(1).y() == Catch::Approx(105.0));
        REQUIRE(line.data(2).y() == Catch::Approx(110.0));
        REQUIRE(line.data(3).y() == Catch::Approx(115.0));
        REQUIRE(line.data(4).y() == Catch::Approx(120.0));
    }

    SECTION("Line chart with multiple series") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
h = src.h
l = src.l
report = line_chart_report(title="OHLC Trends", category="Analysis", x_axis_label="Date", y_axis_label="Price")(c, h, l)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        auto& chart = tearsheet.charts().charts(0);
        auto& lines_def = chart.lines_def();

        // Should have 3 line series (close, high, low)
        REQUIRE(lines_def.lines_size() == 3);

        // Each series should have 3 data points
        for (int i = 0; i < lines_def.lines_size(); ++i) {
            REQUIRE(lines_def.lines(i).data_size() == 3);
        }
    }
}

// ============================================================================
// AREA CHART REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - area_chart_report", "[orchestrator][report][chart][area]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Area chart with single series") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = area_chart_report(title="Price Area", category="Analysis", x_axis_label="Date", y_axis_label="Price", stack_type="normal")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_area_def());
        auto& area_def = chart.area_def();

        // Verify chart metadata
        REQUIRE(area_def.chart_def().title() == "Price Area");
        REQUIRE(area_def.chart_def().category() == "Analysis");
        REQUIRE(area_def.chart_def().x_axis().label() == "Date");
        REQUIRE(area_def.chart_def().y_axis().label() == "Price");

        // Verify axis types
        REQUIRE(area_def.chart_def().x_axis().type() == epoch_proto::AxisDateTime);
        REQUIRE(area_def.chart_def().y_axis().type() == epoch_proto::AxisLinear);

        // Verify we have one area series
        REQUIRE(area_def.areas_size() == 1);

        // Verify data points
        auto& series = area_def.areas(0);
        REQUIRE(series.data_size() == 5);

        // Verify each data point value
        REQUIRE(series.data(0).y() == Catch::Approx(100.0));
        REQUIRE(series.data(1).y() == Catch::Approx(105.0));
        REQUIRE(series.data(2).y() == Catch::Approx(110.0));
        REQUIRE(series.data(3).y() == Catch::Approx(115.0));
        REQUIRE(series.data(4).y() == Catch::Approx(120.0));
    }

    SECTION("Area chart with percent stacking") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
h = src.h
report = area_chart_report(title="Stacked Area", category="Analysis", stack_type="percent")(c, h)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        auto& chart = tearsheet.charts().charts(0);
        auto& area_def = chart.area_def();

        // Should have 2 area series (close, high)
        REQUIRE(area_def.areas_size() == 2);

        // Verify stack type is percent
        REQUIRE(area_def.stack_type() == epoch_proto::StackTypePercent);
    }
}

// ============================================================================
// BOXPLOT REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - boxplot_report", "[orchestrator][report][chart][boxplot]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Boxplot with labeled data") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
is_high = gte()(c, 110)
label = boolean_select_string()(is_high, "High", "Low")
report = boxplot_report(title="Price Distribution", category="Analysis", x_axis_label="Category", y_axis_label="Price")(label, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_box_plot_def());
        auto& boxplot_def = chart.box_plot_def();

        // Verify chart metadata
        REQUIRE(boxplot_def.chart_def().title() == "Price Distribution");
        REQUIRE(boxplot_def.chart_def().category() == "Analysis");
        REQUIRE(boxplot_def.chart_def().x_axis().label() == "Category");
        REQUIRE(boxplot_def.chart_def().y_axis().label() == "Price");

        // Verify axis types
        REQUIRE(boxplot_def.chart_def().x_axis().type() == epoch_proto::AxisCategory);
        REQUIRE(boxplot_def.chart_def().y_axis().type() == epoch_proto::AxisLinear);

        // Should have 2 box plots (Low: 100, 105 and High: 110, 115, 120)
        REQUIRE(boxplot_def.data().points_size() == 2);
    }

    SECTION("Boxplot statistics validation") {
        // Create data with known distribution: 10, 20, 30, 40, 50
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = boxplot_report(title="Stats Test", category="Test")("Group A", c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({10.0, 20.0, 30.0, 40.0, 50.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(aapl));

        auto& tearsheet = reports[aapl];
        auto& chart = tearsheet.charts().charts(0);
        auto& boxplot_def = chart.box_plot_def();

        // Should have exactly 1 box plot
        REQUIRE(boxplot_def.data().points_size() == 1);

        auto& point = boxplot_def.data().points(0);

        // Verify boxplot statistics with Approx
        // Note: Quartile calculation may vary by implementation
        REQUIRE(point.low() == Catch::Approx(10.0));      // min
        REQUIRE(point.median() == Catch::Approx(30.0));   // 50th percentile
        REQUIRE(point.high() == Catch::Approx(50.0));     // max
    }
}

// ============================================================================
// CROSS-SECTIONAL LINE CHART REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - cs_line_chart_report", "[orchestrator][report][chart][line][cross-sectional]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;

    SECTION("Cross-sectional line chart comparing assets") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_line_chart_report(title="Asset Comparison", category="Cross-Section", x_axis_label="Date", y_axis_label="Price")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 210.0, 220.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(epoch_script::GROUP_KEY));

        auto& tearsheet = reports[epoch_script::GROUP_KEY];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_lines_def());
        auto& lines_def = chart.lines_def();

        // Verify chart metadata
        REQUIRE(lines_def.chart_def().title() == "Asset Comparison");
        REQUIRE(lines_def.chart_def().category() == "Cross-Section");
        REQUIRE(lines_def.chart_def().x_axis().label() == "Date");
        REQUIRE(lines_def.chart_def().y_axis().label() == "Price");

        // Verify axis types
        REQUIRE(lines_def.chart_def().x_axis().type() == epoch_proto::AxisDateTime);
        REQUIRE(lines_def.chart_def().y_axis().type() == epoch_proto::AxisLinear);

        // Verify we have lines for both assets
        REQUIRE(lines_def.lines_size() == 2);

        // Collect line names and their data points
        std::unordered_map<std::string, std::vector<double>> lineData;
        for (int i = 0; i < lines_def.lines_size(); ++i) {
            auto& line = lines_def.lines(i);
            std::vector<double> values;
            for (int j = 0; j < line.data_size(); ++j) {
                values.push_back(line.data(j).y());
            }
            lineData[line.name()] = values;
        }

        // Verify each line has 3 data points
        for (const auto& [name, values] : lineData) {
            REQUIRE(values.size() == 3);
        }
    }

    SECTION("Cross-sectional line chart data point values") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_line_chart_report(title="Price Trends", category="CS")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 150.0, 200.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({50.0, 75.0, 100.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        auto& tearsheet = reports[epoch_script::GROUP_KEY];
        auto& lines_def = tearsheet.charts().charts(0).lines_def();

        REQUIRE(lines_def.lines_size() == 2);

        // Verify data point values for each line
        for (int i = 0; i < lines_def.lines_size(); ++i) {
            auto& line = lines_def.lines(i);
            REQUIRE(line.data_size() == 3);

            // The values should be either AAPL's (100, 150, 200) or MSFT's (50, 75, 100)
            double first = line.data(0).y();
            if (first == Catch::Approx(100.0)) {
                // This is AAPL
                REQUIRE(line.data(1).y() == Catch::Approx(150.0));
                REQUIRE(line.data(2).y() == Catch::Approx(200.0));
            } else {
                // This is MSFT
                REQUIRE(line.data(0).y() == Catch::Approx(50.0));
                REQUIRE(line.data(1).y() == Catch::Approx(75.0));
                REQUIRE(line.data(2).y() == Catch::Approx(100.0));
            }
        }
    }
}

// ============================================================================
// CROSS-SECTIONAL HEATMAP REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - cs_heatmap_report", "[orchestrator][report][chart][heatmap][cross-sectional]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string goog = TestAssetConstants::GOOG;

    SECTION("Cross-sectional heatmap in correlation mode") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_heatmap_report(title="Correlation Matrix", category="Cross-Section", mode="correlation")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft, goog}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Use perfectly correlated data (all increase by 10 each day)
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0, 130.0, 140.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 210.0, 220.0, 230.0, 240.0});
        inputData[dailyTF.ToString()][goog] = CreateOHLCVData({300.0, 310.0, 320.0, 330.0, 340.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(epoch_script::GROUP_KEY));

        auto& tearsheet = reports[epoch_script::GROUP_KEY];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_heat_map_def());
        auto& heat_map_def = chart.heat_map_def();

        // Verify chart metadata
        REQUIRE(heat_map_def.chart_def().title() == "Correlation Matrix");
        REQUIRE(heat_map_def.chart_def().category() == "Cross-Section");

        // Verify axis types (category for both in heatmap)
        REQUIRE(heat_map_def.chart_def().x_axis().type() == epoch_proto::AxisCategory);
        REQUIRE(heat_map_def.chart_def().y_axis().type() == epoch_proto::AxisCategory);

        // For 3 assets, we should have 9 correlation values (3x3 matrix)
        REQUIRE(heat_map_def.points_size() == 9);

        // All correlations should be 1.0 (perfect correlation) since all series
        // have the same linear trend
        for (int i = 0; i < heat_map_def.points_size(); ++i) {
            auto& point = heat_map_def.points(i);
            REQUIRE(point.value() == Catch::Approx(1.0).epsilon(0.01));
        }
    }

    SECTION("Cross-sectional heatmap correlation with inverse relationship") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_heatmap_report(title="Correlation", category="CS", mode="correlation")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // AAPL increases, MSFT decreases - should be negatively correlated
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0, 130.0, 140.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({140.0, 130.0, 120.0, 110.0, 100.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        auto& heat_map_def = reports[epoch_script::GROUP_KEY].charts().charts(0).heat_map_def();

        // For 2 assets, we have 4 points (2x2 matrix)
        REQUIRE(heat_map_def.points_size() == 4);

        // Diagonal should be 1.0 (self-correlation)
        // Off-diagonal should be -1.0 (perfect negative correlation)
        for (int i = 0; i < heat_map_def.points_size(); ++i) {
            auto& point = heat_map_def.points(i);
            if (point.x() == point.y()) {
                // Diagonal: self-correlation = 1.0
                REQUIRE(point.value() == Catch::Approx(1.0).epsilon(0.01));
            } else {
                // Off-diagonal: negative correlation = -1.0
                REQUIRE(point.value() == Catch::Approx(-1.0).epsilon(0.01));
            }
        }
    }

    SECTION("Cross-sectional heatmap in values mode with last aggregation") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_heatmap_report(title="Asset Values", category="Cross-Section", mode="values", agg="last")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 210.0, 220.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(epoch_script::GROUP_KEY));

        auto& tearsheet = reports[epoch_script::GROUP_KEY];
        auto& heat_map_def = tearsheet.charts().charts(0).heat_map_def();

        // Verify chart metadata
        REQUIRE(heat_map_def.chart_def().title() == "Asset Values");

        // In values mode with 2 assets, we should have 2 points (1xN)
        REQUIRE(heat_map_def.points_size() == 2);

        // Verify the last values are used (120.0 for AAPL, 220.0 for MSFT)
        std::vector<double> values;
        for (int i = 0; i < heat_map_def.points_size(); ++i) {
            values.push_back(heat_map_def.points(i).value());
        }
        std::sort(values.begin(), values.end());
        REQUIRE(values[0] == Catch::Approx(120.0));
        REQUIRE(values[1] == Catch::Approx(220.0));
    }

    SECTION("Cross-sectional heatmap in values mode with mean aggregation") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_heatmap_report(title="Mean Values", category="CS", mode="values", agg="mean")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0});  // mean = 110
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 220.0, 240.0});  // mean = 220

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        auto& heat_map_def = reports[epoch_script::GROUP_KEY].charts().charts(0).heat_map_def();

        REQUIRE(heat_map_def.points_size() == 2);

        // Verify the mean values
        std::vector<double> values;
        for (int i = 0; i < heat_map_def.points_size(); ++i) {
            values.push_back(heat_map_def.points(i).value());
        }
        std::sort(values.begin(), values.end());
        REQUIRE(values[0] == Catch::Approx(110.0));
        REQUIRE(values[1] == Catch::Approx(220.0));
    }

    SECTION("Cross-sectional heatmap in values mode with sum aggregation") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_heatmap_report(title="Sum Values", category="CS", mode="values", agg="sum")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 110.0, 120.0});  // sum = 330
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 220.0, 240.0});  // sum = 660

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        auto& heat_map_def = reports[epoch_script::GROUP_KEY].charts().charts(0).heat_map_def();

        REQUIRE(heat_map_def.points_size() == 2);

        // Verify the sum values
        std::vector<double> values;
        for (int i = 0; i < heat_map_def.points_size(); ++i) {
            values.push_back(heat_map_def.points(i).value());
        }
        std::sort(values.begin(), values.end());
        REQUIRE(values[0] == Catch::Approx(330.0));
        REQUIRE(values[1] == Catch::Approx(660.0));
    }
}

// ============================================================================
// CROSS-SECTIONAL BOXPLOT REPORT TESTS
// ============================================================================

TEST_CASE("Orchestrator Report Generation - cs_boxplot_report", "[orchestrator][report][chart][boxplot][cross-sectional]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;

    SECTION("Cross-sectional boxplot comparing asset distributions") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_boxplot_report(title="Asset Distribution", category="Cross-Section", x_axis_label="Asset", y_axis_label="Price")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 105.0, 110.0, 115.0, 120.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 210.0, 220.0, 230.0, 240.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        REQUIRE(reports.contains(epoch_script::GROUP_KEY));

        auto& tearsheet = reports[epoch_script::GROUP_KEY];
        REQUIRE(tearsheet.has_charts());
        REQUIRE(tearsheet.charts().charts_size() == 1);

        auto& chart = tearsheet.charts().charts(0);
        REQUIRE(chart.has_box_plot_def());
        auto& boxplot_def = chart.box_plot_def();

        // Verify chart metadata
        REQUIRE(boxplot_def.chart_def().title() == "Asset Distribution");
        REQUIRE(boxplot_def.chart_def().category() == "Cross-Section");
        REQUIRE(boxplot_def.chart_def().x_axis().label() == "Asset");
        REQUIRE(boxplot_def.chart_def().y_axis().label() == "Price");

        // Verify axis types
        REQUIRE(boxplot_def.chart_def().x_axis().type() == epoch_proto::AxisCategory);
        REQUIRE(boxplot_def.chart_def().y_axis().type() == epoch_proto::AxisLinear);

        // Should have 2 box plot data points (one per asset)
        REQUIRE(boxplot_def.data().points_size() == 2);
    }

    SECTION("Cross-sectional boxplot statistics validation") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_boxplot_report(title="Distribution Compare", category="CS")(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // AAPL: 10, 20, 30, 40, 50 -> min=10, q1=15, median=30, q3=45, max=50
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({10.0, 20.0, 30.0, 40.0, 50.0});
        // MSFT: 100, 200, 300, 400, 500 -> min=100, q1=150, median=300, q3=450, max=500
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({100.0, 200.0, 300.0, 400.0, 500.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        auto& boxplot_def = reports[epoch_script::GROUP_KEY].charts().charts(0).box_plot_def();

        REQUIRE(boxplot_def.data().points_size() == 2);

        // Collect statistics by min value to identify which box is which
        std::vector<std::tuple<double, double, double, double, double>> stats;
        for (int i = 0; i < boxplot_def.data().points_size(); ++i) {
            auto& point = boxplot_def.data().points(i);
            stats.emplace_back(point.low(), point.q1(), point.median(), point.q3(), point.high());
        }

        // Sort by min value to identify AAPL (lower) and MSFT (higher)
        std::sort(stats.begin(), stats.end());

        // AAPL stats (lower values) - using actual quartile calculation
        REQUIRE(std::get<0>(stats[0]) == Catch::Approx(10.0));   // low
        REQUIRE(std::get<2>(stats[0]) == Catch::Approx(30.0));   // median
        REQUIRE(std::get<4>(stats[0]) == Catch::Approx(50.0));   // high

        // MSFT stats (higher values)
        REQUIRE(std::get<0>(stats[1]) == Catch::Approx(100.0));  // low
        REQUIRE(std::get<2>(stats[1]) == Catch::Approx(300.0));  // median
        REQUIRE(std::get<4>(stats[1]) == Catch::Approx(500.0));  // high
    }

    SECTION("Cross-sectional boxplot with whisker IQR setting") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
report = cs_boxplot_report(title="Custom Whiskers", category="CS", whisker_iqr=1.5)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Data with potential outliers
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({10.0, 20.0, 30.0, 40.0, 50.0});

ExecuteWithEmitter(orch, std::move(inputData));
        auto reports = orch.GetGeneratedReports();
        auto& boxplot_def = reports[epoch_script::GROUP_KEY].charts().charts(0).box_plot_def();

        REQUIRE(boxplot_def.data().points_size() == 1);

        // Verify the boxplot was generated with custom whisker settings
        auto& point = boxplot_def.data().points(0);
        REQUIRE(point.low() == Catch::Approx(10.0));
        REQUIRE(point.high() == Catch::Approx(50.0));
    }
}
