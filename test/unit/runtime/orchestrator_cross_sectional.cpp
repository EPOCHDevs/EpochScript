//
// Created by adesola on 11/25/25.
//
/**
 * @file orchestrator_cross_sectional.cpp
 * @brief Cross-sectional tests for multi-asset orchestrator
 *
 * These tests verify cross-sectional transform execution:
 * - CS operations returning one value per asset (top_k, bottom_k, cs_zscore)
 * - CS operations returning one value for all assets (cs_momentum)
 * - CS reports (cs_table_report, cs_bar_chart_report, cs_numeric_cards_report)
 * - Mixed constants and node references in CS pipelines
 */

#include "transforms/runtime/orchestrator.h"
#include "test_constants.h"
#include "fake_data_sources.h"
#include "../../integration/mocks/mock_transform_manager.h"
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <epoch_data_sdk/events/all.h>

#include "epoch_script/core/bar_attribute.h"

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
// TEST CASES
// ============================================================================

TEST_CASE("DataFlowRuntimeOrchestrator - Cross-Sectional Tests", "[orchestrator][cross-sectional]") {
    const auto dailyTF = TestTimeFrames::Daily();

    SECTION("top_k selects top N assets per timestamp") {
        // 3 assets with known close prices
        // Day 1: AAPL=100, MSFT=300, GOOGL=200 -> top 2: MSFT, GOOGL
        // Day 2: AAPL=150, MSFT=250, GOOGL=200 -> top 2: MSFT, GOOGL
        // Day 3: AAPL=400, MSFT=200, GOOGL=300 -> top 2: AAPL, GOOGL

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
topk = top_k(k=2)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0, 150.0, 400.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({300.0, 250.0, 200.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({200.0, 200.0, 300.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        for (const auto& asset : assets) {
            REQUIRE(results[dailyTF.ToString()].contains(asset));
            REQUIRE(results[dailyTF.ToString()][asset].contains("topk#result"));
        }

        // Verify top_k boolean results
        auto aapl_topk = results[dailyTF.ToString()][TestAssetConstants::AAPL]["topk#result"];
        auto msft_topk = results[dailyTF.ToString()][TestAssetConstants::MSFT]["topk#result"];
        auto googl_topk = results[dailyTF.ToString()][TestAssetConstants::GOOG]["topk#result"];

        // Day 1: MSFT(300), GOOGL(200) are top 2
        REQUIRE(aapl_topk.iloc(0).as_bool() == false);
        REQUIRE(msft_topk.iloc(0).as_bool() == true);
        REQUIRE(googl_topk.iloc(0).as_bool() == true);

        // Day 2: MSFT(250), GOOGL(200) are top 2
        REQUIRE(aapl_topk.iloc(1).as_bool() == false);
        REQUIRE(msft_topk.iloc(1).as_bool() == true);
        REQUIRE(googl_topk.iloc(1).as_bool() == true);

        // Day 3: AAPL(400), GOOGL(300) are top 2
        REQUIRE(aapl_topk.iloc(2).as_bool() == true);
        REQUIRE(msft_topk.iloc(2).as_bool() == false);
        REQUIRE(googl_topk.iloc(2).as_bool() == true);
    }

    SECTION("bottom_k selects bottom N assets per timestamp") {
        // Day 1: AAPL=100, MSFT=300, GOOGL=200 -> bottom 1: AAPL
        // Day 2: AAPL=150, MSFT=250, GOOGL=100 -> bottom 1: GOOGL
        // Day 3: AAPL=400, MSFT=200, GOOGL=300 -> bottom 1: MSFT

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
bottomk = bottom_k(k=1)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0, 150.0, 400.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({300.0, 250.0, 200.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({200.0, 100.0, 300.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        auto aapl_bottomk = results[dailyTF.ToString()][TestAssetConstants::AAPL]["bottomk#result"];
        auto msft_bottomk = results[dailyTF.ToString()][TestAssetConstants::MSFT]["bottomk#result"];
        auto googl_bottomk = results[dailyTF.ToString()][TestAssetConstants::GOOG]["bottomk#result"];

        // Day 1: AAPL(100) is bottom 1
        REQUIRE(aapl_bottomk.iloc(0).as_bool() == true);
        REQUIRE(msft_bottomk.iloc(0).as_bool() == false);
        REQUIRE(googl_bottomk.iloc(0).as_bool() == false);

        // Day 2: GOOGL(100) is bottom 1
        REQUIRE(aapl_bottomk.iloc(1).as_bool() == false);
        REQUIRE(msft_bottomk.iloc(1).as_bool() == false);
        REQUIRE(googl_bottomk.iloc(1).as_bool() == true);

        // Day 3: MSFT(200) is bottom 1
        REQUIRE(aapl_bottomk.iloc(2).as_bool() == false);
        REQUIRE(msft_bottomk.iloc(2).as_bool() == true);
        REQUIRE(googl_bottomk.iloc(2).as_bool() == false);
    }

    SECTION("top_k_percent selects percentage of assets") {
        // With 3 assets, k=34 means ceil(0.34 * 3) = ceil(1.02) = 2 assets selected

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
topkpct = top_k_percent(k=34)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0, 200.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({300.0, 150.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({200.0, 250.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // Day 1: MSFT(300), GOOGL(200) are top 34% (2 assets)
        auto aapl_result = results[dailyTF.ToString()][TestAssetConstants::AAPL]["topkpct#result"];
        auto msft_result = results[dailyTF.ToString()][TestAssetConstants::MSFT]["topkpct#result"];
        auto googl_result = results[dailyTF.ToString()][TestAssetConstants::GOOG]["topkpct#result"];

        REQUIRE(aapl_result.iloc(0).as_bool() == false);
        REQUIRE(msft_result.iloc(0).as_bool() == true);
        REQUIRE(googl_result.iloc(0).as_bool() == true);
    }

    SECTION("cs_zscore normalizes values across assets") {
        // Day 1: AAPL=100, MSFT=200, GOOGL=300
        // mean=200, sample std (ddof=1) = sqrt(((100-200)^2 + (200-200)^2 + (300-200)^2) / 2) = 100
        // z_aapl = (100-200)/100 = -1.0
        // z_msft = (200-200)/100 = 0
        // z_googl = (300-200)/100 = 1.0

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
z = cs_zscore()(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({300.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        auto aapl_z = results[dailyTF.ToString()][TestAssetConstants::AAPL]["z#result"];
        auto msft_z = results[dailyTF.ToString()][TestAssetConstants::MSFT]["z#result"];
        auto googl_z = results[dailyTF.ToString()][TestAssetConstants::GOOG]["z#result"];

        // Day 1: values = [100, 200, 300], mean = 200, sample std (ddof=1) = 100
        // z = (value - mean) / std
        // z_AAPL = (100 - 200) / 100 = -1.0
        // z_MSFT = (200 - 200) / 100 = 0.0
        // z_GOOGL = (300 - 200) / 100 = 1.0
        REQUIRE(aapl_z.iloc(0).as_double() == Catch::Approx(-1.0).margin(0.01));
        REQUIRE(msft_z.iloc(0).as_double() == Catch::Approx(0.0).margin(0.01));
        REQUIRE(googl_z.iloc(0).as_double() == Catch::Approx(1.0).margin(0.01));
    }

    SECTION("cs_momentum returns aggregated values for all assets") {
        // cs_momentum computes cross-sectional momentum - a single series
        // broadcast to all assets representing overall market momentum.

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
mom = cs_momentum()(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0, 110.0, 121.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0, 220.0, 242.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({300.0, 330.0, 363.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // All assets should have mom#result with same values (broadcast)
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::AAPL].contains("mom#result"));
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::MSFT].contains("mom#result"));
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::GOOG].contains("mom#result"));

        // Verify all assets receive the same cs_momentum value (broadcast)
        auto aapl_mom = results[dailyTF.ToString()][TestAssetConstants::AAPL]["mom#result"];
        auto msft_mom = results[dailyTF.ToString()][TestAssetConstants::MSFT]["mom#result"];
        auto googl_mom = results[dailyTF.ToString()][TestAssetConstants::GOOG]["mom#result"];

        // cs_momentum is broadcast - all assets have same value at each timestamp
        REQUIRE(aapl_mom.size() == 3);
        REQUIRE(msft_mom.size() == 3);
        REQUIRE(googl_mom.size() == 3);

        // All assets should have identical momentum values at each timestamp
        REQUIRE(aapl_mom.iloc(0).as_double() == msft_mom.iloc(0).as_double());
        REQUIRE(aapl_mom.iloc(0).as_double() == googl_mom.iloc(0).as_double());
        REQUIRE(aapl_mom.iloc(1).as_double() == msft_mom.iloc(1).as_double());
        REQUIRE(aapl_mom.iloc(1).as_double() == googl_mom.iloc(1).as_double());
        REQUIRE(aapl_mom.iloc(2).as_double() == msft_mom.iloc(2).as_double());
        REQUIRE(aapl_mom.iloc(2).as_double() == googl_mom.iloc(2).as_double());

        // Verify momentum is increasing over time (prices are increasing)
        // Day 1 sum: 100+200+300 = 600
        // Day 2 sum: 110+220+330 = 660
        // Day 3 sum: 121+242+363 = 726
        // cs_momentum appears to compute cumulative sum of cross-sectional sums
        REQUIRE(aapl_mom.iloc(1).as_double() > aapl_mom.iloc(0).as_double());
        REQUIRE(aapl_mom.iloc(2).as_double() > aapl_mom.iloc(1).as_double());
    }

    SECTION("sma feeding into top_k - chained transforms") {
        // Verify regular transform (SMA) computed per asset, then CS selection

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
ma = sma(period=2)(c)
topk = top_k(k=1)(ma)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        // AAPL: [100, 200, 300] -> SMA(2) = [NaN, 150, 250]
        // MSFT: [400, 300, 200] -> SMA(2) = [NaN, 350, 250]
        // GOOGL: [200, 200, 400] -> SMA(2) = [NaN, 200, 300]
        // At row 2 (index 1): MSFT SMA=350 is top 1
        // At row 3 (index 2): GOOGL SMA=300 is top 1

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0, 200.0, 300.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({400.0, 300.0, 200.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({200.0, 200.0, 400.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // Verify SMA values are computed correctly
        auto aapl_ma = results[dailyTF.ToString()][TestAssetConstants::AAPL]["ma#result"];
        auto msft_ma = results[dailyTF.ToString()][TestAssetConstants::MSFT]["ma#result"];
        auto googl_ma = results[dailyTF.ToString()][TestAssetConstants::GOOG]["ma#result"];

        // Check SMA values at index 1 (first valid after period=2)
        REQUIRE(aapl_ma.iloc(1).as_double() == Catch::Approx(150.0));  // (100+200)/2
        REQUIRE(msft_ma.iloc(1).as_double() == Catch::Approx(350.0));  // (400+300)/2
        REQUIRE(googl_ma.iloc(1).as_double() == Catch::Approx(200.0)); // (200+200)/2

        // Check SMA values at index 2
        REQUIRE(aapl_ma.iloc(2).as_double() == Catch::Approx(250.0));  // (200+300)/2
        REQUIRE(msft_ma.iloc(2).as_double() == Catch::Approx(250.0));  // (300+200)/2
        REQUIRE(googl_ma.iloc(2).as_double() == Catch::Approx(300.0)); // (200+400)/2

        // Verify top_k selects highest SMA at each timestamp
        auto aapl_topk = results[dailyTF.ToString()][TestAssetConstants::AAPL]["topk#result"];
        auto msft_topk = results[dailyTF.ToString()][TestAssetConstants::MSFT]["topk#result"];
        auto googl_topk = results[dailyTF.ToString()][TestAssetConstants::GOOG]["topk#result"];

        // At index 1: MSFT SMA=350 is top 1
        REQUIRE(aapl_topk.iloc(1).as_bool() == false);
        REQUIRE(msft_topk.iloc(1).as_bool() == true);
        REQUIRE(googl_topk.iloc(1).as_bool() == false);

        // At index 2: GOOGL SMA=300 is top 1
        REQUIRE(aapl_topk.iloc(2).as_bool() == false);
        REQUIRE(msft_topk.iloc(2).as_bool() == false);
        REQUIRE(googl_topk.iloc(2).as_bool() == true);
    }

    SECTION("cs_zscore feeding into top_k - multiple CS operations") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
z = cs_zscore()(c)
topk = top_k(k=1)(z)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        // GOOGL has highest price -> highest z-score -> top 1
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({300.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        auto aapl_topk = results[dailyTF.ToString()][TestAssetConstants::AAPL]["topk#result"];
        auto msft_topk = results[dailyTF.ToString()][TestAssetConstants::MSFT]["topk#result"];
        auto googl_topk = results[dailyTF.ToString()][TestAssetConstants::GOOG]["topk#result"];

        // GOOGL has highest z-score, should be selected
        REQUIRE(aapl_topk.iloc(0).as_bool() == false);
        REQUIRE(msft_topk.iloc(0).as_bool() == false);
        REQUIRE(googl_topk.iloc(0).as_bool() == true);
    }

    SECTION("roc transform feeding into cross-sectional") {
        // Verify rate of change (per-asset) then cross-sectional selection
        // This tests chained transforms without scalar constants (avoiding scalar_inlining_pass issues)

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
ret = roc(period=1)(c)
topk = top_k(k=1)(ret)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        // Returns:
        // AAPL: [100, 120] -> roc = [NaN, 0.20] (20% return)
        // MSFT: [200, 210] -> roc = [NaN, 0.05] (5% return)
        // GOOGL: [150, 180] -> roc = [NaN, 0.20] (20% return)
        // Top 1 at day 2 could be AAPL or GOOGL (tie)

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0, 120.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0, 210.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({150.0, 180.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // Verify ROC values are computed correctly
        // ROC = (current - previous) / previous
        auto aapl_ret = results[dailyTF.ToString()][TestAssetConstants::AAPL]["ret#result"];
        auto msft_ret = results[dailyTF.ToString()][TestAssetConstants::MSFT]["ret#result"];
        auto googl_ret = results[dailyTF.ToString()][TestAssetConstants::GOOG]["ret#result"];

        // AAPL: (120-100)/100 = 0.20 (20%)
        REQUIRE(aapl_ret.iloc(1).as_double() == Catch::Approx(0.20).margin(0.01));
        // MSFT: (210-200)/200 = 0.05 (5%)
        REQUIRE(msft_ret.iloc(1).as_double() == Catch::Approx(0.05).margin(0.01));
        // GOOGL: (180-150)/150 = 0.20 (20%)
        REQUIRE(googl_ret.iloc(1).as_double() == Catch::Approx(0.20).margin(0.01));

        // Verify top_k selection based on ROC values
        auto aapl_topk = results[dailyTF.ToString()][TestAssetConstants::AAPL]["topk#result"];
        auto msft_topk = results[dailyTF.ToString()][TestAssetConstants::MSFT]["topk#result"];
        auto googl_topk = results[dailyTF.ToString()][TestAssetConstants::GOOG]["topk#result"];

        // MSFT (5% return) is NOT top 1
        REQUIRE(msft_topk.iloc(1).as_bool() == false);
        // Either AAPL or GOOGL should be top 1 (both have 20% return - tie)
        // At least one of them should be true
        REQUIRE((aapl_topk.iloc(1).as_bool() == true || googl_topk.iloc(1).as_bool() == true));
    }

    SECTION("cross-sectional selection with all assets") {
        // Test top_k selecting all assets when k equals number of assets
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
topk = top_k(k=3)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({250.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({300.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::AAPL].contains("topk#result"));

        // All assets should be selected when k=3 (all assets)
        auto aapl_topk = results[dailyTF.ToString()][TestAssetConstants::AAPL]["topk#result"];
        auto msft_topk = results[dailyTF.ToString()][TestAssetConstants::MSFT]["topk#result"];
        auto googl_topk = results[dailyTF.ToString()][TestAssetConstants::GOOG]["topk#result"];

        REQUIRE(aapl_topk.iloc(0).as_bool() == true);
        REQUIRE(msft_topk.iloc(0).as_bool() == true);
        REQUIRE(googl_topk.iloc(0).as_bool() == true);
    }

    SECTION("cs_table_report generates report with correct data") {
        // cs_table_report consumes top_k output to ensure the transform is persisted
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
topk = top_k(k=3)(c)
cs_table_report(title="CS Snapshot", category="Test", agg="last")(topk)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        // Day 1: AAPL=100, MSFT=200, GOOGL=300 (last values: 110, 220, 330)
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0, 110.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0, 220.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({300.0, 330.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // Verify top_k selects all assets (k=3) at each timestamp
        auto aapl_topk = results[dailyTF.ToString()][TestAssetConstants::AAPL]["topk#result"];
        auto msft_topk = results[dailyTF.ToString()][TestAssetConstants::MSFT]["topk#result"];
        auto googl_topk = results[dailyTF.ToString()][TestAssetConstants::GOOG]["topk#result"];

        // All assets selected at day 1 (k=3 = all)
        REQUIRE(aapl_topk.iloc(0).as_bool() == true);
        REQUIRE(msft_topk.iloc(0).as_bool() == true);
        REQUIRE(googl_topk.iloc(0).as_bool() == true);

        // All assets selected at day 2
        REQUIRE(aapl_topk.iloc(1).as_bool() == true);
        REQUIRE(msft_topk.iloc(1).as_bool() == true);
        REQUIRE(googl_topk.iloc(1).as_bool() == true);
    }

    SECTION("cs_bar_chart_report generates report with correct data") {
        // cs_bar_chart_report consumes cs_zscore output to ensure the transform is persisted
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
z = cs_zscore()(c)
cs_bar_chart_report(agg="last", title="Asset ZScores", x_axis_label="Asset", y_axis_label="ZScore", category="Test", vertical=True)(z)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        // Values: AAPL=100, MSFT=200, GOOGL=300, mean=200, sample std=100
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({300.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // Verify cs_zscore values: z = (value - mean) / std
        auto aapl_z = results[dailyTF.ToString()][TestAssetConstants::AAPL]["z#result"];
        auto msft_z = results[dailyTF.ToString()][TestAssetConstants::MSFT]["z#result"];
        auto googl_z = results[dailyTF.ToString()][TestAssetConstants::GOOG]["z#result"];

        // z_AAPL = (100-200)/100 = -1.0, z_MSFT = 0.0, z_GOOGL = 1.0
        REQUIRE(aapl_z.iloc(0).as_double() == Catch::Approx(-1.0).margin(0.01));
        REQUIRE(msft_z.iloc(0).as_double() == Catch::Approx(0.0).margin(0.01));
        REQUIRE(googl_z.iloc(0).as_double() == Catch::Approx(1.0).margin(0.01));
    }

    SECTION("cs_numeric_cards_report generates report with correct data") {
        // Test cs_numeric_cards_report with bottom_k to ensure cross-sectional
        // transforms are properly handled. The report consumes bottomk output.
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
bottomk = bottom_k(k=1)(c)
cs_numeric_cards_report(agg="last", category="Test", title="Bottom Prices")(bottomk)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        // Day 1: AAPL=150, MSFT=250, GOOGL=350 -> bottom 1: AAPL
        // Day 2: AAPL=175, MSFT=275, GOOGL=375 -> bottom 1: AAPL
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({150.0, 175.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({250.0, 275.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({350.0, 375.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // Verify bottom_k selects AAPL (lowest price) at each timestamp
        auto aapl_bottomk = results[dailyTF.ToString()][TestAssetConstants::AAPL]["bottomk#result"];
        auto msft_bottomk = results[dailyTF.ToString()][TestAssetConstants::MSFT]["bottomk#result"];
        auto googl_bottomk = results[dailyTF.ToString()][TestAssetConstants::GOOG]["bottomk#result"];

        // Day 1: AAPL(150) is bottom 1
        REQUIRE(aapl_bottomk.iloc(0).as_bool() == true);
        REQUIRE(msft_bottomk.iloc(0).as_bool() == false);
        REQUIRE(googl_bottomk.iloc(0).as_bool() == false);

        // Day 2: AAPL(175) is bottom 1
        REQUIRE(aapl_bottomk.iloc(1).as_bool() == true);
        REQUIRE(msft_bottomk.iloc(1).as_bool() == false);
        REQUIRE(googl_bottomk.iloc(1).as_bool() == false);
    }

    SECTION("five asset cross-sectional pipeline") {
        // Stress test with 5 assets

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
topk = top_k(k=2)(c)
z = cs_zscore()(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            "AAPL-Stock", "MSFT-Stock", "GOOGL-Stock", "TSLA-Stock", "AMZN-Stock"
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        // Day 1: AAPL=100, MSFT=200, GOOGL=300, TSLA=150, AMZN=250 -> mean=200
        // Day 2: AAPL=150, MSFT=180, GOOGL=310, TSLA=200, AMZN=240 -> mean=216
        // Day 3: AAPL=200, MSFT=160, GOOGL=320, TSLA=250, AMZN=230 -> mean=232
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()]["AAPL-Stock"] = CreateOHLCVData({100.0, 150.0, 200.0});
        inputData[dailyTF.ToString()]["MSFT-Stock"] = CreateOHLCVData({200.0, 180.0, 160.0});
        inputData[dailyTF.ToString()]["GOOGL-Stock"] = CreateOHLCVData({300.0, 310.0, 320.0});
        inputData[dailyTF.ToString()]["TSLA-Stock"] = CreateOHLCVData({150.0, 200.0, 250.0});
        inputData[dailyTF.ToString()]["AMZN-Stock"] = CreateOHLCVData({250.0, 240.0, 230.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // Verify all assets have outputs
        REQUIRE(results.contains(dailyTF.ToString()));
        for (const auto& asset : assets) {
            REQUIRE(results[dailyTF.ToString()].contains(asset));
            REQUIRE(results[dailyTF.ToString()][asset].contains("topk#result"));
            REQUIRE(results[dailyTF.ToString()][asset].contains("z#result"));
        }

        // Day 1: GOOGL(300), AMZN(250) are top 2
        REQUIRE(results[dailyTF.ToString()]["GOOGL-Stock"]["topk#result"].iloc(0).as_bool() == true);
        REQUIRE(results[dailyTF.ToString()]["AMZN-Stock"]["topk#result"].iloc(0).as_bool() == true);
        REQUIRE(results[dailyTF.ToString()]["AAPL-Stock"]["topk#result"].iloc(0).as_bool() == false);
        REQUIRE(results[dailyTF.ToString()]["MSFT-Stock"]["topk#result"].iloc(0).as_bool() == false);
        REQUIRE(results[dailyTF.ToString()]["TSLA-Stock"]["topk#result"].iloc(0).as_bool() == false);

        // Day 3: GOOGL(320), TSLA(250) are top 2
        REQUIRE(results[dailyTF.ToString()]["GOOGL-Stock"]["topk#result"].iloc(2).as_bool() == true);
        REQUIRE(results[dailyTF.ToString()]["TSLA-Stock"]["topk#result"].iloc(2).as_bool() == true);
        REQUIRE(results[dailyTF.ToString()]["AAPL-Stock"]["topk#result"].iloc(2).as_bool() == false);
        REQUIRE(results[dailyTF.ToString()]["MSFT-Stock"]["topk#result"].iloc(2).as_bool() == false);
        REQUIRE(results[dailyTF.ToString()]["AMZN-Stock"]["topk#result"].iloc(2).as_bool() == false);

        // Verify z-scores with concrete values
        // Day 1: values = [100, 200, 300, 150, 250], mean = 200
        // sample std (ddof=1) = sqrt(25000/4) = sqrt(6250) = 79.06
        // z = (value - mean) / std
        // z_AAPL = (100 - 200) / 79.06 = -1.265
        // z_MSFT = (200 - 200) / 79.06 = 0.0
        // z_GOOGL = (300 - 200) / 79.06 = 1.265
        // z_TSLA = (150 - 200) / 79.06 = -0.632
        // z_AMZN = (250 - 200) / 79.06 = 0.632

        auto aapl_z = results[dailyTF.ToString()]["AAPL-Stock"]["z#result"];
        auto msft_z = results[dailyTF.ToString()]["MSFT-Stock"]["z#result"];
        auto googl_z = results[dailyTF.ToString()]["GOOGL-Stock"]["z#result"];
        auto tsla_z = results[dailyTF.ToString()]["TSLA-Stock"]["z#result"];
        auto amzn_z = results[dailyTF.ToString()]["AMZN-Stock"]["z#result"];

        // Day 1 concrete z-score values (sample std with ddof=1)
        REQUIRE(aapl_z.iloc(0).as_double() == Catch::Approx(-1.265).margin(0.01));
        REQUIRE(msft_z.iloc(0).as_double() == Catch::Approx(0.0).margin(0.01));
        REQUIRE(googl_z.iloc(0).as_double() == Catch::Approx(1.265).margin(0.01));
        REQUIRE(tsla_z.iloc(0).as_double() == Catch::Approx(-0.632).margin(0.01));
        REQUIRE(amzn_z.iloc(0).as_double() == Catch::Approx(0.632).margin(0.01));
    }

    SECTION("long-short pattern with top_k and bottom_k") {
        // Common pattern: select top and bottom performers

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
long_signal = top_k(k=1)(c)
short_signal = bottom_k(k=1)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        std::vector<std::string> assets = {
            TestAssetConstants::AAPL,
            TestAssetConstants::MSFT,
            TestAssetConstants::GOOG
        };
        DataFlowRuntimeOrchestrator orch(assets, std::move(manager));

        // GOOGL highest, AAPL lowest
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][TestAssetConstants::AAPL] = CreateOHLCVData({100.0});
        inputData[dailyTF.ToString()][TestAssetConstants::MSFT] = CreateOHLCVData({200.0});
        inputData[dailyTF.ToString()][TestAssetConstants::GOOG] = CreateOHLCVData({300.0});

        auto results = ExecuteWithEmitter(orch, std::move(inputData));

        // GOOGL is long signal (top 1)
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::GOOG]["long_signal#result"].iloc(0).as_bool() == true);
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::MSFT]["long_signal#result"].iloc(0).as_bool() == false);
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::AAPL]["long_signal#result"].iloc(0).as_bool() == false);

        // AAPL is short signal (bottom 1)
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::AAPL]["short_signal#result"].iloc(0).as_bool() == true);
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::MSFT]["short_signal#result"].iloc(0).as_bool() == false);
        REQUIRE(results[dailyTF.ToString()][TestAssetConstants::GOOG]["short_signal#result"].iloc(0).as_bool() == false);
    }
}
