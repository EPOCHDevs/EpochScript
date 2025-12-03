/**
 * @file orchestrator_graph_topologies_real_test.cpp
 * @brief Tests for various DAG topologies using REAL transforms (not mocks)
 *
 * These tests verify graph execution order and data flow correctness with
 * real transform implementations. Each test uses known input values to verify
 * expected output values, ensuring proper dependency resolution.
 *
 * Graph patterns tested:
 * 1. Linear chain (A -> B -> C)
 * 2. Diamond (A -> B,C -> D)
 * 3. Wide parallel (A, B, C all independent)
 * 4. Multi-level tree (A -> B,C -> D,E)
 * 5. Cross-sectional in chain (regular -> cs -> regular)
 * 6. Multiple cross-sectionals (cs1 -> regular -> cs2)
 * 7. Cross-sectional fan-out (cs -> reg1, reg2, reg3)
 * 8. Complex realistic pipeline (sma -> cs_zscore -> boolean)
 */

#include "transforms/runtime/orchestrator.h"
#include "../common/test_constants.h"
#include "fake_data_sources.h"
#include "../../integration/mocks/mock_transform_manager.h"
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_data_sdk/events/all.h>
#include <cmath>

#include "epoch_script/core/bar_attribute.h"

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// ============================================================================
// TEST CASES
// ============================================================================

TEST_CASE("DataFlowRuntimeOrchestrator - Graph Topologies with Real Transforms", "[orchestrator][graph][real]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string googl = TestAssetConstants::GOOG;

    SECTION("Linear chain: src -> add -> mul (100 -> 110 -> 220)") {
        // Pipeline: close -> add(10) -> mul(2)
        // Input: 100 -> add(10) = 110 -> mul(2) = 220
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
b = add()(c, 10)
result = mul()(b, 2)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 200.0, 300.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        REQUIRE(results.contains(dailyTF.ToString()));
        REQUIRE(results[dailyTF.ToString()].contains(aapl));

        auto df = results[dailyTF.ToString()][aapl];
        REQUIRE(df.contains("b#result"));
        REQUIRE(df.contains("result#result"));

        // Verify intermediate: add(100, 10) = 110, add(200, 10) = 210, add(300, 10) = 310
        auto b_col = df["b#result"];
        REQUIRE(b_col.iloc(0).as_double() == Catch::Approx(110.0));
        REQUIRE(b_col.iloc(1).as_double() == Catch::Approx(210.0));
        REQUIRE(b_col.iloc(2).as_double() == Catch::Approx(310.0));

        // Verify final: mul(110, 2) = 220, mul(210, 2) = 420, mul(310, 2) = 620
        auto result_col = df["result#result"];
        REQUIRE(result_col.iloc(0).as_double() == Catch::Approx(220.0));
        REQUIRE(result_col.iloc(1).as_double() == Catch::Approx(420.0));
        REQUIRE(result_col.iloc(2).as_double() == Catch::Approx(620.0));
    }

    SECTION("Diamond: A -> B,C -> D where D = B + C") {
        // Pipeline: close -> (add(5), mul(2)) -> add(B, C)
        // Input 100: B=105, C=200 -> D=305
        auto code = R"(
src = market_data_source(timeframe="1D")()
close = src.c
b = add()(close, 5)
c = mul()(close, 2)
d = add()(b, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 50.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        auto df = results[dailyTF.ToString()][aapl];
        REQUIRE(df.contains("b#result"));
        REQUIRE(df.contains("c#result"));
        REQUIRE(df.contains("d#result"));

        // Row 0: close=100, b=105, c=200, d=305
        REQUIRE(df["b#result"].iloc(0).as_double() == Catch::Approx(105.0));
        REQUIRE(df["c#result"].iloc(0).as_double() == Catch::Approx(200.0));
        REQUIRE(df["d#result"].iloc(0).as_double() == Catch::Approx(305.0));

        // Row 1: close=50, b=55, c=100, d=155
        REQUIRE(df["b#result"].iloc(1).as_double() == Catch::Approx(55.0));
        REQUIRE(df["c#result"].iloc(1).as_double() == Catch::Approx(100.0));
        REQUIRE(df["d#result"].iloc(1).as_double() == Catch::Approx(155.0));
    }

    SECTION("Wide parallel: A, B, C all independent from same source") {
        // Three independent transforms from same input
        auto code = R"(
src = market_data_source(timeframe="1D")()
close = src.c
a = add()(close, 1)
b = add()(close, 2)
c = add()(close, 3)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 200.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        auto df = results[dailyTF.ToString()][aapl];
        REQUIRE(df.contains("a#result"));
        REQUIRE(df.contains("b#result"));
        REQUIRE(df.contains("c#result"));

        // Row 0: input=100, a=101, b=102, c=103
        REQUIRE(df["a#result"].iloc(0).as_double() == Catch::Approx(101.0));
        REQUIRE(df["b#result"].iloc(0).as_double() == Catch::Approx(102.0));
        REQUIRE(df["c#result"].iloc(0).as_double() == Catch::Approx(103.0));

        // Row 1: input=200, a=201, b=202, c=203
        REQUIRE(df["a#result"].iloc(1).as_double() == Catch::Approx(201.0));
        REQUIRE(df["b#result"].iloc(1).as_double() == Catch::Approx(202.0));
        REQUIRE(df["c#result"].iloc(1).as_double() == Catch::Approx(203.0));
    }

    SECTION("Multi-level tree: A -> B,C -> D,E") {
        // Tree: A -> (B, C) -> (D from B, E from C)
        auto code = R"(
src = market_data_source(timeframe="1D")()
close = src.c
b = add()(close, 10)
c = mul()(close, 2)
d = mul()(b, 2)
e = add()(c, 5)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        auto df = results[dailyTF.ToString()][aapl];

        // input=100, b=110, c=200, d=220, e=205
        REQUIRE(df.contains("b#result"));
        REQUIRE(df.contains("c#result"));
        REQUIRE(df.contains("d#result"));
        REQUIRE(df.contains("e#result"));
        REQUIRE(df["b#result"].iloc(0).as_double() == Catch::Approx(110.0));  // 100+10
        REQUIRE(df["c#result"].iloc(0).as_double() == Catch::Approx(200.0));  // 100*2
        REQUIRE(df["d#result"].iloc(0).as_double() == Catch::Approx(220.0));  // 110*2
        REQUIRE(df["e#result"].iloc(0).as_double() == Catch::Approx(205.0));  // 200+5
    }

    SECTION("Cross-sectional in chain: regular -> cs_zscore -> regular") {
        // Pipeline: close -> cs_zscore -> mul(10)
        // With 3 assets at prices [100, 200, 300]:
        // mean = 200, sample_std = 100 (using n-1 = 2)
        // zscore(100) = (100-200)/100 = -1
        // zscore(200) = 0
        // zscore(300) = 1
        auto code = R"(
src = market_data_source(timeframe="1D")()
close = src.c
z = cs_zscore()(close)
scaled = mul()(z, 10)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft, googl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0});
        inputData[dailyTF.ToString()][googl] = CreateOHLCVData({300.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        // Verify all 3 assets have output
        REQUIRE(results[dailyTF.ToString()].contains(aapl));
        REQUIRE(results[dailyTF.ToString()].contains(msft));
        REQUIRE(results[dailyTF.ToString()].contains(googl));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        auto msft_df = results[dailyTF.ToString()][msft];
        auto googl_df = results[dailyTF.ToString()][googl];

        // Verify zscore exists
        REQUIRE(aapl_df.contains("z#result"));
        REQUIRE(msft_df.contains("z#result"));
        REQUIRE(googl_df.contains("z#result"));

        // Calculate expected values: mean=200, sample_std=sqrt(((100-200)^2+(0)^2+(100)^2)/(3-1)) = 100
        double mean = 200.0;
        double std_dev = std::sqrt(((100.0-mean)*(100.0-mean) + (200.0-mean)*(200.0-mean) + (300.0-mean)*(300.0-mean)) / 2.0);  // sample std (n-1)
        double z_aapl = (100.0 - mean) / std_dev;  // -1
        double z_msft = (200.0 - mean) / std_dev;  // 0
        double z_googl = (300.0 - mean) / std_dev; // 1

        REQUIRE(aapl_df["z#result"].iloc(0).as_double() == Catch::Approx(z_aapl).epsilon(0.01));
        REQUIRE(msft_df["z#result"].iloc(0).as_double() == Catch::Approx(z_msft).epsilon(0.01));
        REQUIRE(googl_df["z#result"].iloc(0).as_double() == Catch::Approx(z_googl).epsilon(0.01));

        // Verify scaled = zscore * 10
        REQUIRE(aapl_df.contains("scaled#result"));
        REQUIRE(aapl_df["scaled#result"].iloc(0).as_double() == Catch::Approx(z_aapl * 10.0).epsilon(0.01));
        REQUIRE(msft_df["scaled#result"].iloc(0).as_double() == Catch::Approx(z_msft * 10.0).epsilon(0.01));
        REQUIRE(googl_df["scaled#result"].iloc(0).as_double() == Catch::Approx(z_googl * 10.0).epsilon(0.01));
    }

    SECTION("Multiple cross-sectionals: cs1 -> regular -> cs2") {
        // Two sequential cross-sectional transforms with regular transform between
        auto code = R"(
src = market_data_source(timeframe="1D")()
close = src.c
z1 = cs_zscore()(close)
offset = add()(z1, 100)
z2 = cs_zscore()(offset)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft, googl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0});
        inputData[dailyTF.ToString()][googl] = CreateOHLCVData({300.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        auto aapl_df = results[dailyTF.ToString()][aapl];
        auto msft_df = results[dailyTF.ToString()][msft];
        auto googl_df = results[dailyTF.ToString()][googl];

        // Verify both zscore outputs exist
        REQUIRE(aapl_df.contains("z1#result"));
        REQUIRE(aapl_df.contains("offset#result"));
        REQUIRE(aapl_df.contains("z2#result"));

        // First zscore: mean=200, sample_std=100 (using n-1)
        double mean1 = 200.0;
        double std1 = std::sqrt(((100.0-mean1)*(100.0-mean1) + (200.0-mean1)*(200.0-mean1) + (300.0-mean1)*(300.0-mean1)) / 2.0);
        double z1_aapl = (100.0 - mean1) / std1;
        double z1_msft = (200.0 - mean1) / std1;
        double z1_googl = (300.0 - mean1) / std1;

        REQUIRE(aapl_df["z1#result"].iloc(0).as_double() == Catch::Approx(z1_aapl).epsilon(0.01));

        // offset = z1 + 100
        double offset_aapl = z1_aapl + 100.0;
        double offset_msft = z1_msft + 100.0;
        double offset_googl = z1_googl + 100.0;

        REQUIRE(aapl_df["offset#result"].iloc(0).as_double() == Catch::Approx(offset_aapl).epsilon(0.01));
        REQUIRE(msft_df["offset#result"].iloc(0).as_double() == Catch::Approx(offset_msft).epsilon(0.01));
        REQUIRE(googl_df["offset#result"].iloc(0).as_double() == Catch::Approx(offset_googl).epsilon(0.01));

        // Second zscore on offset values: z2 should be same as z1 (adding constant doesn't change relative positions)
        // mean2 = mean(offset) = 100 (since mean(z1) = 0)
        // std2 = std(offset) = std1 (adding constant doesn't change std)
        // z2 = (offset - mean2) / std2 = z1 (same as first zscore)
        REQUIRE(aapl_df["z2#result"].iloc(0).as_double() == Catch::Approx(z1_aapl).epsilon(0.01));
        REQUIRE(msft_df["z2#result"].iloc(0).as_double() == Catch::Approx(z1_msft).epsilon(0.01));
        REQUIRE(googl_df["z2#result"].iloc(0).as_double() == Catch::Approx(z1_googl).epsilon(0.01));
    }

    SECTION("Cross-sectional fan-out: cs -> reg1, reg2, reg3") {
        // One cross-sectional feeding multiple regular transforms
        auto code = R"(
src = market_data_source(timeframe="1D")()
close = src.c
z = cs_zscore()(close)
out1 = add()(z, 1)
out2 = mul()(z, 2)
out3 = add()(z, 3)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        auto aapl_df = results[dailyTF.ToString()][aapl];
        auto msft_df = results[dailyTF.ToString()][msft];

        // zscore for 2 assets: mean=150, sample_std = sqrt(2500 + 2500) = 70.71 (n-1 = 1)
        // z_aapl = (100-150)/70.71 = -0.7071
        // z_msft = (200-150)/70.71 = 0.7071
        double mean = 150.0;
        double std_dev = std::sqrt((50.0*50.0 + 50.0*50.0) / 1.0);  // sample std (n-1 = 1)
        double z_aapl = (100.0 - mean) / std_dev;
        double z_msft = (200.0 - mean) / std_dev;

        REQUIRE(aapl_df["z#result"].iloc(0).as_double() == Catch::Approx(z_aapl).epsilon(0.01));
        REQUIRE(msft_df["z#result"].iloc(0).as_double() == Catch::Approx(z_msft).epsilon(0.01));

        // Verify all 3 fan-out outputs for AAPL
        REQUIRE(aapl_df["out1#result"].iloc(0).as_double() == Catch::Approx(z_aapl + 1.0).epsilon(0.01));
        REQUIRE(aapl_df["out2#result"].iloc(0).as_double() == Catch::Approx(z_aapl * 2.0).epsilon(0.01));
        REQUIRE(aapl_df["out3#result"].iloc(0).as_double() == Catch::Approx(z_aapl + 3.0).epsilon(0.01));

        // Verify all 3 fan-out outputs for MSFT
        REQUIRE(msft_df["out1#result"].iloc(0).as_double() == Catch::Approx(z_msft + 1.0).epsilon(0.01));
        REQUIRE(msft_df["out2#result"].iloc(0).as_double() == Catch::Approx(z_msft * 2.0).epsilon(0.01));
        REQUIRE(msft_df["out3#result"].iloc(0).as_double() == Catch::Approx(z_msft + 3.0).epsilon(0.01));
    }

    SECTION("Complex pipeline: sma -> cs_zscore -> gte boolean") {
        // Realistic pipeline: calculate SMA, normalize across assets, generate signal
        auto code = R"(
src = market_data_source(timeframe="1D")()
close = src.c
ma = sma(period=2)(close)
z = cs_zscore()(ma)
signal = gte()(z, 0)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft, googl}, std::move(manager));

        // 2-day data for each asset
        // AAPL: [100, 102] -> SMA(2) = [NaN, 101]
        // MSFT: [200, 204] -> SMA(2) = [NaN, 202]
        // GOOGL: [300, 306] -> SMA(2) = [NaN, 303]
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 102.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 204.0});
        inputData[dailyTF.ToString()][googl] = CreateOHLCVData({300.0, 306.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        auto aapl_df = results[dailyTF.ToString()][aapl];
        auto msft_df = results[dailyTF.ToString()][msft];
        auto googl_df = results[dailyTF.ToString()][googl];

        // Verify SMA values at index 1 (after NaN)
        REQUIRE(aapl_df.contains("ma#result"));
        REQUIRE(aapl_df["ma#result"].iloc(1).as_double() == Catch::Approx(101.0));  // (100+102)/2
        REQUIRE(msft_df["ma#result"].iloc(1).as_double() == Catch::Approx(202.0));  // (200+204)/2
        REQUIRE(googl_df["ma#result"].iloc(1).as_double() == Catch::Approx(303.0));  // (300+306)/2

        // zscore at index 1: mean=202, values=[101, 202, 303]
        // sample_std = sqrt(((101-202)^2 + 0 + (101)^2) / 2) = sqrt(20402/2) = 101
        double mean = 202.0;
        double std_dev = std::sqrt((101.0*101.0 + 0.0 + 101.0*101.0) / 2.0);  // sample std (n-1)
        double z_aapl = (101.0 - mean) / std_dev;  // -1
        double z_msft = (202.0 - mean) / std_dev;  // 0
        double z_googl = (303.0 - mean) / std_dev; // 1

        REQUIRE(aapl_df["z#result"].iloc(1).as_double() == Catch::Approx(z_aapl).epsilon(0.01));
        REQUIRE(msft_df["z#result"].iloc(1).as_double() == Catch::Approx(z_msft).epsilon(0.01));
        REQUIRE(googl_df["z#result"].iloc(1).as_double() == Catch::Approx(z_googl).epsilon(0.01));

        // Signal: gte(z, 0) -> true if zscore >= 0
        REQUIRE(aapl_df.contains("signal#result"));
        REQUIRE(aapl_df["signal#result"].iloc(1).as_bool() == false);   // z_aapl < 0
        REQUIRE(msft_df["signal#result"].iloc(1).as_bool() == true);    // z_msft = 0
        REQUIRE(googl_df["signal#result"].iloc(1).as_bool() == true);   // z_googl > 0
    }

    SECTION("Multi-asset data isolation - each asset processed independently") {
        // Verify that per-asset transforms don't leak data between assets
        auto code = R"(
src = market_data_source(timeframe="1D")()
close = src.c
doubled = mul()(close, 2)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft, googl}, std::move(manager));

        // Each asset has unique values
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({10.0, 11.0, 12.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({20.0, 21.0, 22.0});
        inputData[dailyTF.ToString()][googl] = CreateOHLCVData({30.0, 31.0, 32.0});

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        // Verify AAPL values are 2x its input (not contaminated by MSFT/GOOGL)
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df["doubled#result"].iloc(0).as_double() == Catch::Approx(20.0));
        REQUIRE(aapl_df["doubled#result"].iloc(1).as_double() == Catch::Approx(22.0));
        REQUIRE(aapl_df["doubled#result"].iloc(2).as_double() == Catch::Approx(24.0));

        // Verify MSFT values are 2x its input
        auto msft_df = results[dailyTF.ToString()][msft];
        REQUIRE(msft_df["doubled#result"].iloc(0).as_double() == Catch::Approx(40.0));
        REQUIRE(msft_df["doubled#result"].iloc(1).as_double() == Catch::Approx(42.0));
        REQUIRE(msft_df["doubled#result"].iloc(2).as_double() == Catch::Approx(44.0));

        // Verify GOOGL values are 2x its input
        auto googl_df = results[dailyTF.ToString()][googl];
        REQUIRE(googl_df["doubled#result"].iloc(0).as_double() == Catch::Approx(60.0));
        REQUIRE(googl_df["doubled#result"].iloc(1).as_double() == Catch::Approx(62.0));
        REQUIRE(googl_df["doubled#result"].iloc(2).as_double() == Catch::Approx(64.0));
    }
}
