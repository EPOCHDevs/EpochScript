/**
 * @file pairs_trading_test.cpp
 * @brief Realistic integration tests for pairs trading strategies
 *
 * Tests the full pairs trading pipeline with:
 * - asset_ref_passthrough for universe filtering (run strategy only on specific assets)
 * - Cointegration transforms (rolling_adf, engle_granger, half_life_ar1)
 * - Spread calculation and z-score signals
 * - Multiple assets with real compiled epoch scripts
 */

#include "transforms/runtime/orchestrator.h"
#include "transform_manager/transform_manager.h"
#include "fake_data_sources.h"
#include "../common/test_constants.h"
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/core/constants.h>
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;

namespace {

// Helper to compile source and build TransformManager
std::unique_ptr<TransformManager> CompileSource(const std::string& sourceCode) {
    auto pythonSource = epoch_script::strategy::PythonSource(sourceCode, true);
    return std::make_unique<TransformManager>(pythonSource);
}

} // anonymous namespace

TEST_CASE("Pairs Trading - Asset Reference Passthrough", "[pairs_trading][asset_ref_passthrough]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;

    SECTION("Basic asset_ref_passthrough filters to single ticker") {
        // Use asset_ref_passthrough to get SPY data only
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
spy_close = asset_ref_passthrough(ticker="SPY")(src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy, aapl, msft}, std::move(transformManager));

        // Create price data for all assets
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData({400.0, 401.0, 402.0, 403.0});
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({150.0, 151.0, 152.0, 153.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({300.0, 301.0, 302.0, 303.0});

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // SPY should have the passthrough output
        REQUIRE(dailyResult.contains(spy));
        auto spyDf = dailyResult.at(spy);
        INFO("SPY columns: " << spyDf.num_cols());
        REQUIRE(spyDf.contains("spy_close#result"));
        REQUIRE(spyDf["spy_close#result"].size() == 4);

        // AAPL should NOT have the passthrough output (filtered out)
        if (dailyResult.contains(aapl)) {
            auto aaplDf = dailyResult.at(aapl);
            REQUIRE_FALSE(aaplDf.contains("spy_close#result"));
        }

        // MSFT should NOT have the passthrough output (filtered out)
        if (dailyResult.contains(msft)) {
            auto msftDf = dailyResult.at(msft);
            REQUIRE_FALSE(msftDf.contains("spy_close#result"));
        }
    }

    SECTION("Empty ticker filter passes through all assets") {
        // Empty ticker means match all assets
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
all_close = asset_ref_passthrough(ticker="")(src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy, aapl}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData({400.0, 401.0, 402.0});
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({150.0, 151.0, 152.0});

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // Both SPY and AAPL should have passthrough output
        REQUIRE(dailyResult.contains(spy));
        REQUIRE(dailyResult.at(spy).contains("all_close#result"));

        REQUIRE(dailyResult.contains(aapl));
        REQUIRE(dailyResult.at(aapl).contains("all_close#result"));
    }

    SECTION("Case-insensitive ticker matching") {
        // Lowercase "spy" should match "SPY"
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
spy_data = asset_ref_passthrough(ticker="spy")(src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData({400.0, 401.0, 402.0});

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // SPY should match even with lowercase filter
        REQUIRE(dailyResult.contains(spy));
        REQUIRE(dailyResult.at(spy).contains("spy_data#result"));
    }
}

TEST_CASE("Pairs Trading - Universe Filtering with Calculations", "[pairs_trading][spread]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;

    SECTION("Run strategy only on filtered assets with data verification") {
        // asset_ref_passthrough filters WHICH assets get the output
        // SPY gets passthrough, AAPL and MSFT do not
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Filter to SPY only - only SPY will have this output
spy_close = asset_ref_passthrough(ticker="SPY")(src.c)

# For SPY: spread = src.c - spy_close = 0 (self-reference)
# For AAPL/MSFT: spy_close doesn't exist, so spread won't be computed
spread = src.c - spy_close
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy, aapl, msft}, std::move(transformManager));

        // Create price data for all assets
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData({400.0, 401.0, 402.0, 403.0});
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({150.0, 151.0, 152.0, 153.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({300.0, 301.0, 302.0, 303.0});

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // SPY should have passthrough and spread outputs
        REQUIRE(dailyResult.contains(spy));
        auto spyDf = dailyResult.at(spy);
        REQUIRE(spyDf.contains("spy_close#result"));
        REQUIRE(spyDf.contains("spread#result"));

        // Verify SPY passthrough values match input close prices
        auto passthroughCol = spyDf["spy_close#result"];
        REQUIRE(passthroughCol.size() == 4);
        auto passthroughValues = passthroughCol.contiguous_array().to_vector<double>();
        REQUIRE(passthroughValues[0] == Catch::Approx(400.0));
        REQUIRE(passthroughValues[1] == Catch::Approx(401.0));
        REQUIRE(passthroughValues[2] == Catch::Approx(402.0));
        REQUIRE(passthroughValues[3] == Catch::Approx(403.0));

        // Self-spread should be 0 (SPY - SPY)
        auto spreadCol = spyDf["spread#result"];
        REQUIRE(spreadCol.size() == 4);
        auto spreadValues = spreadCol.contiguous_array().to_vector<double>();
        REQUIRE(spreadValues[0] == Catch::Approx(0.0));
        REQUIRE(spreadValues[1] == Catch::Approx(0.0));
        REQUIRE(spreadValues[2] == Catch::Approx(0.0));
        REQUIRE(spreadValues[3] == Catch::Approx(0.0));

        // AAPL should NOT have the passthrough output (filtered out)
        if (dailyResult.contains(aapl)) {
            auto aaplDf = dailyResult.at(aapl);
            REQUIRE_FALSE(aaplDf.contains("spy_close#result"));
        }

        // MSFT should NOT have the passthrough output (filtered out)
        if (dailyResult.contains(msft)) {
            auto msftDf = dailyResult.at(msft);
            REQUIRE_FALSE(msftDf.contains("spy_close#result"));
        }
    }

    SECTION("Verify passthrough preserves exact values for all matching assets") {
        // Empty ticker matches all assets - each gets its own data passed through
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()
all_close = asset_ref_passthrough(ticker="")(src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy, aapl}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData({400.0, 405.0, 410.0});
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({150.0, 155.0, 160.0});

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // SPY passthrough should have SPY's close prices
        REQUIRE(dailyResult.contains(spy));
        auto spyDf = dailyResult.at(spy);
        REQUIRE(spyDf.contains("all_close#result"));
        auto spyPassthrough = spyDf["all_close#result"].contiguous_array().to_vector<double>();
        REQUIRE(spyPassthrough[0] == Catch::Approx(400.0));
        REQUIRE(spyPassthrough[1] == Catch::Approx(405.0));
        REQUIRE(spyPassthrough[2] == Catch::Approx(410.0));

        // AAPL passthrough should have AAPL's close prices
        REQUIRE(dailyResult.contains(aapl));
        auto aaplDf = dailyResult.at(aapl);
        REQUIRE(aaplDf.contains("all_close#result"));
        auto aaplPassthrough = aaplDf["all_close#result"].contiguous_array().to_vector<double>();
        REQUIRE(aaplPassthrough[0] == Catch::Approx(150.0));
        REQUIRE(aaplPassthrough[1] == Catch::Approx(155.0));
        REQUIRE(aaplPassthrough[2] == Catch::Approx(160.0));
    }
}

TEST_CASE("Pairs Trading - Cointegration Analysis", "[pairs_trading][cointegration]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling ADF test on price series") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Rolling ADF test for stationarity
adf_result = rolling_adf(window=20)(src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create longer price series for rolling window
        std::vector<double> prices;
        for (int i = 0; i < 30; i++) {
            prices.push_back(400.0 + i * 0.5 + (i % 3) * 0.2);  // Trending with noise
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto spyDf = dailyResult.at(spy);

        // Print columns for debugging
        INFO("SPY columns: " << spyDf.num_cols());
        for (const auto& col : spyDf.column_names()) {
            INFO("Column: " << col);
        }

        // Verify ADF stat column exists
        REQUIRE(spyDf.contains("adf_result#adf_stat"));
        REQUIRE(spyDf.contains("adf_result#p_value"));
    }

    SECTION("Half-life AR(1) estimation") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Half-life of mean reversion
half_life = half_life_ar1(window=20)(src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create mean-reverting series
        std::vector<double> prices;
        double price = 100.0;
        for (int i = 0; i < 30; i++) {
            // Mean reversion to 100 with noise
            price = price + 0.3 * (100.0 - price) + (i % 2 == 0 ? 1.0 : -1.0);
            prices.push_back(price);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto spyDf = dailyResult.at(spy);

        // Print columns for debugging
        INFO("SPY columns: " << spyDf.num_cols());
        for (const auto& col : spyDf.column_names()) {
            INFO("Column: " << col);
        }

        // Verify half-life column exists
        REQUIRE(spyDf.contains("half_life#half_life"));
    }
}

TEST_CASE("Pairs Trading - Z-Score Signal Generation", "[pairs_trading][zscore][signal]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Z-score based trading signal") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Calculate z-score of returns (using [1] for lag)
returns = src.c / src.c[1] - 1
zs = zscore(window=20)(returns)

# Generate signals: buy when z < -2, sell when z > 2
buy_signal = zs < -2
sell_signal = zs > 2
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create price series with some extreme moves
        std::vector<double> prices = {
            100, 101, 102, 103, 104, 105, 106, 107, 108, 109,
            110, 111, 112, 113, 114, 115, 116, 117, 118, 119,
            120, 121, 122, 123, 90,  // Big drop
            91, 92, 93, 140,  // Big jump
            139
        };

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto spyDf = dailyResult.at(spy);

        // Verify all columns exist (single output nodes use #result)
        REQUIRE(spyDf.contains("zs#result"));
        REQUIRE(spyDf.contains("buy_signal#result"));
        REQUIRE(spyDf.contains("sell_signal#result"));
    }
}

TEST_CASE("Pairs Trading - Full Strategy Pipeline", "[pairs_trading][strategy][integration]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Pairs trading with reference_stocks for cross-asset spread") {
        // This demonstrates proper pairs trading:
        // 1. Load SPY data via reference_stocks (available to all assets)
        // 2. Calculate spread = current_asset - SPY
        // 3. Z-score the spread for signals
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Step 1: Load SPY reference data (available to ALL assets)
spy = reference_stocks(ticker="SPY", timeframe="1D")()

# Step 2: Calculate spread = current_asset - SPY
spread = src.c - spy.c

# Step 3: Z-score of spread for entry/exit signals
spread_zscore = zscore(window=20)(spread)

# Step 4: Trading signals based on z-score
long_entry = spread_zscore < -2
short_entry = spread_zscore > 2
exit_signal = (spread_zscore > -0.5) & (spread_zscore < 0.5)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(transformManager));

        // Create price data for AAPL
        std::vector<double> prices;
        for (int i = 0; i < 50; i++) {
            prices.push_back(150.0 + std::sin(i * 0.3) * 5.0);  // Oscillating prices
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData(prices);
        // Note: SPY data would be loaded by reference_stocks from external source

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(aapl));
        auto aaplDf = dailyResult.at(aapl);

        // Print columns for debugging
        INFO("AAPL columns: " << aaplDf.num_cols());
        for (const auto& col : aaplDf.column_names()) {
            INFO("Column: " << col);
        }

        // Verify strategy components exist
        REQUIRE(aaplDf.contains("spread#result"));
        REQUIRE(aaplDf.contains("spread_zscore#result"));
        REQUIRE(aaplDf.contains("long_entry#result"));
        REQUIRE(aaplDf.contains("short_entry#result"));
        REQUIRE(aaplDf.contains("exit_signal#result"));

        // Verify data integrity
        auto spreadCol = aaplDf["spread#result"];
        REQUIRE(spreadCol.size() == 50);
    }
}

TEST_CASE("Pairs Trading - Universe Filtering", "[pairs_trading][universe]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string jpm = "JPM";

    SECTION("Filter strategy to specific tickers only") {
        // Only apply strategy to AAPL
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Filter to AAPL only - other assets will have no output
aapl_only = asset_ref_passthrough(ticker="AAPL")(src.c)

# Strategy only runs where passthrough has data (using [1] for lag)
returns = aapl_only / aapl_only[1] - 1
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl, msft, jpm}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({150.0, 151.0, 152.0, 153.0, 154.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({300.0, 301.0, 302.0, 303.0, 304.0});
        inputData[dailyTF.ToString()][jpm] = CreateOHLCVData({140.0, 141.0, 142.0, 143.0, 144.0});

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // AAPL should have the passthrough output
        REQUIRE(dailyResult.contains(aapl));
        REQUIRE(dailyResult.at(aapl).contains("aapl_only#result"));

        // MSFT and JPM should NOT have the passthrough output
        if (dailyResult.contains(msft)) {
            REQUIRE_FALSE(dailyResult.at(msft).contains("aapl_only#result"));
        }
        if (dailyResult.contains(jpm)) {
            REQUIRE_FALSE(dailyResult.at(jpm).contains("aapl_only#result"));
        }
    }
}

TEST_CASE("Pairs Trading - Is Asset Reference", "[pairs_trading][is_asset_ref]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const std::string spy = "SPY";

    SECTION("is_asset_ref returns true for matching asset, false for others") {
        // is_asset_ref is a scalar that returns true/false per asset
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Check if current asset is AAPL
is_aapl = is_asset_ref(ticker="AAPL")()
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl, msft, spy}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({150.0, 151.0, 152.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({300.0, 301.0, 302.0});
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData({400.0, 401.0, 402.0});

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // AAPL should have is_aapl = true (scalar broadcasted to all rows)
        REQUIRE(dailyResult.contains(aapl));
        auto aaplDf = dailyResult.at(aapl);
        REQUIRE(aaplDf.contains("is_aapl#result"));
        auto aaplBool = aaplDf["is_aapl#result"].contiguous_array().to_vector<bool>();
        REQUIRE(aaplBool.size() == 3);  // Scalar broadcasted to match data length
        REQUIRE(aaplBool[0] == true);
        REQUIRE(aaplBool[1] == true);
        REQUIRE(aaplBool[2] == true);

        // MSFT should have is_aapl = false (all rows)
        REQUIRE(dailyResult.contains(msft));
        auto msftDf = dailyResult.at(msft);
        REQUIRE(msftDf.contains("is_aapl#result"));
        auto msftBool = msftDf["is_aapl#result"].contiguous_array().to_vector<bool>();
        REQUIRE(msftBool.size() == 3);
        REQUIRE(msftBool[0] == false);
        REQUIRE(msftBool[1] == false);
        REQUIRE(msftBool[2] == false);

        // SPY should have is_aapl = false (all rows)
        REQUIRE(dailyResult.contains(spy));
        auto spyDf = dailyResult.at(spy);
        REQUIRE(spyDf.contains("is_aapl#result"));
        auto spyBool = spyDf["is_aapl#result"].contiguous_array().to_vector<bool>();
        REQUIRE(spyBool.size() == 3);
        REQUIRE(spyBool[0] == false);
        REQUIRE(spyBool[1] == false);
        REQUIRE(spyBool[2] == false);
    }

    SECTION("is_asset_ref with conditional_select for pairs trading signals") {
        // Best pattern: use conditional_select for multi-asset signal generation
        // AAPL: enter_long when spread < -2 (oversold, expect reversion up)
        // MSFT: enter_long when spread > 2 (overbought for AAPL means oversold for MSFT)
        // This creates the pairs trade: long AAPL + short MSFT (or vice versa)
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Check which asset we're on
is_aapl = is_asset_ref(ticker="AAPL")()
is_msft = is_asset_ref(ticker="MSFT")()

# Spread z-score (simulated)
spread_z = zscore(window=10)(src.c)

# Pairs trading entry signals using conditional_select:
# AAPL enters long when spread oversold (z < -2)
# MSFT enters long when spread overbought (z > 2) - opposite side of the trade
enter_long = conditional_select_boolean()(
    is_aapl and (spread_z < -2), bool_true()(),
    is_msft and (spread_z > 2), bool_true()(),
    bool_false()()
)

# AAPL enters short when spread overbought (z > 2)
# MSFT enters short when spread oversold (z < -2)
enter_short = conditional_select_boolean()(
    is_aapl and (spread_z > 2), bool_true()(),
    is_msft and (spread_z < -2), bool_true()(),
    bool_false()()
)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl, msft, spy}, std::move(transformManager));

        // Create price data with some z-score variance
        std::vector<double> prices;
        for (int i = 0; i < 20; i++) {
            prices.push_back(100.0 + i * 0.5);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData(prices);
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData(prices);
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // All assets should have the signal outputs
        REQUIRE(dailyResult.contains(aapl));
        auto aaplDf = dailyResult.at(aapl);

        REQUIRE(aaplDf.contains("is_aapl#result"));
        REQUIRE(aaplDf.contains("is_msft#result"));
        REQUIRE(aaplDf.contains("enter_long#value"));
        REQUIRE(aaplDf.contains("enter_short#value"));

        REQUIRE(dailyResult.contains(msft));
        auto msftDf = dailyResult.at(msft);
        REQUIRE(msftDf.contains("is_aapl#result"));
        REQUIRE(msftDf.contains("is_msft#result"));
        REQUIRE(msftDf.contains("enter_long#value"));
        REQUIRE(msftDf.contains("enter_short#value"));

        REQUIRE(dailyResult.contains(spy));
        auto spyDf = dailyResult.at(spy);
        REQUIRE(spyDf.contains("is_aapl#result"));
        REQUIRE(spyDf.contains("is_msft#result"));
        REQUIRE(spyDf.contains("enter_long#value"));
        REQUIRE(spyDf.contains("enter_short#value"));

        // Verify is_asset_ref values
        auto aaplIsAapl = aaplDf["is_aapl#result"].contiguous_array().to_vector<bool>();
        auto msftIsAapl = msftDf["is_aapl#result"].contiguous_array().to_vector<bool>();
        auto spyIsAapl = spyDf["is_aapl#result"].contiguous_array().to_vector<bool>();
        REQUIRE(aaplIsAapl[0] == true);
        REQUIRE(msftIsAapl[0] == false);
        REQUIRE(spyIsAapl[0] == false);

        auto aaplIsMsft = aaplDf["is_msft#result"].contiguous_array().to_vector<bool>();
        auto msftIsMsft = msftDf["is_msft#result"].contiguous_array().to_vector<bool>();
        auto spyIsMsft = spyDf["is_msft#result"].contiguous_array().to_vector<bool>();
        REQUIRE(aaplIsMsft[0] == false);
        REQUIRE(msftIsMsft[0] == true);
        REQUIRE(spyIsMsft[0] == false);
    }

    SECTION("is_asset_ref with empty ticker matches all assets") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Empty ticker matches all
is_any = is_asset_ref(ticker="")()
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(transformManager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({150.0, 151.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({300.0, 301.0});

        auto result = orch.ExecutePipeline(std::move(inputData));

        const auto& dailyResult = result[dailyTF.ToString()];

        // Both should have is_any = true (empty ticker matches all)
        REQUIRE(dailyResult.contains(aapl));
        auto aaplBool = dailyResult.at(aapl)["is_any#result"].contiguous_array().to_vector<bool>();
        REQUIRE(aaplBool[0] == true);

        REQUIRE(dailyResult.contains(msft));
        auto msftBool = dailyResult.at(msft)["is_any#result"].contiguous_array().to_vector<bool>();
        REQUIRE(msftBool[0] == true);
    }
}
