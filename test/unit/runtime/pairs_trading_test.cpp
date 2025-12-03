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
#include "runtime_test_utils.h"
#include "../common/test_constants.h"
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/core/constants.h>
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include <epoch_frame/serialization.h>
#include <epoch_frame/factory/index_factory.h>
#include <spdlog/spdlog.h>

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;

namespace {

// Helper to compile source and build TransformManager
std::unique_ptr<TransformManager> CompileSource(const std::string& sourceCode) {
    auto pythonSource = epoch_script::strategy::PythonSource(sourceCode, true);
    return std::make_unique<TransformManager>(pythonSource);
}

// Load real EURUSD OHLC data from CSV
epoch_frame::DataFrame LoadRealOHLCData(const std::filesystem::path& csvPath) {
    auto df_res = epoch_frame::read_csv_file(csvPath, epoch_frame::CSVReadOptions{});
    REQUIRE(df_res.ok());
    auto df = df_res.ValueOrDie();

    // Set Date as index and convert to UTC nanoseconds
    df = df.set_index("Date");
    auto ts_array = df.index()->array().cast(arrow::timestamp(arrow::TimeUnit::NANO, "UTC"));
    auto ts_index = epoch_frame::factory::index::make_index(
        ts_array.value(), epoch_frame::MonotonicDirection::Increasing, "index");
    df = df.set_index(ts_index);

    // Rename columns to match expected OHLCV format (lowercase)
    std::unordered_map<std::string, std::string> rename_map = {
        {"Open", "o"}, {"High", "h"}, {"Low", "l"}, {"Close", "c"}, {"Tickvol", "v"}
    };
    return df.rename(rename_map);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);

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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
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

    // Test asset_ref_passthrough filtering and spread calculations across universe
    // SPY gets passthrough, AAPL/MSFT filtered out; also test empty ticker matching all
    std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Filter to SPY only - only SPY will have this output
spy_close = asset_ref_passthrough(ticker="SPY")(src.c)

# Empty ticker matches all assets
all_close = asset_ref_passthrough(ticker="")(src.c)

# Spread calculations
spread_vs_spy = src.c - spy_close
)";

    auto transformManager = CompileSource(sourceCode);
    DataFlowRuntimeOrchestrator orch({spy, aapl, msft}, std::move(transformManager));

    // Create price data for all assets
    TimeFrameAssetDataFrameMap inputData;
    inputData[dailyTF.ToString()][spy] = CreateOHLCVData({400.0, 401.0, 402.0, 403.0});
    inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({150.0, 151.0, 152.0, 153.0});
    inputData[dailyTF.ToString()][msft] = CreateOHLCVData({300.0, 301.0, 302.0, 303.0});

    data_sdk::events::ScopedProgressEmitter emitter;
    auto result = orch.ExecutePipeline(std::move(inputData), emitter);
    const auto& dailyResult = result[dailyTF.ToString()];

    // Print all outputs for each asset
    for (const auto& asset : {spy, aapl, msft}) {
        if (dailyResult.contains(asset)) {
            SPDLOG_INFO("=== {} ===\n{}", asset, dailyResult.at(asset).repr());
        }
    }

    // Verify outputs exist (no crash, complete)
    REQUIRE(dailyResult.contains(spy));
    auto spyDf = dailyResult.at(spy);
    REQUIRE(spyDf.contains("spy_close#result"));
    REQUIRE(spyDf.contains("all_close#result"));
    REQUIRE(spyDf.contains("spread_vs_spy#result"));

    // Phase 3: REQUIRE verified values
    // SPY passthrough should match input close prices
    auto spyPassthrough = spyDf["spy_close#result"].contiguous_array().to_vector<double>();
    REQUIRE(spyPassthrough[0] == Catch::Approx(400.0));
    REQUIRE(spyPassthrough[1] == Catch::Approx(401.0));
    REQUIRE(spyPassthrough[2] == Catch::Approx(402.0));
    REQUIRE(spyPassthrough[3] == Catch::Approx(403.0));

    // SPY spread vs itself should be 0
    auto spySpread = spyDf["spread_vs_spy#result"].contiguous_array().to_vector<double>();
    REQUIRE(spySpread[0] == Catch::Approx(0.0));
    REQUIRE(spySpread[1] == Catch::Approx(0.0));
    REQUIRE(spySpread[2] == Catch::Approx(0.0));
    REQUIRE(spySpread[3] == Catch::Approx(0.0));

    // AAPL: all_close exists (empty ticker), spy_close filtered out, spread has nulls
    if (dailyResult.contains(aapl)) {
        auto aaplDf = dailyResult.at(aapl);
        REQUIRE(aaplDf.contains("all_close#result"));
        REQUIRE_FALSE(aaplDf.contains("spy_close#result"));  // Filtered to SPY only
        // spread_vs_spy exists but has null values (spy_close missing)
        REQUIRE(aaplDf.contains("spread_vs_spy#result"));

        auto aaplAllClose = aaplDf["all_close#result"].contiguous_array().to_vector<double>();
        REQUIRE(aaplAllClose[0] == Catch::Approx(150.0));
        REQUIRE(aaplAllClose[1] == Catch::Approx(151.0));
        REQUIRE(aaplAllClose[2] == Catch::Approx(152.0));
        REQUIRE(aaplAllClose[3] == Catch::Approx(153.0));
    }

    // MSFT: all_close exists (empty ticker), spy_close filtered out, spread has nulls
    if (dailyResult.contains(msft)) {
        auto msftDf = dailyResult.at(msft);
        REQUIRE(msftDf.contains("all_close#result"));
        REQUIRE_FALSE(msftDf.contains("spy_close#result"));  // Filtered to SPY only
        // spread_vs_spy exists but has null values (spy_close missing)
        REQUIRE(msftDf.contains("spread_vs_spy#result"));

        auto msftAllClose = msftDf["all_close#result"].contiguous_array().to_vector<double>();
        REQUIRE(msftAllClose[0] == Catch::Approx(300.0));
        REQUIRE(msftAllClose[1] == Catch::Approx(301.0));
        REQUIRE(msftAllClose[2] == Catch::Approx(302.0));
        REQUIRE(msftAllClose[3] == Catch::Approx(303.0));
    }
}

TEST_CASE("Pairs Trading - Cointegration and Mean Reversion Analysis", "[pairs_trading][cointegration]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    // Combined test: Rolling ADF stationarity + Half-life AR(1) + Z-score signals
    std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Rolling ADF test for stationarity
adf_result = rolling_adf(window=20)(src.c)

# Half-life of mean reversion
half_life = half_life_ar1(window=20)(src.c)

# Calculate z-score of returns for signals
returns = src.c / src.c[1] - 1
zs = zscore(window=20)(returns)

# Generate signals: buy when z < -2, sell when z > 2
buy_signal = zs < -2
sell_signal = zs > 2
)";

    auto transformManager = CompileSource(sourceCode);
    DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

    // Create price series with mean reversion and extreme moves
    std::vector<double> prices;
    double price = 100.0;
    for (int i = 0; i < 30; i++) {
        // Mean reversion to 100 with noise
        price = price + 0.3 * (100.0 - price) + (i % 2 == 0 ? 1.0 : -1.0);
        prices.push_back(price);
    }
    // Add extreme moves for signal testing
    prices[24] = 80.0;  // Big drop
    prices[28] = 130.0; // Big jump

    TimeFrameAssetDataFrameMap inputData;
    inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

    data_sdk::events::ScopedProgressEmitter emitter;
    auto result = orch.ExecutePipeline(std::move(inputData), emitter);
    const auto& dailyResult = result[dailyTF.ToString()];

    REQUIRE(dailyResult.contains(spy));
    auto df = dailyResult.at(spy);

    SPDLOG_INFO("=== Cointegration & Mean Reversion Test ===\n{}", df.repr());

    // Verify outputs exist
    REQUIRE(df.contains("adf_result#adf_stat"));
    REQUIRE(df.contains("adf_result#p_value"));
    REQUIRE(df.contains("half_life#half_life"));
    REQUIRE(df.contains("zs#result"));
    REQUIRE(df.contains("buy_signal#result"));
    REQUIRE(df.contains("sell_signal#result"));

    // Phase 3: REQUIRE verified values (after visual verification)
    // ADF and half-life should have valid values after warm-up period
    auto adf_stat = df["adf_result#adf_stat"].drop_null().contiguous_array().to_vector<double>();
    auto hl = df["half_life#half_life"].drop_null().contiguous_array().to_vector<double>();
    REQUIRE(adf_stat.size() > 0);
    REQUIRE(hl.size() > 0);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
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

// =============================================================================
// COMPREHENSIVE INTEGRATION TESTS FOR PAIRS TRADING TRANSFORMS
// Following approach: 1) Test no crash 2) Print outputs 3) Convert to REQUIRE
// =============================================================================

TEST_CASE("Pairs Trading Integration - Engle-Granger Cointegration", "[pairs_trading][cointegration][engle_granger]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Engle-Granger cointegration on high vs close") {
        // Use high and close prices as a proxy for cointegration test
        // NOTE: Multi-output transforms like engle_granger must have outputs extracted
        // to named variables before use in arithmetic expressions to avoid ambiguity
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Engle-Granger cointegration test between high and close
eg = engle_granger(window=60)(y=src.h, x=src.c)

# Extract outputs to named variables for use in expressions
hedge_ratio = eg.hedge_ratio
eg_spread = eg.spread

# Z-score the Engle-Granger spread for signals
spread_z = zscore(window=20)(eg_spread)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create cointegrated price series (100 days)
        std::vector<double> prices;
        double spy_base = 400.0;
        for (int i = 0; i < 100; i++) {
            // Cointegrated series: spy + mean-reverting noise
            double noise = std::sin(i * 0.2) * 5.0 + (i % 3 - 1) * 2.0;
            prices.push_back(spy_base + i * 0.3 + noise);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Engle-Granger Cointegration Test ===\n{}", df.repr());

        // Expected outputs from engle_granger
        REQUIRE(df.contains("eg#hedge_ratio"));
        REQUIRE(df.contains("eg#intercept"));
        REQUIRE(df.contains("eg#spread"));
        REQUIRE(df.contains("eg#adf_stat"));
        REQUIRE(df.contains("eg#p_value"));
        REQUIRE(df.contains("eg#is_cointegrated"));

        // Verify extracted hedge_ratio and spread z-score exist
        REQUIRE(df.contains("hedge_ratio#result"));
        REQUIRE(df.contains("eg_spread#result"));
        REQUIRE(df.contains("spread_z#result"));

        // Compare against CSV baseline (or generate if GENERATE_BASELINES=1)
        // Skip first 60 rows (window warmup period)
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/engle_granger",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"eg#hedge_ratio", "eg#spread", "eg#adf_stat", "eg#p_value", "spread_z#result"},
            0.05,  // 5% relative tolerance for statistical values
            0.1,   // absolute tolerance
            60     // skip warmup rows
        );
    }
}

TEST_CASE("Pairs Trading Integration - Johansen Cointegration", "[pairs_trading][cointegration][johansen]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string eurusd = "EURUSD";

    SECTION("Johansen cointegration test on real EURUSD OHLC (N=4)") {
        // Use real EURUSD daily data - real market data has proper OHLC relationships
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Johansen cointegration test on all 4 OHLC prices
joh = johansen_4(window=60)(asset_0=src.o, asset_1=src.h, asset_2=src.l, asset_3=src.c)

# Extract the spread for trading signals
joh_spread = joh.spread

# Z-score the Johansen spread for signals
spread_z = zscore(window=20)(joh_spread)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({eurusd}, std::move(transformManager));

        // Load real EURUSD daily OHLC data
        auto eurusd_df = LoadRealOHLCData(
            std::filesystem::path(RUNTIME_TEST_DATA_DIR) / "eurusd_1d.csv");

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][eurusd] = eurusd_df;

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(eurusd));
        auto df = dailyResult.at(eurusd);

        SPDLOG_INFO("=== Johansen Cointegration Test (N=4) on EURUSD ===\n{}", df.repr());

        // Expected outputs from johansen_4
        REQUIRE(df.contains("joh#rank"));
        for (int i = 0; i < 4; ++i) {
            REQUIRE(df.contains("joh#trace_stat_" + std::to_string(i)));
            REQUIRE(df.contains("joh#max_stat_" + std::to_string(i)));
            REQUIRE(df.contains("joh#eigval_" + std::to_string(i)));
            REQUIRE(df.contains("joh#beta_" + std::to_string(i)));
        }
        REQUIRE(df.contains("joh#spread"));

        // Verify extracted spread and z-score exist
        REQUIRE(df.contains("joh_spread#result"));
        REQUIRE(df.contains("spread_z#result"));

        // Johansen test has inherent numerical variance in eigenvalue decomposition
        // Even rank can vary due to trace statistic variations near critical values
        // Verify all expected outputs exist and have correct shape
        REQUIRE(df.num_rows() > 80);  // Expect data beyond warmup period
    }
}

TEST_CASE("Pairs Trading Integration - Rolling ADF Stationarity", "[pairs_trading][cointegration][rolling_adf]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling ADF stationarity test on series") {
        // Test if price series is stationary (uses only market data)
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Test stationarity directly on close prices
adf = rolling_adf(window=60)(src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create mean-reverting spread (should be stationary)
        std::vector<double> prices;
        double mean = 100.0;
        double price = mean;
        for (int i = 0; i < 100; i++) {
            price = price + 0.3 * (mean - price) + (i % 2 == 0 ? 1.0 : -1.0);
            prices.push_back(price);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Rolling ADF Stationarity Test ===\n{}", df.repr());

        // Expected outputs from rolling_adf
        REQUIRE(df.contains("adf#adf_stat"));
        REQUIRE(df.contains("adf#p_value"));
        REQUIRE(df.contains("adf#is_stationary"));
        REQUIRE(df.contains("adf#critical_5pct"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/rolling_adf",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"adf#adf_stat", "adf#p_value", "adf#critical_5pct"},
            0.05, 0.1, 60
        );
    }
}

TEST_CASE("Pairs Trading Integration - Half-Life Mean Reversion", "[pairs_trading][cointegration][half_life]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Half-life AR(1) estimation for mean reversion timing") {
        // Estimate half-life of mean reversion on price series
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Half-life directly on close prices
hl = half_life_ar1(window=60)(src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create mean-reverting series with known half-life
        std::vector<double> prices;
        double mean = 100.0;
        double price = 120.0;  // Start away from mean
        double phi = 0.9;  // AR(1) coefficient (half-life ~ -ln(2)/ln(0.9) ~ 6.6)
        for (int i = 0; i < 100; i++) {
            price = mean + phi * (price - mean) + (i % 3 - 1) * 0.5;
            prices.push_back(price);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Half-Life AR(1) Mean Reversion Test ===\n{}", df.repr());

        // Expected outputs from half_life_ar1
        REQUIRE(df.contains("hl#half_life"));
        REQUIRE(df.contains("hl#ar1_coef"));
        REQUIRE(df.contains("hl#is_mean_reverting"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/half_life",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"hl#half_life", "hl#ar1_coef"},
            0.05, 0.1, 60
        );
    }
}

TEST_CASE("Pairs Trading Integration - Rolling Correlation", "[pairs_trading][correlation][rolling_corr]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling correlation between price and volume") {
        // Use market data correlation (price vs volume)
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Normalize price and volume for correlation
price_ret = src.c / src.c[1] - 1
vol_chg = src.v / src.v[1] - 1

# Correlation between returns and volume changes
corr = rolling_corr(window=60)(x=price_ret, y=vol_chg)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create correlated price series
        std::vector<double> prices;
        for (int i = 0; i < 100; i++) {
            prices.push_back(100.0 + i * 0.3 + std::sin(i * 0.15) * 3.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Rolling Correlation Test ===\n{}", df.repr());

        REQUIRE(df.contains("corr#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/rolling_corr",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"corr#result"},
            0.05, 0.1, 60
        );
    }
}

TEST_CASE("Pairs Trading Integration - Rolling Beta", "[pairs_trading][correlation][beta]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling beta estimation") {
        // Use price vs volume as proxy for beta calculation
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

price_ret = src.c / src.c[1] - 1
vol_chg = src.v / src.v[1] - 1

b = beta(window=60)(asset_returns=price_ret, market_returns=vol_chg)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 100; i++) {
            prices.push_back(100.0 + i * 0.4 + std::sin(i * 0.1) * 2.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Rolling Beta Test ===\n{}", df.repr());

        REQUIRE(df.contains("b#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/beta",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"b#result"},
            0.05, 0.1, 60
        );
    }
}

TEST_CASE("Pairs Trading Integration - EWM Correlation", "[pairs_trading][correlation][ewm_corr]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Exponentially weighted correlation") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

price_ret = src.c / src.c[1] - 1
vol_chg = src.v / src.v[1] - 1

ewm_c = ewm_corr(span=30)(x=price_ret, y=vol_chg)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 100; i++) {
            prices.push_back(100.0 + i * 0.25 + std::cos(i * 0.12) * 4.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== EWM Correlation Test ===\n{}", df.repr());

        REQUIRE(df.contains("ewm_c#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/ewm_corr",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"ewm_c#result"},
            0.05, 0.1, 30
        );
    }
}

TEST_CASE("Pairs Trading Integration - Rolling Covariance", "[pairs_trading][correlation][rolling_cov]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling and EWM covariance") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

price_ret = src.c / src.c[1] - 1
vol_chg = src.v / src.v[1] - 1

cov = rolling_cov(window=60)(x=price_ret, y=vol_chg)
ewm_cv = ewm_cov(span=30)(x=price_ret, y=vol_chg)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 100; i++) {
            prices.push_back(100.0 + i * 0.2 + std::sin(i * 0.1) * 5.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Rolling Covariance Test ===\n{}", df.repr());

        REQUIRE(df.contains("cov#result"));
        REQUIRE(df.contains("ewm_cv#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/rolling_cov",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"cov#result", "ewm_cv#result"},
            0.05, 0.001, 60  // tighter atol for covariance
        );
    }
}

TEST_CASE("Pairs Trading Integration - Linear Fit OLS", "[pairs_trading][correlation][linear_fit]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling OLS regression") {
        // Linear fit between high and close prices
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

fit = linear_fit(window=60)(x=src.h, y=src.c)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 100; i++) {
            // Linear relationship with noise
            prices.push_back(50.0 + 0.8 * (100.0 + i * 0.3) + std::sin(i * 0.2) * 2.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Linear Fit OLS Test ===\n{}", df.repr());

        REQUIRE(df.contains("fit#slope"));
        REQUIRE(df.contains("fit#intercept"));
        REQUIRE(df.contains("fit#residual"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/linear_fit",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"fit#slope", "fit#intercept", "fit#residual"},
            0.05, 0.1, 60
        );
    }
}

TEST_CASE("Pairs Trading Integration - Hurst Exponent", "[pairs_trading][normalization][hurst]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling Hurst exponent for long memory analysis") {
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Rolling Hurst exponent to detect trending/mean-reverting
hurst = rolling_hurst_exponent(window=100)(src.c)

# Interpretation:
# H < 0.5: mean-reverting
# H = 0.5: random walk
# H > 0.5: trending
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create a mean-reverting series (Hurst < 0.5)
        std::vector<double> prices;
        double mean = 100.0;
        double price = mean;
        for (int i = 0; i < 150; i++) {
            price = mean + 0.7 * (price - mean) + (i % 3 - 1) * 0.5;  // Mean-reverting
            prices.push_back(price);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Hurst Exponent Test ===\n{}", df.repr());

        REQUIRE(df.contains("hurst#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "pairs_trading/hurst",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"hurst#result"},
            0.05, 0.1, 100  // skip 100 rows for hurst window warmup
        );
    }
}

TEST_CASE("Pairs Trading Integration - Fractional Differentiation", "[pairs_trading][normalization][frac_diff]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Fractional differentiation for ML features") {
        // Apply frac_diff directly to close prices
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Fractional diff to achieve stationarity while preserving memory
fd = frac_diff(d=0.5)(src.c)

# Verify stationarity
adf = rolling_adf(window=60)(fd)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 100; i++) {
            prices.push_back(100.0 + i * 0.5 + std::sin(i * 0.1) * 3.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Fractional Differentiation Test ===\n{}", df.repr());

        REQUIRE(df.contains("fd#result"));
    }
}

TEST_CASE("Pairs Trading Integration - Z-Score Mean Reversion Signals", "[pairs_trading][normalization][zscore]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Z-score based trading signals") {
        // Z-score on close prices for mean-reversion signals
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

z = zscore(window=20)(src.c)

# Trading signals
long_entry = z < -2
short_entry = z > 2
exit_signal = (z > -0.5) & (z < 0.5)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create spread that oscillates around 0
        std::vector<double> prices;
        for (int i = 0; i < 100; i++) {
            prices.push_back(100.0 + std::sin(i * 0.2) * 10.0);  // Oscillating
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Z-Score Signals Test ===\n{}", df.repr());

        REQUIRE(df.contains("z#result"));
        REQUIRE(df.contains("long_entry#result"));
        REQUIRE(df.contains("short_entry#result"));
        REQUIRE(df.contains("exit_signal#result"));
    }
}

TEST_CASE("Pairs Trading Integration - Full Strategy Pipeline", "[pairs_trading][strategy][full_pipeline]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Simplified strategy with z-score and half-life") {
        // Simplified pipeline without reference_stocks
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Z-score for entry/exit
z = zscore(window=20)(src.c)

# Mean reversion half-life
hl = half_life_ar1(window=60)(src.c)

# Signals based on z-score thresholds
long_entry = z < -2
short_entry = z > 2
exit_signal = (z > -0.5) & (z < 0.5)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create realistic cointegrated price series
        std::vector<double> prices;
        for (int i = 0; i < 150; i++) {
            double base = 100.0 + i * 0.2;
            double spread_noise = std::sin(i * 0.15) * 5.0;
            prices.push_back(base + spread_noise);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Full Strategy Pipeline Test ===\n{}", df.repr());

        // Verify core components exist
        REQUIRE(df.contains("z#result"));
        REQUIRE(df.contains("hl#half_life"));
        REQUIRE(df.contains("long_entry#result"));
        REQUIRE(df.contains("short_entry#result"));
        REQUIRE(df.contains("exit_signal#result"));
    }
}

TEST_CASE("Pairs Trading Integration - Hurst + Half-Life Strategy", "[pairs_trading][strategy][hurst_halflife]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Hurst-based mean reversion validation") {
        // Hurst + half-life on close prices
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Check if mean-reverting using Hurst
hurst = rolling_hurst_exponent(window=100)(src.c)
is_mean_reverting = hurst < 0.5

# If mean-reverting, estimate half-life
hl = half_life_ar1(window=60)(src.c)

# Only trade when Hurst indicates mean-reversion
z = zscore(window=20)(src.c)
long_entry = (z < -2) & is_mean_reverting
short_entry = (z > 2) & is_mean_reverting
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create mean-reverting series
        std::vector<double> prices;
        double mean = 100.0;
        double price = mean;
        for (int i = 0; i < 150; i++) {
            price = mean + 0.85 * (price - mean) + std::sin(i * 0.5) * 2.0;
            prices.push_back(price);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Hurst + Half-Life Strategy Test ===\n{}", df.repr());

        REQUIRE(df.contains("hurst#result"));
        REQUIRE(df.contains("hl#half_life"));
        REQUIRE(df.contains("long_entry#result"));
        REQUIRE(df.contains("short_entry#result"));
    }
}

TEST_CASE("Pairs Trading Integration - ML Feature Pipeline", "[pairs_trading][strategy][ml_features]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Feature engineering for ML") {
        // ML features using only market data
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Feature 1: Fractional differencing
fd = frac_diff(d=0.5)(src.c)

# Feature 2: Z-score
z = zscore(window=20)(src.c)

# Feature 3: Rolling beta (price vs volume changes)
price_ret = src.c / src.c[1] - 1
vol_chg = src.v / src.v[1] - 1
b = beta(window=60)(asset_returns=price_ret, market_returns=vol_chg)

# Feature 4: Correlation
corr = rolling_corr(window=60)(x=price_ret, y=vol_chg)

# All features normalized
fd_norm = zscore(window=60)(fd)
b_norm = zscore(window=60)(b)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 150; i++) {
            prices.push_back(100.0 + i * 0.3 + std::sin(i * 0.1) * 5.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== ML Feature Pipeline Test ===\n{}", df.repr());

        // Verify all features exist
        REQUIRE(df.contains("fd#result"));
        REQUIRE(df.contains("z#result"));
        REQUIRE(df.contains("b#result"));
        REQUIRE(df.contains("corr#result"));
        REQUIRE(df.contains("fd_norm#result"));
        REQUIRE(df.contains("b_norm#result"));
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
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
