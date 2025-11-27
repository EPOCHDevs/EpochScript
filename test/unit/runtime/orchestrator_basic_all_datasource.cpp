/**
 * @file orchestrator_basic_all_datasource.cpp
 * @brief Basic timeseries path tests for all supported data sources
 *
 * These tests verify that the orchestrator correctly executes timeseries
 * transforms (no cross-sectional) for each data source type. Each test:
 * - Creates fake data with the appropriate columns for the data source
 * - Runs a simple transform pipeline on the data
 * - Verifies the output structure and values
 *
 * Data sources tested:
 * - market_data_source (OHLCV)
 * - dividends
 * - short_volume
 * - short_interest
 * - news
 * - splits
 * - balance_sheet
 * - income_statement
 * - cash_flow
 * - financial_ratios
 * - economic_indicator (FRED)
 */

#include "transforms/runtime/orchestrator.h"
#include "../common/test_constants.h"
#include "../../integration/mocks/mock_transform_manager.h"
#include "fake_data_sources.h"
#include <epoch_core/catch_defs.h>
#include <catch2/catch_test_macros.hpp>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

#include "epoch_script/core/bar_attribute.h"

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// ============================================================================
// MARKET DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - market_data_source timeseries path", "[orchestrator][datasource][market_data][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Basic SMA on close price") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
ma = sma(period=3)(c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        // Create OHLCV data: close = [100, 102, 104, 106, 108]
        // SMA(3) = [NaN, NaN, 102, 104, 106]
        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 102.0, 104.0, 106.0, 108.0});

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        REQUIRE(results[dailyTF.ToString()].contains(aapl));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("ma#result"));

        auto sma = aapl_df["ma#result"].drop_null();
        REQUIRE(sma.size() >= 3);
        REQUIRE(sma.iloc(0).as_double() == Catch::Approx(102.0));
        REQUIRE(sma.iloc(1).as_double() == Catch::Approx(104.0));
        REQUIRE(sma.iloc(2).as_double() == Catch::Approx(106.0));
    }

    SECTION("Boolean comparison on close") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
signal = gte()(c, 105)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 103.0, 105.0, 107.0, 110.0});

        auto results = orch.ExecutePipeline(std::move(inputData));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("signal#result"));

        auto signal = aapl_df["signal#result"];
        REQUIRE(signal.iloc(0).as_bool() == false);  // 100 >= 105 = false
        REQUIRE(signal.iloc(1).as_bool() == false);  // 103 >= 105 = false
        REQUIRE(signal.iloc(2).as_bool() == true);   // 105 >= 105 = true
        REQUIRE(signal.iloc(3).as_bool() == true);   // 107 >= 105 = true
        REQUIRE(signal.iloc(4).as_bool() == true);   // 110 >= 105 = true
    }

    SECTION("Arithmetic operations on price") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
doubled = mul()(c, 2)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 150.0, 200.0});

        auto results = orch.ExecutePipeline(std::move(inputData));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("doubled#result"));

        auto doubled = aapl_df["doubled#result"];
        REQUIRE(doubled.iloc(0).as_double() == Catch::Approx(200.0));
        REQUIRE(doubled.iloc(1).as_double() == Catch::Approx(300.0));
        REQUIRE(doubled.iloc(2).as_double() == Catch::Approx(400.0));
    }

    SECTION("Multi-asset data isolation") {
        const std::string msft = TestAssetConstants::MSFT;

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
result = add()(c, 10)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 101.0, 102.0});
        inputData[dailyTF.ToString()][msft] = CreateOHLCVData({200.0, 201.0, 202.0});

        auto results = orch.ExecutePipeline(std::move(inputData));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        auto msft_df = results[dailyTF.ToString()][msft];

        REQUIRE(aapl_df.contains("result#result"));
        REQUIRE(msft_df.contains("result#result"));

        // AAPL: 100+10=110, 101+10=111, 102+10=112
        REQUIRE(aapl_df["result#result"].iloc(0).as_double() == Catch::Approx(110.0));
        REQUIRE(aapl_df["result#result"].iloc(1).as_double() == Catch::Approx(111.0));

        // MSFT: 200+10=210, 201+10=211, 202+10=212
        REQUIRE(msft_df["result#result"].iloc(0).as_double() == Catch::Approx(210.0));
        REQUIRE(msft_df["result#result"].iloc(1).as_double() == Catch::Approx(211.0));
    }
}

// ============================================================================
// DIVIDENDS DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - dividends timeseries path", "[orchestrator][datasource][dividends][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Basic operations on dividend cash_amount") {
        auto code = R"(
src = dividends(timeframe="1D")()
amt = src.cash_amount
doubled = mul()(amt, 2)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateDividendData(5, 0.25);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        REQUIRE(results[dailyTF.ToString()].contains(aapl));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("doubled#result"));

        // Base dividend is 0.25, doubled should be 0.50
        auto doubled = aapl_df["doubled#result"];
        REQUIRE(doubled.iloc(0).as_double() == Catch::Approx(0.50));
    }
}

// ============================================================================
// SHORT VOLUME DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - short_volume timeseries path", "[orchestrator][datasource][short_volume][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Operations on short_volume_ratio") {
        auto code = R"(
src = short_volume(timeframe="1D")()
ratio = src.short_volume_ratio
threshold = gte()(ratio, 30)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateShortVolumeData(5);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("threshold#result"));

        // Short volume ratio is always 30% (shortVol = total * 0.3)
        // So gte(ratio, 30) should be TRUE for all rows
        auto threshold = aapl_df["threshold#result"];
        REQUIRE(threshold.iloc(0).as_bool() == true);
        REQUIRE(threshold.iloc(1).as_bool() == true);
        REQUIRE(threshold.iloc(2).as_bool() == true);
    }
}

// ============================================================================
// SHORT INTEREST DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - short_interest timeseries path", "[orchestrator][datasource][short_interest][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Basic operations on short_interest") {
        auto code = R"(
src = short_interest(timeframe="1D")()
si = src.short_interest
scaled = div()(si, 1000000)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateShortInterestData(5, 5000000);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("scaled#result"));

        // 5000000 / 1000000 = 5.0
        auto scaled = aapl_df["scaled#result"];
        REQUIRE(scaled.iloc(0).as_double() == Catch::Approx(5.0));
    }
}

// ============================================================================
// BALANCE SHEET DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - balance_sheet timeseries path", "[orchestrator][datasource][balance_sheet][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Operations on cash and debt") {
        auto code = R"(
src = balance_sheet(period="quarterly", timeframe="1D")()
cash = src.cash
debt = src.lt_debt
net_cash = sub()(cash, debt)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateBalanceSheetData(4, 10000000.0);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("net_cash#result"));

        // baseCash = 10M
        // cash[0] = 10M * 1.0 = 10M
        // debt[0] = 10M * 0.5 * 1.0 = 5M
        // net_cash[0] = 10M - 5M = 5M
        auto net_cash = aapl_df["net_cash#result"];
        REQUIRE(net_cash.iloc(0).as_double() == Catch::Approx(5000000.0));
    }
}

// ============================================================================
// INCOME STATEMENT DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - income_statement timeseries path", "[orchestrator][datasource][income_statement][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Calculate net margin from revenue and net_income") {
        auto code = R"(
src = income_statement(period="quarterly", timeframe="1D")()
revenue = src.revenue
net_income = src.net_income
margin = div()(net_income, revenue)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateIncomeStatementData(4);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("margin#result"));

        // Net margin should be ~0.15 (15%)
        auto margin = aapl_df["margin#result"];
        REQUIRE(margin.iloc(0).as_double() == Catch::Approx(0.15));
    }
}

// ============================================================================
// CASH FLOW DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - cash_flow timeseries path", "[orchestrator][datasource][cash_flow][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Calculate free cash flow (CFO + CapEx)") {
        auto code = R"(
src = cash_flow(period="quarterly", timeframe="1D")()
cfo = src.cfo
capex = src.capex
fcf = add()(cfo, capex)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateCashFlowData(4, 20000000.0);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("fcf#result"));

        // FCF = CFO + CapEx = 20M + (-6M) = 14M (since capex is negative)
        auto fcf = aapl_df["fcf#result"];
        REQUIRE(fcf.iloc(0).as_double() == Catch::Approx(14000000.0));
    }
}

// ============================================================================
// FINANCIAL RATIOS DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - financial_ratios timeseries path", "[orchestrator][datasource][financial_ratios][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("P/E threshold signal") {
        auto code = R"(
src = financial_ratios(timeframe="1D")()
pe = src.price_to_earnings
cheap = lte()(pe, 18)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateFinancialRatiosData(5, 20.0);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("cheap#result"));

        // basePE = 20.0, pe[i] = 20 + (i % 10) - 5
        // pe[0] = 20 + 0 - 5 = 15 -> lte(15, 18) = TRUE
        // pe[1] = 20 + 1 - 5 = 16 -> lte(16, 18) = TRUE
        // pe[2] = 20 + 2 - 5 = 17 -> lte(17, 18) = TRUE
        // pe[3] = 20 + 3 - 5 = 18 -> lte(18, 18) = TRUE
        // pe[4] = 20 + 4 - 5 = 19 -> lte(19, 18) = FALSE
        auto cheap = aapl_df["cheap#result"];
        REQUIRE(cheap.iloc(0).as_bool() == true);   // 15 <= 18
        REQUIRE(cheap.iloc(1).as_bool() == true);   // 16 <= 18
        REQUIRE(cheap.iloc(2).as_bool() == true);   // 17 <= 18
        REQUIRE(cheap.iloc(3).as_bool() == true);   // 18 <= 18
        REQUIRE(cheap.iloc(4).as_bool() == false);  // 19 <= 18
    }
}

// ============================================================================
// ECONOMIC INDICATOR (FRED) DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - economic_indicator timeseries path", "[orchestrator][datasource][fred][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Operations on economic indicator value") {
        auto code = R"(
src = economic_indicator(category="CPI", timeframe="1D")()
val = src.value
high_inflation = gte()(val, 3.5)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        // Category must match what's in the Python code: economic_indicator(category="CPI", ...)
        inputData[dailyTF.ToString()][aapl] = CreateEconomicIndicatorData("CPI", 5, 3.0);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("high_inflation#result"));

        // baseValue = 3.0, values[i] = 3.0 + (i % 6) * 0.1
        // values = [3.0, 3.1, 3.2, 3.3, 3.4]
        // gte(val, 3.5): all should be FALSE since max is 3.4 < 3.5
        auto high_inflation = aapl_df["high_inflation#result"];
        REQUIRE(high_inflation.iloc(0).as_bool() == false);  // 3.0 >= 3.5
        REQUIRE(high_inflation.iloc(1).as_bool() == false);  // 3.1 >= 3.5
        REQUIRE(high_inflation.iloc(2).as_bool() == false);  // 3.2 >= 3.5
        REQUIRE(high_inflation.iloc(3).as_bool() == false);  // 3.3 >= 3.5
        REQUIRE(high_inflation.iloc(4).as_bool() == false);  // 3.4 >= 3.5
    }
}

// ============================================================================
// NEWS DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - news timeseries path", "[orchestrator][datasource][news][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("News with finbert sentiment analysis") {
        // Connect news.title to finbert_sentiment for sentiment analysis
        auto code = R"(
src = news(timeframe="1D")()
title = src.title
sentiment = finbert_sentiment()(title)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateNewsData(3);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        REQUIRE(results[dailyTF.ToString()].contains(aapl));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        // finbert_sentiment outputs: positive, neutral, negative (bool), confidence (double)
        REQUIRE(aapl_df.contains("sentiment#positive"));
        REQUIRE(aapl_df.contains("sentiment#neutral"));
        REQUIRE(aapl_df.contains("sentiment#negative"));
        REQUIRE(aapl_df.contains("sentiment#confidence"));
    }
}

// ============================================================================
// SPLITS DATA SOURCE TESTS
// ============================================================================

TEST_CASE("Orchestrator - splits timeseries path", "[orchestrator][datasource][splits][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Calculate split ratio from split_to / split_from") {
        auto code = R"(
src = splits(timeframe="1D")()
split_from = src.split_from
split_to = src.split_to
ratio = div()(split_to, split_from)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateSplitsData(3);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        REQUIRE(results[dailyTF.ToString()].contains(aapl));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("ratio#result"));

        // split_from = 1.0 for all rows
        // split_to[i] = 2 + i = [2.0, 3.0, 4.0]
        // ratio[i] = split_to / split_from = [2.0, 3.0, 4.0]
        auto ratio = aapl_df["ratio#result"];
        REQUIRE(ratio.iloc(0).as_double() == Catch::Approx(2.0));  // 2:1 split
        REQUIRE(ratio.iloc(1).as_double() == Catch::Approx(3.0));  // 3:1 split
        REQUIRE(ratio.iloc(2).as_double() == Catch::Approx(4.0));  // 4:1 split
    }
}

// ============================================================================
// TABLE REPORT TESTS (Timeseries)
// ============================================================================

TEST_CASE("Orchestrator - table_report timeseries path", "[orchestrator][report][table][timeseries]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Table report with filter on price data") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
signal = gte()(c, 105)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});

        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateOHLCVData({100.0, 103.0, 105.0, 107.0, 110.0});

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        auto aapl_df = results[dailyTF.ToString()][aapl];
        REQUIRE(aapl_df.contains("signal#result"));

        // close = [100, 103, 105, 107, 110]
        // gte(close, 105) = [false, false, true, true, true]
        auto signal = aapl_df["signal#result"];
        REQUIRE(signal.iloc(0).as_bool() == false);  // 100 >= 105
        REQUIRE(signal.iloc(1).as_bool() == false);  // 103 >= 105
        REQUIRE(signal.iloc(2).as_bool() == true);   // 105 >= 105
        REQUIRE(signal.iloc(3).as_bool() == true);   // 107 >= 105
        REQUIRE(signal.iloc(4).as_bool() == true);   // 110 >= 105
    }
}
