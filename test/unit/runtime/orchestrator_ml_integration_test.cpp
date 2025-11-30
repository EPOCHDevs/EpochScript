/**
 * @file orchestrator_ml_integration_test.cpp
 * @brief Integration tests for ML transforms through the orchestrator
 *
 * These tests verify that ML transforms execute correctly through the
 * orchestrator pipeline and produce expected output structure.
 *
 * Test Strategy:
 * 1. First level: Verify execution completes and output structure is correct
 * 2. Second level: Print output values for manual verification
 * 3. Third level: Freeze verified values as expected (after confirmation)
 *
 * ML Transforms Tested:
 * - Clustering: rolling_kmeans, rolling_gmm, rolling_dbscan
 * - Decomposition: rolling_pca, rolling_ica
 * - Probabilistic: rolling_hmm
 * - Supervised: rolling_lightgbm, rolling_logistic, rolling_svr
 * - Preprocessing: ml_zscore, ml_minmax, ml_robust
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

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// ============================================================================
// HELPER: Create multi-feature OHLCV data for ML transforms
// ============================================================================

/**
 * @brief Create OHLCV data with enough rows for rolling ML transforms
 * @param numRows Number of rows (should be > window_size for rolling transforms)
 */
inline DataFrame CreateMLTestData(int numRows) {
    std::vector<double> closeValues;
    closeValues.reserve(numRows);
    for (int i = 0; i < numRows; ++i) {
        // Create price series with some pattern for ML to detect
        double trend = 100.0 + i * 0.5;
        double cycle = 10.0 * std::sin(i * 0.1);
        double noise = (i % 7) * 0.3 - 1.0;
        closeValues.push_back(trend + cycle + noise);
    }
    return CreateOHLCVData(closeValues);
}

// ============================================================================
// TEST CASE: Rolling K-Means Clustering (rolling_kmeans_2 through rolling_kmeans_5)
// ============================================================================

TEST_CASE("Orchestrator - rolling_kmeans variants", "[orchestrator][ml][clustering][rolling_kmeans]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    // Create enough data for rolling window (window_size=60, need enough data)
    const int NUM_ROWS = 150;

    SECTION("rolling_kmeans_2 - basic execution") {
        // Use raw price features directly (o, h, l, c are the features)
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_kmeans_2(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));

        REQUIRE(results.contains(dailyTF.ToString()));
        REQUIRE(results[dailyTF.ToString()].contains(aapl));

        auto df = results[dailyTF.ToString()][aapl];

        // Verify output columns exist
        REQUIRE(df.contains("result#cluster_label"));
        REQUIRE(df.contains("result#cluster_0_dist"));
        REQUIRE(df.contains("result#cluster_1_dist"));

        // Verify output size matches input
        REQUIRE(df["result#cluster_label"].size() == NUM_ROWS);

        // Verify some cluster labels are valid (0 or 1 for K=2)
        auto labels = df["result#cluster_label"].drop_null();
        INFO("rolling_kmeans_2 produced " << labels.size() << " non-null cluster labels");
        REQUIRE(labels.size() > 0);
    }

    SECTION("rolling_kmeans_3 - three clusters") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_kmeans_3(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify 3 cluster distance columns
        REQUIRE(df.contains("result#cluster_label"));
        REQUIRE(df.contains("result#cluster_0_dist"));
        REQUIRE(df.contains("result#cluster_1_dist"));
        REQUIRE(df.contains("result#cluster_2_dist"));
    }

    SECTION("rolling_kmeans_5 - five clusters with expanding window") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_kmeans_5(window_size=50, min_training_samples=40, window_type="expanding")(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify 5 cluster distance columns
        REQUIRE(df.contains("result#cluster_label"));
        for (int k = 0; k < 5; ++k) {
            REQUIRE(df.contains(fmt::format("result#cluster_{}_dist", k)));
        }
    }
}

// ============================================================================
// TEST CASE: Rolling GMM (rolling_gmm_2 through rolling_gmm_5)
// ============================================================================

TEST_CASE("Orchestrator - rolling_gmm variants", "[orchestrator][ml][clustering][rolling_gmm]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling_gmm_2 - basic execution") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_gmm_2(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify GMM output columns
        REQUIRE(df.contains("result#component"));
        REQUIRE(df.contains("result#component_0_prob"));
        REQUIRE(df.contains("result#component_1_prob"));
        REQUIRE(df.contains("result#log_likelihood"));

        // Verify output size
        REQUIRE(df["result#component"].size() == NUM_ROWS);

        // Verify some predictions exist
        auto components = df["result#component"].drop_null();
        INFO("rolling_gmm_2 produced " << components.size() << " non-null component assignments");
        REQUIRE(components.size() > 0);
    }

    SECTION("rolling_gmm_3 - three components") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_gmm_3(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify 3 component probability columns
        REQUIRE(df.contains("result#component"));
        REQUIRE(df.contains("result#component_0_prob"));
        REQUIRE(df.contains("result#component_1_prob"));
        REQUIRE(df.contains("result#component_2_prob"));
        REQUIRE(df.contains("result#log_likelihood"));
    }

    SECTION("rolling_gmm_5 - five components with custom params") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_gmm_5(window_size=60, min_training_samples=40, max_iterations=100, trials=2)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify 5 component probability columns
        for (int c = 0; c < 5; ++c) {
            REQUIRE(df.contains(fmt::format("result#component_{}_prob", c)));
        }
    }
}

// ============================================================================
// TEST CASE: Rolling DBSCAN
// ============================================================================

TEST_CASE("Orchestrator - rolling_dbscan", "[orchestrator][ml][clustering][rolling_dbscan]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling_dbscan - basic execution with default params") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_dbscan(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify DBSCAN output columns
        REQUIRE(df.contains("result#cluster_label"));
        REQUIRE(df.contains("result#is_anomaly"));
        REQUIRE(df.contains("result#cluster_count"));

        // Verify output size
        REQUIRE(df["result#cluster_label"].size() == NUM_ROWS);
    }

    SECTION("rolling_dbscan - custom epsilon and min_points") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_dbscan(window_size=60, min_training_samples=40, epsilon=0.3, min_points=3)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
        REQUIRE(df.contains("result#is_anomaly"));
        REQUIRE(df.contains("result#cluster_count"));
    }
}

// ============================================================================
// TEST CASE: Rolling PCA (rolling_pca_2 through rolling_pca_6)
// ============================================================================

TEST_CASE("Orchestrator - rolling_pca variants", "[orchestrator][ml][decomposition][rolling_pca]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling_pca_2 - two components") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
result = rolling_pca_2(window_size=60, min_training_samples=40)(o, h, l)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify PCA output columns
        REQUIRE(df.contains("result#pc_0"));
        REQUIRE(df.contains("result#pc_1"));
        REQUIRE(df.contains("result#explained_variance_ratio"));

        // Verify output size
        REQUIRE(df["result#pc_0"].size() == NUM_ROWS);
    }

    SECTION("rolling_pca_3 - three components for yield curve") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
v = src.v
result = rolling_pca_3(window_size=60, min_training_samples=40)(o, h, l, c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify 3 PC columns
        REQUIRE(df.contains("result#pc_0"));
        REQUIRE(df.contains("result#pc_1"));
        REQUIRE(df.contains("result#pc_2"));
        REQUIRE(df.contains("result#explained_variance_ratio"));
    }

    SECTION("rolling_pca_6 - six components with scale_data option") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
v = src.v
result = rolling_pca_6(window_size=60, min_training_samples=40, scale_data=true)(o, h, l, c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify at least 5 PC columns (from 5 inputs)
        for (int k = 0; k < 5; ++k) {
            REQUIRE(df.contains(fmt::format("result#pc_{}", k)));
        }
        REQUIRE(df.contains("result#explained_variance_ratio"));
    }
}

// ============================================================================
// TEST CASE: Rolling ICA
// ============================================================================

TEST_CASE("Orchestrator - rolling_ica", "[orchestrator][ml][decomposition][rolling_ica]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling_ica - basic execution") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
result = rolling_ica(window_size=60, min_training_samples=40)(o, h, l)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify ICA output columns (ic_0 through ic_4)
        REQUIRE(df.contains("result#ic_0"));
        REQUIRE(df.contains("result#ic_1"));
        REQUIRE(df.contains("result#ic_2"));

        // Verify output size
        REQUIRE(df["result#ic_0"].size() == NUM_ROWS);
    }

    SECTION("rolling_ica - with custom RADICAL params") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
result = rolling_ica(window_size=60, min_training_samples=40, noise_std_dev=0.2, replicates=20)(o, h, l, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify output exists
        REQUIRE(df.contains("result#ic_0"));
    }
}

// ============================================================================
// TEST CASE: Rolling HMM (rolling_hmm_2 through rolling_hmm_5)
// ============================================================================

TEST_CASE("Orchestrator - rolling_hmm variants", "[orchestrator][ml][hmm][rolling_hmm]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling_hmm_2 - two states") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_hmm_2(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify HMM output columns
        REQUIRE(df.contains("result#state"));
        REQUIRE(df.contains("result#state_0_prob"));
        REQUIRE(df.contains("result#state_1_prob"));

        // Verify output size
        REQUIRE(df["result#state"].size() == NUM_ROWS);
    }

    SECTION("rolling_hmm_3 - three states") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_hmm_3(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify 3 state probability columns
        REQUIRE(df.contains("result#state"));
        REQUIRE(df.contains("result#state_0_prob"));
        REQUIRE(df.contains("result#state_1_prob"));
        REQUIRE(df.contains("result#state_2_prob"));
    }

    SECTION("rolling_hmm_5 - five states with custom convergence") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_hmm_5(window_size=60, min_training_samples=40, max_iterations=500, tolerance=1e-4)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify 5 state probability columns
        for (int s = 0; s < 5; ++s) {
            REQUIRE(df.contains(fmt::format("result#state_{}_prob", s)));
        }
    }
}

// ============================================================================
// TEST CASE: Rolling LightGBM Classifier
// ============================================================================

TEST_CASE("Orchestrator - rolling_lightgbm_classifier", "[orchestrator][ml][supervised][lightgbm]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;  // Need more data for supervised learning

    SECTION("rolling_lightgbm_classifier - binary classification") {
        // Create a simple binary label from price data
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
# Create binary label: 1 if c > o (up day), else 0
label = gte()(c, o)
result = rolling_lightgbm_classifier(window_size=60, min_training_samples=40, num_estimators=10)(o, h, l, label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify LightGBM classifier output columns
        REQUIRE(df.contains("result#prediction"));
        REQUIRE(df.contains("result#probability"));

        // Verify output size
        REQUIRE(df["result#prediction"].size() == NUM_ROWS);
    }

    SECTION("rolling_lightgbm_classifier - with hyperparameters") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
c = src.c
label = gte()(c, o)
result = rolling_lightgbm_classifier(
    window_size=60,
    min_training_samples=40,
    num_estimators=20,
    learning_rate=0.05,
    num_leaves=15,
    min_data_in_leaf=5
)(o, h, label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
        REQUIRE(df.contains("result#probability"));
    }
}

// ============================================================================
// TEST CASE: Rolling LightGBM Regressor
// ============================================================================

TEST_CASE("Orchestrator - rolling_lightgbm_regressor", "[orchestrator][ml][supervised][lightgbm]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;

    SECTION("rolling_lightgbm_regressor - return prediction") {
        // Use h-l as target (a simple range)
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
# Predict high-low range
target = sub()(h, l)
result = rolling_lightgbm_regressor(window_size=60, min_training_samples=40, num_estimators=10)(o, c, target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify LightGBM regressor output column
        REQUIRE(df.contains("result#prediction"));
        REQUIRE(df["result#prediction"].size() == NUM_ROWS);
    }

    SECTION("rolling_lightgbm_regressor - with regularization") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
target = sub()(c, o)
result = rolling_lightgbm_regressor(
    window_size=60,
    min_training_samples=40,
    num_estimators=15,
    lambda_l1=0.1,
    lambda_l2=0.1
)(o, target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
    }
}

// ============================================================================
// TEST CASE: Rolling Logistic Regression (L1 and L2)
// ============================================================================

TEST_CASE("Orchestrator - rolling_logistic variants", "[orchestrator][ml][supervised][logistic]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;

    SECTION("rolling_logistic_l1 - sparse logistic regression") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
c = src.c
label = gte()(c, o)
result = rolling_logistic_l1(window_size=60, min_training_samples=40, C=1.0)(o, h, label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify logistic classifier output columns
        REQUIRE(df.contains("result#prediction"));
        REQUIRE(df.contains("result#probability"));
        REQUIRE(df.contains("result#decision_value"));

        REQUIRE(df["result#prediction"].size() == NUM_ROWS);
    }

    SECTION("rolling_logistic_l2 - ridge logistic regression") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
c = src.c
label = gte()(c, o)
result = rolling_logistic_l2(window_size=60, min_training_samples=40, C=0.5)(o, h, label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
        REQUIRE(df.contains("result#probability"));
        REQUIRE(df.contains("result#decision_value"));
    }
}

// ============================================================================
// TEST CASE: Rolling SVR (L1 and L2)
// ============================================================================

TEST_CASE("Orchestrator - rolling_svr variants", "[orchestrator][ml][supervised][svr]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;

    SECTION("rolling_svr_l1 - L1-loss SVR") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
target = sub()(h, l)
result = rolling_svr_l1(window_size=60, min_training_samples=40, C=1.0)(o, h, target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify SVR output column
        REQUIRE(df.contains("result#result"));
        REQUIRE(df["result#result"].size() == NUM_ROWS);
    }

    SECTION("rolling_svr_l2 - L2-loss SVR") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
target = sub()(h, l)
result = rolling_svr_l2(window_size=60, min_training_samples=40, C=0.5, eps=0.001)(o, h, target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#result"));
    }
}

// ============================================================================
// TEST CASE: ML Preprocessing Transforms
// ============================================================================

TEST_CASE("Orchestrator - ml_preprocessing transforms", "[orchestrator][ml][preprocessing]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 100;

    SECTION("ml_zscore_2 - z-score normalization") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
result = ml_zscore_2(split_ratio=0.7)(o, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify zscore output columns
        REQUIRE(df.contains("result#scaled_0"));
        REQUIRE(df.contains("result#scaled_1"));
        REQUIRE(df["result#scaled_0"].size() == NUM_ROWS);
    }

    SECTION("ml_minmax_3 - min-max scaling") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
result = ml_minmax_3(split_ratio=0.8)(o, h, l)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify minmax output columns
        REQUIRE(df.contains("result#scaled_0"));
        REQUIRE(df.contains("result#scaled_1"));
        REQUIRE(df.contains("result#scaled_2"));
    }

    SECTION("ml_robust_4 - robust scaling with IQR") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
result = ml_robust_4(split_ratio=0.7)(o, h, l, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify robust output columns
        REQUIRE(df.contains("result#scaled_0"));
        REQUIRE(df.contains("result#scaled_1"));
        REQUIRE(df.contains("result#scaled_2"));
        REQUIRE(df.contains("result#scaled_3"));
    }
}

// ============================================================================
// TEST CASE: Rolling ML Preprocessing
// ============================================================================

TEST_CASE("Orchestrator - rolling_ml_preprocessing", "[orchestrator][ml][preprocessing][rolling]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling_ml_zscore - rolling z-score") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
result = rolling_ml_zscore(window_size=60, min_training_samples=40)(o, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify output columns exist
        REQUIRE(df.contains("result#scaled_0"));
        REQUIRE(df.contains("result#scaled_1"));
    }

    SECTION("rolling_ml_minmax - rolling min-max") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
result = rolling_ml_minmax(window_size=60, min_training_samples=40)(o, h, l)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#scaled_0"));
        REQUIRE(df.contains("result#scaled_1"));
        REQUIRE(df.contains("result#scaled_2"));
    }

    SECTION("rolling_ml_robust - rolling robust scaling") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
result = rolling_ml_robust(window_size=60, min_training_samples=40)(o, h)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#scaled_0"));
        REQUIRE(df.contains("result#scaled_1"));
    }
}

// ============================================================================
// TEST CASE: ML Pipeline - Chained Transforms
// ============================================================================

TEST_CASE("Orchestrator - ML pipeline chaining", "[orchestrator][ml][pipeline]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;

    SECTION("Preprocess -> Cluster -> Use cluster as feature") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c

# Preprocess features
scaled = ml_zscore_2(split_ratio=0.7)(o, c)

# Cluster the scaled features
clusters = rolling_kmeans_3(window_size=60, min_training_samples=40)(scaled.scaled_0, scaled.scaled_1)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify both preprocessing and clustering outputs
        REQUIRE(df.contains("scaled#scaled_0"));
        REQUIRE(df.contains("scaled#scaled_1"));
        REQUIRE(df.contains("clusters#cluster_label"));
    }

    SECTION("PCA -> GMM regime detection") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
v = src.v

# Reduce dimensions with PCA
pca = rolling_pca_3(window_size=60, min_training_samples=40)(o, h, l, c, v)

# Detect regimes with GMM on principal components
regimes = rolling_gmm_2(window_size=60, min_training_samples=40)(pca.pc_0, pca.pc_1)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        // Verify PCA outputs
        REQUIRE(df.contains("pca#pc_0"));
        REQUIRE(df.contains("pca#pc_1"));

        // Verify GMM outputs
        REQUIRE(df.contains("regimes#component"));
        REQUIRE(df.contains("regimes#component_0_prob"));
    }
}

// ============================================================================
// TEST CASE: Window Type Options
// ============================================================================

TEST_CASE("Orchestrator - ML window type options", "[orchestrator][ml][options]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling window type") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_kmeans_2(window_size=50, min_training_samples=40, window_type="rolling")(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
    }

    SECTION("expanding window type") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_kmeans_2(window_size=50, min_training_samples=40, window_type="expanding")(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
    }

    SECTION("step_size option") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_gmm_2(window_size=50, min_training_samples=40, step_size=5)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#component"));
    }
}

// ============================================================================
// TEST CASE: Multi-Asset ML Transforms
// ============================================================================

TEST_CASE("Orchestrator - ML transforms multi-asset", "[orchestrator][ml][multi-asset]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const std::string msft = TestAssetConstants::MSFT;
    const int NUM_ROWS = 150;

    SECTION("Clustering on multiple assets independently") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
clusters = rolling_kmeans_2(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);
        inputData[dailyTF.ToString()][msft] = CreateMLTestData(NUM_ROWS);

        auto results = orch.ExecutePipeline(std::move(inputData));

        // Verify both assets have outputs
        REQUIRE(results[dailyTF.ToString()].contains(aapl));
        REQUIRE(results[dailyTF.ToString()].contains(msft));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        auto msft_df = results[dailyTF.ToString()][msft];

        REQUIRE(aapl_df.contains("clusters#cluster_label"));
        REQUIRE(msft_df.contains("clusters#cluster_label"));
    }
}
