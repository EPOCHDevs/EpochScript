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
 * ======================
 *
 * 1. Statistical/Probabilistic Models:
 *    - hmm_N (N=2-5): Hidden Markov Models
 *    - kmeans_N (N=2-5): K-Means Clustering
 *    - dbscan: Density-based clustering
 *    - pca: Principal Component Analysis
 *
 * 2. Supervised ML Models:
 *    - lightgbm_classifier: LightGBM Classification
 *    - lightgbm_regressor: LightGBM Regression
 *    - logistic_l1: L1-regularized Logistic Regression
 *    - logistic_l2: L2-regularized Logistic Regression
 *    - svr_l1: L1-loss Support Vector Regression
 *    - svr_l2: L2-loss Support Vector Regression
 *
 * 3. ML Preprocessing:
 *    - ml_zscore_N (N=2-6): Z-score normalization
 *    - ml_minmax_N (N=2-6): Min-Max scaling
 *    - ml_robust_N (N=2-6): Robust scaling (IQR)
 *
 * 4. Rolling ML Transforms (walk-forward):
 *    - rolling_kmeans_N, rolling_gmm_N, rolling_hmm_N
 *    - rolling_dbscan, rolling_pca_N, rolling_ica
 *    - rolling_lightgbm_classifier/regressor
 *    - rolling_logistic_l1/l2, rolling_svr_l1/l2
 *    - rolling_ml_zscore, rolling_ml_minmax, rolling_ml_robust
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
#include <epoch_data_sdk/events/all.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// ============================================================================
// HELPER FUNCTIONS
// ============================================================================

/**
 * @brief Create OHLCV data with enough rows for rolling ML transforms
 * @param numRows Number of rows (should be > window_size for rolling transforms)
 *
 * Creates a synthetic price series with:
 * - Linear trend component
 * - Sinusoidal cycle component
 * - Noise component
 *
 * This gives ML algorithms some structure to learn from.
 */
inline DataFrame CreateMLTestData(int numRows) {
    std::vector<double> closeValues;
    closeValues.reserve(numRows);
    for (int i = 0; i < numRows; ++i) {
        double trend = 100.0 + i * 0.5;
        double cycle = 10.0 * std::sin(i * 0.1);
        double noise = (i % 7) * 0.3 - 1.0;
        closeValues.push_back(trend + cycle + noise);
    }
    return CreateOHLCVData(closeValues);
}

/**
 * @brief Helper to verify DataFrame contains expected columns
 */
inline void VerifyColumnsExist(const DataFrame& df, const std::vector<std::string>& columns) {
    for (const auto& col : columns) {
        INFO("Checking for column: " << col);
        REQUIRE(df.contains(col));
    }
}

/**
 * @brief Helper to verify output size matches expected
 */
inline void VerifyOutputSize(const DataFrame& df, const std::string& col, int expected_size) {
    REQUIRE(df[col].size() == expected_size);
}

/**
 * @brief Helper to verify non-null values exist in output
 */
inline void VerifyNonNullOutput(const DataFrame& df, const std::string& col) {
    auto non_null = df[col].drop_null();
    INFO("Column " << col << " has " << non_null.size() << " non-null values");
    REQUIRE(non_null.size() > 0);
}

// ============================================================================
// SECTION 1: ROLLING K-MEANS CLUSTERING TESTS
// Variants: rolling_kmeans_2, rolling_kmeans_3, rolling_kmeans_4, rolling_kmeans_5
// ============================================================================

TEST_CASE("ML Integration - Rolling K-Means Clustering", "[orchestrator][ml][clustering][kmeans]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling_kmeans_2 - two clusters basic") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        REQUIRE(results.contains(dailyTF.ToString()));
        REQUIRE(results[dailyTF.ToString()].contains(aapl));

        auto df = results[dailyTF.ToString()][aapl];

        // K=2: cluster_label + cluster_0_dist + cluster_1_dist
        VerifyColumnsExist(df, {"result#cluster_label", "result#cluster_0_dist", "result#cluster_1_dist"});
        VerifyOutputSize(df, "result#cluster_label", NUM_ROWS);
        VerifyNonNullOutput(df, "result#cluster_label");
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // K=3: cluster_label + 3 distance columns
        VerifyColumnsExist(df, {
            "result#cluster_label",
            "result#cluster_0_dist",
            "result#cluster_1_dist",
            "result#cluster_2_dist"
        });
    }

    SECTION("rolling_kmeans_4 - four clusters") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
result = rolling_kmeans_4(window_size=60, min_training_samples=40)(o, h, l, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // K=4: cluster_label + 4 distance columns
        for (int k = 0; k < 4; ++k) {
            REQUIRE(df.contains(fmt::format("result#cluster_{}_dist", k)));
        }
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // K=5: cluster_label + 5 distance columns
        REQUIRE(df.contains("result#cluster_label"));
        for (int k = 0; k < 5; ++k) {
            REQUIRE(df.contains(fmt::format("result#cluster_{}_dist", k)));
        }
    }

    SECTION("rolling_kmeans - with step_size option") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_kmeans_2(window_size=60, min_training_samples=40, step_size=5)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
        VerifyOutputSize(df, "result#cluster_label", NUM_ROWS);
    }
}

// ============================================================================
// SECTION 2: ROLLING DBSCAN CLUSTERING TESTS
// ============================================================================

TEST_CASE("ML Integration - Rolling DBSCAN Clustering", "[orchestrator][ml][clustering][dbscan]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 150;

    SECTION("rolling_dbscan - default parameters") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // DBSCAN outputs: cluster_label, is_anomaly, cluster_count
        VerifyColumnsExist(df, {
            "result#cluster_label",
            "result#is_anomaly",
            "result#cluster_count"
        });
        VerifyOutputSize(df, "result#cluster_label", NUM_ROWS);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#cluster_label", "result#is_anomaly", "result#cluster_count"});
    }

    SECTION("rolling_dbscan - tight epsilon for anomaly detection") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_dbscan(window_size=60, min_training_samples=40, epsilon=0.1, min_points=5)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#is_anomaly"));
    }
}

// ============================================================================
// SECTION 4: ROLLING PCA DECOMPOSITION TESTS
// Variants: rolling_pca_2 through rolling_pca_6
// ============================================================================

TEST_CASE("ML Integration - Rolling PCA Decomposition", "[orchestrator][ml][decomposition][pca]") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // PCA outputs: pc_0, pc_1, explained_variance_ratio
        VerifyColumnsExist(df, {"result#pc_0", "result#pc_1", "result#explained_variance_ratio"});
        VerifyOutputSize(df, "result#pc_0", NUM_ROWS);
    }

    SECTION("rolling_pca_3 - three components") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#pc_0", "result#pc_1", "result#pc_2", "result#explained_variance_ratio"});
    }

    SECTION("rolling_pca_4 - four components") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
result = rolling_pca_4(window_size=60, min_training_samples=40)(o, h, l, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        for (int k = 0; k < 4; ++k) {
            REQUIRE(df.contains(fmt::format("result#pc_{}", k)));
        }
    }

    SECTION("rolling_pca_5 - five components") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
v = src.v
result = rolling_pca_5(window_size=60, min_training_samples=40)(o, h, l, c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        for (int k = 0; k < 5; ++k) {
            REQUIRE(df.contains(fmt::format("result#pc_{}", k)));
        }
    }

    SECTION("rolling_pca_6 - six components with scale_data") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
v = src.v
result = rolling_pca_6(window_size=60, min_training_samples=40, scale_data=True)(o, h, l, c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // With 5 inputs, max 5 components (even though pca_6 requests 6)
        for (int k = 0; k < 5; ++k) {
            REQUIRE(df.contains(fmt::format("result#pc_{}", k)));
        }
        REQUIRE(df.contains("result#explained_variance_ratio"));
    }
}

// ============================================================================
// SECTION 5: ROLLING HMM TESTS
// Variants: rolling_hmm_2, rolling_hmm_3, rolling_hmm_4, rolling_hmm_5
// ============================================================================

TEST_CASE("ML Integration - Rolling HMM", "[orchestrator][ml][hmm]") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // HMM outputs: state + N state probability columns
        VerifyColumnsExist(df, {"result#state", "result#state_0_prob", "result#state_1_prob"});
        VerifyOutputSize(df, "result#state", NUM_ROWS);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {
            "result#state",
            "result#state_0_prob",
            "result#state_1_prob",
            "result#state_2_prob"
        });
    }

    SECTION("rolling_hmm_4 - four states") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_hmm_4(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#state"));
        for (int s = 0; s < 4; ++s) {
            REQUIRE(df.contains(fmt::format("result#state_{}_prob", s)));
        }
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        for (int s = 0; s < 5; ++s) {
            REQUIRE(df.contains(fmt::format("result#state_{}_prob", s)));
        }
    }
}

// ============================================================================
// SECTION 7: ROLLING LIGHTGBM TESTS (CLASSIFIER AND REGRESSOR)
// ============================================================================

TEST_CASE("ML Integration - Rolling LightGBM Classifier", "[orchestrator][ml][supervised][lightgbm][classifier]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;

    SECTION("rolling_lightgbm_classifier - binary classification basic") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
label = gte()(c, o)
result = rolling_lightgbm_classifier(window_size=60, min_training_samples=40, num_estimators=10)(o, h, l, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // LightGBM classifier outputs: prediction, probability
        VerifyColumnsExist(df, {"result#prediction", "result#probability"});
        VerifyOutputSize(df, "result#prediction", NUM_ROWS);
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
)(o, h, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
        REQUIRE(df.contains("result#probability"));
    }

    SECTION("rolling_lightgbm_classifier - with regularization") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
label = gte()(c, o)
result = rolling_lightgbm_classifier(
    window_size=60,
    min_training_samples=40,
    num_estimators=15,
    lambda_l1=0.1,
    lambda_l2=0.1
)(o, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
    }
}

TEST_CASE("ML Integration - Rolling LightGBM Regressor", "[orchestrator][ml][supervised][lightgbm][regressor]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;

    SECTION("rolling_lightgbm_regressor - basic regression") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
target = sub()(h, l)
result = rolling_lightgbm_regressor(window_size=60, min_training_samples=40, num_estimators=10)(o, c, target=target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // LightGBM regressor output: prediction
        REQUIRE(df.contains("result#prediction"));
        VerifyOutputSize(df, "result#prediction", NUM_ROWS);
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
)(o, target=target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
    }

    SECTION("rolling_lightgbm_regressor - with custom options") {
        // Test with num_leaves option (integer) instead of max_depth (select)
        // to avoid YAML parser issues with numeric string values
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
target = sub()(h, o)
result = rolling_lightgbm_regressor(
    window_size=60,
    min_training_samples=40,
    num_estimators=10,
    num_leaves=20
)(o, h, target=target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
    }
}

// ============================================================================
// SECTION 8: ROLLING LOGISTIC REGRESSION TESTS (L1 AND L2)
// ============================================================================

TEST_CASE("ML Integration - Rolling Logistic Regression", "[orchestrator][ml][supervised][logistic]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;

    SECTION("rolling_logistic_l1 - L1-regularized") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
c = src.c
label = gte()(c, o)
result = rolling_logistic_l1(window_size=60, min_training_samples=40, C=1.0)(o, h, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // Logistic outputs: prediction, probability, decision_value
        VerifyColumnsExist(df, {"result#prediction", "result#probability", "result#decision_value"});
        VerifyOutputSize(df, "result#prediction", NUM_ROWS);
    }

    SECTION("rolling_logistic_l2 - L2-regularized") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
c = src.c
label = gte()(c, o)
result = rolling_logistic_l2(window_size=60, min_training_samples=40, C=0.5)(o, h, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#prediction", "result#probability", "result#decision_value"});
    }

    SECTION("rolling_logistic_l1 - strong regularization") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
label = gte()(c, o)
result = rolling_logistic_l1(window_size=60, min_training_samples=40, C=0.1)(o, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
    }
}

// ============================================================================
// SECTION 9: ROLLING SVR TESTS (L1 AND L2)
// ============================================================================

TEST_CASE("ML Integration - Rolling SVR", "[orchestrator][ml][supervised][svr]") {
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
result = rolling_svr_l1(window_size=60, min_training_samples=40, C=1.0)(o, h, target=target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // SVR output: prediction
        REQUIRE(df.contains("result#prediction"));
        VerifyOutputSize(df, "result#prediction", NUM_ROWS);
    }

    SECTION("rolling_svr_l2 - L2-loss SVR") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
target = sub()(h, l)
result = rolling_svr_l2(window_size=60, min_training_samples=40, C=0.5, epsilon=0.001)(o, h, target=target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
    }

    SECTION("rolling_svr_l1 - strong regularization") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
target = sub()(c, o)
result = rolling_svr_l1(window_size=60, min_training_samples=40, C=0.1)(o, target=target)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
    }
}

// ============================================================================
// SECTION 10: STATIC ML PREPROCESSING TESTS
// Variants: ml_zscore_N, ml_minmax_N, ml_robust_N (N=2-6)
// ============================================================================

TEST_CASE("ML Integration - Static Preprocessing", "[orchestrator][ml][preprocessing][static]") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#scaled_0", "result#scaled_1"});
        VerifyOutputSize(df, "result#scaled_0", NUM_ROWS);
    }

    SECTION("ml_zscore_3 - three features") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
result = ml_zscore_3(split_ratio=0.7)(o, h, l)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#scaled_0", "result#scaled_1", "result#scaled_2"});
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#scaled_0", "result#scaled_1", "result#scaled_2"});
    }

    SECTION("ml_minmax_4 - four features") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
result = ml_minmax_4(split_ratio=0.7)(o, h, l, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        for (int i = 0; i < 4; ++i) {
            REQUIRE(df.contains(fmt::format("result#scaled_{}", i)));
        }
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#scaled_0", "result#scaled_1", "result#scaled_2", "result#scaled_3"});
    }

    SECTION("ml_robust_5 - five features") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
v = src.v
result = ml_robust_5(split_ratio=0.7)(o, h, l, c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        for (int i = 0; i < 5; ++i) {
            REQUIRE(df.contains(fmt::format("result#scaled_{}", i)));
        }
    }
}

// ============================================================================
// SECTION 10B: STATIC SUPERVISED ML TESTS
// ============================================================================

TEST_CASE("ML Integration - Static LightGBM", "[orchestrator][ml][supervised][static][lightgbm]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 100;

    SECTION("lightgbm_regressor - basic regression") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
tgt = sub()(h, o)
result = lightgbm_regressor(split_ratio=0.7, num_estimators=10)(o, target=tgt)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
        VerifyOutputSize(df, "result#prediction", NUM_ROWS);
    }
}

TEST_CASE("ML Integration - Static Linear Models", "[orchestrator][ml][supervised][static][linear]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 100;

    SECTION("logistic_l1 - L1 regularized classification") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
label = gte()(c, o)
result = logistic_l1(split_ratio=0.7, C=1.0, min_training_samples=50)(o, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
        REQUIRE(df.contains("result#probability"));
    }

    SECTION("logistic_l2 - L2 regularized classification") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
label = gte()(c, o)
result = logistic_l2(split_ratio=0.7, C=0.5, min_training_samples=50)(o, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#prediction"));
        REQUIRE(df.contains("result#probability"));
    }

    SECTION("svr_l1 - L1 support vector regression") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
tgt = sub()(h, o)
result = svr_l1(split_ratio=0.7, C=1.0, min_training_samples=50)(o, target=tgt)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#result"));
        VerifyOutputSize(df, "result#result", NUM_ROWS);
    }

    SECTION("svr_l2 - L2 support vector regression") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
tgt = sub()(h, l)
result = svr_l2(split_ratio=0.7, C=0.5, eps=0.01, min_training_samples=50)(o, target=tgt)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#result"));
        VerifyOutputSize(df, "result#result", NUM_ROWS);
    }
}

// ============================================================================
// SECTION 11: ROLLING ML PREPROCESSING TESTS
// ============================================================================

TEST_CASE("ML Integration - Rolling Preprocessing", "[orchestrator][ml][preprocessing][rolling]") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#scaled_0", "result#scaled_1"});
        VerifyOutputSize(df, "result#scaled_0", NUM_ROWS);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#scaled_0", "result#scaled_1", "result#scaled_2"});
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"result#scaled_0", "result#scaled_1"});
    }

    SECTION("rolling_ml_zscore - with step_size") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c
result = rolling_ml_zscore(window_size=60, min_training_samples=40, step_size=5)(o, c)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#scaled_0"));
    }
}

// ============================================================================
// SECTION 12: WINDOW TYPE OPTIONS TESTS
// ============================================================================

TEST_CASE("ML Integration - Window Type Options", "[orchestrator][ml][options][window_type]") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
        VerifyOutputSize(df, "result#cluster_label", NUM_ROWS);
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
    }

    SECTION("step_size option affects retraining frequency") {
        // Use rolling_kmeans_2 to test step_size (GMM was removed)
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_kmeans_2(window_size=50, min_training_samples=40, step_size=10)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
    }
}

// ============================================================================
// SECTION 13: ML PIPELINE CHAINING TESTS
// ============================================================================

TEST_CASE("ML Integration - Pipeline Chaining", "[orchestrator][ml][pipeline]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;
    const int NUM_ROWS = 200;

    SECTION("Preprocess -> Cluster") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
c = src.c

scaled = ml_zscore_2(split_ratio=0.7)(o, c)
clusters = rolling_kmeans_3(window_size=60, min_training_samples=40)(scaled.scaled_0, scaled.scaled_1)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // Verify preprocessing outputs
        VerifyColumnsExist(df, {"scaled#scaled_0", "scaled#scaled_1"});
        // Verify clustering outputs
        REQUIRE(df.contains("clusters#cluster_label"));
    }

    SECTION("PCA -> HMM regime detection") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
v = src.v

pca = rolling_pca_3(window_size=60, min_training_samples=40)(o, h, l, c, v)
regimes = rolling_hmm_2(window_size=60, min_training_samples=40)(pca.pc_0, pca.pc_1)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // Verify PCA outputs
        VerifyColumnsExist(df, {"pca#pc_0", "pca#pc_1", "pca#pc_2"});
        // Verify HMM outputs
        VerifyColumnsExist(df, {"regimes#state", "regimes#state_0_prob"});
    }

    SECTION("Preprocessing -> Supervised ML") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
c = src.c

scaled = ml_zscore_2(split_ratio=0.7)(o, h)
label = gte()(c, o)
classifier = rolling_logistic_l2(window_size=60, min_training_samples=40, C=1.0)(scaled.scaled_0, scaled.scaled_1, target=label)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"scaled#scaled_0", "scaled#scaled_1"});
        VerifyColumnsExist(df, {"classifier#prediction", "classifier#probability"});
    }

    SECTION("HMM -> Use state as feature") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v

hmm_states = rolling_hmm_2(window_size=60, min_training_samples=40)(c, v)
clusters = rolling_kmeans_2(window_size=60, min_training_samples=40)(c, hmm_states.state_0_prob)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        VerifyColumnsExist(df, {"hmm_states#state", "hmm_states#state_0_prob"});
        REQUIRE(df.contains("clusters#cluster_label"));
    }
}

// ============================================================================
// SECTION 14: MULTI-ASSET TESTS
// ============================================================================

TEST_CASE("ML Integration - Multi-Asset Execution", "[orchestrator][ml][multi-asset]") {
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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        // Verify both assets have outputs
        REQUIRE(results[dailyTF.ToString()].contains(aapl));
        REQUIRE(results[dailyTF.ToString()].contains(msft));

        auto aapl_df = results[dailyTF.ToString()][aapl];
        auto msft_df = results[dailyTF.ToString()][msft];

        REQUIRE(aapl_df.contains("clusters#cluster_label"));
        REQUIRE(msft_df.contains("clusters#cluster_label"));
    }

    SECTION("Multiple ML transforms on multiple assets") {
        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v

pca = rolling_pca_2(window_size=60, min_training_samples=40)(c, v)
hmm = rolling_hmm_2(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl, msft}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);
        inputData[dailyTF.ToString()][msft] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);

        auto aapl_df = results[dailyTF.ToString()][aapl];
        auto msft_df = results[dailyTF.ToString()][msft];

        // Both assets should have both PCA and HMM outputs
        REQUIRE(aapl_df.contains("pca#pc_0"));
        REQUIRE(aapl_df.contains("hmm#state"));
        REQUIRE(msft_df.contains("pca#pc_0"));
        REQUIRE(msft_df.contains("hmm#state"));
    }
}

// ============================================================================
// SECTION 15: EDGE CASE TESTS
// ============================================================================

TEST_CASE("ML Integration - Edge Cases", "[orchestrator][ml][edge_cases]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string aapl = TestAssetConstants::AAPL;

    SECTION("Minimum data for window size") {
        // Test with exactly enough data for window_size + some prediction
        const int NUM_ROWS = 70;  // window_size=60, min_training_samples=40

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

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
        VerifyOutputSize(df, "result#cluster_label", NUM_ROWS);
    }

    SECTION("Two correlated features ML transform") {
        const int NUM_ROWS = 150;

        auto code = R"(
src = market_data_source(timeframe="1D")()
c = src.c
v = src.v
result = rolling_pca_2(window_size=60, min_training_samples=40)(c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        // With 2 inputs, 2 PCs possible
        REQUIRE(df.contains("result#pc_0"));
        REQUIRE(df.contains("result#pc_1"));
    }

    SECTION("Many features ML transform") {
        const int NUM_ROWS = 150;

        auto code = R"(
src = market_data_source(timeframe="1D")()
o = src.o
h = src.h
l = src.l
c = src.c
v = src.v
result = rolling_kmeans_2(window_size=60, min_training_samples=40)(o, h, l, c, v)
)";
        auto manager = CreateTransformManager(strategy::PythonSource{code, true});
        DataFlowRuntimeOrchestrator orch({aapl}, std::move(manager));

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][aapl] = CreateMLTestData(NUM_ROWS);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto results = orch.ExecutePipeline(std::move(inputData), emitter);
        auto df = results[dailyTF.ToString()][aapl];

        REQUIRE(df.contains("result#cluster_label"));
    }
}
