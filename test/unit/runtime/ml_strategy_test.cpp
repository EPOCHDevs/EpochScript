/**
 * @file ml_strategy_test.cpp
 * @brief Practical ML strategy integration tests
 *
 * Tests the full ML pipeline with real trading strategies:
 * - Rolling PCA for factor extraction and dimensionality reduction
 * - Rolling K-Means/DBSCAN for regime detection
 * - Rolling LightGBM for supervised predictions
 * - Feature engineering with preprocessing
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
#include <spdlog/spdlog.h>

using namespace epoch_script::runtime;
using namespace epoch_script::runtime::test;
using namespace epoch_script;
using epoch_script::runtime::test::CompareOrGenerateBaseline;

namespace {

// Helper to compile source and build TransformManager
std::unique_ptr<TransformManager> CompileSource(const std::string& sourceCode) {
    auto pythonSource = epoch_script::strategy::PythonSource(sourceCode, true);
    return std::make_unique<TransformManager>(pythonSource);
}

} // anonymous namespace

// =============================================================================
// ROLLING PCA STRATEGIES
// =============================================================================

TEST_CASE("ML Strategy - PCA Factor Extraction", "[ml_strategy][pca][factors]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling PCA extracts principal components from OHLCV") {
        // Use rolling PCA to extract factors from price data
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Calculate returns for each price column
ret_o = src.o / src.o[1] - 1
ret_h = src.h / src.h[1] - 1
ret_l = src.l / src.l[1] - 1
ret_c = src.c / src.c[1] - 1

# Rolling PCA on returns - extract 2 principal components
pca = rolling_pca_2(window_size=100, min_training_samples=50, step_size=1)(ret_o, ret_h, ret_l, ret_c)

# First component often captures market direction
pc1 = pca.pc_0
# Second component often captures volatility/dispersion
pc2 = pca.pc_1
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        // Create price data
        std::vector<double> prices;
        for (int i = 0; i < 150; i++) {
            prices.push_back(100.0 + i * 0.3 + std::sin(i * 0.15) * 5.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== PCA Factor Extraction ===\n{}", df.repr());

        // Verify PCA outputs exist
        REQUIRE(df.contains("pca#pc_0"));
        REQUIRE(df.contains("pca#pc_1"));
        REQUIRE(df.contains("pc1#result"));
        REQUIRE(df.contains("pc2#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "ml_strategy/pca_factors",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"pca#pc_0", "pca#pc_1"},
            0.1, 0.01, 100  // higher tolerance for ML components
        );
    }

    SECTION("PCA with z-score for trading signals") {
        // Use PCA component z-scores for mean-reversion signals
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Returns
ret_o = src.o / src.o[1] - 1
ret_h = src.h / src.h[1] - 1
ret_l = src.l / src.l[1] - 1
ret_c = src.c / src.c[1] - 1

# Rolling PCA
pca = rolling_pca_2(window_size=100, min_training_samples=50)(ret_o, ret_h, ret_l, ret_c)

# Z-score of first principal component
pc1 = pca.pc_0
z_pc1 = zscore(window=20)(pc1)

# Trading signals based on PC1 z-score
long_signal = z_pc1 < -2
short_signal = z_pc1 > 2
exit_signal = (z_pc1 > -0.5) & (z_pc1 < 0.5)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 120; i++) {
            prices.push_back(100.0 + i * 0.2 + std::sin(i * 0.1) * 8.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== PCA Z-Score Signals ===\n{}", df.repr());

        REQUIRE(df.contains("z_pc1#result"));
        REQUIRE(df.contains("long_signal#result"));
        REQUIRE(df.contains("short_signal#result"));
        REQUIRE(df.contains("exit_signal#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "ml_strategy/pca_zscore",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"z_pc1#result"},
            0.1, 0.01, 100
        );
    }
}

// =============================================================================
// ROLLING CLUSTERING STRATEGIES
// =============================================================================

TEST_CASE("ML Strategy - K-Means Regime Detection", "[ml_strategy][kmeans][regime]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling K-Means identifies market regimes") {
        // Cluster market states based on returns and volatility
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Features for clustering
ret = src.c / src.c[1] - 1
vol = stddev(period=20)(ret)

# Rolling K-Means with 3 clusters (bull, bear, sideways)
km = rolling_kmeans_3(window_size=100, min_training_samples=50)(ret, vol)

# Get cluster assignment
regime = km.cluster_label
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        double price = 100.0;
        for (int i = 0; i < 120; i++) {
            // Create different regimes
            if (i < 40) {
                price += 0.5;  // Bull regime
            } else if (i < 80) {
                price -= 0.3;  // Bear regime
            } else {
                price += (i % 2 == 0 ? 0.2 : -0.2);  // Sideways
            }
            prices.push_back(price);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== K-Means Regime Detection ===\n{}", df.repr());

        REQUIRE(df.contains("km#cluster_label"));
        REQUIRE(df.contains("regime#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "ml_strategy/kmeans_regime",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"km#cluster_label"},
            0.1, 0.1, 100
        );
    }
}

TEST_CASE("ML Strategy - DBSCAN Outlier Detection", "[ml_strategy][dbscan][outlier]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling DBSCAN identifies outlier days") {
        // DBSCAN can identify outlier events (cluster -1)
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Features for clustering
ret = src.c / src.c[1] - 1
vol_chg = src.v / src.v[1] - 1

# Rolling DBSCAN (min_points is the correct option name)
db = rolling_dbscan(window_size=100, min_training_samples=50, epsilon=0.5, min_points=3)(ret, vol_chg)

# Outliers are cluster -1
cluster = db.cluster_label
is_outlier = cluster == -1
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 150; i++) {
            prices.push_back(100.0 + i * 0.2 + std::sin(i * 0.15) * 3.0);
        }
        // Add some outlier moves
        prices[110] = prices[109] + 10.0;  // Large up move
        prices[130] = prices[129] - 8.0;   // Large down move

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== DBSCAN Outlier Detection ===\n{}", df.repr());

        REQUIRE(df.contains("db#cluster_label"));
        REQUIRE(df.contains("cluster#result"));
        REQUIRE(df.contains("is_outlier#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "ml_strategy/dbscan_outlier",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"db#cluster_label"},
            0.1, 0.1, 100
        );
    }
}

// =============================================================================
// ROLLING LIGHTGBM SUPERVISED STRATEGIES
// =============================================================================

TEST_CASE("ML Strategy - LightGBM Return Prediction", "[ml_strategy][lightgbm][prediction]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Rolling LightGBM regressor predicts returns") {
        // Predict next-day returns using rolling LightGBM
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Features
ret = src.c / src.c[1] - 1
vol = stddev(period=5)(ret)
mom = src.c / src.c[5] - 1

# Target: next-day return (shifted)
target = ret[1]

# Rolling LightGBM regressor (larger step for speed)
lgb = rolling_lightgbm_regressor(window_size=100, min_training_samples=50, step_size=20)(ret, vol, mom, target=target)

# Prediction output
pred = lgb.prediction
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 150; i++) {
            prices.push_back(100.0 + i * 0.2 + std::sin(i * 0.12) * 6.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== LightGBM Return Prediction ===\n{}", df.repr());

        REQUIRE(df.contains("lgb#prediction"));
        REQUIRE(df.contains("pred#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "ml_strategy/lightgbm_regressor",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"lgb#prediction"},
            0.15, 0.05, 100  // higher tolerance for ML predictions
        );
    }

    SECTION("LightGBM classifier for direction prediction") {
        // Classify next-day direction (up/down)
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Features
ret = src.c / src.c[1] - 1
vol = stddev(period=5)(ret)
mom = src.c / src.c[5] - 1
rsi_val = rsi(period=14)(src.c)

# Target: direction (1 if up, 0 if down) - convert bool to numeric
next_ret = ret[1]
target = (next_ret > 0) * 1

# Rolling LightGBM classifier (larger step for speed)
lgb = rolling_lightgbm_classifier(window_size=100, min_training_samples=50, step_size=20)(ret, vol, mom, rsi_val, target=target)

# Outputs
pred_class = lgb.prediction
pred_prob = lgb.probability

# Trade when confident
long_signal = pred_prob > 0.6
short_signal = pred_prob < 0.4
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 200; i++) {
            prices.push_back(100.0 + i * 0.15 + std::sin(i * 0.1) * 5.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== LightGBM Direction Classifier ===\n{}", df.repr());

        REQUIRE(df.contains("lgb#prediction"));
        REQUIRE(df.contains("lgb#probability"));
        REQUIRE(df.contains("pred_class#result"));
        REQUIRE(df.contains("pred_prob#result"));
        REQUIRE(df.contains("long_signal#result"));
        REQUIRE(df.contains("short_signal#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "ml_strategy/lightgbm_classifier",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"lgb#prediction", "lgb#probability"},
            0.15, 0.05, 100  // higher tolerance for ML predictions
        );
    }
}

// =============================================================================
// ML FEATURE ENGINEERING PIPELINE
// =============================================================================

TEST_CASE("ML Strategy - Feature Engineering Pipeline", "[ml_strategy][feature_engineering]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Complete feature engineering for ML") {
        // Build comprehensive feature set
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Price-based features
ret = src.c / src.c[1] - 1
log_c = ln()(src.c)
log_c_prev = ln()(src.c[1])
log_ret = log_c - log_c_prev

# Momentum features
mom_5 = src.c / src.c[5] - 1
mom_10 = src.c / src.c[10] - 1
mom_20 = src.c / src.c[20] - 1

# Volatility features
vol_5 = stddev(period=5)(ret)
vol_20 = stddev(period=20)(ret)
vol_ratio = vol_5 / vol_20

# Mean reversion features
z_5 = zscore(window=5)(src.c)
z_20 = zscore(window=20)(src.c)

# Technical indicators
sma_fast = sma(period=10)(src.c)
sma_slow = sma(period=30)(src.c)
sma_ratio = sma_fast / sma_slow - 1

rsi_val = rsi(period=14)(src.c)

# Preprocess for ML: normalize all features using z-score
feat_ret = zscore(window=60)(ret)
feat_mom = zscore(window=60)(mom_20)
feat_vol = zscore(window=60)(vol_ratio)
feat_z = zscore(window=60)(z_20)
feat_sma = zscore(window=60)(sma_ratio)
feat_rsi = zscore(window=60)(rsi_val)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        for (int i = 0; i < 150; i++) {
            prices.push_back(100.0 + i * 0.1 + std::sin(i * 0.08) * 8.0);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Feature Engineering Pipeline ===\n{}", df.repr());

        // Verify raw features
        REQUIRE(df.contains("ret#result"));
        REQUIRE(df.contains("mom_20#result"));
        REQUIRE(df.contains("vol_ratio#result"));
        REQUIRE(df.contains("z_20#result"));
        REQUIRE(df.contains("sma_ratio#result"));
        REQUIRE(df.contains("rsi_val#result"));

        // Verify normalized features
        REQUIRE(df.contains("feat_ret#result"));
        REQUIRE(df.contains("feat_mom#result"));
        REQUIRE(df.contains("feat_vol#result"));
        REQUIRE(df.contains("feat_z#result"));
        REQUIRE(df.contains("feat_sma#result"));
        REQUIRE(df.contains("feat_rsi#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "ml_strategy/feature_engineering",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"feat_ret#result", "feat_mom#result", "feat_vol#result", "feat_z#result", "feat_sma#result", "feat_rsi#result"},
            0.1, 0.01, 90  // features after warmup
        );
    }
}

// =============================================================================
// COMBINED ML STRATEGY
// =============================================================================

TEST_CASE("ML Strategy - Full Trading Pipeline", "[ml_strategy][full_pipeline]") {
    const auto dailyTF = TestTimeFrames::Daily();
    const std::string spy = "SPY";

    SECTION("Complete ML trading strategy with regime detection and prediction") {
        // Comprehensive ML pipeline combining multiple models
        std::string sourceCode = R"(
src = market_data_source(timeframe="1D")()

# Basic features
ret = src.c / src.c[1] - 1
vol = stddev(period=20)(ret)
mom = src.c / src.c[20] - 1
z_score = zscore(window=20)(src.c)

# Step 1: PCA for factor reduction (use larger step for speed)
pca = rolling_pca_2(window_size=100, min_training_samples=50, step_size=10)(ret, vol, mom, z_score)
pc1 = pca.pc_0
pc2 = pca.pc_1

# Step 2: K-Means for regime detection (3 regimes)
km = rolling_kmeans_3(window_size=100, min_training_samples=50, step_size=10)(pc1, pc2)
regime = km.cluster_label

# Step 3: Generate signals based on regime and z-score
# In trending regime (cluster 0), use momentum
# In mean-reverting regime (cluster 1), use z-score
is_trending = regime == 0
is_mean_rev = regime == 1

# Momentum signal: buy on positive momentum
mom_signal = mom > 0.02

# Mean-reversion signal: buy on oversold
mr_long = z_score < -2
mr_short = z_score > 2

# Combined signal based on regime
long_signal = (is_trending & mom_signal) | (is_mean_rev & mr_long)
short_signal = (is_trending & (mom < -0.02)) | (is_mean_rev & mr_short)
)";

        auto transformManager = CompileSource(sourceCode);
        DataFlowRuntimeOrchestrator orch({spy}, std::move(transformManager));

        std::vector<double> prices;
        double price = 100.0;
        // Need 250+ rows to support chained rolling windows (zscore(20) + PCA(100) + KMeans(100))
        for (int i = 0; i < 250; i++) {
            // Create mixed regime data
            if (i < 80) {
                price += 0.3 + std::sin(i * 0.1) * 0.2;  // Trending up
            } else if (i < 170) {
                price += std::sin(i * 0.3) * 2.0;  // Mean-reverting
            } else {
                price -= 0.2 + std::cos(i * 0.1) * 0.3;  // Trending down
            }
            prices.push_back(price);
        }

        TimeFrameAssetDataFrameMap inputData;
        inputData[dailyTF.ToString()][spy] = CreateOHLCVData(prices);

        data_sdk::events::ScopedProgressEmitter emitter;
        auto result = orch.ExecutePipeline(std::move(inputData), emitter);
        const auto& dailyResult = result[dailyTF.ToString()];

        REQUIRE(dailyResult.contains(spy));
        auto df = dailyResult.at(spy);

        SPDLOG_INFO("=== Full ML Trading Pipeline ===\n{}", df.repr());

        // Verify all components exist
        REQUIRE(df.contains("pca#pc_0"));
        REQUIRE(df.contains("pca#pc_1"));
        REQUIRE(df.contains("km#cluster_label"));
        REQUIRE(df.contains("regime#result"));
        REQUIRE(df.contains("is_trending#result"));
        REQUIRE(df.contains("is_mean_rev#result"));
        REQUIRE(df.contains("long_signal#result"));
        REQUIRE(df.contains("short_signal#result"));

        // Compare against CSV baseline
        CompareOrGenerateBaseline(
            df,
            "ml_strategy/full_pipeline",
            std::filesystem::path(RUNTIME_TEST_DATA_DIR),
            {"pca#pc_0", "pca#pc_1", "km#cluster_label"},
            0.15, 0.05, 150  // large warmup for chained ML transforms
        );
    }
}
