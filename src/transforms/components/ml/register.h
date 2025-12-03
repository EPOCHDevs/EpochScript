#pragma once
// Machine Learning transforms registration
// Provides supervised learning, unsupervised learning, preprocessing, and rolling window ML models
//
// Categories:
// 1. Static ML Models - train once on historical data, predict forward
//    - LightGBM (gradient boosting)
//    - LIBLINEAR (logistic regression, SVR)
// 2. Unsupervised Learning - clustering and dimensionality reduction
//    - KMeans, DBSCAN, PCA, HMM
// 3. Rolling ML Models - adaptive models that retrain as data arrives
//    - Rolling versions of all above models
// 4. Preprocessing - feature scaling for ML pipelines
//    - Z-Score, MinMax, Robust scaling

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Static ML model implementations
#include "lightgbm.h"
#include "linear_model.h"
#include "ml_preprocess.h"
#include "sagemaker_sentiment.h"

// Rolling ML model implementations
#include "rolling_lightgbm.h"
#include "rolling_linear_model.h"
#include "rolling_ml_preprocess.h"
#include "rolling_kmeans.h"
#include "rolling_dbscan.h"
#include "rolling_hmm.h"
#include "rolling_pca.h"

// Metadata definitions
#include "lightgbm_metadata.h"
#include "liblinear_metadata.h"
#include "ml_preprocess_metadata.h"
#include "sagemaker_sentiment_metadata.h"
#include "rolling_transforms_metadata.h"

// Note: Static clustering/decomposition (KMeans, DBSCAN, PCA, HMM) are in statistics/ module

namespace epoch_script::transform::ml {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;
using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// Registration function - registers all ML transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // GRADIENT BOOSTING (LightGBM)
    // =========================================================================
    // High-performance gradient boosting for classification and regression.
    // Captures nonlinear patterns and feature interactions.
    // Use for: direction prediction, return forecasting, regime classification.
    // Warning: Risk of overfitting - requires careful cross-validation.

    // lightgbm_classifier: Binary/multiclass classification with gradient boosting
    // Outputs: prediction (class label), probability (confidence)
    epoch_script::transform::Register<LightGBMClassifier>("lightgbm_classifier");

    // lightgbm_regressor: Continuous prediction with gradient boosting
    // Outputs: prediction (continuous value)
    epoch_script::transform::Register<LightGBMRegressor>("lightgbm_regressor");

    // =========================================================================
    // LINEAR MODELS (LIBLINEAR)
    // =========================================================================
    // Fast, interpretable linear models with L1/L2 regularization.
    // L1 (Lasso): sparse feature selection, drives unimportant weights to zero
    // L2 (Ridge): stable coefficients, handles correlated features better
    // Use for: interpretable signals, feature importance analysis, baseline models.

    // logistic_l1: L1-regularized logistic regression for sparse classification
    // Outputs: prediction, probability, decision_value
    epoch_script::transform::Register<LogisticL1Transform>("logistic_l1");

    // logistic_l2: L2-regularized logistic regression for stable classification
    // Outputs: prediction, probability, decision_value
    epoch_script::transform::Register<LogisticL2Transform>("logistic_l2");

    // svr_l1: L1-regularized support vector regression for sparse prediction
    // Outputs: result (continuous prediction)
    epoch_script::transform::Register<SVRL1Transform>("svr_l1");

    // svr_l2: L2-regularized support vector regression for stable prediction
    // Outputs: result (continuous prediction)
    epoch_script::transform::Register<SVRL2Transform>("svr_l2");

    // =========================================================================
    // FEATURE PREPROCESSING
    // =========================================================================
    // Scale features for ML models. Essential for algorithms sensitive to scale.
    // Fit parameters on training data, apply to full dataset to prevent leakage.
    // Variants: _2 through _6 for different numbers of input features.

    // ml_zscore_N: Standardize to zero mean, unit variance
    // Formula: z = (x - mean) / std
    // Best for: normally distributed data, linear models, neural networks
    for (int n = 2; n <= 6; ++n) {
        epoch_script::transform::Register<MLZScore>("ml_zscore_" + std::to_string(n));
    }

    // ml_minmax_N: Scale to [0, 1] range
    // Formula: x_scaled = (x - min) / (max - min)
    // Best for: bounded algorithms, neural networks with sigmoid activations
    for (int n = 2; n <= 6; ++n) {
        epoch_script::transform::Register<MLMinMax>("ml_minmax_" + std::to_string(n));
    }

    // ml_robust_N: Scale using median and IQR (outlier-resistant)
    // Formula: x_scaled = (x - median) / IQR
    // Best for: data with outliers, financial data with fat tails
    for (int n = 2; n <= 6; ++n) {
        epoch_script::transform::Register<MLRobust>("ml_robust_" + std::to_string(n));
    }

    // =========================================================================
    // NLP / SENTIMENT ANALYSIS
    // =========================================================================
    // Financial NLP using pre-trained transformer models.

    // finbert_sentiment: FinBERT sentiment analysis for financial text
    // Outputs: positive, neutral, negative (booleans), confidence [0-1]
    // Use with: news, earnings transcripts, analyst reports
    epoch_script::transform::Register<SageMakerFinBERTTransform>("finbert_sentiment");

    // =========================================================================
    // ROLLING GRADIENT BOOSTING
    // =========================================================================
    // Adaptive LightGBM that retrains as new data arrives.
    // Adapts to evolving market conditions and regime changes.
    // Higher computational cost but better for non-stationary data.

    // rolling_lightgbm_classifier: Adaptive classification with gradient boosting
    epoch_script::transform::Register<RollingLightGBMClassifier>("rolling_lightgbm_classifier");

    // rolling_lightgbm_regressor: Adaptive regression with gradient boosting
    epoch_script::transform::Register<RollingLightGBMRegressor>("rolling_lightgbm_regressor");

    // =========================================================================
    // ROLLING LINEAR MODELS
    // =========================================================================
    // Adaptive linear models that retrain over rolling windows.
    // Provides evolving feature importance and coefficient estimates.

    // rolling_logistic_l1: Adaptive L1 logistic regression
    epoch_script::transform::Register<RollingLogisticL1Transform>("rolling_logistic_l1");

    // rolling_logistic_l2: Adaptive L2 logistic regression
    epoch_script::transform::Register<RollingLogisticL2Transform>("rolling_logistic_l2");

    // rolling_svr_l1: Adaptive L1 support vector regression
    epoch_script::transform::Register<RollingSVRL1Transform>("rolling_svr_l1");

    // rolling_svr_l2: Adaptive L2 support vector regression
    epoch_script::transform::Register<RollingSVRL2Transform>("rolling_svr_l2");

    // =========================================================================
    // ROLLING PREPROCESSING
    // =========================================================================
    // Rolling normalization that updates statistics as data arrives.
    // Computes scaling parameters from training window, applies to test window.

    // rolling_ml_zscore: Rolling z-score normalization
    epoch_script::transform::Register<RollingMLZScore>("rolling_ml_zscore");

    // rolling_ml_minmax: Rolling min-max normalization
    epoch_script::transform::Register<RollingMLMinMax>("rolling_ml_minmax");

    // rolling_ml_robust: Rolling robust normalization
    epoch_script::transform::Register<RollingMLRobust>("rolling_ml_robust");

    // =========================================================================
    // ROLLING CLUSTERING
    // =========================================================================
    // Adaptive clustering for regime detection with evolving cluster definitions.

    // rolling_kmeans_N: Adaptive K-Means with N clusters
    // Outputs: cluster_label, cluster_N_dist (distance to each centroid)
    epoch_script::transform::Register<RollingKMeans2Transform>("rolling_kmeans_2");
    epoch_script::transform::Register<RollingKMeans3Transform>("rolling_kmeans_3");
    epoch_script::transform::Register<RollingKMeans4Transform>("rolling_kmeans_4");
    epoch_script::transform::Register<RollingKMeans5Transform>("rolling_kmeans_5");

    // rolling_dbscan: Adaptive density-based clustering
    // Outputs: cluster_label (-1 = anomaly), is_anomaly, cluster_count
    // Use for: rolling anomaly detection, adaptive regime detection
    epoch_script::transform::Register<RollingDBSCANTransform>("rolling_dbscan");

    // =========================================================================
    // ROLLING PROBABILISTIC MODELS
    // =========================================================================
    // Adaptive sequence models for regime detection with temporal dependencies.

    // rolling_hmm_N: Adaptive Hidden Markov Model with N states
    // Outputs: state, state_N_prob (probability of each state)
    // Use for: sequential regime detection, market state classification
    epoch_script::transform::Register<RollingHMM2Transform>("rolling_hmm_2");
    epoch_script::transform::Register<RollingHMM3Transform>("rolling_hmm_3");
    epoch_script::transform::Register<RollingHMM4Transform>("rolling_hmm_4");
    epoch_script::transform::Register<RollingHMM5Transform>("rolling_hmm_5");

    // =========================================================================
    // ROLLING DIMENSIONALITY REDUCTION
    // =========================================================================
    // Adaptive factor extraction with evolving loadings.

    // rolling_pca_N: Adaptive PCA with N principal components
    // Outputs: pc_0 through pc_{N-1}, explained_variance_ratio
    // Use for: adaptive factor extraction, yield curve analysis, risk decomposition
    epoch_script::transform::Register<RollingPCA2Transform>("rolling_pca_2");
    epoch_script::transform::Register<RollingPCA3Transform>("rolling_pca_3");
    epoch_script::transform::Register<RollingPCA4Transform>("rolling_pca_4");
    epoch_script::transform::Register<RollingPCA5Transform>("rolling_pca_5");
    epoch_script::transform::Register<RollingPCA6Transform>("rolling_pca_6");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // LightGBM metadata
    for (const auto& metadata : MakeLightGBMMetaData()) {
        metaRegistry.Register(metadata);
    }

    // LIBLINEAR metadata
    for (const auto& metadata : MakeLiblinearMetaData()) {
        metaRegistry.Register(metadata);
    }

    // ML preprocessing metadata (ml_zscore_N, ml_minmax_N, ml_robust_N)
    for (const auto& metadata : MakeMLPreprocessMetaData()) {
        metaRegistry.Register(metadata);
    }

    // SageMaker sentiment metadata
    for (const auto& metadata : MakeSageMakerSentimentTransforms()) {
        metaRegistry.Register(metadata);
    }

    // All rolling ML metadata (LightGBM, LIBLINEAR, preprocessing, clustering, HMM, PCA)
    for (const auto& metadata : MakeAllRollingMLMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::ml
