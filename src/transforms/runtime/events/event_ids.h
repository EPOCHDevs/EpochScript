#pragma once
#include <string_view>

namespace epoch_script::events {

/**
 * @file event_ids.h
 * @brief Constexpr string IDs for the EpochScript event system
 *
 * This file centralizes all operation_type and operation_name identifiers
 * used throughout the transform pipeline for consistency and type safety.
 *
 * NOTE: For SDK-level IDs (Pipeline, Stage, DataLoading, etc.), use:
 *       #include <epoch_data_sdk/events/event_ids.h>
 */

// =============================================================================
// Operation Types (operation_type parameter in EmitStarted/Completed/Failed)
// =============================================================================

namespace OperationType {

/// Transform-level operations (individual transform execution)
/// Used in: src/transforms/runtime/execution/execution_context.h:49,58,67
constexpr std::string_view Transform = "transform";

/// Node-level operations (execution node in DAG)
/// Used in: src/transforms/runtime/execution/execution_context.h:49,58,67
constexpr std::string_view Node = "node";

/// Asset-level operations (per-asset processing within a transform)
/// Used in: src/transforms/components (various per-asset processing)
constexpr std::string_view Asset = "asset";

/// Window-level operations (rolling window iterations)
/// Used in: src/transforms/components/ml/rolling_ml_base.h:134,268
constexpr std::string_view Window = "window";

/// Training operations (ML model training)
/// Used in: src/transforms/components/ml/lightgbm.h, linear_model.h
constexpr std::string_view Training = "training";

/// Prediction operations (ML model inference)
/// Used in: src/transforms/components/ml/lightgbm.h, linear_model.h
constexpr std::string_view Prediction = "prediction";

} // namespace OperationType

// =============================================================================
// Transform Names (operation_name for Transform operations)
// =============================================================================

namespace TransformName {

// ML Transforms
/// Used in: src/transforms/components/ml/rolling_pca.h
constexpr std::string_view RollingPCA = "RollingPCA";

/// Used in: src/transforms/components/ml/rolling_ica.h
constexpr std::string_view RollingICA = "RollingICA";

/// Used in: src/transforms/components/ml/rolling_lightgbm.h
constexpr std::string_view RollingLightGBM = "RollingLightGBM";

/// Used in: src/transforms/components/ml/rolling_linear_model.h
constexpr std::string_view RollingLinearModel = "RollingLinearModel";

/// Used in: src/transforms/components/ml/rolling_dbscan.h
constexpr std::string_view RollingDBSCAN = "RollingDBSCAN";

/// Used in: src/transforms/components/ml/lightgbm.h
constexpr std::string_view LightGBM = "LightGBM";

/// Used in: src/transforms/components/ml/linear_model.h
constexpr std::string_view LinearModel = "LinearModel";

// Statistical Transforms
/// Used in: src/transforms/components/statistics/pca.h
constexpr std::string_view PCA = "PCA";

/// Used in: src/transforms/components/statistics/kmeans.h
constexpr std::string_view KMeans = "KMeans";

/// Used in: src/transforms/components/statistics/dbscan.h
constexpr std::string_view DBSCAN = "DBSCAN";

/// Used in: src/transforms/components/statistics/hmm.h
constexpr std::string_view HMM = "HMM";

// Hosseinmoein Transforms
/// Used in: src/transforms/components/hosseinmoein/statistics/cointegration_metadata.h
constexpr std::string_view Cointegration = "Cointegration";

} // namespace TransformName

// =============================================================================
// Context Keys (keys used in SetContext() calls)
// =============================================================================

namespace ContextKey {

// Rolling window context
/// Current window index
/// Used in: src/transforms/components/ml/rolling_ml_base.h:134,268
constexpr std::string_view CurrentWindow = "current_window";

/// Total number of windows
/// Used in: src/transforms/components/ml/rolling_ml_base.h:134,268
constexpr std::string_view TotalWindows = "total_windows";

/// Window start index
/// Used in: src/transforms/components/ml/rolling_ml_base.h
constexpr std::string_view WindowStart = "window_start";

/// Window end index
/// Used in: src/transforms/components/ml/rolling_ml_base.h
constexpr std::string_view WindowEnd = "window_end";

// ML Training context
/// Training loss value
/// Used in: src/transforms/components/ml/lightgbm.h
constexpr std::string_view Loss = "loss";

/// Model accuracy
/// Used in: src/transforms/components/ml/lightgbm.h
constexpr std::string_view Accuracy = "accuracy";

/// Current training epoch/iteration
/// Used in: src/transforms/components/ml/lightgbm.h
constexpr std::string_view Epoch = "epoch";

/// Learning rate
/// Used in: src/transforms/components/ml/lightgbm.h
constexpr std::string_view LearningRate = "lr";

/// Validation loss
/// Used in: src/transforms/components/ml/lightgbm.h
constexpr std::string_view ValidationLoss = "val_loss";

/// Validation accuracy
/// Used in: src/transforms/components/ml/lightgbm.h
constexpr std::string_view ValidationAccuracy = "val_accuracy";

/// Number of training samples
/// Used in: src/transforms/components/ml/rolling_ml_base.h
constexpr std::string_view TrainingSamples = "training_samples";

/// Number of features
/// Used in: src/transforms/components/ml/rolling_ml_base.h
constexpr std::string_view NumFeatures = "num_features";

// Clustering context
/// Number of clusters
/// Used in: src/transforms/components/statistics/kmeans.h
constexpr std::string_view NumClusters = "num_clusters";

/// Number of components (PCA)
/// Used in: src/transforms/components/statistics/pca.h
constexpr std::string_view NumComponents = "num_components";

/// Explained variance ratio
/// Used in: src/transforms/components/statistics/pca.h
constexpr std::string_view ExplainedVariance = "explained_variance";

// Execution context
/// Transform name being executed
/// Used in: src/transforms/runtime/execution/execution_context.h:49
constexpr std::string_view TransformName = "transform_name";

/// Node ID in DAG
/// Used in: src/transforms/runtime/execution/execution_context.h:58,67
constexpr std::string_view NodeId = "node_id";

/// Whether transform is cross-sectional
/// Used in: src/transforms/runtime/execution/execution_context.h:50
constexpr std::string_view IsCrossSectional = "is_cross_sectional";

/// Number of assets being processed
/// Used in: src/transforms/runtime/execution/execution_context.h:51
constexpr std::string_view AssetCount = "asset_count";

/// Number of assets successfully processed
/// Used in: src/transforms/runtime/execution/execution_context.h:59
constexpr std::string_view AssetsProcessed = "assets_processed";

/// Number of assets that failed
/// Used in: src/transforms/runtime/execution/execution_context.h:60
constexpr std::string_view AssetsFailed = "assets_failed";

} // namespace ContextKey

// =============================================================================
// Progress Units (unit parameter in EmitProgress)
// =============================================================================

namespace ProgressUnit {

/// Windows processed
constexpr std::string_view Windows = "windows";

/// Rows processed
constexpr std::string_view Rows = "rows";

/// Assets processed
constexpr std::string_view Assets = "assets";

/// Epochs completed
constexpr std::string_view Epochs = "epochs";

/// Iterations completed
constexpr std::string_view Iterations = "iterations";

/// Samples processed
constexpr std::string_view Samples = "samples";

} // namespace ProgressUnit

} // namespace epoch_script::events
