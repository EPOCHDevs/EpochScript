#pragma once
//
// Metadata for Rolling ML Transforms
//
// This file contains metadata definitions for all rolling ML transforms:
// - Rolling LightGBM (classifier, regressor)
// - Rolling LIBLINEAR (logistic_l1, logistic_l2, svr_l1, svr_l2)
// - Rolling Preprocessors (ml_zscore, ml_minmax, ml_robust)
// - Rolling Clustering (kmeans_2-5, dbscan)
// - Rolling Decomposition (pca, ica)
// - Rolling Probabilistic (gmm_2-5, hmm_2-5)
//

#include <epoch_script/transforms/core/metadata.h>
#include "rolling_ml_metadata.h"  // For MakeRollingMLOptions(), CombineWithRollingOptions()

namespace epoch_script::transform {

// =============================================================================
// Rolling LightGBM Metadata
// =============================================================================

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeRollingLightGBMMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  auto makeLightGBMOptions = []() -> MetaDataOptionList {
    return {
      MetaDataOption{
        .id = "num_estimators",
        .name = "Number of Trees",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(100.0),
        .min = 1,
        .max = 10000,
        .desc = "Number of boosting rounds"
      },
      MetaDataOption{
        .id = "learning_rate",
        .name = "Learning Rate",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.1),
        .min = 0.001,
        .max = 1.0,
        .desc = "Step size shrinkage for gradient descent"
      },
      MetaDataOption{
        .id = "num_leaves",
        .name = "Number of Leaves",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(31.0),
        .min = 2,
        .max = 256,
        .desc = "Maximum number of leaves in one tree"
      },
      MetaDataOption{
        .id = "min_data_in_leaf",
        .name = "Min Data in Leaf",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(20.0),
        .min = 1,
        .max = 1000,
        .desc = "Minimum number of samples in a leaf node"
      },
      MetaDataOption{
        .id = "max_depth",
        .name = "Max Depth",
        .type = epoch_core::MetaDataOptionType::Select,
        .defaultValue = MetaDataOptionDefinition(std::string("auto")),
        .selectOption = {
          {"Auto (-1)", "auto"},
          {"Shallow (3)", "3"},
          {"Medium (6)", "6"},
          {"Deep (12)", "12"}
        },
        .desc = "Maximum tree depth. Auto means no limit."
      },
      MetaDataOption{
        .id = "boosting_type",
        .name = "Boosting Type",
        .type = epoch_core::MetaDataOptionType::Select,
        .defaultValue = MetaDataOptionDefinition(std::string("gbdt")),
        .selectOption = {
          {"Gradient Boosting (GBDT)", "gbdt"},
          {"Dropout (DART)", "dart"},
          {"Random Forest", "rf"}
        },
        .desc = "Type of boosting algorithm"
      },
      MetaDataOption{
        .id = "lambda_l1",
        .name = "L1 Regularization",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.0),
        .min = 0.0,
        .desc = "L1 regularization term (Lasso)"
      },
      MetaDataOption{
        .id = "lambda_l2",
        .name = "L2 Regularization",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.0),
        .min = 0.0,
        .desc = "L2 regularization term (Ridge)"
      }
    };
  };

  auto makeLightGBMInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false},
      {epoch_core::IODataType::Number, "target", "Target", false, false}
    };
  };

  // Rolling LightGBM Classifier
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_lightgbm_classifier",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::lightgbm,
    .name = "Rolling LightGBM Classifier",
    .options = CombineWithRollingOptions(makeLightGBMOptions()),
    .isCrossSectional = false,
    .desc = "Rolling window gradient boosting classifier. Retrains model as new data arrives, "
            "adapting to evolving market conditions.",
    .inputs = makeLightGBMInputs(),
    .outputs = {
      {epoch_core::IODataType::Integer, "prediction", "Prediction", true, false},
      {epoch_core::IODataType::Decimal, "probability", "Probability", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"lightgbm", "ml", "classification", "rolling", "adaptive"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "adaptive-strategy"},
    .relatedTransforms = {"lightgbm_classifier", "rolling_lightgbm_regressor", "rolling_logistic_l1"},
    .usageContext = "Use for adaptive classification that updates as new data arrives. "
                    "Better for non-stationary financial data than static models.",
    .limitations = "Higher computational cost due to retraining. May overfit to recent data."
  });

  // Rolling LightGBM Regressor
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_lightgbm_regressor",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::lightgbm,
    .name = "Rolling LightGBM Regressor",
    .options = CombineWithRollingOptions(makeLightGBMOptions()),
    .isCrossSectional = false,
    .desc = "Rolling window gradient boosting regressor. Retrains model as new data arrives, "
            "adapting return predictions to current market regime.",
    .inputs = makeLightGBMInputs(),
    .outputs = {
      {epoch_core::IODataType::Decimal, "prediction", "Prediction", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"lightgbm", "ml", "regression", "rolling", "adaptive"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "adaptive-strategy", "return-prediction"},
    .relatedTransforms = {"lightgbm_regressor", "rolling_lightgbm_classifier", "rolling_svr_l1"},
    .usageContext = "Use for adaptive return prediction that updates as new data arrives.",
    .limitations = "Higher computational cost due to retraining. May overfit to recent patterns."
  });

  return metadataList;
}

// =============================================================================
// Rolling LIBLINEAR Metadata
// =============================================================================

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeRollingLiblinearMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  auto makeLiblinearOptions = []() -> MetaDataOptionList {
    return {
      MetaDataOption{
        .id = "C",
        .name = "Regularization (C)",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 0.0001,
        .max = 10000.0,
        .desc = "Inverse of regularization strength"
      },
      MetaDataOption{
        .id = "epsilon",
        .name = "Epsilon",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.01),
        .min = 0.0001,
        .max = 1.0,
        .desc = "Tolerance for stopping criterion"
      }
    };
  };

  auto makeClassifierInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false},
      {epoch_core::IODataType::Number, "target", "Target", false, false}
    };
  };

  // Rolling Logistic L1
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_logistic_l1",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::linear_model,
    .name = "Rolling Logistic L1",
    .options = CombineWithRollingOptions(makeLiblinearOptions()),
    .isCrossSectional = false,
    .desc = "Rolling window L1-regularized logistic regression. Provides sparse feature selection "
            "that adapts to changing market conditions.",
    .inputs = makeClassifierInputs(),
    .outputs = {
      {epoch_core::IODataType::Integer, "prediction", "Prediction", true, false},
      {epoch_core::IODataType::Decimal, "probability", "Probability", true, false},
      {epoch_core::IODataType::Decimal, "decision_value", "Decision Value", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"liblinear", "ml", "classification", "rolling", "l1", "lasso"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "adaptive-strategy"},
    .relatedTransforms = {"logistic_l1", "rolling_logistic_l2", "rolling_lightgbm_classifier"},
    .usageContext = "Use for adaptive classification with automatic feature selection.",
    .limitations = "May be unstable with highly correlated features."
  });

  // Rolling Logistic L2
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_logistic_l2",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::linear_model,
    .name = "Rolling Logistic L2",
    .options = CombineWithRollingOptions(makeLiblinearOptions()),
    .isCrossSectional = false,
    .desc = "Rolling window L2-regularized logistic regression. Provides stable classification "
            "that adapts to changing market conditions.",
    .inputs = makeClassifierInputs(),
    .outputs = {
      {epoch_core::IODataType::Integer, "prediction", "Prediction", true, false},
      {epoch_core::IODataType::Decimal, "probability", "Probability", true, false},
      {epoch_core::IODataType::Decimal, "decision_value", "Decision Value", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"liblinear", "ml", "classification", "rolling", "l2", "ridge"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "adaptive-strategy"},
    .relatedTransforms = {"logistic_l2", "rolling_logistic_l1", "rolling_lightgbm_classifier"},
    .usageContext = "Use for adaptive classification with stable coefficient estimates.",
    .limitations = "All features retained; no automatic feature selection."
  });

  // Rolling SVR L1
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_svr_l1",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::linear_model,
    .name = "Rolling SVR L1",
    .options = CombineWithRollingOptions(makeLiblinearOptions()),
    .isCrossSectional = false,
    .desc = "Rolling window L1-regularized support vector regression. Provides sparse return prediction "
            "that adapts to changing market conditions.",
    .inputs = makeClassifierInputs(),
    .outputs = {
      {epoch_core::IODataType::Decimal, "prediction", "Prediction", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"liblinear", "ml", "regression", "rolling", "l1", "svr"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "adaptive-strategy", "return-prediction"},
    .relatedTransforms = {"svr_l1", "rolling_svr_l2", "rolling_lightgbm_regressor"},
    .usageContext = "Use for adaptive return prediction with automatic feature selection.",
    .limitations = "May be unstable with highly correlated features."
  });

  // Rolling SVR L2
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_svr_l2",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::linear_model,
    .name = "Rolling SVR L2",
    .options = CombineWithRollingOptions(makeLiblinearOptions()),
    .isCrossSectional = false,
    .desc = "Rolling window L2-regularized support vector regression. Provides stable return prediction "
            "that adapts to changing market conditions.",
    .inputs = makeClassifierInputs(),
    .outputs = {
      {epoch_core::IODataType::Decimal, "prediction", "Prediction", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"liblinear", "ml", "regression", "rolling", "l2", "svr"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "adaptive-strategy", "return-prediction"},
    .relatedTransforms = {"svr_l2", "rolling_svr_l1", "rolling_lightgbm_regressor"},
    .usageContext = "Use for adaptive return prediction with stable coefficient estimates.",
    .limitations = "All features retained; no automatic feature selection."
  });

  return metadataList;
}

// =============================================================================
// Rolling ML Preprocessor Metadata
// =============================================================================

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeRollingMLPreprocessMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  auto makePreprocessInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false}
    };
  };

  auto makePreprocessOutputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Decimal, "SLOT", "Normalized Features", true, false}
    };
  };

  // Rolling ML ZScore
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_ml_zscore",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "Rolling ML Z-Score",
    .options = MakeRollingMLOptions(),
    .isCrossSectional = false,
    .desc = "Rolling window z-score normalization. Computes mean and standard deviation over the "
            "training window and normalizes test data accordingly.",
    .inputs = makePreprocessInputs(),
    .outputs = makePreprocessOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"preprocessing", "ml", "normalization", "rolling", "zscore"},
    .requiresTimeFrame = false,
    .strategyTypes = {"feature-engineering", "preprocessing"},
    .relatedTransforms = {"ml_zscore", "rolling_ml_minmax", "rolling_ml_robust"},
    .usageContext = "Use for adaptive feature normalization that updates statistics as data arrives.",
    .limitations = "Sensitive to outliers in training window."
  });

  // Rolling ML MinMax
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_ml_minmax",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "Rolling ML Min-Max",
    .options = MakeRollingMLOptions(),
    .isCrossSectional = false,
    .desc = "Rolling window min-max normalization. Scales features to [0, 1] range based on "
            "training window min/max values.",
    .inputs = makePreprocessInputs(),
    .outputs = makePreprocessOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"preprocessing", "ml", "normalization", "rolling", "minmax"},
    .requiresTimeFrame = false,
    .strategyTypes = {"feature-engineering", "preprocessing"},
    .relatedTransforms = {"ml_minmax", "rolling_ml_zscore", "rolling_ml_robust"},
    .usageContext = "Use when features need to be bounded to a fixed range.",
    .limitations = "Sensitive to outliers that set extreme min/max values."
  });

  // Rolling ML Robust
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_ml_robust",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "Rolling ML Robust",
    .options = MakeRollingMLOptions(),
    .isCrossSectional = false,
    .desc = "Rolling window robust normalization using median and IQR. Less sensitive to outliers "
            "than z-score or min-max scaling.",
    .inputs = makePreprocessInputs(),
    .outputs = makePreprocessOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"preprocessing", "ml", "normalization", "rolling", "robust"},
    .requiresTimeFrame = false,
    .strategyTypes = {"feature-engineering", "preprocessing"},
    .relatedTransforms = {"ml_robust", "rolling_ml_zscore", "rolling_ml_minmax"},
    .usageContext = "Use when data contains outliers that would distort other normalization methods.",
    .limitations = "Requires more samples for stable quantile estimation."
  });

  return metadataList;
}

// =============================================================================
// Rolling Clustering Metadata (KMeans, DBSCAN)
// =============================================================================

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeRollingClusteringMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  auto makeKMeansOptions = []() -> MetaDataOptionList {
    return {
      MetaDataOption{
        .id = "max_iterations",
        .name = "Max Iterations",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1000.0),
        .min = 10,
        .max = 10000,
        .desc = "Maximum number of K-Means iterations"
      },
      MetaDataOption{
        .id = "compute_zscore",
        .name = "Z-Score Normalization",
        .type = epoch_core::MetaDataOptionType::Boolean,
        .defaultValue = MetaDataOptionDefinition(true),
        .desc = "Standardize features before clustering"
      }
    };
  };

  auto makeClusteringInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false}
    };
  };

  auto makeKMeansOutputs = [](size_t k) -> std::vector<epoch_script::transforms::IOMetaData> {
    std::vector<epoch_script::transforms::IOMetaData> outputs;
    outputs.push_back({epoch_core::IODataType::Integer, "cluster_label", "Cluster Label", true, false});
    for (size_t c = 0; c < k; ++c) {
      outputs.push_back({
        epoch_core::IODataType::Decimal,
        "cluster_" + std::to_string(c) + "_dist",
        "Distance to Cluster " + std::to_string(c),
        true, false
      });
    }
    return outputs;
  };

  // Rolling K-Means 2-5 clusters
  for (size_t k = 2; k <= 5; ++k) {
    metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "rolling_kmeans_" + std::to_string(k),
      .category = epoch_core::TransformCategory::ML,
      .plotKind = epoch_core::TransformPlotKind::kmeans,
      .name = "Rolling K-Means (" + std::to_string(k) + " Clusters)",
      .options = CombineWithRollingOptions(makeKMeansOptions()),
      .isCrossSectional = false,
      .desc = "Rolling window K-Means clustering with " + std::to_string(k) + " clusters. "
              "Retrains centroids as new data arrives for adaptive regime detection.",
      .inputs = makeClusteringInputs(),
      .outputs = makeKMeansOutputs(k),
      .atLeastOneInputRequired = true,
      .tags = {"kmeans", "ml", "clustering", "rolling", "adaptive", "regime"},
      .requiresTimeFrame = false,
      .strategyTypes = {"regime-based", "adaptive-strategy"},
      .relatedTransforms = {"kmeans_" + std::to_string(k), "rolling_gmm_" + std::to_string(k)},
      .usageContext = "Use for adaptive regime detection where cluster definitions evolve over time.",
      .limitations = "Higher computational cost. Clusters may shift between windows."
    });
  }

  // Rolling DBSCAN
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_dbscan",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::dbscan,
    .name = "Rolling DBSCAN",
    .options = CombineWithRollingOptions({
      MetaDataOption{
        .id = "epsilon",
        .name = "Epsilon (Neighborhood Radius)",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.5),
        .min = 0.001,
        .max = 10.0,
        .desc = "Maximum distance for two points to be neighbors"
      },
      MetaDataOption{
        .id = "min_points",
        .name = "Min Points",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(5.0),
        .min = 2,
        .max = 100,
        .desc = "Minimum points required to form a dense region"
      },
      MetaDataOption{
        .id = "compute_zscore",
        .name = "Z-Score Normalization",
        .type = epoch_core::MetaDataOptionType::Boolean,
        .defaultValue = MetaDataOptionDefinition(true),
        .desc = "Standardize features before clustering"
      }
    }),
    .isCrossSectional = false,
    .desc = "Rolling window DBSCAN clustering for adaptive anomaly detection. "
            "Number of clusters adapts to data density.",
    .inputs = makeClusteringInputs(),
    .outputs = {
      {epoch_core::IODataType::Integer, "cluster_label", "Cluster Label (-1 for noise)", true, false},
      {epoch_core::IODataType::Integer, "is_anomaly", "Is Anomaly (1=noise, 0=in cluster)", true, false},
      {epoch_core::IODataType::Integer, "cluster_count", "Total Clusters Found", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"dbscan", "ml", "clustering", "rolling", "anomaly", "adaptive"},
    .requiresTimeFrame = false,
    .strategyTypes = {"anomaly-detection", "adaptive-strategy"},
    .relatedTransforms = {"dbscan", "rolling_kmeans_3"},
    .usageContext = "Use for rolling anomaly detection without fixed cluster count.",
    .limitations = "Sensitive to epsilon and min_points. Density thresholds may need adjustment over time."
  });

  return metadataList;
}

// =============================================================================
// Rolling Decomposition Metadata (PCA, ICA)
// =============================================================================

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeRollingDecompositionMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  auto makeDecompositionInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false}
    };
  };

  // Rolling PCA
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_pca",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "Rolling PCA",
    .options = CombineWithRollingOptions({
      MetaDataOption{
        .id = "n_components",
        .name = "Number of Components",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(0.0),
        .min = 0,
        .max = 100,
        .desc = "Number of components to keep (0 = keep all)"
      },
      MetaDataOption{
        .id = "variance_retained",
        .name = "Variance Retained",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.0),
        .min = 0.0,
        .max = 1.0,
        .desc = "Keep components to retain this fraction of variance"
      },
      MetaDataOption{
        .id = "scale_data",
        .name = "Scale Data",
        .type = epoch_core::MetaDataOptionType::Boolean,
        .defaultValue = MetaDataOptionDefinition(true),
        .desc = "Standardize features before PCA"
      }
    }),
    .isCrossSectional = false,
    .desc = "Rolling window Principal Component Analysis. Recomputes principal components as new data "
            "arrives, adapting factor loadings to current market structure.",
    .inputs = makeDecompositionInputs(),
    .outputs = {
      {epoch_core::IODataType::Decimal, "pc_0", "Principal Component 0", true, false},
      {epoch_core::IODataType::Decimal, "pc_1", "Principal Component 1", true, false},
      {epoch_core::IODataType::Decimal, "pc_2", "Principal Component 2", true, false},
      {epoch_core::IODataType::Decimal, "pc_3", "Principal Component 3", true, false},
      {epoch_core::IODataType::Decimal, "pc_4", "Principal Component 4", true, false},
      {epoch_core::IODataType::Decimal, "explained_variance_ratio", "Cumulative Explained Variance", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"pca", "ml", "decomposition", "rolling", "factor", "adaptive"},
    .requiresTimeFrame = false,
    .strategyTypes = {"factor-investing", "risk-decomposition", "adaptive-strategy"},
    .relatedTransforms = {"pca", "rolling_ica"},
    .usageContext = "Use for adaptive factor extraction where loadings evolve over time.",
    .limitations = "Component interpretations may shift between windows. Sign flips possible."
  });

  // Rolling ICA
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "rolling_ica",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "Rolling ICA",
    .options = CombineWithRollingOptions({
      MetaDataOption{
        .id = "noise_std_dev",
        .name = "Noise Std Dev",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.175),
        .min = 0.01,
        .max = 1.0,
        .desc = "Standard deviation of Gaussian noise for RADICAL algorithm"
      },
      MetaDataOption{
        .id = "replicates",
        .name = "Replicates",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(30.0),
        .min = 5,
        .max = 100,
        .desc = "Number of perturbed replicates per data point"
      },
      MetaDataOption{
        .id = "angles",
        .name = "Angles",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(150.0),
        .min = 30,
        .max = 500,
        .desc = "Number of angles to consider in rotation search"
      }
    }),
    .isCrossSectional = false,
    .desc = "Rolling window Independent Component Analysis. Separates mixed signals into independent "
            "sources that adapt to current market dynamics.",
    .inputs = makeDecompositionInputs(),
    .outputs = {
      {epoch_core::IODataType::Decimal, "ic_0", "Independent Component 0", true, false},
      {epoch_core::IODataType::Decimal, "ic_1", "Independent Component 1", true, false},
      {epoch_core::IODataType::Decimal, "ic_2", "Independent Component 2", true, false},
      {epoch_core::IODataType::Decimal, "ic_3", "Independent Component 3", true, false},
      {epoch_core::IODataType::Decimal, "ic_4", "Independent Component 4", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"ica", "ml", "decomposition", "rolling", "factor", "adaptive"},
    .requiresTimeFrame = false,
    .strategyTypes = {"factor-investing", "signal-separation", "adaptive-strategy"},
    .relatedTransforms = {"ica", "rolling_pca"},
    .usageContext = "Use for adaptive independent source separation.",
    .limitations = "Higher computational cost. Component ordering may change between windows."
  });

  return metadataList;
}

// =============================================================================
// Rolling Probabilistic Metadata (GMM, HMM)
// =============================================================================

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeRollingProbabilisticMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  auto makeGMMOptions = []() -> MetaDataOptionList {
    return {
      MetaDataOption{
        .id = "max_iterations",
        .name = "Max Iterations",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(300.0),
        .min = 10,
        .max = 10000,
        .desc = "Maximum number of EM iterations"
      },
      MetaDataOption{
        .id = "tolerance",
        .name = "Convergence Tolerance",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(1e-10),
        .min = 1e-15,
        .max = 1e-3,
        .desc = "Convergence tolerance for EM algorithm"
      },
      MetaDataOption{
        .id = "trials",
        .name = "EM Restarts",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 1,
        .max = 10,
        .desc = "Number of EM algorithm restarts"
      },
      MetaDataOption{
        .id = "covariance_type",
        .name = "Covariance Type",
        .type = epoch_core::MetaDataOptionType::Select,
        .defaultValue = MetaDataOptionDefinition(std::string("full")),
        .selectOption = {
          {"Full", "full"},
          {"Diagonal", "diagonal"}
        },
        .desc = "Covariance matrix type"
      }
    };
  };

  auto makeHMMOptions = []() -> MetaDataOptionList {
    return {
      MetaDataOption{
        .id = "max_iterations",
        .name = "Max Iterations",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(300.0),
        .min = 10,
        .max = 10000,
        .desc = "Maximum number of EM (Baum-Welch) iterations"
      },
      MetaDataOption{
        .id = "tolerance",
        .name = "Convergence Tolerance",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(1e-10),
        .min = 1e-15,
        .max = 1e-3,
        .desc = "Convergence tolerance for Baum-Welch algorithm"
      }
    };
  };

  auto makeProbabilisticInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false}
    };
  };

  auto makeGMMOutputs = [](size_t n) -> std::vector<epoch_script::transforms::IOMetaData> {
    std::vector<epoch_script::transforms::IOMetaData> outputs;
    outputs.push_back({epoch_core::IODataType::Integer, "component", "Component", true, false});
    for (size_t c = 0; c < n; ++c) {
      outputs.push_back({
        epoch_core::IODataType::Decimal,
        "component_" + std::to_string(c) + "_prob",
        "Component " + std::to_string(c) + " Probability",
        true, false
      });
    }
    outputs.push_back({epoch_core::IODataType::Decimal, "log_likelihood", "Log Likelihood", true, false});
    return outputs;
  };

  auto makeHMMOutputs = [](size_t n) -> std::vector<epoch_script::transforms::IOMetaData> {
    std::vector<epoch_script::transforms::IOMetaData> outputs;
    outputs.push_back({epoch_core::IODataType::Integer, "state", "State", true, false});
    for (size_t s = 0; s < n; ++s) {
      outputs.push_back({
        epoch_core::IODataType::Decimal,
        "state_" + std::to_string(s) + "_prob",
        "State " + std::to_string(s) + " Probability",
        true, false
      });
    }
    return outputs;
  };

  // Rolling GMM 2-5 components
  for (size_t n = 2; n <= 5; ++n) {
    metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "rolling_gmm_" + std::to_string(n),
      .category = epoch_core::TransformCategory::ML,
      .plotKind = epoch_core::TransformPlotKind::gmm,
      .name = "Rolling GMM (" + std::to_string(n) + " Components)",
      .options = CombineWithRollingOptions(makeGMMOptions()),
      .isCrossSectional = false,
      .desc = "Rolling window Gaussian Mixture Model with " + std::to_string(n) + " components. "
              "Adapts mixture distributions to evolving market conditions.",
      .inputs = makeProbabilisticInputs(),
      .outputs = makeGMMOutputs(n),
      .atLeastOneInputRequired = true,
      .tags = {"gmm", "ml", "clustering", "rolling", "probabilistic", "regime"},
      .requiresTimeFrame = false,
      .strategyTypes = {"regime-based", "anomaly-detection", "adaptive-strategy"},
      .relatedTransforms = {"gmm_" + std::to_string(n), "rolling_hmm_" + std::to_string(n)},
      .usageContext = "Use for adaptive probabilistic regime detection.",
      .limitations = "Component labels may swap between windows. Higher computational cost."
    });
  }

  // Rolling HMM 2-5 states
  for (size_t n = 2; n <= 5; ++n) {
    metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "rolling_hmm_" + std::to_string(n),
      .category = epoch_core::TransformCategory::ML,
      .plotKind = epoch_core::TransformPlotKind::hmm,
      .name = "Rolling HMM (" + std::to_string(n) + " States)",
      .options = CombineWithRollingOptions(makeHMMOptions()),
      .isCrossSectional = false,
      .desc = "Rolling window Hidden Markov Model with " + std::to_string(n) + " states. "
              "Adapts state transitions and emissions to evolving market dynamics.",
      .inputs = makeProbabilisticInputs(),
      .outputs = makeHMMOutputs(n),
      .atLeastOneInputRequired = true,
      .tags = {"hmm", "ml", "sequence", "rolling", "probabilistic", "regime"},
      .requiresTimeFrame = false,
      .strategyTypes = {"regime-based", "sequential", "adaptive-strategy"},
      .relatedTransforms = {"hmm_" + std::to_string(n), "rolling_gmm_" + std::to_string(n)},
      .usageContext = "Use for adaptive sequential regime detection with temporal dependencies.",
      .limitations = "State labels may swap between windows. Higher computational cost."
    });
  }

  return metadataList;
}

// =============================================================================
// Master function to register all rolling ML metadata
// =============================================================================

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeAllRollingMLMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> all;

  auto append = [&all](std::vector<epoch_script::transforms::TransformsMetaData> mds) {
    all.insert(all.end(), mds.begin(), mds.end());
  };

  append(MakeRollingLightGBMMetaData());
  append(MakeRollingLiblinearMetaData());
  append(MakeRollingMLPreprocessMetaData());
  append(MakeRollingClusteringMetaData());
  append(MakeRollingDecompositionMetaData());
  append(MakeRollingProbabilisticMetaData());

  return all;
}

} // namespace epoch_script::transform
