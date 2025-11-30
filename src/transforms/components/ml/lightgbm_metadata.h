#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

/**
 * @brief Create LightGBM metadata for classifier and regressor transforms
 *
 * Variants:
 * - lightgbm_classifier: Binary/multiclass classification
 * - lightgbm_regressor: Return prediction
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeLightGBMMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Common options for all LightGBM transforms
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
      },
      MetaDataOption{
        .id = "lookback_window",
        .name = "Lookback Window",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(0.0),
        .min = 0,
        .desc = "Number of bars for training (0 = use all data for research mode)"
      },
      MetaDataOption{
        .id = "min_training_samples",
        .name = "Min Training Samples",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(100.0),
        .min = 10,
        .max = 10000,
        .desc = "Minimum samples required for training"
      },
    };
  };

  // Common inputs - SLOT approach for features + target
  auto makeLightGBMInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false},
      {epoch_core::IODataType::Number, "target", "Target", false, false}
    };
  };

  // Classifier outputs
  auto makeClassifierOutputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Integer, "prediction", "Prediction", true, false},
      {epoch_core::IODataType::Decimal, "probability", "Probability", true, false}
    };
  };

  // Regressor outputs
  auto makeRegressorOutputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Decimal, "prediction", "Prediction", true, false}
    };
  };

  // lightgbm_classifier
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "lightgbm_classifier",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::lightgbm,
    .name = "LightGBM Classifier",
    .options = makeLightGBMOptions(),
    .isCrossSectional = false,
    .desc = "Gradient boosting classifier using LightGBM. Supports binary and multiclass classification. "
            "Outputs class prediction and probability of the predicted class.",
    .inputs = makeLightGBMInputs(),
    .outputs = makeClassifierOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"lightgbm", "ml", "classification", "gradient-boosting", "gbdt"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "signal-generation"},
    .relatedTransforms = {"lightgbm_regressor", "logistic_l1", "logistic_l2"},
    .usageContext = "Use for direction prediction or regime classification. "
                    "More powerful than linear models for capturing nonlinear patterns.",
    .limitations = "Risk of overfitting with small datasets. Requires careful hyperparameter tuning. "
                   "Feature scaling recommended but less critical than linear models."
  });

  // lightgbm_regressor
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "lightgbm_regressor",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::lightgbm,
    .name = "LightGBM Regressor",
    .options = makeLightGBMOptions(),
    .isCrossSectional = false,
    .desc = "Gradient boosting regressor using LightGBM for return prediction. "
            "Outputs continuous prediction value.",
    .inputs = makeLightGBMInputs(),
    .outputs = makeRegressorOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"lightgbm", "ml", "regression", "gradient-boosting", "gbdt"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "return-prediction"},
    .relatedTransforms = {"lightgbm_classifier", "svr_l1", "svr_l2"},
    .usageContext = "Use for return prediction. "
                    "Can capture nonlinear relationships between features and returns.",
    .limitations = "Risk of overfitting with small datasets. May overfit to noise in financial data. "
                   "Requires careful cross-validation and regularization."
  });

  return metadataList;
}

} // namespace epoch_script::transform
