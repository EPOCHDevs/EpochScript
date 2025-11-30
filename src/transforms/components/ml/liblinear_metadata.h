#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

/**
 * @brief Create LIBLINEAR metadata for 4 transform variants
 *
 * Variants:
 * - logistic_l1: L1-regularized Logistic Regression (sparse feature selection)
 * - logistic_l2: L2-regularized Logistic Regression (stable, all features)
 * - svr_l1: L1-regularized Support Vector Regression (sparse)
 * - svr_l2: L2-regularized Support Vector Regression (stable)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeLiblinearMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Common options for all LIBLINEAR transforms
  auto makeLinearOptions = []() -> MetaDataOptionList {
    return {
      MetaDataOption{
        .id = "C",
        .name = "Regularization (C)",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 0.0001,
        .max = 1000.0,
        .desc = "Regularization parameter. Higher = less regularization"
      },
      MetaDataOption{
        .id = "eps",
        .name = "Tolerance",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.01),
        .min = 0.00001,
        .max = 1.0,
        .desc = "Stopping tolerance for optimization"
      },
      MetaDataOption{
        .id = "bias",
        .name = "Bias Term",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = -1.0,
        .desc = "Bias term (-1 to disable)"
      },
      MetaDataOption{
        .id = "lookback_window",
        .name = "Lookback Window",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(252.0),
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
  auto makeLinearInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false},
      {epoch_core::IODataType::Number, "target", "Target", false, false}
    };
  };

  // Classifier outputs (logistic models)
  auto makeClassifierOutputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Integer, "prediction", "Prediction", true, false},
      {epoch_core::IODataType::Decimal, "probability", "Probability", true, false},
      {epoch_core::IODataType::Decimal, "decision_value", "Decision Value", true, false}
    };
  };

  // Regressor outputs (SVR models)
  auto makeRegressorOutputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Decimal, "prediction", "Prediction", true, false}
    };
  };

  // logistic_l1 - L1-regularized Logistic Regression
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "logistic_l1",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::linear_model,
    .name = "Logistic Regression (L1)",
    .options = makeLinearOptions(),
    .isCrossSectional = false,
    .desc = "L1-regularized Logistic Regression for binary classification. "
            "L1 penalty produces sparse solutions, effectively performing feature selection. "
            "Outputs class prediction, probability, and decision value.",
    .inputs = makeLinearInputs(),
    .outputs = makeClassifierOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"liblinear", "ml", "classification", "logistic", "l1", "sparse"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "signal-generation"},
    .relatedTransforms = {"logistic_l2", "svr_l1", "svr_l2", "lightgbm_classifier"},
    .usageContext = "Use for direction prediction when you want automatic feature selection. "
                    "L1 penalty drives unimportant feature weights to zero.",
    .limitations = "Binary classification only. Target should be 0/1 or -1/+1. "
                   "Feature scaling recommended."
  });

  // logistic_l2 - L2-regularized Logistic Regression
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "logistic_l2",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::linear_model,
    .name = "Logistic Regression (L2)",
    .options = makeLinearOptions(),
    .isCrossSectional = false,
    .desc = "L2-regularized Logistic Regression for binary classification. "
            "L2 penalty shrinks all coefficients uniformly, providing stable predictions. "
            "Outputs class prediction, probability, and decision value.",
    .inputs = makeLinearInputs(),
    .outputs = makeClassifierOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"liblinear", "ml", "classification", "logistic", "l2", "ridge"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "signal-generation"},
    .relatedTransforms = {"logistic_l1", "svr_l1", "svr_l2", "lightgbm_classifier"},
    .usageContext = "Use for direction prediction when you want to keep all features. "
                    "More stable than L1 when features are correlated.",
    .limitations = "Binary classification only. Target should be 0/1 or -1/+1. "
                   "Feature scaling recommended."
  });

  // svr_l1 - L1-regularized Support Vector Regression
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "svr_l1",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::linear_model,
    .name = "SVR (L1)",
    .options = makeLinearOptions(),
    .isCrossSectional = false,
    .desc = "L1-regularized Support Vector Regression for return prediction. "
            "L1 penalty produces sparse solutions with automatic feature selection. "
            "Outputs continuous prediction value.",
    .inputs = makeLinearInputs(),
    .outputs = makeRegressorOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"liblinear", "ml", "regression", "svr", "l1", "sparse"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "return-prediction"},
    .relatedTransforms = {"svr_l2", "logistic_l1", "logistic_l2", "lightgbm_regressor"},
    .usageContext = "Use for return prediction when you want automatic feature selection. "
                    "L1 penalty identifies the most predictive features.",
    .limitations = "Linear model - cannot capture nonlinear relationships. "
                   "Feature scaling recommended. May underfit complex patterns."
  });

  // svr_l2 - L2-regularized Support Vector Regression
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "svr_l2",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::linear_model,
    .name = "SVR (L2)",
    .options = makeLinearOptions(),
    .isCrossSectional = false,
    .desc = "L2-regularized Support Vector Regression for return prediction. "
            "L2 penalty provides stable predictions using all features. "
            "Outputs continuous prediction value.",
    .inputs = makeLinearInputs(),
    .outputs = makeRegressorOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"liblinear", "ml", "regression", "svr", "l2", "ridge"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-based", "return-prediction"},
    .relatedTransforms = {"svr_l1", "logistic_l1", "logistic_l2", "lightgbm_regressor"},
    .usageContext = "Use for return prediction when you want stable coefficient estimates. "
                    "L2 penalty handles correlated features better than L1.",
    .limitations = "Linear model - cannot capture nonlinear relationships. "
                   "Feature scaling recommended. May underfit complex patterns."
  });

  return metadataList;
}

} // namespace epoch_script::transform
