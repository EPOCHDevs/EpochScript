#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

/**
 * @brief Create ML preprocessing metadata
 *
 * Variants:
 * - ml_zscore: Standardization (z-score normalization)
 * - ml_minmax: Min-Max scaling to [0,1]
 * - ml_robust: Robust scaling using median and IQR
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeMLPreprocessMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Common option: split_ratio for all preprocessing transforms
  auto makeSplitRatioOption = []() -> MetaDataOption {
    return MetaDataOption{
      .id = "split_ratio",
      .name = "Train Split Ratio",
      .type = epoch_core::MetaDataOptionType::Decimal,
      .defaultValue = MetaDataOptionDefinition(0.7),
      .min = 0.1,
      .max = 1.0,
      .desc = "Fraction of data used for fitting scaling parameters (0.7 = first 70% is training)"
    };
  };

  // Common inputs - SLOT approach for multiple features
  auto makePreprocessInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Decimal, "SLOT", "Features", true, false}
    };
  };

  // Dynamic outputs - one scaled column per input
  // Note: outputs are generated dynamically based on number of inputs
  auto makePreprocessOutputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Decimal, "scaled_0", "Scaled Feature 0", true, false},
      {epoch_core::IODataType::Decimal, "scaled_1", "Scaled Feature 1", true, false},
      {epoch_core::IODataType::Decimal, "scaled_2", "Scaled Feature 2", true, false},
      {epoch_core::IODataType::Decimal, "scaled_3", "Scaled Feature 3", true, false}
    };
  };

  // ml_zscore
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "ml_zscore",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "ML Z-Score Standardization",
    .options = {makeSplitRatioOption()},
    .isCrossSectional = false,
    .desc = "Standardizes features by removing mean and scaling to unit variance. "
            "Fits parameters on training data (split_ratio), applies to full dataset. "
            "Formula: z = (x - mean) / std",
    .inputs = makePreprocessInputs(),
    .outputs = makePreprocessOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"ml", "preprocessing", "zscore", "standardization", "normalization", "feature-scaling"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-pipeline", "feature-engineering"},
    .relatedTransforms = {"ml_minmax", "ml_robust", "zscore"},
    .usageContext = "Use before ML models to standardize features to zero mean and unit variance. "
                    "Essential for algorithms sensitive to feature scaling (linear models, SVM, neural networks). "
                    "Fit on training data to prevent data leakage.",
    .limitations = "Assumes approximately Gaussian distribution. Sensitive to outliers which affect mean and std. "
                   "For non-Gaussian data with outliers, consider ml_robust instead."
  });

  // ml_minmax
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "ml_minmax",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "ML Min-Max Scaling",
    .options = {makeSplitRatioOption()},
    .isCrossSectional = false,
    .desc = "Scales features to [0, 1] range using min and max values. "
            "Fits parameters on training data (split_ratio), applies to full dataset. "
            "Formula: x_scaled = (x - min) / (max - min)",
    .inputs = makePreprocessInputs(),
    .outputs = makePreprocessOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"ml", "preprocessing", "minmax", "scaling", "normalization", "feature-scaling"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-pipeline", "feature-engineering"},
    .relatedTransforms = {"ml_zscore", "ml_robust"},
    .usageContext = "Use when you need features in a bounded [0,1] range. "
                    "Good for neural networks with sigmoid activations or algorithms that expect bounded inputs. "
                    "Values can exceed [0,1] on test data if outside training range.",
    .limitations = "Very sensitive to outliers which determine min/max. "
                   "Test data can have values outside [0,1] if it exceeds training range. "
                   "Does not center data - use ml_zscore if centering is needed."
  });

  // ml_robust
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "ml_robust",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "ML Robust Scaling",
    .options = {makeSplitRatioOption()},
    .isCrossSectional = false,
    .desc = "Scales features using statistics robust to outliers (median and IQR). "
            "Fits parameters on training data (split_ratio), applies to full dataset. "
            "Formula: x_scaled = (x - median) / IQR",
    .inputs = makePreprocessInputs(),
    .outputs = makePreprocessOutputs(),
    .atLeastOneInputRequired = true,
    .tags = {"ml", "preprocessing", "robust", "scaling", "outlier-resistant", "feature-scaling"},
    .requiresTimeFrame = false,
    .strategyTypes = {"ml-pipeline", "feature-engineering"},
    .relatedTransforms = {"ml_zscore", "ml_minmax"},
    .usageContext = "Use for data with outliers or non-Gaussian distributions. "
                    "Financial data often has fat tails - this is more appropriate than ml_zscore. "
                    "Centers on median and scales by IQR (interquartile range).",
    .limitations = "IQR may be small or zero for low-variance features. "
                   "Does not bound output range like ml_minmax. "
                   "Slightly less interpretable than standard z-scores."
  });

  return metadataList;
}

} // namespace epoch_script::transform
