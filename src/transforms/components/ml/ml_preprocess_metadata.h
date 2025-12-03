#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

namespace detail {

/**
 * @brief Generate N inputs with names feature_0, feature_1, ..., feature_{n-1}
 */
inline std::vector<epoch_script::transforms::IOMetaData> MakeNInputs(int n) {
  std::vector<epoch_script::transforms::IOMetaData> inputs;
  inputs.reserve(n);
  for (int i = 0; i < n; ++i) {
    inputs.push_back({
      .type = epoch_core::IODataType::Decimal,
      .id = "feature_" + std::to_string(i),
      .name = "Feature " + std::to_string(i),
      .allowMultipleConnections = false,
      .isFilter = false
    });
  }
  return inputs;
}

/**
 * @brief Generate N outputs with names scaled_0, scaled_1, ..., scaled_{n-1}
 */
inline std::vector<epoch_script::transforms::IOMetaData> MakeNOutputs(int n) {
  std::vector<epoch_script::transforms::IOMetaData> outputs;
  outputs.reserve(n);
  for (int i = 0; i < n; ++i) {
    outputs.push_back({
      .type = epoch_core::IODataType::Decimal,
      .id = "scaled_" + std::to_string(i),
      .name = "Scaled Feature " + std::to_string(i),
      .allowMultipleConnections = false,
      .isFilter = false
    });
  }
  return outputs;
}

/**
 * @brief Common split_ratio option for all preprocessing transforms
 */
inline MetaDataOption MakeSplitRatioOption() {
  return MetaDataOption{
    .id = "split_ratio",
    .name = "Train Split Ratio",
    .type = epoch_core::MetaDataOptionType::Decimal,
    .defaultValue = MetaDataOptionDefinition(0.7),
    .min = 0.1,
    .max = 1.0,
    .desc = "Fraction of data used for fitting scaling parameters (0.7 = first 70% is training)"
  };
}

} // namespace detail

/**
 * @brief Create ML preprocessing metadata
 *
 * Creates N→N template variants for each preprocessing transform:
 * - ml_zscore_2, ml_zscore_3, ..., ml_zscore_6
 * - ml_minmax_2, ml_minmax_3, ..., ml_minmax_6
 * - ml_robust_2, ml_robust_3, ..., ml_robust_6
 *
 * Each variant has N inputs (feature_0, ..., feature_{n-1})
 * and N outputs (scaled_0, ..., scaled_{n-1}).
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeMLPreprocessMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Generate variants for N = 2 to 6 features
  for (int n = 2; n <= 6; ++n) {
    const std::string suffix = "_" + std::to_string(n);

    // ml_zscore_N
    metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "ml_zscore" + suffix,
      .category = epoch_core::TransformCategory::ML,
      .plotKind = epoch_core::TransformPlotKind::panel_line,
      .name = "ML Z-Score (" + std::to_string(n) + " features)",
      .options = {detail::MakeSplitRatioOption()},
      .isCrossSectional = false,
      .desc = "Standardizes " + std::to_string(n) + " features by removing mean and scaling to unit variance. "
              "Fits parameters on training data (split_ratio), applies to full dataset. "
              "Formula: z = (x - mean) / std",
      .inputs = detail::MakeNInputs(n),
      .outputs = detail::MakeNOutputs(n),
      .atLeastOneInputRequired = true,
      .tags = {"ml", "preprocessing", "zscore", "standardization", "normalization", "feature-scaling"},
      .requiresTimeFrame = false,
      .strategyTypes = {"ml-pipeline", "feature-engineering"},
      .relatedTransforms = {"ml_minmax" + suffix, "ml_robust" + suffix, "zscore"},
      .usageContext = "Use before ML models to standardize features to zero mean and unit variance. "
                      "Essential for algorithms sensitive to feature scaling (linear models, SVM, neural networks). "
                      "Fit on training data to prevent data leakage.",
      .limitations = "Assumes approximately Gaussian distribution. Sensitive to outliers which affect mean and std. "
                     "For non-Gaussian data with outliers, consider ml_robust instead."
    });

    // ml_minmax_N
    metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "ml_minmax" + suffix,
      .category = epoch_core::TransformCategory::ML,
      .plotKind = epoch_core::TransformPlotKind::panel_line,
      .name = "ML Min-Max (" + std::to_string(n) + " features)",
      .options = {detail::MakeSplitRatioOption()},
      .isCrossSectional = false,
      .desc = "Scales " + std::to_string(n) + " features to [0, 1] range using min and max values. "
              "Fits parameters on training data (split_ratio), applies to full dataset. "
              "Formula: x_scaled = (x - min) / (max - min)",
      .inputs = detail::MakeNInputs(n),
      .outputs = detail::MakeNOutputs(n),
      .atLeastOneInputRequired = true,
      .tags = {"ml", "preprocessing", "minmax", "scaling", "normalization", "feature-scaling"},
      .requiresTimeFrame = false,
      .strategyTypes = {"ml-pipeline", "feature-engineering"},
      .relatedTransforms = {"ml_zscore" + suffix, "ml_robust" + suffix},
      .usageContext = "Use when you need features in a bounded [0,1] range. "
                      "Good for neural networks with sigmoid activations or algorithms that expect bounded inputs. "
                      "Values can exceed [0,1] on test data if outside training range.",
      .limitations = "Very sensitive to outliers which determine min/max. "
                     "Test data can have values outside [0,1] if it exceeds training range. "
                     "Does not center data - use ml_zscore if centering is needed."
    });

    // ml_robust_N
    metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "ml_robust" + suffix,
      .category = epoch_core::TransformCategory::ML,
      .plotKind = epoch_core::TransformPlotKind::panel_line,
      .name = "ML Robust (" + std::to_string(n) + " features)",
      .options = {detail::MakeSplitRatioOption()},
      .isCrossSectional = false,
      .desc = "Scales " + std::to_string(n) + " features using statistics robust to outliers (median and IQR). "
              "Fits parameters on training data (split_ratio), applies to full dataset. "
              "Formula: x_scaled = (x - median) / IQR",
      .inputs = detail::MakeNInputs(n),
      .outputs = detail::MakeNOutputs(n),
      .atLeastOneInputRequired = true,
      .tags = {"ml", "preprocessing", "robust", "scaling", "outlier-resistant", "feature-scaling"},
      .requiresTimeFrame = false,
      .strategyTypes = {"ml-pipeline", "feature-engineering"},
      .relatedTransforms = {"ml_zscore" + suffix, "ml_minmax" + suffix},
      .usageContext = "Use for data with outliers or non-Gaussian distributions. "
                      "Financial data often has fat tails - this is more appropriate than ml_zscore. "
                      "Centers on median and scales by IQR (interquartile range).",
      .limitations = "IQR may be small or zero for low-variance features. "
                     "Does not bound output range like ml_minmax. "
                     "Slightly less interpretable than standard z-scores."
    });
  }

  return metadataList;
}

} // namespace epoch_script::transform
