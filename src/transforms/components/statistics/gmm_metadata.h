#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

/**
 * @brief Create GMM metadata for 2-5 component variants
 *
 * GMM (Gaussian Mixture Model) performs static clustering without temporal dependencies.
 * Unlike HMM, GMM treats each observation independently.
 *
 * Financial Applications:
 * - Return distribution modeling (fat tails, multiple regimes)
 * - Static regime classification (without temporal dependencies)
 * - Outlier/anomaly detection (low log-likelihood = unusual observation)
 * - Cross-sectional clustering by return characteristics
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeGMMMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // Common options shared by all GMM variants
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
        .id = "min_training_samples",
        .name = "Min Training Samples",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(100.0),
        .min = 10,
        .max = 10000,
        .desc = "Minimum number of samples required for training"
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
        .id = "trials",
        .name = "EM Restarts",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(1.0),
        .min = 1,
        .max = 10,
        .desc = "Number of EM algorithm restarts to avoid local minima"
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
        .desc = "Covariance matrix type: 'full' for correlated features, 'diagonal' for independent"
      }
    };
  };

  // Common inputs - SLOT approach for features
  auto makeGMMInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
    return {
      {epoch_core::IODataType::Number, "SLOT", "Features", true, false}
    };
  };

  // Helper to generate outputs for N components
  auto makeGMMOutputs = [](size_t n_components) -> std::vector<epoch_script::transforms::IOMetaData> {
    std::vector<epoch_script::transforms::IOMetaData> outputs;

    // Component assignment (most likely component)
    outputs.push_back({epoch_core::IODataType::Integer, "component", "Component", true, false});

    // Probability for each component
    for (size_t c = 0; c < n_components; ++c) {
      outputs.push_back({
        epoch_core::IODataType::Decimal,
        "component_" + std::to_string(c) + "_prob",
        "Component " + std::to_string(c) + " Probability",
        true,
        false
      });
    }

    // Log-likelihood for anomaly detection
    outputs.push_back({epoch_core::IODataType::Decimal, "log_likelihood", "Log Likelihood", true, false});

    return outputs;
  };

  // GMM 2-component
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "gmm_2",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::gmm,
    .name = "GMM (2 Components)",
    .options = makeGMMOptions(),
    .isCrossSectional = false,
    .desc = "Gaussian Mixture Model with 2 components for static regime clustering. "
            "Outputs component assignment, posterior probabilities, and log-likelihood for anomaly detection.",
    .inputs = makeGMMInputs(),
    .outputs = makeGMMOutputs(2),
    .atLeastOneInputRequired = true,
    .tags = {"gmm", "ml", "clustering", "unsupervised", "regime", "anomaly"},
    .requiresTimeFrame = false,
    .strategyTypes = {"regime-based", "anomaly-detection", "clustering"},
    .relatedTransforms = {"hmm_2", "gmm_3", "gmm_4", "gmm_5"},
    .usageContext = "Use for simple binary regime detection (e.g., high/low volatility) or anomaly detection. "
                    "Unlike HMM, treats observations independently without temporal transitions.",
    .limitations = "No temporal transitions - each observation classified independently. "
                   "Requires sufficient samples per component. Sensitive to feature scaling."
  });

  // GMM 3-component
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "gmm_3",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::gmm,
    .name = "GMM (3 Components)",
    .options = makeGMMOptions(),
    .isCrossSectional = false,
    .desc = "Gaussian Mixture Model with 3 components for static regime clustering. "
            "Outputs component assignment, posterior probabilities, and log-likelihood for anomaly detection.",
    .inputs = makeGMMInputs(),
    .outputs = makeGMMOutputs(3),
    .atLeastOneInputRequired = true,
    .tags = {"gmm", "ml", "clustering", "unsupervised", "regime", "anomaly"},
    .requiresTimeFrame = false,
    .strategyTypes = {"regime-based", "anomaly-detection", "clustering"},
    .relatedTransforms = {"hmm_3", "gmm_2", "gmm_4", "gmm_5"},
    .usageContext = "Use for three-regime detection (e.g., bear/neutral/bull) or anomaly detection. "
                    "Unlike HMM, treats observations independently without temporal transitions.",
    .limitations = "No temporal transitions - each observation classified independently. "
                   "Requires sufficient samples per component. Sensitive to feature scaling."
  });

  // GMM 4-component
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "gmm_4",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::gmm,
    .name = "GMM (4 Components)",
    .options = makeGMMOptions(),
    .isCrossSectional = false,
    .desc = "Gaussian Mixture Model with 4 components for static regime clustering. "
            "Outputs component assignment, posterior probabilities, and log-likelihood for anomaly detection.",
    .inputs = makeGMMInputs(),
    .outputs = makeGMMOutputs(4),
    .atLeastOneInputRequired = true,
    .tags = {"gmm", "ml", "clustering", "unsupervised", "regime", "anomaly"},
    .requiresTimeFrame = false,
    .strategyTypes = {"regime-based", "anomaly-detection", "clustering"},
    .relatedTransforms = {"hmm_4", "gmm_2", "gmm_3", "gmm_5"},
    .usageContext = "Use for four-regime detection or cross-sectional clustering. "
                    "Unlike HMM, treats observations independently without temporal transitions.",
    .limitations = "No temporal transitions - each observation classified independently. "
                   "Requires more samples than simpler models. Sensitive to feature scaling."
  });

  // GMM 5-component
  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "gmm_5",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::gmm,
    .name = "GMM (5 Components)",
    .options = makeGMMOptions(),
    .isCrossSectional = false,
    .desc = "Gaussian Mixture Model with 5 components for static regime clustering. "
            "Outputs component assignment, posterior probabilities, and log-likelihood for anomaly detection.",
    .inputs = makeGMMInputs(),
    .outputs = makeGMMOutputs(5),
    .atLeastOneInputRequired = true,
    .tags = {"gmm", "ml", "clustering", "unsupervised", "regime", "anomaly"},
    .requiresTimeFrame = false,
    .strategyTypes = {"regime-based", "anomaly-detection", "clustering"},
    .relatedTransforms = {"hmm_5", "gmm_2", "gmm_3", "gmm_4"},
    .usageContext = "Use for five-regime detection or fine-grained cross-sectional clustering. "
                    "Unlike HMM, treats observations independently without temporal transitions.",
    .limitations = "No temporal transitions - each observation classified independently. "
                   "Most complex variant - requires many samples. Risk of overfitting. Sensitive to feature scaling."
  });

  return metadataList;
}

} // namespace epoch_script::transform
