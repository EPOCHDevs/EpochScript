#pragma once
// Statistical analysis transforms registration
// Provides clustering, dimensionality reduction, regime detection, and outlier handling
//
// Categories:
// 1. Clustering - group data points by similarity
//    - K-Means (fixed number of centroids)
//    - DBSCAN (density-based, automatic cluster count, anomaly detection)
// 2. Dimensionality Reduction - extract latent factors
//    - PCA (principal components, variance decomposition)
// 3. Probabilistic Models - sequence-aware regime detection
//    - HMM (Hidden Markov Models with temporal dependencies)
// 4. Outlier Handling
//    - Winsorize (cap extreme values at percentiles)

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "kmeans.h"
#include "dbscan.h"
#include "pca.h"
#include "hmm.h"
#include "winsorize.h"

// Metadata definitions
#include "clustering_metadata.h"

namespace epoch_script::transform::statistics {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;
using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// HMM Metadata Factory (static HMM transforms - not in clustering_metadata.h)
// =============================================================================

inline std::vector<TransformsMetaData> MakeHMMMetaData() {
    std::vector<TransformsMetaData> metadataList;

    auto makeHMMOptions = []() -> MetaDataOptionList {
        return {
            MetaDataOption{
                .id = "max_iterations",
                .name = "Max Iterations",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = MetaDataOptionDefinition(1000.0),
                .min = 10,
                .max = 10000,
                .desc = "Maximum Baum-Welch iterations for HMM training"
            },
            MetaDataOption{
                .id = "tolerance",
                .name = "Convergence Tolerance",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = MetaDataOptionDefinition(1e-5),
                .min = 1e-15,
                .max = 1e-3,
                .desc = "Stops training when log-likelihood improvement falls below this"
            },
            MetaDataOption{
                .id = "min_training_samples",
                .name = "Min Training Samples",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = MetaDataOptionDefinition(100.0),
                .min = 20,
                .max = 10000,
                .desc = "Minimum observations required for HMM training"
            },
            MetaDataOption{
                .id = "split_ratio",
                .name = "Training Split Ratio",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = MetaDataOptionDefinition(1.0),
                .min = 0.1,
                .max = 1.0,
                .desc = "Ratio of data to use for training (1.0 = all data for research mode)"
            },
            MetaDataOption{
                .id = "split_gap",
                .name = "Purge Gap",
                .type = epoch_core::MetaDataOptionType::Integer,
                .defaultValue = MetaDataOptionDefinition(0.0),
                .min = 0,
                .desc = "Gap between training and test data (Marcos Lopez de Prado purging)"
            }
        };
    };

    auto makeHMMInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
        return {
            {epoch_core::IODataType::Number, "SLOT", "Features", true, false}
        };
    };

    auto makeHMMOutputs = [](size_t n) -> std::vector<epoch_script::transforms::IOMetaData> {
        std::vector<epoch_script::transforms::IOMetaData> outputs;
        outputs.push_back({epoch_core::IODataType::Integer, "state", "Most Likely State", true, false});
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

    // HMM 2-5 states
    for (size_t n = 2; n <= 5; ++n) {
        metadataList.emplace_back(TransformsMetaData{
            .id = "hmm_" + std::to_string(n),
            .category = epoch_core::TransformCategory::ML,
            .plotKind = epoch_core::TransformPlotKind::hmm,
            .name = "HMM (" + std::to_string(n) + " States)",
            .options = makeHMMOptions(),
            .isCrossSectional = false,
            .desc = "Hidden Markov Model with " + std::to_string(n) + " hidden states and Gaussian emissions. "
                    "Captures sequential dependencies for regime detection with temporal transitions.",
            .inputs = makeHMMInputs(),
            .outputs = makeHMMOutputs(n),
            .atLeastOneInputRequired = true,
            .tags = {"hmm", "ml", "regime", "sequence", "probabilistic", "unsupervised"},
            .requiresTimeFrame = false,
            .strategyTypes = {"regime-based", "mean-reversion", "trend-following"},
            .relatedTransforms = {"kmeans_" + std::to_string(n), "rolling_hmm_" + std::to_string(n)},
            .usageContext = "Use for regime detection when temporal dependencies matter. States capture market "
                           "conditions (bull/bear/sideways). Transition probabilities show regime persistence.",
            .limitations = "Sensitive to initialization. State labels may not be consistent across runs. "
                          "Assumes Gaussian emissions which may not fit fat-tailed financial returns."
        });
    }

    return metadataList;
}

// =============================================================================
// Registration function - registers all statistics transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // CLUSTERING - K-MEANS
    // =========================================================================
    // Centroid-based clustering that partitions data into K groups.
    // Use for: regime detection, asset grouping, risk state classification.
    // Distance to centroids provides regime certainty measure.

    // kmeans_2: 2-cluster regime detection (e.g., bull/bear)
    epoch_script::transform::Register<KMeans2Transform>("kmeans_2");

    // kmeans_3: 3-cluster regime detection (e.g., bull/bear/sideways)
    epoch_script::transform::Register<KMeans3Transform>("kmeans_3");

    // kmeans_4: 4-cluster regime detection
    epoch_script::transform::Register<KMeans4Transform>("kmeans_4");

    // kmeans_5: 5-cluster regime detection
    epoch_script::transform::Register<KMeans5Transform>("kmeans_5");

    // =========================================================================
    // CLUSTERING - DBSCAN
    // =========================================================================
    // Density-based clustering that finds clusters of arbitrary shape.
    // Automatically determines cluster count and identifies outliers as noise.
    // Use for: anomaly detection, regime discovery without fixed K.

    // dbscan: Density-based clustering with anomaly detection
    // Outputs: cluster_label (-1 = anomaly), is_anomaly, cluster_count
    epoch_script::transform::Register<DBSCANTransform>("dbscan");

    // =========================================================================
    // DIMENSIONALITY REDUCTION - PCA
    // =========================================================================
    // Principal Component Analysis extracts uncorrelated factors from correlated inputs.
    // PC0 typically captures market beta, subsequent PCs capture sector/style factors.
    // Use for: factor extraction, risk decomposition, feature reduction for ML.

    // pca: Principal Component Analysis
    // Outputs: pc_0 through pc_4, explained_variance_ratio
    epoch_script::transform::Register<PCATransform>("pca");

    // =========================================================================
    // PROBABILISTIC MODELS - HMM
    // =========================================================================
    // Hidden Markov Models for sequential regime detection.
    // Captures temporal dependencies - states have transition probabilities.
    // Use for: regime-based strategies where regime persistence matters.

    // hmm_2: 2-state HMM (e.g., high/low volatility regimes)
    epoch_script::transform::Register<HMM2Transform>("hmm_2");

    // hmm_3: 3-state HMM (e.g., bull/bear/sideways market regimes)
    epoch_script::transform::Register<HMM3Transform>("hmm_3");

    // hmm_4: 4-state HMM (more granular regime detection)
    epoch_script::transform::Register<HMM4Transform>("hmm_4");

    // hmm_5: 5-state HMM (fine-grained regime detection)
    epoch_script::transform::Register<HMM5Transform>("hmm_5");

    // =========================================================================
    // OUTLIER HANDLING - WINSORIZE
    // =========================================================================
    // Caps extreme values at specified percentiles.
    // Preserves data points while reducing outlier impact.
    // Use for: cleaning fundamental data, preprocessing features for ML.

    // winsorize: Cap extreme values at percentile cutoffs
    // Default: lower=5%, upper=95%
    epoch_script::transform::Register<Winsorize>("winsorize");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // K-Means clustering metadata (kmeans_2 through kmeans_5)
    for (const auto& metadata : MakeKMeansMetaData()) {
        metaRegistry.Register(metadata);
    }

    // DBSCAN clustering metadata
    for (const auto& metadata : MakeDBSCANMetaData()) {
        metaRegistry.Register(metadata);
    }

    // PCA dimensionality reduction metadata
    for (const auto& metadata : MakePCAMetaData()) {
        metaRegistry.Register(metadata);
    }

    // HMM probabilistic model metadata (hmm_2 through hmm_5)
    for (const auto& metadata : MakeHMMMetaData()) {
        metaRegistry.Register(metadata);
    }

    // Winsorize outlier handling metadata
    for (const auto& metadata : MakeWinsorizeMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::statistics
