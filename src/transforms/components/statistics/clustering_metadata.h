#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

/**
 * @brief Create K-Means metadata for 2-5 cluster variants
 *
 * K-Means performs centroid-based clustering, partitioning data into K groups.
 *
 * Financial Applications:
 * - Market regime detection
 * - Asset grouping by behavior patterns
 * - Risk state identification
 * - Factor-based clustering
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeKMeansMetaData() {
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
        .desc = "Gap between training and test data (Marcos López de Prado purging)"
      }
    };
  };

  auto makeKMeansInputs = []() -> std::vector<epoch_script::transforms::IOMetaData> {
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

  // K-Means 2-5 clusters
  for (size_t k = 2; k <= 5; ++k) {
    metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
      .id = "kmeans_" + std::to_string(k),
      .category = epoch_core::TransformCategory::ML,
      .plotKind = epoch_core::TransformPlotKind::kmeans,
      .name = "K-Means (" + std::to_string(k) + " Clusters)",
      .options = makeKMeansOptions(),
      .isCrossSectional = false,
      .desc = "K-Means clustering with " + std::to_string(k) + " clusters. "
              "Partitions multi-dimensional data into " + std::to_string(k) + " groups based on centroid distance.",
      .inputs = makeKMeansInputs(),
      .outputs = makeKMeansOutputs(k),
      .atLeastOneInputRequired = true,
      .tags = {"kmeans", "ml", "clustering", "unsupervised", "regime"},
      .requiresTimeFrame = false,
      .strategyTypes = {"regime-based", "clustering", "risk-parity"},
      .relatedTransforms = {"hmm_" + std::to_string(k), "dbscan"},
      .usageContext = "Use for regime detection with fixed number of clusters. Distance to each centroid helps "
                      "measure regime certainty. Best when clusters are spherical and roughly equal sized.",
      .limitations = "Requires specifying K upfront. Sensitive to initialization. Assumes spherical clusters. "
                     "May not work well for non-convex cluster shapes."
    });
  }

  return metadataList;
}

/**
 * @brief Create DBSCAN metadata
 *
 * DBSCAN (Density-Based Spatial Clustering) finds clusters of arbitrary shape
 * and identifies outliers as noise points.
 *
 * Financial Applications:
 * - Anomaly/outlier detection in returns
 * - Regime detection without predefined count
 * - Finding natural market groupings
 * - Identifying unusual trading patterns
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeDBSCANMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "dbscan",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::dbscan,
    .name = "DBSCAN Clustering",
    .options = {
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
        .desc = "Minimum points required to form a dense region (core point)"
      },
      MetaDataOption{
        .id = "compute_zscore",
        .name = "Z-Score Normalization",
        .type = epoch_core::MetaDataOptionType::Boolean,
        .defaultValue = MetaDataOptionDefinition(true),
        .desc = "Standardize features before clustering"
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
        .desc = "Gap between training and test data (Marcos López de Prado purging)"
      }
    },
    .isCrossSectional = false,
    .desc = "Density-Based Spatial Clustering that finds clusters of arbitrary shape and identifies "
            "noise/outliers. Does not require specifying number of clusters upfront.",
    .inputs = {{epoch_core::IODataType::Number, "SLOT", "Features", true, false}},
    .outputs = {
      {epoch_core::IODataType::Integer, "cluster_label", "Cluster Label (-1 for noise)", true, false},
      {epoch_core::IODataType::Integer, "is_anomaly", "Is Anomaly (1=noise, 0=in cluster)", true, false},
      {epoch_core::IODataType::Integer, "cluster_count", "Total Clusters Found", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"dbscan", "ml", "clustering", "unsupervised", "anomaly", "outlier"},
    .requiresTimeFrame = false,
    .strategyTypes = {"anomaly-detection", "regime-based", "outlier-filtering"},
    .relatedTransforms = {"kmeans_3", "pca"},
    .usageContext = "Use for anomaly detection and regime discovery when number of clusters is unknown. "
                    "Noise points (label=-1) are potential anomalies. Good for non-spherical cluster shapes.",
    .limitations = "Sensitive to epsilon and min_points parameters. Struggles with varying density clusters. "
                   "May not scale well to very high dimensions."
  });

  return metadataList;
}

/**
 * @brief Create PCA metadata
 *
 * PCA (Principal Component Analysis) performs dimensionality reduction by finding
 * orthogonal axes of maximum variance.
 *
 * Financial Applications:
 * - Factor extraction from correlated assets
 * - Risk factor decomposition
 * - Feature reduction for ML models
 * - Identifying hidden market drivers
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakePCAMetaData() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  metadataList.emplace_back(epoch_script::transforms::TransformsMetaData{
    .id = "pca",
    .category = epoch_core::TransformCategory::ML,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "PCA (Principal Component Analysis)",
    .options = {
      MetaDataOption{
        .id = "n_components",
        .name = "Number of Components",
        .type = epoch_core::MetaDataOptionType::Integer,
        .defaultValue = MetaDataOptionDefinition(0.0),
        .min = 0,
        .max = 100,
        .desc = "Number of components to keep (0 = use variance_retained or keep all)"
      },
      MetaDataOption{
        .id = "variance_retained",
        .name = "Variance Retained",
        .type = epoch_core::MetaDataOptionType::Decimal,
        .defaultValue = MetaDataOptionDefinition(0.0),
        .min = 0.0,
        .max = 1.0,
        .desc = "Keep components to retain this fraction of variance (0 = keep all, 0.95 = 95%)"
      },
      MetaDataOption{
        .id = "scale_data",
        .name = "Scale Data",
        .type = epoch_core::MetaDataOptionType::Boolean,
        .defaultValue = MetaDataOptionDefinition(true),
        .desc = "Standardize features before PCA"
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
        .desc = "Gap between training and test data (Marcos López de Prado purging)"
      }
    },
    .isCrossSectional = false,
    .desc = "Principal Component Analysis transforms correlated features into uncorrelated principal "
            "components ordered by explained variance. Essential for dimensionality reduction.",
    .inputs = {{epoch_core::IODataType::Number, "SLOT", "Features", true, false}},
    .outputs = {
      {epoch_core::IODataType::Decimal, "pc_0", "Principal Component 0", true, false},
      {epoch_core::IODataType::Decimal, "pc_1", "Principal Component 1", true, false},
      {epoch_core::IODataType::Decimal, "pc_2", "Principal Component 2", true, false},
      {epoch_core::IODataType::Decimal, "pc_3", "Principal Component 3", true, false},
      {epoch_core::IODataType::Decimal, "pc_4", "Principal Component 4", true, false},
      {epoch_core::IODataType::Decimal, "explained_variance_ratio", "Cumulative Explained Variance", true, false}
    },
    .atLeastOneInputRequired = true,
    .tags = {"pca", "ml", "dimensionality-reduction", "factor", "decomposition"},
    .requiresTimeFrame = false,
    .strategyTypes = {"factor-investing", "risk-decomposition", "feature-engineering"},
    .relatedTransforms = {"kmeans_3"},
    .usageContext = "Use for extracting hidden factors from multiple correlated series. PC0 often represents "
                    "market beta, subsequent PCs capture sector/style factors. Good for portfolio risk decomposition.",
    .limitations = "Assumes linear relationships. Components are uncorrelated but not independent. "
                   "Sensitive to outliers. Interpretation of components requires domain knowledge."
  });

  return metadataList;
}

} // namespace epoch_script::transform
