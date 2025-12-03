#pragma once
// Cross-Sectional transforms registration
// Provides transforms that operate ACROSS assets at each timestamp
// (as opposed to time-series transforms that operate over time for each asset)
//
// Categories:
// 1. Normalization - Standardize values across assets
//    - cs_zscore: Z-score normalization across assets
//    - cs_winsorize: Cap extreme values at percentile cutoffs
// 2. Ranking - Rank assets at each timestamp
//    - cs_rank: Ordinal ranks (1, 2, 3, ...)
//    - cs_rank_quantile: Percentile ranks (0.0 to 1.0)
//    - top_k, bottom_k: Boolean masks for top/bottom K assets
//    - top_k_percent, bottom_k_percent: Boolean masks for percentiles
// 3. Aggregation - Cross-sectional statistics
//    - cs_momentum: Cumulative cross-sectional mean returns

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "cs_zscore.h"
#include "cs_winsorize.h"
#include "rank.h"
#include "returns.h"

// Metadata
#include "cross_sectional_metadata.h"

namespace epoch_script::transform::cross_sectional {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all cross-sectional transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // NORMALIZATION - Cross-Sectional Standardization
    // =========================================================================
    // Transform values relative to the cross-section at each timestamp.
    // Essential for comparing assets with different scales.

    // cs_zscore: Cross-sectional z-score normalization
    // Input: Multi-column DataFrame (one column per asset)
    // Outputs: z-scores normalized across assets at each timestamp
    // Use for: Normalizing factors before ranking, identifying outliers,
    //          comparing assets on different scales
    // Note: Different from time-series zscore which normalizes over TIME
    epoch_script::transform::Register<CSZScore>("cs_zscore");

    // cs_winsorize: Cap extreme values across assets
    // Input: Multi-column DataFrame
    // Options: lower_limit (default 0.05), upper_limit (default 0.95)
    // Outputs: Values capped at cross-sectional percentile cutoffs
    // Use for: Outlier handling before factor construction,
    //          robust normalization, reducing impact of extreme values
    epoch_script::transform::Register<CSWinsorize>("cs_winsorize");

    // =========================================================================
    // RANKING - Cross-Sectional Ordering
    // =========================================================================
    // Rank assets relative to each other at each timestamp.
    // Foundation for factor-based portfolio construction.

    // cs_rank: Ordinal ranks (1, 2, 3, ...)
    // Input: Multi-column DataFrame
    // Options: ascending (default true - lowest value gets rank 1)
    // Outputs: Integer ranks at each timestamp
    // Use for: Factor portfolio construction, relative strength analysis,
    //          long/short portfolio weights based on rank
    epoch_script::transform::Register<CSRank>("cs_rank");

    // cs_rank_quantile: Percentile ranks (0.0 to 1.0)
    // Input: Multi-column DataFrame
    // Options: ascending (default true - lowest value gets 0.0)
    // Outputs: Percentile ranks normalized to [0, 1]
    // Use for: Normalized factor scores, quintile/decile portfolios,
    //          combining multiple factors with different scales
    epoch_script::transform::Register<CSRankQuantile>("cs_rank_quantile");

    // top_k: Boolean mask for top K assets
    // Input: Multi-column DataFrame (scores/values)
    // Options: k (number of assets to select)
    // Outputs: Boolean mask (true for top K)
    // Use for: Selecting top assets for long portfolio, momentum winners
    epoch_script::transform::Register<CrossSectionalTopKOperation>("top_k");

    // bottom_k: Boolean mask for bottom K assets
    // Input: Multi-column DataFrame (scores/values)
    // Options: k (number of assets to select)
    // Outputs: Boolean mask (true for bottom K)
    // Use for: Selecting bottom assets for short portfolio, value stocks
    epoch_script::transform::Register<CrossSectionalBottomKOperation>("bottom_k");

    // top_k_percent: Boolean mask for top K% of assets
    // Input: Multi-column DataFrame
    // Options: k (percentile, 1-100)
    // Outputs: Boolean mask (true for top K%)
    // Use for: Percentile-based selection (top 10%, top 20%)
    epoch_script::transform::Register<CrossSectionalTopKPercentileOperation>("top_k_percent");

    // bottom_k_percent: Boolean mask for bottom K% of assets
    // Input: Multi-column DataFrame
    // Options: k (percentile, 1-100)
    // Outputs: Boolean mask (true for bottom K%)
    // Use for: Percentile-based selection (bottom 10%, bottom 20%)
    epoch_script::transform::Register<CrossSectionalBottomKPercentileOperation>("bottom_k_percent");

    // =========================================================================
    // AGGREGATION - Cross-Sectional Statistics
    // =========================================================================
    // Compute statistics across assets at each timestamp.

    // cs_momentum: Cross-sectional cumulative returns
    // Input: Multi-column DataFrame of percentage changes
    // Outputs: Cumulative mean returns across all assets
    // Use for: Market-wide momentum indicator, aggregate return tracking
    epoch_script::transform::Register<CrossSectionalMomentumOperation>("cs_momentum");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // Cross-sectional transforms metadata (cs_momentum, top_k, bottom_k, etc.)
    for (const auto& metadata : epoch_script::transforms::MakeCrossSectionalMetaData()) {
        metaRegistry.Register(metadata);
    }

    // Cross-sectional rank metadata
    for (const auto& metadata : MakeCSRankMetaData()) {
        metaRegistry.Register(metadata);
    }

    // Cross-sectional winsorize metadata
    for (const auto& metadata : MakeCSWinsorizeMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::cross_sectional
