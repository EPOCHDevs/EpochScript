#pragma once
// Aggregation transforms registration
// Provides multi-input aggregation functions for combining signals
//
// Categories:
// 1. Boolean Aggregations - Combine multiple boolean signals with logic
//    - agg_all_of: AND all inputs (all must be true)
//    - agg_any_of: OR all inputs (any one true triggers)
//    - agg_none_of: NOR all inputs (none are true)
// 2. Numeric Aggregations - Combine multiple numeric values
//    - agg_max: Maximum value across inputs
//    - agg_min: Minimum value across inputs
//    - agg_sum: Sum all inputs
//    - agg_mean: Average all inputs
// 3. Equality Checks - Verify value consistency across inputs
//    - agg_all_equal: All inputs have same value
//    - agg_all_unique: All inputs have different values

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "agg.h"
#include "agg_metadata.h"

namespace epoch_script::transform::aggregation {

using namespace epoch_script::transform;

// =============================================================================
// Registration function - registers all aggregation transforms and metadata
// =============================================================================

inline void Register() {
    // =========================================================================
    // EXECUTION REGISTRATION
    // =========================================================================

    // Boolean aggregations - combine multiple boolean conditions
    // agg_all_of: AND logic across all inputs
    // Use for: Conservative strategies requiring ALL conditions to be true
    // Example: Enter only when trend_up AND momentum_positive AND volume_high
    epoch_script::transform::Register<AllOfAggregateTransform>("agg_all_of");

    // agg_any_of: OR logic across all inputs
    // Use for: Aggressive strategies where ANY condition triggers action
    // Example: Enter on breakout OR momentum OR volume spike
    epoch_script::transform::Register<AnyOfAggregateTransform>("agg_any_of");

    // agg_none_of: NOR logic - true only when ALL inputs are false
    // Use for: Risk filters, exclusion logic
    // Example: Don't trade when high_volatility OR earnings_week OR low_volume
    epoch_script::transform::Register<NoneOfAggregateTransform>("agg_none_of");

    // Numeric aggregations - combine multiple numeric values
    // agg_sum: Add all inputs together
    // Use for: Composite scores, total exposure, multi-factor aggregation
    epoch_script::transform::Register<SumAggregateTransform>("agg_sum");

    // agg_mean: Average all inputs
    // Use for: Ensemble averaging, consensus signals, noise reduction
    epoch_script::transform::Register<AverageAggregateTransform>("agg_mean");

    // agg_min: Take minimum value across inputs
    // Use for: Conservative position sizing, constraint satisfaction
    epoch_script::transform::Register<MinAggregateTransform>("agg_min");

    // agg_max: Take maximum value across inputs
    // Use for: Composite indicators, worst-case scenarios
    epoch_script::transform::Register<MaxAggregateTransform>("agg_max");

    // Equality checks - verify value consistency
    // agg_all_equal: True if all inputs have identical values
    // Use for: Data validation, synchronization checks
    epoch_script::transform::Register<AllEqualAggregateTransform>("agg_all_equal");

    // agg_all_unique: True if all inputs have distinct values
    // Use for: Duplicate detection, signal diversity verification
    epoch_script::transform::Register<AllUniqueAggregateTransform>("agg_all_unique");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================
    auto& registry = epoch_script::transforms::ITransformRegistry::GetInstance();
    for (const auto& meta : epoch_script::transforms::MakeAggregationMetaData()) {
        registry.Register(meta);
    }
}

}  // namespace epoch_script::transform::aggregation
