#pragma once
// Aggregation transforms metadata
// Provides multi-input aggregation functions for boolean and numeric series
//
// Categories:
// 1. Boolean Aggregations - Combine multiple boolean signals
//    - agg_all_of: AND all inputs (all must be true)
//    - agg_any_of: OR all inputs (any one true)
//    - agg_none_of: NOR all inputs (none are true)
// 2. Numeric Aggregations - Combine multiple numeric values
//    - agg_max: Maximum across inputs
//    - agg_min: Minimum across inputs
//    - agg_sum: Sum all inputs
//    - agg_mean: Average all inputs
// 3. Equality Checks - Value consistency across inputs
//    - agg_all_equal: All inputs have same value
//    - agg_all_unique: All inputs have different values

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transforms {

using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// BOOLEAN AGGREGATIONS
// =============================================================================

// agg_all_of: Logical AND across all inputs
// Returns true only if ALL connected boolean inputs are true.
// Use for: Confirming multiple conditions simultaneously. Entry signals requiring
// all confirmations (trend + momentum + volume). Conservative strategy filtering.
inline TransformsMetaData MakeAggAllOfMetaData() {
    return TransformsMetaData{
        .id = "agg_all_of",
        .category = TransformCategory::Aggregate,
        .name = "All Of",
        .desc = "Returns true only if all connected inputs are true. "
                "Implements multi-way AND logic for combining boolean conditions.",
        .inputs = {{.type = IODataType::Boolean, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"aggregate", "boolean", "logic", "all", "and", "confirmation"},
        .requiresTimeFrame = false,
        .strategyTypes = {"multi-condition", "confirmation-stacking", "conservative-entry"},
        .relatedTransforms = {"agg_any_of", "agg_none_of", "logical_and"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Combine multiple entry/exit conditions with AND logic. All conditions "
                        "must be satisfied simultaneously. Use for conservative strategies "
                        "requiring multiple confirmations (e.g., trend + momentum + volume all aligned).",
        .limitations = "Stricter filtering = fewer signals. More conditions = lower trade frequency. "
                       "One false condition blocks entire signal. Null in any input may propagate."
    };
}

// agg_any_of: Logical OR across all inputs
// Returns true if AT LEAST ONE connected boolean input is true.
// Use for: Combining multiple opportunity patterns, aggressive entry strategies,
// capturing different signal types with one output.
inline TransformsMetaData MakeAggAnyOfMetaData() {
    return TransformsMetaData{
        .id = "agg_any_of",
        .category = TransformCategory::Aggregate,
        .name = "Any Of",
        .desc = "Returns true if at least one of the connected inputs is true. "
                "Implements multi-way OR logic for combining boolean conditions.",
        .inputs = {{.type = IODataType::Boolean, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"aggregate", "boolean", "logic", "any", "or", "multi-pattern"},
        .requiresTimeFrame = false,
        .strategyTypes = {"multi-pattern", "opportunistic-entry", "pattern-basket"},
        .relatedTransforms = {"agg_all_of", "agg_none_of", "logical_or"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Combine multiple conditions with OR logic. Any single condition triggers signal. "
                        "Use for aggressive strategies or multiple entry patterns (e.g., enter on "
                        "breakout OR momentum OR volume spike). Good for capturing different opportunity types.",
        .limitations = "More signals but potentially lower quality. Can trigger on weakest condition. "
                       "May increase false signals. Consider probability weighting for better results."
    };
}

// agg_none_of: Logical NOR across all inputs
// Returns true only if ALL connected boolean inputs are false.
// Use for: Risk filters, avoiding unwanted conditions, exclusion logic.
inline TransformsMetaData MakeAggNoneOfMetaData() {
    return TransformsMetaData{
        .id = "agg_none_of",
        .category = TransformCategory::Aggregate,
        .name = "None Of",
        .desc = "Returns true only if all connected inputs are false. "
                "Implements multi-way NOR logic - the inverse of agg_any_of.",
        .inputs = {{.type = IODataType::Boolean, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"aggregate", "boolean", "logic", "none", "nor", "filtering"},
        .requiresTimeFrame = false,
        .strategyTypes = {"risk-filtering", "condition-avoidance", "exit-logic"},
        .relatedTransforms = {"agg_all_of", "agg_any_of", "logical_not"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Filter out unwanted conditions - all connected filters must be inactive. "
                        "Use for risk filters (e.g., don't trade when: high volatility OR low volume "
                        "OR earnings week). Common in exit logic or position blocking.",
        .limitations = "Inverted logic can be confusing. Often clearer to use NOT with agg_any_of. "
                       "Requires all conditions false simultaneously."
    };
}

// =============================================================================
// NUMERIC AGGREGATIONS
// =============================================================================

// agg_max: Maximum value across inputs
// Returns the maximum value among all connected numeric inputs.
// Use for: Composite indicators, worst-case analysis, selecting highest signal.
inline TransformsMetaData MakeAggMaxMetaData() {
    return TransformsMetaData{
        .id = "agg_max",
        .category = TransformCategory::Aggregate,
        .name = "Maximum",
        .desc = "Returns the maximum value among all connected numeric inputs. "
                "Useful for finding the highest value in a set of indicators or signals.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Maximum"}},
        .tags = {"aggregate", "numeric", "math", "maximum", "highest", "composite"},
        .requiresTimeFrame = false,
        .strategyTypes = {"composite-indicators", "multi-signal", "worst-case-analysis"},
        .relatedTransforms = {"agg_min", "agg_mean", "agg_sum"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Select highest value from multiple indicators or signals. Use for composite "
                        "indicators (max of multiple momentum measures) or worst-case scenarios "
                        "(max drawdown across strategies). Also for multi-asset maximum value selection.",
        .limitations = "Sensitive to outliers - one extreme value dominates. Doesn't consider "
                       "distribution of other values. For cross-asset use, ensure values are "
                       "comparable (normalized)."
    };
}

// agg_min: Minimum value across inputs
// Returns the minimum value among all connected numeric inputs.
// Use for: Conservative sizing, constraint analysis, risk management.
inline TransformsMetaData MakeAggMinMetaData() {
    return TransformsMetaData{
        .id = "agg_min",
        .category = TransformCategory::Aggregate,
        .name = "Minimum",
        .desc = "Returns the minimum value among all connected numeric inputs. "
                "Useful for finding the lowest value in a set of indicators or signals.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Minimum"}},
        .tags = {"aggregate", "numeric", "math", "minimum", "lowest", "risk-management"},
        .requiresTimeFrame = false,
        .strategyTypes = {"risk-management", "conservative-sizing", "constraint-analysis"},
        .relatedTransforms = {"agg_max", "agg_mean", "agg_sum"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Select lowest value from multiple inputs. Use for conservative position sizing "
                        "(minimum of multiple risk limits), best-case analysis, or finding bottleneck "
                        "constraints. Useful in risk management for most conservative estimate.",
        .limitations = "Sensitive to outliers - one extreme low value dominates. Ignores all other values. "
                       "For cross-asset use, ensure values are comparable."
    };
}

// agg_sum: Sum across all inputs
// Calculates the sum of all connected numeric inputs.
// Use for: Composite scores, total exposure, multi-factor aggregation.
inline TransformsMetaData MakeAggSumMetaData() {
    return TransformsMetaData{
        .id = "agg_sum",
        .category = TransformCategory::Aggregate,
        .name = "Sum",
        .desc = "Calculates the sum of all connected numeric inputs. "
                "Useful for adding multiple signals or values together.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Sum"}},
        .tags = {"aggregate", "numeric", "math", "sum", "addition", "multi-factor"},
        .requiresTimeFrame = false,
        .strategyTypes = {"multi-factor", "scoring-systems", "portfolio-aggregation"},
        .relatedTransforms = {"agg_mean", "agg_max", "agg_min"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Combine multiple signals, portfolio values, or factor scores additively. "
                        "Use for composite scoring systems, total portfolio exposure, or cumulative "
                        "indicators. Common in multi-factor models where factors add together.",
        .limitations = "Assumes additive relationship between inputs. Sensitive to scale - "
                       "normalize inputs first if on different scales. Unbounded - can grow "
                       "indefinitely with more inputs."
    };
}

// agg_mean: Average across all inputs
// Calculates the arithmetic mean of all connected numeric inputs.
// Use for: Signal averaging, ensemble methods, consensus indicators.
inline TransformsMetaData MakeAggMeanMetaData() {
    return TransformsMetaData{
        .id = "agg_mean",
        .category = TransformCategory::Aggregate,
        .name = "Mean",
        .desc = "Calculates the arithmetic mean of all connected numeric inputs. "
                "Useful for finding the average value across multiple signals.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Mean"}},
        .tags = {"aggregate", "numeric", "math", "mean", "average", "ensemble"},
        .requiresTimeFrame = false,
        .strategyTypes = {"ensemble-methods", "signal-averaging", "consensus-trading"},
        .relatedTransforms = {"agg_sum", "agg_max", "agg_min"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Average multiple signals or indicators for ensemble approach. Better than "
                        "sum when number of inputs varies. Use for average portfolio returns, "
                        "consensus signals, or reducing indicator noise through averaging.",
        .limitations = "Sensitive to outliers. All inputs weighted equally. For varying reliability, "
                       "use weighted average instead. Requires normalization if inputs on different scales."
    };
}

// =============================================================================
// EQUALITY CHECKS
// =============================================================================

// agg_all_equal: Check if all inputs have the same value
// Returns true if all connected inputs have identical values.
// Use for: Data validation, synchronization checks, multi-source agreement.
inline TransformsMetaData MakeAggAllEqualMetaData() {
    return TransformsMetaData{
        .id = "agg_all_equal",
        .category = TransformCategory::Aggregate,
        .name = "All Equal",
        .desc = "Returns true if all connected inputs have the same value. "
                "Useful for checking value consistency across multiple sources.",
        .inputs = {{.type = IODataType::Any, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "All Equal"}},
        .tags = {"aggregate", "comparison", "equality", "consistency", "validation", "debugging"},
        .requiresTimeFrame = false,
        .strategyTypes = {"data-validation", "sanity-checking", "multi-timeframe-confirmation"},
        .relatedTransforms = {"agg_all_unique", "eq"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Validation and synchronization checks. Verify multiple data sources agree "
                        "(sanity check), or ensure regime indicators all show same state. Useful for "
                        "detecting data discrepancies or confirming multi-timeframe alignment.",
        .limitations = "Rarely useful in trading strategies - mostly for debugging. Exact equality "
                       "can be brittle with floating point. Consider tolerance-based comparison instead."
    };
}

// agg_all_unique: Check if all inputs have unique values
// Returns true if all connected inputs have distinct values.
// Use for: Duplicate detection, signal diversity verification.
inline TransformsMetaData MakeAggAllUniqueMetaData() {
    return TransformsMetaData{
        .id = "agg_all_unique",
        .category = TransformCategory::Aggregate,
        .name = "All Unique",
        .desc = "Returns true if all connected inputs have unique values. "
                "Useful for ensuring no duplicates exist in a set of values.",
        .inputs = {{.type = IODataType::Any, .id = "SLOT", .name = "Values",
                    .allowMultipleConnections = true}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "All Unique"}},
        .tags = {"aggregate", "comparison", "unique", "distinct", "validation", "debugging"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-validation", "debugging", "diversity-checking"},
        .relatedTransforms = {"agg_all_equal"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Detect duplicate signals or ensure diversity in signal sources. Mainly used "
                        "for validation and debugging. Can verify no indicator redundancy "
                        "(all providing unique information).",
        .limitations = "Limited practical trading use. More valuable for system validation than "
                       "strategy logic. Consider correlation analysis for measuring signal independence."
    };
}

// =============================================================================
// METADATA FACTORY
// =============================================================================

inline std::vector<TransformsMetaData> MakeAggregationMetaData() {
    return {
        MakeAggAllOfMetaData(),
        MakeAggAnyOfMetaData(),
        MakeAggNoneOfMetaData(),
        MakeAggMaxMetaData(),
        MakeAggMinMetaData(),
        MakeAggSumMetaData(),
        MakeAggMeanMetaData(),
        MakeAggAllEqualMetaData(),
        MakeAggAllUniqueMetaData()
    };
}

}  // namespace epoch_script::transforms
