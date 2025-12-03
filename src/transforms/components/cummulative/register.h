#pragma once
// Cumulative transforms registration
// Provides running totals and products across time series data

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "cum_op.h"

namespace epoch_script::transform::cummulative {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;
using TransformCategory = epoch_core::TransformCategory;

// cum_prod: Running product of values
// Compounds values multiplicatively from series start to current position.
// Primary use: converting periodic returns (in 1+r format) into cumulative returns.
// Input format: (1 + return) values, NOT raw percentages.
// Example: [1.01, 1.02, 0.99] produces [1.01, 1.0302, 1.019898]
inline TransformsMetaData MakeCumProdMetaData() {
    return TransformsMetaData{
        .id = "cum_prod",
        .category = TransformCategory::Math,
        .name = "Cumulative Product",
        .desc = "Running product of values from series start. Compounds each value "
                "multiplicatively with all prior values.",
        .inputs = {{.type = epoch_core::IODataType::Decimal, .id = ARG, .name = "Input"}},
        .outputs = {{.type = epoch_core::IODataType::Decimal, .id = epoch_script::RESULT, .name = "Cumulative Product"}},
        .tags = {"cumulative", "product", "compounding", "returns", "performance"},
        .requiresTimeFrame = false,
        .strategyTypes = {"performance-tracking", "backtesting", "returns-analysis"},
        .relatedTransforms = {"lag", "cum_sum"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Convert periodic returns to cumulative returns. "
                        "Input values as (1 + return), not raw percentages. "
                        "Essential for calculating total strategy performance from individual period returns.",
        .limitations = "Requires returns in (1+r) format. Does not reset - calculates from series start. "
                       "Sensitive to large losses (one -50% period requires +100% to recover). "
                       "Can overflow with very long series of large values."
    };
}

// cum_sum: Running sum of values
// Accumulates values additively from series start to current position.
// Primary use: tracking cumulative P&L, volume, or any running total.
// Example: [1, 2, 3, -1] produces [1, 3, 6, 5]
inline TransformsMetaData MakeCumSumMetaData() {
    return TransformsMetaData{
        .id = "cum_sum",
        .category = TransformCategory::Math,
        .name = "Cumulative Sum",
        .desc = "Running sum of values from series start. Accumulates each value "
                "additively with all prior values.",
        .inputs = {{.type = epoch_core::IODataType::Decimal, .id = ARG, .name = "Input"}},
        .outputs = {{.type = epoch_core::IODataType::Decimal, .id = epoch_script::RESULT, .name = "Cumulative Sum"}},
        .tags = {"cumulative", "sum", "accumulation", "running-total"},
        .requiresTimeFrame = false,
        .strategyTypes = {"performance-tracking", "volume-analysis", "accumulation"},
        .relatedTransforms = {"cum_prod", "lag"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Track cumulative P&L, accumulated volume, or running totals. "
                        "Converts period values to running totals.",
        .limitations = "Does not reset - calculates from series start. "
                       "Can grow unbounded with persistent positive values. "
                       "For compounding returns, use cum_prod with (1+r) format instead."
    };
}

inline void Register() {
    // Register transforms (execution)
    epoch_script::transform::Register<CumProdOperation>("cum_prod");
    epoch_script::transform::Register<CumSumOperation>("cum_sum");

    // Register metadata (introspection)
    auto& registry = epoch_script::transforms::ITransformRegistry::GetInstance();
    registry.Register(MakeCumProdMetaData());
    registry.Register(MakeCumSumMetaData());
}

}  // namespace epoch_script::transform::cummulative
