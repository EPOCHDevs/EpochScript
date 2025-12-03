#pragma once
// Control Flow Transforms Metadata
// Provides metadata for branching and routing transforms

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transforms {

using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;

// =============================================================================
// BOOLEAN BRANCH
// =============================================================================

inline TransformsMetaData MakeBooleanBranchMetaData() {
    return TransformsMetaData{
        .id = "boolean_branch",
        .category = TransformCategory::ControlFlow,
        .name = "Boolean Branch",
        .desc = "Splits a boolean signal into two separate outputs: one for true values and one for "
                "inverted (false) values.",
        .inputs = {{.type = IODataType::Boolean, .id = "condition", .name = "Condition"}},
        .outputs = {
            {.type = IODataType::Boolean, .id = "true", .name = "True Path"},
            {.type = IODataType::Boolean, .id = "false", .name = "False Path"}},
        .tags = {"branching", "boolean", "split", "flow-control", "signal-processing", "long-short"},
        .requiresTimeFrame = false,
        .strategyTypes = {"long-short", "signal-routing", "signal-inversion"},
        .relatedTransforms = {"logical_not", "boolean_select_boolean"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Route same signal to multiple destinations with opposite logic. Use when you need "
                        "both long and short signals from one condition, or need inverted signal without NOT operator.",
        .limitations = "Simple utility - just provides opposite signals. Can be replaced with NOT operator."
    };
}

// =============================================================================
// RATIO BRANCH
// =============================================================================

inline TransformsMetaData MakeRatioBranchMetaData() {
    return TransformsMetaData{
        .id = "ratio_branch",
        .category = TransformCategory::ControlFlow,
        .name = "Ratio Branch",
        .options = {
            epoch_script::MetaDataOption{
                .id = "threshold_high",
                .name = "Upper Threshold",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(1.5),
                .desc = "Ratio above this value triggers 'high' output",
                .tuningGuidance = "For normalized ratios around 1.0, use 1.2-2.0 for high. "
                                  "For z-scores, use 2.0-3.0 for divergence."},
            epoch_script::MetaDataOption{
                .id = "threshold_low",
                .name = "Lower Threshold",
                .type = epoch_core::MetaDataOptionType::Decimal,
                .defaultValue = epoch_script::MetaDataOptionDefinition(0.5),
                .desc = "Ratio below this value triggers 'low' output",
                .tuningGuidance = "Mirror of threshold_high for symmetry. For z-scores, use -2.0 to -3.0."}},
        .desc = "Splits data based on the ratio between two values: high (above upper threshold), "
                "normal (between thresholds), and low (below lower threshold).",
        .inputs = {{.type = IODataType::Decimal, .id = "ratio", .name = "Ratio"}},
        .outputs = {
            {.type = IODataType::Boolean, .id = "high", .name = "High (Above Upper)"},
            {.type = IODataType::Boolean, .id = "normal", .name = "Normal (Between)"},
            {.type = IODataType::Boolean, .id = "low", .name = "Low (Below Lower)"}},
        .tags = {"branching", "ratio", "threshold", "flow-control", "multi-output", "regime-detection"},
        .requiresTimeFrame = false,
        .strategyTypes = {"regime-detection", "three-state-logic", "ratio-analysis"},
        .relatedTransforms = {"gt", "lt", "boolean_branch"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Three-way regime detection based on ratio values. Use for momentum vs mean-reversion "
                        "strategies or relative strength comparisons.",
        .limitations = "Fixed thresholds - not adaptive. Consider percentile_select for adaptive thresholds."
    };
}

// =============================================================================
// TRADE EXECUTOR ADAPTER
// =============================================================================

inline TransformsMetaData MakeTradeExecutorAdapterMetaData() {
    return TransformsMetaData{
        .id = "trade_executor_adapter",
        .category = TransformCategory::ControlFlow,
        .name = "Trade Executor Adapter",
        .desc = "Converts numerical trade signal/value (Positive, Negative) into boolean long and short "
                "execution flags. Used for connecting signal generators to execution components.",
        .inputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Signal"}},
        .outputs = {
            {.type = IODataType::Boolean, .id = "long", .name = "Long"},
            {.type = IODataType::Boolean, .id = "short", .name = "Short"}},
        .tags = {"signal", "adapter", "trade", "position", "boolean"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-conversion", "execution-logic", "utility"},
        .relatedTransforms = {"boolean_branch", "percentile_select"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Convert continuous signals (momentum indicators, ML scores) into discrete long/short "
                        "boolean flags for execution. Positive input triggers long=true, negative triggers short=true.",
        .limitations = "Simple sign-based conversion with no hysteresis or filtering."
    };
}

// =============================================================================
// VARARGS SWITCH TRANSFORMS
// =============================================================================
// Varargs switch transforms accept variable number of slot inputs.
// For testing, we define metadata with minimum 3 inputs (index + 2 slots).

inline TransformsMetaData MakeSwitchNumberMetaData() {
    return TransformsMetaData{
        .id = "switch_number",
        .category = TransformCategory::ControlFlow,
        .name = "Switch Number (Varargs)",
        .desc = "Selects from a variable number of numeric slot inputs based on an integer index. "
                "Supports any number of inputs (minimum 2 slots).",
        .inputs = {
            {.type = IODataType::Integer, .id = "index", .name = "Index"},
            {.type = IODataType::Decimal, .id = "slot_0", .name = "Slot 0"},
            {.type = IODataType::Decimal, .id = "slot_1", .name = "Slot 1"}},
        .outputs = {{.type = IODataType::Decimal, .id = "SLOT", .name = "Selected Value"}},
        .tags = {"selection", "switch", "routing", "varargs"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-routing", "data-selection"},
        .relatedTransforms = {"switch2_number", "switch3_number"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Select between arbitrary number of numeric inputs based on index value.",
        .limitations = "Index values outside valid range may cause errors."
    };
}

inline TransformsMetaData MakeSwitchStringMetaData() {
    return TransformsMetaData{
        .id = "switch_string",
        .category = TransformCategory::ControlFlow,
        .name = "Switch String (Varargs)",
        .desc = "Selects from a variable number of string slot inputs based on an integer index. "
                "Supports any number of inputs (minimum 2 slots).",
        .inputs = {
            {.type = IODataType::Integer, .id = "index", .name = "Index"},
            {.type = IODataType::String, .id = "slot_0", .name = "Slot 0"},
            {.type = IODataType::String, .id = "slot_1", .name = "Slot 1"}},
        .outputs = {{.type = IODataType::String, .id = "SLOT", .name = "Selected Value"}},
        .tags = {"selection", "switch", "routing", "varargs"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-routing", "data-selection"},
        .relatedTransforms = {"switch2_string", "switch3_string"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Select between arbitrary number of string inputs based on index value.",
        .limitations = "Index values outside valid range may cause errors."
    };
}

inline TransformsMetaData MakeSwitchBooleanMetaData() {
    return TransformsMetaData{
        .id = "switch_boolean",
        .category = TransformCategory::ControlFlow,
        .name = "Switch Boolean (Varargs)",
        .desc = "Selects from a variable number of boolean slot inputs based on an integer index. "
                "Supports any number of inputs (minimum 2 slots).",
        .inputs = {
            {.type = IODataType::Integer, .id = "index", .name = "Index"},
            {.type = IODataType::Boolean, .id = "slot_0", .name = "Slot 0"},
            {.type = IODataType::Boolean, .id = "slot_1", .name = "Slot 1"}},
        .outputs = {{.type = IODataType::Boolean, .id = "SLOT", .name = "Selected Value"}},
        .tags = {"selection", "switch", "routing", "varargs"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-routing", "data-selection"},
        .relatedTransforms = {"switch2_boolean", "switch3_boolean"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Select between arbitrary number of boolean inputs based on index value.",
        .limitations = "Index values outside valid range may cause errors."
    };
}

inline TransformsMetaData MakeSwitchTimestampMetaData() {
    return TransformsMetaData{
        .id = "switch_timestamp",
        .category = TransformCategory::ControlFlow,
        .name = "Switch Timestamp (Varargs)",
        .desc = "Selects from a variable number of timestamp slot inputs based on an integer index. "
                "Supports any number of inputs (minimum 2 slots).",
        .inputs = {
            {.type = IODataType::Integer, .id = "index", .name = "Index"},
            {.type = IODataType::Timestamp, .id = "slot_0", .name = "Slot 0"},
            {.type = IODataType::Timestamp, .id = "slot_1", .name = "Slot 1"}},
        .outputs = {{.type = IODataType::Timestamp, .id = "SLOT", .name = "Selected Value"}},
        .tags = {"selection", "switch", "routing", "varargs"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-routing", "data-selection"},
        .relatedTransforms = {"switch2_timestamp", "switch3_timestamp"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Select between arbitrary number of timestamp inputs based on index value.",
        .limitations = "Index values outside valid range may cause errors."
    };
}

// =============================================================================
// COMBINED METADATA FUNCTION
// =============================================================================

inline std::vector<TransformsMetaData> MakeControlFlowMetaData() {
    return {
        MakeBooleanBranchMetaData(),
        MakeRatioBranchMetaData(),
        MakeTradeExecutorAdapterMetaData(),
        MakeSwitchNumberMetaData(),
        MakeSwitchStringMetaData(),
        MakeSwitchBooleanMetaData(),
        MakeSwitchTimestampMetaData()
    };
}

}  // namespace epoch_script::transforms
