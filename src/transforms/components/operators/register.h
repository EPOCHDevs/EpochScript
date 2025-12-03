#pragma once
// Operators module registration
// Provides comparison, logical, validation, type conversion, and selection transforms

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <epoch_script/transforms/core/metadata.h>

// Operator implementations
#include "equality.h"
#include "logical.h"
#include "validation.h"
#include "static_cast.h"
#include "alias.h"
#include "stringify.h"
#include "modulo.h"
#include "power.h"
#include "select.h"
#include "groupby_agg.h"
#include "controlflow_metadata.h"

namespace epoch_script::transform::operators {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;
using TransformCategory = epoch_core::TransformCategory;
using IODataType = epoch_core::IODataType;
using IOMetaData = epoch_script::transforms::IOMetaData;

// ============================================================================
// COMPARISON OPERATORS
// ============================================================================

// gt: Greater Than comparison
// Returns true where left operand is strictly greater than right operand.
// Requires both operands to be numeric. Null values propagate through comparison.
inline TransformsMetaData MakeGtMetaData() {
    return TransformsMetaData{
        .id = "gt",
        .category = TransformCategory::Utility,
        .name = "Greater Than",
        .desc = "Element-wise greater than comparison. Returns boolean true where "
                "left operand exceeds right operand.",
        .inputs = {{.type = IODataType::Decimal, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Decimal, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"comparison", "boolean", "relational"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-generation", "filtering", "threshold-detection"},
        .relatedTransforms = {"gte", "lt", "lte", "eq", "neq"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for threshold-based signals. Common for detecting breakouts "
                        "(price > resistance), overbought conditions (RSI > 70), or comparing indicators.",
        .limitations = "Null values in either operand produce null output. "
                       "For near-equal comparisons, use tolerance-based approaches."
    };
}

// gte: Greater Than or Equal comparison
inline TransformsMetaData MakeGteMetaData() {
    return TransformsMetaData{
        .id = "gte",
        .category = TransformCategory::Utility,
        .name = "Greater Than or Equal",
        .desc = "Element-wise greater than or equal comparison. Returns boolean true where "
                "left operand is greater than or equal to right operand.",
        .inputs = {{.type = IODataType::Decimal, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Decimal, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"comparison", "boolean", "relational"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-generation", "filtering", "threshold-detection"},
        .relatedTransforms = {"gt", "lt", "lte", "eq", "neq"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use when boundary values should be included. Common for support levels "
                        "(price >= support) or minimum thresholds (volume >= min_volume).",
        .limitations = "Null values in either operand produce null output."
    };
}

// lt: Less Than comparison
inline TransformsMetaData MakeLtMetaData() {
    return TransformsMetaData{
        .id = "lt",
        .category = TransformCategory::Utility,
        .name = "Less Than",
        .desc = "Element-wise less than comparison. Returns boolean true where "
                "left operand is strictly less than right operand.",
        .inputs = {{.type = IODataType::Decimal, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Decimal, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"comparison", "boolean", "relational"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-generation", "filtering", "threshold-detection"},
        .relatedTransforms = {"lte", "gt", "gte", "eq", "neq"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use for detecting breakdowns or oversold conditions. Common for "
                        "support breaks (price < support), oversold signals (RSI < 30).",
        .limitations = "Null values in either operand produce null output."
    };
}

// lte: Less Than or Equal comparison
inline TransformsMetaData MakeLteMetaData() {
    return TransformsMetaData{
        .id = "lte",
        .category = TransformCategory::Utility,
        .name = "Less Than or Equal",
        .desc = "Element-wise less than or equal comparison. Returns boolean true where "
                "left operand is less than or equal to right operand.",
        .inputs = {{.type = IODataType::Decimal, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Decimal, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"comparison", "boolean", "relational"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-generation", "filtering", "threshold-detection"},
        .relatedTransforms = {"lt", "gt", "gte", "eq", "neq"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use when boundary values should be included. Common for maximum thresholds "
                        "(volatility <= max_vol) or upper bounds (position_size <= limit).",
        .limitations = "Null values in either operand produce null output."
    };
}

// eq: Equality comparison
inline TransformsMetaData MakeEqMetaData() {
    return TransformsMetaData{
        .id = "eq",
        .category = TransformCategory::Utility,
        .name = "Equals",
        .desc = "Element-wise equality comparison. Returns boolean true where operands are equal. "
                "Handles type coercion for mixed types (boolean, string, numeric, timestamp).",
        .inputs = {{.type = IODataType::Any, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Any, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"comparison", "boolean", "equality"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-generation", "filtering", "pattern-detection"},
        .relatedTransforms = {"neq", "gt", "lt", "gte", "lte"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use to detect specific values or match conditions. Common for "
                        "categorical comparisons, pattern matching, or exact value detection.",
        .limitations = "Floating-point equality may fail due to precision. For approximate comparison, "
                       "use threshold-based approaches. Null compared to null returns null, not true."
    };
}

// neq: Not Equals comparison
inline TransformsMetaData MakeNeqMetaData() {
    return TransformsMetaData{
        .id = "neq",
        .category = TransformCategory::Utility,
        .name = "Not Equals",
        .desc = "Element-wise inequality comparison. Returns boolean true where operands differ. "
                "Handles type coercion for mixed types.",
        .inputs = {{.type = IODataType::Any, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Any, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"comparison", "boolean", "equality"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-generation", "filtering"},
        .relatedTransforms = {"eq", "gt", "lt", "gte", "lte"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Use to exclude specific values or detect changes. Common for "
                        "filtering out sentinel values or detecting state changes.",
        .limitations = "Floating-point inequality may give unexpected results due to precision. "
                       "Null compared to null returns null, not false."
    };
}

// ============================================================================
// LOGICAL OPERATORS
// ============================================================================

// logical_or: Boolean OR
inline TransformsMetaData MakeLogicalOrMetaData() {
    return TransformsMetaData{
        .id = "logical_or",
        .category = TransformCategory::Utility,
        .name = "Logical OR",
        .desc = "Element-wise boolean OR. Returns true if either operand is true.",
        .inputs = {{.type = IODataType::Boolean, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Boolean, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"boolean", "logical", "combiner"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-combination", "filtering"},
        .relatedTransforms = {"logical_and", "logical_not", "logical_xor"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Combine multiple conditions where any one being true triggers action. "
                        "Common for combining entry signals (buy_signal_1 OR buy_signal_2).",
        .limitations = "Both inputs must be boolean. Null propagates: true OR null = true, "
                       "false OR null = null."
    };
}

// logical_and: Boolean AND
inline TransformsMetaData MakeLogicalAndMetaData() {
    return TransformsMetaData{
        .id = "logical_and",
        .category = TransformCategory::Utility,
        .name = "Logical AND",
        .desc = "Element-wise boolean AND. Returns true only if both operands are true.",
        .inputs = {{.type = IODataType::Boolean, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Boolean, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"boolean", "logical", "combiner"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-combination", "filtering"},
        .relatedTransforms = {"logical_or", "logical_not", "logical_and_not"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Combine multiple conditions where all must be true. "
                        "Common for confirming signals (trend_up AND momentum_positive AND volume_above_average).",
        .limitations = "Both inputs must be boolean. Null propagates: false AND null = false, "
                       "true AND null = null."
    };
}

// logical_xor: Boolean XOR
inline TransformsMetaData MakeLogicalXorMetaData() {
    return TransformsMetaData{
        .id = "logical_xor",
        .category = TransformCategory::Utility,
        .name = "Logical XOR",
        .desc = "Element-wise boolean exclusive OR. Returns true if exactly one operand is true.",
        .inputs = {{.type = IODataType::Boolean, .id = epoch_script::ARG0, .name = "Left operand"},
                   {.type = IODataType::Boolean, .id = epoch_script::ARG1, .name = "Right operand"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"boolean", "logical", "combiner"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-combination", "divergence-detection"},
        .relatedTransforms = {"logical_or", "logical_and", "logical_not"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Detect mutually exclusive conditions. Useful for divergence signals "
                        "where one indicator is bullish while another is bearish.",
        .limitations = "Both inputs must be boolean. Any null input produces null output."
    };
}

// logical_and_not: Boolean AND NOT
inline TransformsMetaData MakeLogicalAndNotMetaData() {
    return TransformsMetaData{
        .id = "logical_and_not",
        .category = TransformCategory::Utility,
        .name = "Logical AND NOT",
        .desc = "Element-wise boolean AND NOT. Returns true if left is true and right is false. "
                "Equivalent to left AND (NOT right).",
        .inputs = {{.type = IODataType::Boolean, .id = epoch_script::ARG0, .name = "Condition"},
                   {.type = IODataType::Boolean, .id = epoch_script::ARG1, .name = "Exclusion"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"boolean", "logical", "combiner"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-combination", "filtering"},
        .relatedTransforms = {"logical_and", "logical_not"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Apply a condition with an exclusion filter. "
                        "Common for signals with veto conditions (buy_signal AND NOT risk_off).",
        .limitations = "Both inputs must be boolean. Combines AND with negation in one step."
    };
}

// logical_not: Boolean NOT
inline TransformsMetaData MakeLogicalNotMetaData() {
    return TransformsMetaData{
        .id = "logical_not",
        .category = TransformCategory::Utility,
        .name = "Logical NOT",
        .desc = "Element-wise boolean negation. Inverts each boolean value.",
        .inputs = {{.type = IODataType::Boolean, .id = epoch_script::ARG, .name = "Input"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"boolean", "logical", "unary"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-inversion", "filtering"},
        .relatedTransforms = {"logical_and", "logical_or"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Invert a boolean condition. Use to create sell signals from buy conditions, "
                        "or to exclude periods where a condition holds.",
        .limitations = "Input must be boolean. Null values remain null after negation."
    };
}

// ============================================================================
// ARITHMETIC OPERATORS
// ============================================================================

// modulo: Remainder after division
inline TransformsMetaData MakeModuloMetaData() {
    return TransformsMetaData{
        .id = "modulo",
        .category = TransformCategory::Math,
        .name = "Modulo",
        .desc = "Element-wise modulo (remainder) operation. Returns dividend mod divisor. "
                "Follows Python behavior for negative numbers.",
        .inputs = {{.type = IODataType::Decimal, .id = epoch_script::ARG0, .name = "Dividend"},
                   {.type = IODataType::Decimal, .id = epoch_script::ARG1, .name = "Divisor"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Remainder"}},
        .tags = {"math", "arithmetic", "modulo"},
        .requiresTimeFrame = false,
        .strategyTypes = {"pattern-detection", "cyclical-analysis"},
        .relatedTransforms = {"power_op"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Detect cyclical patterns or wrap values to a range. "
                        "Use for day-of-week analysis (bar_index mod 5), cyclical indicators.",
        .limitations = "Division by zero produces null. Floating-point modulo may have precision issues."
    };
}

// power_op: Exponentiation
inline TransformsMetaData MakePowerMetaData() {
    return TransformsMetaData{
        .id = "power_op",
        .category = TransformCategory::Math,
        .name = "Power",
        .desc = "Element-wise exponentiation. Raises base to the power of exponent.",
        .inputs = {{.type = IODataType::Decimal, .id = epoch_script::ARG0, .name = "Base"},
                   {.type = IODataType::Decimal, .id = epoch_script::ARG1, .name = "Exponent"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Result"}},
        .tags = {"math", "arithmetic", "exponent"},
        .requiresTimeFrame = false,
        .strategyTypes = {"indicator-calculation", "volatility"},
        .relatedTransforms = {"modulo"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Calculate squared values for variance, polynomial terms, or exponential scaling. "
                        "Common for volatility calculations (returns^2) or growth models.",
        .limitations = "Negative base with non-integer exponent produces null. "
                       "Very large exponents can overflow."
    };
}

// ============================================================================
// SELECTION OPERATORS
// ============================================================================

// boolean_select transforms (typed)
inline TransformsMetaData MakeBooleanSelectStringMetaData() {
    return TransformsMetaData{
        .id = "boolean_select_string",
        .category = TransformCategory::Utility,
        .name = "Boolean Select (String)",
        .desc = "Conditional selection returning String type. Where condition is true, "
                "returns true_value; otherwise returns false_value.",
        .inputs = {{.type = IODataType::Boolean, .id = "condition", .name = "Condition"},
                   {.type = IODataType::String, .id = "true", .name = "True value"},
                   {.type = IODataType::String, .id = "false", .name = "False value"}},
        .outputs = {{.type = IODataType::String, .id = epoch_script::RESULT, .name = "Selected value"}},
        .tags = {"conditional", "selection", "string"},
        .requiresTimeFrame = false,
        .strategyTypes = {"categorization", "labeling"},
        .relatedTransforms = {"boolean_select_number", "boolean_select_boolean", "boolean_select_timestamp"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Assign categorical labels based on conditions. "
                        "Common for creating text labels like 'bullish'/'bearish' or 'buy'/'sell'.",
        .limitations = "Null condition produces null output. Both value inputs must be String type."
    };
}

inline TransformsMetaData MakeBooleanSelectNumberMetaData() {
    return TransformsMetaData{
        .id = "boolean_select_number",
        .category = TransformCategory::Utility,
        .name = "Boolean Select (Number)",
        .desc = "Conditional selection returning Decimal type. Where condition is true, "
                "returns true_value; otherwise returns false_value.",
        .inputs = {{.type = IODataType::Boolean, .id = "condition", .name = "Condition"},
                   {.type = IODataType::Decimal, .id = "true", .name = "True value"},
                   {.type = IODataType::Decimal, .id = "false", .name = "False value"}},
        .outputs = {{.type = IODataType::Decimal, .id = epoch_script::RESULT, .name = "Selected value"}},
        .tags = {"conditional", "selection", "numeric"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-generation", "value-assignment"},
        .relatedTransforms = {"boolean_select_string", "boolean_select_boolean", "boolean_select_timestamp"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Assign numeric values based on conditions. "
                        "Common for position sizing (condition ? full_size : half_size) or "
                        "replacing values conditionally.",
        .limitations = "Null condition produces null output. Both value inputs must be Decimal type."
    };
}

inline TransformsMetaData MakeBooleanSelectBooleanMetaData() {
    return TransformsMetaData{
        .id = "boolean_select_boolean",
        .category = TransformCategory::Utility,
        .name = "Boolean Select (Boolean)",
        .desc = "Conditional selection returning Boolean type. Where condition is true, "
                "returns true_value; otherwise returns false_value.",
        .inputs = {{.type = IODataType::Boolean, .id = "condition", .name = "Condition"},
                   {.type = IODataType::Boolean, .id = "true", .name = "True value"},
                   {.type = IODataType::Boolean, .id = "false", .name = "False value"}},
        .outputs = {{.type = IODataType::Boolean, .id = epoch_script::RESULT, .name = "Selected value"}},
        .tags = {"conditional", "selection", "boolean"},
        .requiresTimeFrame = false,
        .strategyTypes = {"signal-generation", "logic"},
        .relatedTransforms = {"boolean_select_string", "boolean_select_number", "boolean_select_timestamp"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Select between boolean values based on a condition. "
                        "Use for complex logical expressions or conditional flag assignment.",
        .limitations = "Null condition produces null output. All inputs must be Boolean type."
    };
}

inline TransformsMetaData MakeBooleanSelectTimestampMetaData() {
    return TransformsMetaData{
        .id = "boolean_select_timestamp",
        .category = TransformCategory::Utility,
        .name = "Boolean Select (Timestamp)",
        .desc = "Conditional selection returning Timestamp type. Where condition is true, "
                "returns true_value; otherwise returns false_value.",
        .inputs = {{.type = IODataType::Boolean, .id = "condition", .name = "Condition"},
                   {.type = IODataType::Timestamp, .id = "true", .name = "True value"},
                   {.type = IODataType::Timestamp, .id = "false", .name = "False value"}},
        .outputs = {{.type = IODataType::Timestamp, .id = epoch_script::RESULT, .name = "Selected value"}},
        .tags = {"conditional", "selection", "timestamp"},
        .requiresTimeFrame = false,
        .strategyTypes = {"event-tracking", "time-analysis"},
        .relatedTransforms = {"boolean_select_string", "boolean_select_number", "boolean_select_boolean"},
        .assetRequirements = {"single-asset"},
        .usageContext = "Select between timestamp values based on a condition. "
                        "Use for tracking event times conditionally.",
        .limitations = "Null condition produces null output. Both value inputs must be Timestamp type."
    };
}

// ============================================================================
// REGISTRATION FUNCTION
// ============================================================================

inline void Register() {
    // -----------------------------------------------------------------------
    // Register transforms (execution)
    // -----------------------------------------------------------------------

    // Comparison operators
    epoch_script::transform::Register<VectorGT>("gt");
    epoch_script::transform::Register<VectorGTE>("gte");
    epoch_script::transform::Register<VectorLT>("lt");
    epoch_script::transform::Register<VectorLTE>("lte");
    epoch_script::transform::Register<VectorEQ>("eq");
    epoch_script::transform::Register<VectorNEQ>("neq");

    // Logical operators
    epoch_script::transform::Register<LogicalOR>("logical_or");
    epoch_script::transform::Register<LogicalAND>("logical_and");
    epoch_script::transform::Register<LogicalXOR>("logical_xor");
    epoch_script::transform::Register<LogicalAND_NOT>("logical_and_not");
    epoch_script::transform::Register<LogicalNot>("logical_not");

    // Validation transforms
    epoch_script::transform::Register<IsNull>("is_null");
    epoch_script::transform::Register<IsValid>("is_valid");
    epoch_script::transform::Register<IsZero>("is_zero");
    epoch_script::transform::Register<IsOne>("is_one");

    // Type conversion transforms
    epoch_script::transform::Register<Stringify>("stringify");

    // Static cast transforms (compiler-inserted)
    epoch_script::transform::Register<StaticCastToInteger>("static_cast_to_integer");
    epoch_script::transform::Register<StaticCastToDecimal>("static_cast_to_decimal");
    epoch_script::transform::Register<StaticCastToBoolean>("static_cast_to_boolean");
    epoch_script::transform::Register<StaticCastToString>("static_cast_to_string");
    epoch_script::transform::Register<StaticCastToTimestamp>("static_cast_to_timestamp");

    // Alias transforms (compiler-inserted)
    epoch_script::transform::Register<AliasDecimal>("alias_decimal");
    epoch_script::transform::Register<AliasBoolean>("alias_boolean");
    epoch_script::transform::Register<AliasString>("alias_string");
    epoch_script::transform::Register<AliasInteger>("alias_integer");
    epoch_script::transform::Register<AliasTimestamp>("alias_timestamp");

    // Arithmetic operators
    epoch_script::transform::Register<ModuloTransform>("modulo");
    epoch_script::transform::Register<PowerTransform>("power_op");

    // Boolean select transforms (typed)
    epoch_script::transform::Register<BooleanSelectString>("boolean_select_string");
    epoch_script::transform::Register<BooleanSelectNumber>("boolean_select_number");
    epoch_script::transform::Register<BooleanSelectBoolean>("boolean_select_boolean");
    epoch_script::transform::Register<BooleanSelectTimestamp>("boolean_select_timestamp");

    // -----------------------------------------------------------------------
    // Register metadata (introspection)
    // -----------------------------------------------------------------------
    auto& registry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // Comparison operators
    registry.Register(MakeGtMetaData());
    registry.Register(MakeGteMetaData());
    registry.Register(MakeLtMetaData());
    registry.Register(MakeLteMetaData());
    registry.Register(MakeEqMetaData());
    registry.Register(MakeNeqMetaData());

    // Logical operators
    registry.Register(MakeLogicalOrMetaData());
    registry.Register(MakeLogicalAndMetaData());
    registry.Register(MakeLogicalXorMetaData());
    registry.Register(MakeLogicalAndNotMetaData());
    registry.Register(MakeLogicalNotMetaData());

    // Arithmetic operators
    registry.Register(MakeModuloMetaData());
    registry.Register(MakePowerMetaData());

    // Boolean select transforms
    registry.Register(MakeBooleanSelectStringMetaData());
    registry.Register(MakeBooleanSelectNumberMetaData());
    registry.Register(MakeBooleanSelectBooleanMetaData());
    registry.Register(MakeBooleanSelectTimestampMetaData());

    // Register metadata from existing metadata files (validation, static_cast, alias, stringify)
    for (const auto& meta : MakeValidationMetaData()) {
        registry.Register(meta);
    }
    for (const auto& meta : MakeStaticCastMetaData()) {
        registry.Register(meta);
    }
    for (const auto& meta : MakeAliasMetaData()) {
        registry.Register(meta);
    }
    for (const auto& meta : MakeStringifyMetaData()) {
        registry.Register(meta);
    }

    // Control flow transforms metadata (boolean_branch, ratio_branch, trade_executor_adapter)
    for (const auto& meta : epoch_script::transforms::MakeControlFlowMetaData()) {
        registry.Register(meta);
    }
}

}  // namespace epoch_script::transform::operators
