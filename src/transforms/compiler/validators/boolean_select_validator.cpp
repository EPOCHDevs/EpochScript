//
// Created by Claude Code
// Validator for boolean_select transform
//
// Validates that:
// 1. All three inputs (condition, true, false) are provided
// 2. The 'true' and 'false' inputs have compatible types
//

#include "boolean_select_validator.h"
#include "../type_checker.h"
#include <stdexcept>

namespace epoch_script
{
    void BooleanSelectValidator::ValidateInputs(const ValidationContext& ctx) const {
        // boolean_select_* takes positional arguments: (condition, true, false)
        // Check that we have exactly 3 positional arguments
        if (ctx.args.size() != 3) {
            throw std::runtime_error(
                "'" + ctx.component_name + "' requires exactly 3 inputs (condition, true, false) for node '" +
                ctx.target_node_id + "', got " + std::to_string(ctx.args.size()));
        }

        // Get the handles for true and false values (positions 1 and 2)
        // Position 0 is the condition, which we don't need to validate type-wise
        const auto& true_handle = ctx.args[1];       // Position 1: true value
        const auto& false_handle = ctx.args[2];      // Position 2: false value

        // Get the types of the true and false inputs
        DataType true_type = ctx.type_checker.GetNodeOutputType(true_handle);
        DataType false_type = ctx.type_checker.GetNodeOutputType(false_handle);

        // If either type is still Any after GetNodeOutputType (which tries to resolve),
        // we still need to validate - numeric vs string types should be caught
        // Note: GetNodeOutputType already calls ResolveAnyOutputType internally when it encounters Any

        // Check if types are compatible
        // Both can be Any, or they must be compatible
        if (true_type == DataType::Any || false_type == DataType::Any) {
            // If either is Any, we can't validate at compile time
            return;
        }

        // Check if the types are compatible using the type checker's logic
        if (!ctx.type_checker.IsTypeCompatible(true_type, false_type) &&
            !ctx.type_checker.IsTypeCompatible(false_type, true_type)) {
            throw std::runtime_error(
                "'" + ctx.component_name + "' requires 'true' and 'false' inputs to have compatible types for node '" +
                ctx.target_node_id + "'. Got true: " +
                TypeChecker::DataTypeToString(true_type) + ", false: " +
                TypeChecker::DataTypeToString(false_type));
        }
    }

    // Explicit registration function - register for all type-specialized variants
    void RegisterBooleanSelectValidator() {
        auto validator = std::make_shared<BooleanSelectValidator>();
        SpecialNodeValidatorRegistry::Instance().Register("boolean_select_string", validator);
        SpecialNodeValidatorRegistry::Instance().Register("boolean_select_number", validator);
        SpecialNodeValidatorRegistry::Instance().Register("boolean_select_boolean", validator);
        SpecialNodeValidatorRegistry::Instance().Register("boolean_select_timestamp", validator);
    }

} // namespace epoch_script
