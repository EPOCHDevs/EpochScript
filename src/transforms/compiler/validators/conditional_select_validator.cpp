//
// Created by Claude Code
// Validator for conditional_select transform
//
// Validates that:
// 1. At least 2 inputs (minimum 1 condition + 1 value)
// 2. Even-indexed inputs (0, 2, 4, ...) are Boolean (conditions)
// Value type compatibility is handled by existing type checker.
//

#include "conditional_select_validator.h"
#include "../type_checker.h"
#include <stdexcept>

namespace epoch_script
{
    void ConditionalSelectValidator::ValidateInputs(const ValidationContext& ctx) const {
        // Validate at least 2 inputs (1 condition + 1 value minimum)
        if (ctx.args.size() < 2) {
            throw std::runtime_error(
                "'conditional_select' requires at least 2 inputs (condition, value) for node '" +
                ctx.target_node_id + "'");
        }

        // Validate alternating boolean conditions at even indices
        size_t num_pairs = ctx.args.size() / 2;
        for (size_t i = 0; i < num_pairs; i++) {
            size_t cond_idx = i * 2;
            const auto& cond_handle = ctx.args[cond_idx];

            DataType cond_type = ctx.type_checker.GetNodeOutputType(cond_handle);

            if (cond_type != DataType::Boolean && cond_type != DataType::Any) {
                throw std::runtime_error(
                    "'conditional_select' input at position " + std::to_string(cond_idx) +
                    " must be Boolean (condition) for node '" + ctx.target_node_id +
                    "', got " + TypeChecker::DataTypeToString(cond_type));
            }
        }

        // Value type compatibility is checked by existing type checker
    }

    // Auto-register this validator
    REGISTER_SPECIAL_VALIDATOR("conditional_select", ConditionalSelectValidator)

} // namespace epoch_script
