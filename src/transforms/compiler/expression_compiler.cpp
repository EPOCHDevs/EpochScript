//
// Created by Claude Code
// EpochScript Expression Compiler Implementation
//

#include "expression_compiler.h"
#include "constructor_parser.h"
#include "option_validator.h"
#include "special_parameter_handler.h"
#include "error_formatting/all_errors.h"
#include <epoch_core/enum_wrapper.h>
#include <algorithm>
#include <stdexcept>

namespace epoch_script
{

    strategy::InputValue ExpressionCompiler::VisitExpr(const Expr& expr)
    {
        if (auto* call = dynamic_cast<const Call*>(&expr))
        {
            return VisitCall(*call);
        }
        else if (auto* attr = dynamic_cast<const Attribute*>(&expr))
        {
            return VisitAttribute(*attr);
        }
        else if (auto* name = dynamic_cast<const Name*>(&expr))
        {
            return VisitName(*name);
        }
        else if (auto* constant = dynamic_cast<const Constant*>(&expr))
        {
            return VisitConstant(*constant);
        }
        else if (auto* bin_op = dynamic_cast<const BinOp*>(&expr))
        {
            return VisitBinOp(*bin_op);
        }
        else if (auto* unary_op = dynamic_cast<const UnaryOp*>(&expr))
        {
            return VisitUnaryOp(*unary_op);
        }
        else if (auto* compare = dynamic_cast<const Compare*>(&expr))
        {
            return VisitCompare(*compare);
        }
        else if (auto* bool_op = dynamic_cast<const BoolOp*>(&expr))
        {
            return VisitBoolOp(*bool_op);
        }
        else if (auto* if_exp = dynamic_cast<const IfExp*>(&expr))
        {
            return VisitIfExp(*if_exp);
        }
        else if (auto* subscript = dynamic_cast<const Subscript*>(&expr))
        {
            return VisitSubscript(*subscript);
        }

        // Determine what type of unsupported expression this is
        std::string expr_type_name = typeid(expr).name(); // C++ type name
        ThrowError("Unsupported expression type. This expression cannot be used in this context. "
                   "Supported: function calls, variables, constants, arithmetic (+,-,*,/), comparisons (>,<,==), "
                   "boolean logic (and,or), conditionals (if/else), subscripts. (Internal type: " + expr_type_name + ")",
                   expr.lineno, expr.col_offset);
        glz::unreachable();
    }

    strategy::InputValue ExpressionCompiler::VisitCall(const Call& call)
    {
        // Phase 2: Handle inline constructor calls in expressions
        // Examples: gt(a, b), abs(value), ema(10)(src.c)

        // Parse as constructor call
        auto parse_result = constructor_parser_->ParseConstructorAndFeeds(call);

        // Validate component exists
        if (!context_.HasComponent(parse_result.ctor_name))
        {
            ThrowError("Unknown component '" + parse_result.ctor_name + "'", call.lineno, call.col_offset);
        }

        const auto& comp_meta = context_.GetComponentMetadata(parse_result.ctor_name);

        // Check if component has no outputs (is a sink/reporter)
        if (comp_meta.outputs.empty())
        {
            ThrowError("Direct call to component with outputs must be assigned to a variable", call.lineno, call.col_offset);
        }

        // Create synthetic node ID using component name (like: sma_0, ema_0, etc.)
        std::string synthetic_id = UniqueNodeId(parse_result.ctor_name);

        // Canonicalize special parameters
        auto params = parse_result.ctor_kwargs;
        special_param_handler_.CanonicalizeTimeframe(params);
        special_param_handler_.CanonicalizeSession(params);

        // Validate and apply option defaults/clamping
        option_validator_.ValidateAndApplyOptions(synthetic_id, comp_meta, params, call);

        // Create AlgorithmNode
        epoch_script::strategy::AlgorithmNode algo;
        algo.id = synthetic_id;
        algo.type = parse_result.ctor_name;

        // Convert regular options (excluding timeframe and session)
        for (const auto& [key, value] : params)
        {
            if (key != "timeframe" && key != "session")
            {
                algo.options[key] = epoch_script::MetaDataOptionDefinition{value};
            }
        }

        // Apply special fields (timeframe and session)
        special_param_handler_.ApplySpecialFields(algo, params);

        // Add to algorithms list
        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[synthetic_id] = context_.algorithms.size() - 1;
        context_.var_to_binding[synthetic_id] = parse_result.ctor_name;

        // Track executor count
        if (parse_result.ctor_name == "trade_signal_executor")
        {
            context_.executor_count++;
        }

        // Wire inputs from feed steps
        for (const auto& [args, kwargs] : parse_result.feed_steps)
        {
            WireInputs(synthetic_id, parse_result.ctor_name, args, kwargs);
        }

        // Return the output handle
        // For single-output components, use the first (and only) output
        if (comp_meta.outputs.size() == 1)
        {
            std::string out_handle = comp_meta.outputs[0].id;
            return strategy::NodeReference{synthetic_id, out_handle};
        }
        else
        {
            // Multi-output component used in expression context - error
            ThrowError("Component '" + parse_result.ctor_name + "' has " +
                       std::to_string(comp_meta.outputs.size()) +
                       " outputs; must be assigned to tuple");
        }
    }

    strategy::InputValue ExpressionCompiler::VisitAttribute(const Attribute& attr)
    {
        // Phase 2: Support attribute access on any expression, not just names
        // Examples: call().result, (ternary).result

        const Expr* base_expr = attr.value.get();

        // Check if base is a Name (simple case: var.handle)
        if (dynamic_cast<const Name*>(base_expr))
        {
            // Traditional attribute access: name.handle
            auto [var, handle] = AttributeToTuple(attr);
            return ResolveHandle(var, handle);
        }
        else
        {
            // Expression-based attribute access: expr.handle
            // Evaluate the base expression first
            strategy::InputValue base_handle = VisitExpr(*base_expr);

            // Now access the requested attribute (handle) on the result
            // The attr.attr field is the handle name we're accessing
            return strategy::NodeReference{base_handle.GetNodeReference().GetNodeId(), attr.attr};
        }
    }

    strategy::InputValue ExpressionCompiler::VisitName(const Name& name)
    {
        // Handle lowercase boolean literals as special keywords
        if (name.id == "true")
        {
            return MaterializeBoolean(true);
        }
        else if (name.id == "false")
        {
            return MaterializeBoolean(false);
        }

        // Look up variable in bindings
        auto it = context_.var_to_binding.find(name.id);
        if (it == context_.var_to_binding.end())
        {
            ThrowError("Unknown variable '" + name.id + "'. Variable has not been defined or assigned. "
                       "Make sure to define the variable before using it (e.g., " + name.id + " = some_value).",
                       name.lineno, name.col_offset);
        }

        const std::string& ref = it->second;

        // Check if bound to a specific node.handle
        size_t dot_pos = ref.find('.');
        if (dot_pos != std::string::npos)
        {
            std::string node_id = ref.substr(0, dot_pos);
            std::string handle = ref.substr(dot_pos + 1);
            return strategy::NodeReference{node_id, handle};
        }

        // Otherwise, ref is a component name - need to resolve single output
        const std::string& comp_name = ref;

        // Check if it's a synthetic literal node
        if (comp_name == "number" || comp_name == "bool_true" ||
            comp_name == "bool_false" || comp_name == "text" || comp_name == "null")
        {
            return strategy::NodeReference{name.id, "result"};
        }

        // Look up component metadata
        if (!context_.HasComponent(comp_name))
        {
            ThrowError("Unknown component '" + comp_name + "'", name.lineno, name.col_offset);
        }

        const auto& comp_meta = context_.GetComponentMetadata(comp_name);
        const auto& outputs = comp_meta.outputs;

        if (outputs.empty())
        {
            ThrowError("Component '" + comp_name + "' has no outputs", name.lineno, name.col_offset);
        }

        // Must have exactly one output for unambiguous resolution
        if (outputs.size() != 1)
        {
            ThrowError("Ambiguous output for '" + name.id + "'", name.lineno, name.col_offset);
        }

        // Get the output handle
        std::string handle = outputs[0].id;

        return strategy::NodeReference{name.id, handle};
    }

    strategy::InputValue ExpressionCompiler::VisitConstant(const Constant& constant)
    {
        // Materialize constants as nodes
        return std::visit([this, &constant](auto&& value) -> strategy::InputValue
        {
            using T = std::decay_t<decltype(value)>;

            if constexpr (std::is_same_v<T, int>) {
                return MaterializeNumber(static_cast<double>(value));
            } else if constexpr (std::is_same_v<T, double>) {
                return MaterializeNumber(value);
            } else if constexpr (std::is_same_v<T, bool>) {
                return MaterializeBoolean(value);
            } else if constexpr (std::is_same_v<T, std::string>) {
                return MaterializeText(value);
            } else if constexpr (std::is_same_v<T, std::monostate>) {
                return MaterializeNull();
            } else {
                ThrowError("Unsupported constant type", constant.lineno, constant.col_offset);
                std::unreachable();
            }
        }, constant.value);
    }

    strategy::InputValue ExpressionCompiler::VisitBinOp(const BinOp& bin_op)
    {
        // Map operator type to component name
        std::string comp_name;
        switch (bin_op.op)
        {
        case BinOpType::Add:
            comp_name = "add";
            break;
        case BinOpType::Sub:
            comp_name = "sub";
            break;
        case BinOpType::Mult:
            comp_name = "mul";
            break;
        case BinOpType::Div:
            comp_name = "div";
            break;
        case BinOpType::Lt:
            comp_name = "lt";
            break;
        case BinOpType::Gt:
            comp_name = "gt";
            break;
        case BinOpType::LtE:
            comp_name = "lte";
            break;
        case BinOpType::GtE:
            comp_name = "gte";
            break;
        case BinOpType::Eq:
            comp_name = "eq";
            break;
        case BinOpType::NotEq:
            comp_name = "neq";
            break;
        case BinOpType::And:
            comp_name = "logical_and";
            break;
        case BinOpType::Or:
            comp_name = "logical_or";
            break;
        case BinOpType::Mod:
            comp_name = "modulo";
            break;
        case BinOpType::Pow:
            comp_name = "power_op";
            break;
        default:
            ThrowError("Unsupported binary operator. Supported operators: +, -, *, /, %, ** (power), "
                       "<, >, <=, >=, ==, !=, and, or", bin_op.lineno, bin_op.col_offset);
        }

        // Validate component exists
        if (!context_.HasComponent(comp_name))
        {
            ThrowError("Unknown operator component '" + comp_name + "'", bin_op.lineno, bin_op.col_offset);
        }

        const auto& comp_meta = context_.GetComponentMetadata(comp_name);

        // IMPORTANT: Create node ID and add placeholder to algorithms_ BEFORE recursing
        // This matches Python's behavior where parent nodes get lower IDs than children
        std::string node_id = UniqueNodeId(comp_name);
        epoch_script::strategy::AlgorithmNode algo;
        algo.id = node_id;
        algo.type = comp_name;

        // Add placeholder to algorithms_ to reserve the ID
        // Store the index since algorithms_ may reallocate during recursion
        size_t node_index = context_.algorithms.size();
        context_.algorithms.push_back(std::move(algo));

        // Now resolve left and right operands (may create child nodes with higher IDs)
        strategy::InputValue left = VisitExpr(*bin_op.left);
        strategy::InputValue right = VisitExpr(*bin_op.right);

        // Get input names and types from component metadata dynamically
        std::vector<std::string> input_names;
        std::unordered_map<std::string, DataType> input_types;
        const auto& inputs = comp_meta.inputs;

        for (const auto& input : inputs)
        {
            std::string input_id = input.id;

            // Handle SLOT naming convention (* -> SLOT, *0 -> SLOT0, etc.)
            if (!input_id.empty() && input_id[0] == '*')
            {
                std::string suffix = input_id.substr(1);
                input_id = suffix.empty() ? "SLOT" : "SLOT" + suffix;
            }

            input_names.push_back(input_id);

            // Map input type from IOMetaData to DataType enum
            std::string type_str = epoch_core::EnumWrapper<epoch_core::IODataType>::type::ToString(input.type);
            if (type_str == "Boolean")
                input_types[input_id] = DataType::Boolean;
            else if (type_str == "Integer")
                input_types[input_id] = DataType::Integer;
            else if (type_str == "Decimal")
                input_types[input_id] = DataType::Decimal;
            else if (type_str == "Number")
                input_types[input_id] = DataType::Number;
            else if (type_str == "String")
                input_types[input_id] = DataType::String;
            else
                input_types[input_id] = DataType::Any;
        }

        // Binary operators should have exactly 2 inputs
        if (input_names.size() != 2)
        {
            ThrowError("Binary operator '" + comp_name + "' must have exactly 2 inputs, got " +
                       std::to_string(input_names.size()),
                       bin_op.lineno, bin_op.col_offset);
        }

        // Extract first two input names dynamically (works for SLOT0/SLOT1, named inputs, etc.)
        std::string left_input_name = input_names[0];
        std::string right_input_name = input_names[1];

        // Type checking and casting for left operand
        DataType left_source_type = type_checker_.GetNodeOutputType(left);
        DataType left_target_type = input_types[left_input_name];

        if (!type_checker_.IsTypeCompatible(left_source_type, left_target_type))
        {
            auto cast_result = type_checker_.NeedsTypeCast(left_source_type, left_target_type);
            if (cast_result.has_value() && cast_result.value() != "incompatible")
            {
                left = type_checker_.InsertTypeCast(left, left_source_type, left_target_type);
            }
            else
            {
                ThrowError("Type error in binary operation '" + comp_name + "': " +
                           "left operand ('" + left_input_name + "') must be " + TypeChecker::DataTypeToString(left_target_type) +
                           ", but received " + TypeChecker::DataTypeToString(left_source_type) +
                           " from '" + left.GetColumnIdentifier() + "'",
                           bin_op.lineno, bin_op.col_offset);
            }
        }

        // Type checking and casting for right operand
        DataType right_source_type = type_checker_.GetNodeOutputType(right);
        DataType right_target_type = input_types[right_input_name];

        if (!type_checker_.IsTypeCompatible(right_source_type, right_target_type))
        {
            auto cast_result = type_checker_.NeedsTypeCast(right_source_type, right_target_type);
            if (cast_result.has_value() && cast_result.value() != "incompatible")
            {
                right = type_checker_.InsertTypeCast(right, right_source_type, right_target_type);
            }
            else
            {
                ThrowError("Type error in binary operation '" + comp_name + "': " +
                           "right operand ('" + right_input_name + "') must be " + TypeChecker::DataTypeToString(right_target_type) +
                           ", but received " + TypeChecker::DataTypeToString(right_source_type) +
                           " from '" + right.GetColumnIdentifier() + "'",
                           bin_op.lineno, bin_op.col_offset);
            }
        }

        // Wire inputs to the node we created earlier using dynamic input names
        context_.algorithms[node_index].inputs[left_input_name].push_back(left);
        context_.algorithms[node_index].inputs[right_input_name].push_back(right);

        // Update node_lookup_ AFTER recursion (index never invalidated)
        context_.node_lookup[node_id] = node_index;

        // Track output type for operators
        if (comp_name == "lt" || comp_name == "gt" || comp_name == "lte" ||
            comp_name == "gte" || comp_name == "eq" || comp_name == "neq" ||
            comp_name == "logical_and" || comp_name == "logical_or")
        {
            context_.node_output_types[node_id]["result"] = DataType::Boolean;
        }
        else if (comp_name == "add" || comp_name == "sub" ||
                 comp_name == "mul" || comp_name == "div")
        {
            context_.node_output_types[node_id]["result"] = DataType::Decimal;
        }

        // Get output handle from metadata
        std::string out_handle = "result";
        const auto& outputs = comp_meta.outputs;
        if (!outputs.empty())
        {
            out_handle = outputs[0].id;
        }

        return strategy::NodeReference{node_id, out_handle};
    }

    strategy::InputValue ExpressionCompiler::VisitUnaryOp(const UnaryOp& unary_op)
    {
        // Handle unary plus (idempotent - just return the operand)
        if (unary_op.op == UnaryOpType::UAdd)
        {
            return VisitExpr(*unary_op.operand);
        }

        // Handle negation as multiplication by -1
        if (unary_op.op == UnaryOpType::USub)
        {
            // Create -1 number node
            strategy::InputValue minus_one = MaterializeNumber(-1.0);

            // Resolve operand
            strategy::InputValue operand = VisitExpr(*unary_op.operand);

            // Create mul AlgorithmNode
            std::string node_id = UniqueNodeId("mul");
            epoch_script::strategy::AlgorithmNode algo;
            algo.id = node_id;
            algo.type = "mul";

            // Wire inputs: (-1) * operand
            algo.inputs[ARG0].push_back(minus_one);
            algo.inputs[ARG1].push_back(operand);

            // Add to algorithms list
            context_.algorithms.push_back(std::move(algo));
            context_.node_lookup[node_id] = context_.algorithms.size() - 1;

            // Track output type
            context_.node_output_types[node_id]["result"] = DataType::Decimal;

            return strategy::NodeReference{node_id, "result"};
        }

        // Handle logical not
        if (unary_op.op == UnaryOpType::Not)
        {
            std::string comp_name = "logical_not";

            // Validate component exists
            if (!context_.HasComponent(comp_name))
            {
                ThrowError("Unknown operator component '" + comp_name + "'", unary_op.lineno, unary_op.col_offset);
            }

            const auto& comp_meta = context_.GetComponentMetadata(comp_name);

            // Resolve operand FIRST (child-first/topological ordering required for timeframe resolution)
            strategy::InputValue operand = VisitExpr(*unary_op.operand);

            // Create node AFTER resolving operand
            std::string node_id = UniqueNodeId(comp_name);
            epoch_script::strategy::AlgorithmNode algo;
            algo.id = node_id;
            algo.type = comp_name;

            // Wire input (SLOT for unary operators)
            algo.inputs[ARG].push_back(operand);

            // Add to algorithms list
            context_.algorithms.push_back(std::move(algo));
            context_.node_lookup[node_id] = context_.algorithms.size() - 1;

            // Track output type
            context_.node_output_types[node_id]["result"] = DataType::Boolean;

            // Get output handle
            std::string out_handle = "result";
            const auto& outputs = comp_meta.outputs;
            if (!outputs.empty())
            {
                out_handle = outputs[0].id;
            }

            return strategy::NodeReference{node_id, out_handle};
        }

        ThrowError("Unsupported unary operator. Supported unary operators: - (negation), + (identity), not (logical negation)",
                   unary_op.lineno, unary_op.col_offset);
        std::unreachable();
    }

    strategy::InputValue ExpressionCompiler::VisitCompare(const Compare& compare)
    {
        // Only single comparisons supported (a < b, not a < b < c)
        if (compare.ops.size() != 1 || compare.comparators.size() != 1)
        {
            ThrowError("Only single comparisons supported", compare.lineno, compare.col_offset);
        }

        // Map comparison operator to component name
        std::string comp_name;
        switch (compare.ops[0])
        {
        case BinOpType::Lt:
            comp_name = "lt";
            break;
        case BinOpType::Gt:
            comp_name = "gt";
            break;
        case BinOpType::LtE:
            comp_name = "lte";
            break;
        case BinOpType::GtE:
            comp_name = "gte";
            break;
        case BinOpType::Eq:
            comp_name = "eq";
            break;
        case BinOpType::NotEq:
            comp_name = "neq";
            break;
        default:
            ThrowError("Unsupported comparison operator. Supported: <, >, <=, >=, ==, !=",
                       compare.lineno, compare.col_offset);
        }

        // Validate component exists
        if (!context_.HasComponent(comp_name))
        {
            ThrowError("Unknown operator component '" + comp_name + "'", compare.lineno, compare.col_offset);
        }

        const auto& comp_meta = context_.GetComponentMetadata(comp_name);

        // Resolve operands FIRST (child-first/topological ordering required for timeframe resolution)
        strategy::InputValue left = VisitExpr(*compare.left);
        strategy::InputValue right = VisitExpr(*compare.comparators[0]);

        // Create node AFTER resolving operands
        std::string node_id = UniqueNodeId(comp_name);
        epoch_script::strategy::AlgorithmNode algo;
        algo.id = node_id;
        algo.type = comp_name;

        // Get input names and types from component metadata dynamically
        std::vector<std::string> input_names;
        std::unordered_map<std::string, DataType> input_types;
        const auto& inputs = comp_meta.inputs;

        for (const auto& input : inputs)
        {
            std::string input_id = input.id;

            // Handle SLOT naming convention (* -> SLOT, *0 -> SLOT0, etc.)
            if (!input_id.empty() && input_id[0] == '*')
            {
                std::string suffix = input_id.substr(1);
                input_id = suffix.empty() ? "SLOT" : "SLOT" + suffix;
            }

            input_names.push_back(input_id);

            // Map input type from IOMetaData to DataType enum
            std::string type_str = epoch_core::EnumWrapper<epoch_core::IODataType>::type::ToString(input.type);
            if (type_str == "Boolean")
                input_types[input_id] = DataType::Boolean;
            else if (type_str == "Integer")
                input_types[input_id] = DataType::Integer;
            else if (type_str == "Decimal")
                input_types[input_id] = DataType::Decimal;
            else if (type_str == "Number")
                input_types[input_id] = DataType::Number;
            else if (type_str == "String")
                input_types[input_id] = DataType::String;
            else
                input_types[input_id] = DataType::Any;
        }

        // Comparison operators should have exactly 2 inputs
        if (input_names.size() != 2)
        {
            ThrowError("Comparison operator '" + comp_name + "' must have exactly 2 inputs, got " +
                       std::to_string(input_names.size()),
                       compare.lineno, compare.col_offset);
        }

        // Extract first two input names dynamically
        std::string left_input_name = input_names[0];
        std::string right_input_name = input_names[1];

        // Type checking and casting for left operand
        DataType left_source_type = type_checker_.GetNodeOutputType(left);
        DataType left_target_type = input_types[left_input_name];

        if (!type_checker_.IsTypeCompatible(left_source_type, left_target_type))
        {
            auto cast_result = type_checker_.NeedsTypeCast(left_source_type, left_target_type);
            if (cast_result.has_value() && cast_result.value() != "incompatible")
            {
                left = type_checker_.InsertTypeCast(left, left_source_type, left_target_type);
            }
            else
            {
                ThrowError("Type error in comparison '" + comp_name + "': " +
                           "left operand ('" + left_input_name + "') must be " + TypeChecker::DataTypeToString(left_target_type) +
                           ", but received " + TypeChecker::DataTypeToString(left_source_type) +
                           " from '" + left.GetColumnIdentifier() + "'",
                           compare.lineno, compare.col_offset);
            }
        }

        // Type checking and casting for right operand
        DataType right_source_type = type_checker_.GetNodeOutputType(right);
        DataType right_target_type = input_types[right_input_name];

        if (!type_checker_.IsTypeCompatible(right_source_type, right_target_type))
        {
            auto cast_result = type_checker_.NeedsTypeCast(right_source_type, right_target_type);
            if (cast_result.has_value() && cast_result.value() != "incompatible")
            {
                right = type_checker_.InsertTypeCast(right, right_source_type, right_target_type);
            }
            else
            {
                ThrowError("Type error in comparison '" + comp_name + "': " +
                           "right operand ('" + right_input_name + "') must be " + TypeChecker::DataTypeToString(right_target_type) +
                           ", but received " + TypeChecker::DataTypeToString(right_source_type) +
                           " from '" + right.GetColumnIdentifier() + "'",
                           compare.lineno, compare.col_offset);
            }
        }

        // Wire inputs
        algo.inputs[left_input_name].push_back(left);
        algo.inputs[right_input_name].push_back(right);

        // Add to algorithms list
        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[node_id] = context_.algorithms.size() - 1;

        // Track output type (comparisons return Boolean)
        context_.node_output_types[node_id]["result"] = DataType::Boolean;

        // Get output handle
        std::string out_handle = "result";
        const auto& outputs = comp_meta.outputs;
        if (!outputs.empty())
        {
            out_handle = outputs[0].id;
        }

        return strategy::NodeReference{node_id, out_handle};
    }

    strategy::InputValue ExpressionCompiler::VisitBoolOp(const BoolOp& bool_op)
    {
        // Boolean operations (and, or) with multiple operands
        // Convert to nested binary operations: (a and b and c) -> (a and (b and c))

        if (bool_op.values.size() < 2)
        {
            ThrowError("Boolean operation needs at least 2 operands", bool_op.lineno, bool_op.col_offset);
        }

        // Evaluate all operands and cast to Boolean if needed
        std::vector<strategy::InputValue> handles;
        for (const auto& value : bool_op.values)
        {
            strategy::InputValue handle = VisitExpr(*value);

            // Type check and cast to Boolean if needed
            DataType source_type = type_checker_.GetNodeOutputType(handle);
            DataType target_type = DataType::Boolean;

            if (!type_checker_.IsTypeCompatible(source_type, target_type))
            {
                auto cast_result = type_checker_.NeedsTypeCast(source_type, target_type);
                if (cast_result.has_value() && cast_result.value() != "incompatible")
                {
                    handle = type_checker_.InsertTypeCast(handle, source_type, target_type);
                }
                else
                {
                    ThrowError("Cannot use type " + type_checker_.DataTypeToString(source_type) +
                              " in boolean operation (and/or)", bool_op.lineno, bool_op.col_offset);
                }
            }

            handles.push_back(handle);
        }

        // Build nested structure: (a and b and c) -> logical_and_0(a, logical_and_1(b, c))
        std::string comp_name = (bool_op.op == BinOpType::And) ? "logical_and" : "logical_or";

        // Pre-create all logical_and/logical_or nodes needed (n-1 nodes for n operands)
        std::vector<size_t> node_indices;
        std::vector<std::string> node_ids;
        for (size_t i = 0; i < handles.size() - 1; ++i)
        {
            std::string node_id = UniqueNodeId(comp_name);
            epoch_script::strategy::AlgorithmNode algo;
            algo.id = node_id;
            algo.type = comp_name;

            size_t node_index = context_.algorithms.size();
            context_.algorithms.push_back(std::move(algo));

            node_indices.push_back(node_index);
            node_ids.push_back(node_id);
        }

        // Now wire them up: (a and b and c) -> logical_and_0(a, logical_and_1(b, c))
        if (handles.size() == 2)
        {
            // Simple case: just two operands
            context_.algorithms[node_indices[0]].inputs[ARG0].push_back(handles[0]);
            context_.algorithms[node_indices[0]].inputs[ARG1].push_back(handles[1]);
        }
        else
        {
            // Complex case: a and b and c ...
            context_.algorithms[node_indices[0]].inputs[ARG0].push_back(handles[0]);
            context_.algorithms[node_indices[0]].inputs[ARG1].emplace_back(strategy::NodeReference(node_ids[1], "result"));

            // Middle nodes
            for (size_t i = 1; i < node_ids.size() - 1; ++i)
            {
                context_.algorithms[node_indices[i]].inputs[ARG0].push_back(handles[i]);
                context_.algorithms[node_indices[i]].inputs[ARG1].emplace_back(strategy::NodeReference(node_ids[i + 1], "result"));
            }

            // Last node
            size_t last_idx = node_ids.size() - 1;
            context_.algorithms[node_indices[last_idx]].inputs[ARG0].push_back(handles[last_idx]);
            context_.algorithms[node_indices[last_idx]].inputs[ARG1].push_back(handles[last_idx + 1]);
        }

        // Update node_lookup_ and track output types
        for (size_t i = 0; i < node_ids.size(); ++i)
        {
            context_.node_lookup[node_ids[i]] = node_indices[i];
            context_.node_output_types[node_ids[i]]["result"] = DataType::Boolean;
        }

        return strategy::NodeReference(node_ids[0], "result");
    }

    std::string ExpressionCompiler::DetermineBooleanSelectVariant(DataType true_type, DataType false_type)
    {
        // Determine the correct type-specialized boolean_select variant based on branch types
        // Priority order: String > Timestamp > Boolean > Number (default)

        // String has highest priority - if either branch is String, use string variant
        if (true_type == DataType::String || false_type == DataType::String)
        {
            return "boolean_select_string";
        }

        // Timestamp second priority
        if (true_type == DataType::Timestamp || false_type == DataType::Timestamp)
        {
            return "boolean_select_timestamp";
        }

        // Boolean third priority - both must be Boolean
        if (true_type == DataType::Boolean && false_type == DataType::Boolean)
        {
            return "boolean_select_boolean";
        }

        // Default to number for numeric types (Decimal, Integer, Number) and Any
        return "boolean_select_number";
    }

    std::string ExpressionCompiler::DetermineLagVariant(DataType input_type)
    {
        // Determine the correct type-specialized lag variant based on input type
        // Maps DataType to lag_string, lag_number, lag_boolean, or lag_timestamp

        switch (input_type)
        {
            case DataType::String:
                return "lag_string";

            case DataType::Boolean:
                return "lag_boolean";

            case DataType::Timestamp:
                return "lag_timestamp";

            case DataType::Integer:
            case DataType::Decimal:
            case DataType::Number:
            case DataType::Any:
            default:
                // Default to lag_number for numeric types and Any
                return "lag_number";
        }
    }

    std::string ExpressionCompiler::DetermineNullVariant(DataType expected_type)
    {
        // Determine the correct type-specialized null variant based on expected type
        // Maps DataType to null_string, null_number, null_boolean, or null_timestamp

        switch (expected_type)
        {
            case DataType::String:
                return "null_string";

            case DataType::Boolean:
                return "null_boolean";

            case DataType::Timestamp:
                return "null_timestamp";

            case DataType::Integer:
            case DataType::Decimal:
            case DataType::Number:
            case DataType::Any:
            default:
                // Default to null_number for numeric types and Any
                return "null_number";
        }
    }

    strategy::InputValue ExpressionCompiler::VisitIfExp(const IfExp& if_exp)
    {
        // Ternary expression: test ? body : orelse
        // Lower to type-specialized boolean_select_* based on branch types

        // Resolve inputs FIRST (child-first/topological ordering required for timeframe resolution)
        strategy::InputValue condition = VisitExpr(*if_exp.test);
        strategy::InputValue true_val = VisitExpr(*if_exp.body);
        strategy::InputValue false_val = VisitExpr(*if_exp.orelse);

        // Infer types from true/false branches
        DataType true_type = type_checker_.GetNodeOutputType(true_val);
        DataType false_type = type_checker_.GetNodeOutputType(false_val);

        // Determine the correct type-specialized variant
        std::string comp_name = DetermineBooleanSelectVariant(true_type, false_type);

        // Validate component exists
        if (!context_.HasComponent(comp_name))
        {
            ThrowError("Unknown component '" + comp_name + "'", if_exp.lineno, if_exp.col_offset);
        }

        const auto& comp_meta = context_.GetComponentMetadata(comp_name);

        // Create node AFTER resolving inputs (inputs already resolved above for type inference)
        std::string node_id = UniqueNodeId("ifexp");
        epoch_script::strategy::AlgorithmNode algo;
        algo.id = node_id;
        algo.type = comp_name;

        // Wire inputs to named handles
        algo.inputs["condition"].push_back(condition);
        algo.inputs["true"].push_back(true_val);
        algo.inputs["false"].push_back(false_val);

        // Add to algorithms list
        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[node_id] = context_.algorithms.size() - 1;

        // Get output handle
        std::string out_handle = "result";
        const auto& outputs = comp_meta.outputs;
        if (!outputs.empty())
        {
            out_handle = outputs[0].id;
        }

        return strategy::NodeReference(node_id, out_handle);
    }

    strategy::InputValue ExpressionCompiler::VisitSubscript(const Subscript& subscript)
    {
        // Subscript notation interpreted as lag operator
        // e.g., src.c[1] becomes lag(period=1)(src.c)

        // Extract lag period from slice
        int lag_period = 0;
        if (auto* constant = dynamic_cast<const Constant*>(subscript.slice.get()))
        {
            if (std::holds_alternative<int>(constant->value))
            {
                lag_period = std::get<int>(constant->value);
            }
            else
            {
                ThrowError("Subscript index must be an integer", subscript.lineno, subscript.col_offset);
            }
        }
        else if (auto* unary_op = dynamic_cast<const UnaryOp*>(subscript.slice.get()))
        {
            // Handle negative indices: -N is UnaryOp(USub, Constant(N))
            if (unary_op->op == UnaryOpType::USub)
            {
                if (auto* operand = dynamic_cast<const Constant*>(unary_op->operand.get()))
                {
                    if (std::holds_alternative<int>(operand->value))
                    {
                        lag_period = -std::get<int>(operand->value);
                    }
                    else
                    {
                        ThrowError("Subscript index must be an integer", subscript.lineno, subscript.col_offset);
                    }
                }
                else
                {
                    ThrowError("Subscript index must be a constant integer", subscript.lineno, subscript.col_offset);
                }
            }
            else
            {
                ThrowError("Unsupported unary operator in subscript", subscript.lineno, subscript.col_offset);
            }
        }
        else
        {
            ThrowError("Subscript index must be a constant integer", subscript.lineno, subscript.col_offset);
        }

        // Validate lag period is non-zero
        if (lag_period == 0)
        {
            ThrowError("Lag period must be a non-zero integer", subscript.lineno, subscript.col_offset);
        }

        // Resolve the value being lagged
        strategy::InputValue value = VisitExpr(*subscript.value);

        // Infer input type to determine the correct typed lag variant
        DataType input_type = type_checker_.GetNodeOutputType(value);
        std::string comp_name = DetermineLagVariant(input_type);

        // Validate component exists
        if (!context_.HasComponent(comp_name))
        {
            ThrowError("Unknown lag variant '" + comp_name + "' for type " +
                       TypeChecker::DataTypeToString(input_type), subscript.lineno, subscript.col_offset);
        }

        // Create AlgorithmNode for typed lag variant
        std::string node_id = UniqueNodeId("lag");
        epoch_script::strategy::AlgorithmNode algo;
        algo.id = node_id;
        algo.type = comp_name;  // Use type-specialized variant (lag_number, lag_string, etc.)

        // Add period option
        algo.options["period"] = epoch_script::MetaDataOptionDefinition{static_cast<double>(lag_period)};

        // Wire the value to the lag input
        algo.inputs["SLOT"].push_back(value);

        // Add to algorithms list
        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[node_id] = context_.algorithms.size() - 1;
        context_.var_to_binding[node_id] = comp_name;

        // Track output type (lag preserves input type)
        context_.node_output_types[node_id]["result"] = input_type;

        return strategy::NodeReference(node_id, "result");
    }

    // Materialize literal nodes

    strategy::InputValue ExpressionCompiler::MaterializeNumber(double value)
    {
        std::string node_id = UniqueNodeId("number");

        epoch_script::strategy::AlgorithmNode algo;
        algo.id = node_id;
        algo.type = "number";
        algo.options["value"] = epoch_script::MetaDataOptionDefinition{value};

        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[node_id] = context_.algorithms.size() - 1;
        context_.var_to_binding[node_id] = "number";
        context_.node_output_types[node_id]["result"] = DataType::Decimal;

        return strategy::NodeReference(node_id, "result");
    }

    strategy::InputValue ExpressionCompiler::MaterializeBoolean(bool value)
    {
        std::string node_type = value ? "bool_true" : "bool_false";
        std::string node_id = UniqueNodeId(node_type);

        epoch_script::strategy::AlgorithmNode algo;
        algo.id = node_id;
        algo.type = node_type;
        // No options needed for boolean nodes

        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[node_id] = context_.algorithms.size() - 1;
        context_.var_to_binding[node_id] = node_type;
        context_.node_output_types[node_id]["result"] = DataType::Boolean;

        return strategy::NodeReference(node_id, "result");
    }

    strategy::InputValue ExpressionCompiler::MaterializeText(const std::string& value)
    {
        std::string node_id = UniqueNodeId("text");

        spdlog::debug("[MaterializeText] Created node_id='{}' for value='{}'", node_id, value);

        epoch_script::strategy::AlgorithmNode algo;
        algo.id = node_id;
        algo.type = "text";
        algo.options["value"] = epoch_script::MetaDataOptionDefinition{value};

        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[node_id] = context_.algorithms.size() - 1;
        context_.var_to_binding[node_id] = "text";
        context_.node_output_types[node_id]["result"] = DataType::String;

        return strategy::NodeReference(node_id, "result");
    }

    strategy::InputValue ExpressionCompiler::MaterializeNull()
    {
        // Use null_number as the default typed null variant
        // Most common use case for None/null literals is numeric contexts
        std::string comp_name = "null_number";
        std::string node_id = UniqueNodeId("null");

        epoch_script::strategy::AlgorithmNode algo;
        algo.id = node_id;
        algo.type = comp_name;
        // No options needed for null node

        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[node_id] = context_.algorithms.size() - 1;
        context_.var_to_binding[node_id] = comp_name;
        context_.node_output_types[node_id]["result"] = DataType::Number;

        return strategy::NodeReference(node_id, "result");
    }

    // Private helper methods

    std::pair<std::string, std::string> ExpressionCompiler::AttributeToTuple(const Attribute& attr)
    {
        std::vector<std::string> parts;
        const Expr* current = &attr;

        // Walk backwards through the attribute chain
        while (auto* attr_node = dynamic_cast<const Attribute*>(current))
        {
            parts.push_back(attr_node->attr);
            current = attr_node->value.get();
        }

        // Base should be a Name
        if (auto* name_node = dynamic_cast<const Name*>(current))
        {
            parts.push_back(name_node->id);
        }
        else
        {
            ThrowError("Invalid attribute base - must be a name");
        }

        // Reverse to get correct order
        std::reverse(parts.begin(), parts.end());

        if (parts.size() < 2)
        {
            ThrowError("Attribute must have at least base.handle");
        }

        std::string var = parts[0];
        std::string handle = parts[1];
        for (size_t i = 2; i < parts.size(); ++i)
        {
            handle += "." + parts[i];
        }

        return std::pair(var, handle);
    }

    strategy::InputValue ExpressionCompiler::ResolveHandle(const std::string& var, const std::string& handle)
    {
        // Check if var is bound to a node.handle
        auto it = context_.var_to_binding.find(var);
        if (it != context_.var_to_binding.end())
        {
            const std::string& ref = it->second;
            if (ref.find('.') != std::string::npos)
            {
                ThrowError("Cannot access handle '" + handle + "' on '" + var +
                           "' which is already bound to '" + ref + "'");
            }
        }

        // Var should be a node name - look up its component type
        std::string comp_name;
        if (it != context_.var_to_binding.end())
        {
            comp_name = it->second;
        }
        else
        {
            // Check if it's a direct node reference (use node_lookup)
            if (context_.node_lookup.contains(var))
            {
                comp_name = context_.algorithms[context_.node_lookup[var]].type;
            }
            else
            {
                ThrowError("Unknown node '" + var + "'");
            }
        }

        // Validate component exists
        if (!context_.HasComponent(comp_name))
        {
            ThrowError("Unknown component '" + comp_name + "'");
        }

        const auto& comp_meta = context_.GetComponentMetadata(comp_name);

        // Extract valid handles from outputs and inputs
        std::set<std::string> valid_handles;

        const auto& outputs = comp_meta.outputs;
        for (const auto& output : outputs)
        {
            valid_handles.insert(output.id);
        }

        const auto& inputs = comp_meta.inputs;
        for (const auto& input : inputs)
        {
            std::string input_id = input.id;

            // Handle SLOT naming convention
            if (!input_id.empty() && input_id[0] == '*')
            {
                std::string suffix = input_id.substr(1);
                input_id = suffix.empty() ? "SLOT" : "SLOT" + suffix;
            }
            valid_handles.insert(input_id);
        }

        if (valid_handles.find(handle) == valid_handles.end())
        {
            ThrowError("Unknown handle '" + handle + "' on '" + var + "'");
        }

        return strategy::NodeReference(var, handle);
    }

    std::string ExpressionCompiler::UniqueNodeId(const std::string& base)
    {
        // Use O(1) lookup instead of O(n) iteration
        int idx = 0;
        std::string candidate = base + "_" + std::to_string(idx);
        while (context_.used_node_ids.count(candidate))
        {
            ++idx;
            candidate = base + "_" + std::to_string(idx);
        }

        // Track this ID as used
        context_.used_node_ids.insert(candidate);
        return candidate;
    }

    std::string ExpressionCompiler::JoinId(const std::string& node_id, const std::string& handle)
    {
        return node_id + "#" + handle;
    }

    void ExpressionCompiler::WireInputs(
        const std::string& target_node_id,
        const std::string& component_name,
        const std::vector<strategy::InputValue>& args,
        const std::unordered_map<std::string, strategy::InputValue>& kwargs)
    {
        // Get component metadata
        if (!context_.HasComponent(component_name))
        {
            ThrowError("Unknown component '" + component_name + "'");
        }

        const auto& comp_meta = context_.GetComponentMetadata(component_name);

        // Extract input IDs and types from component metadata
        std::vector<std::string> input_ids;
        std::unordered_map<std::string, DataType> input_types;
        const auto& inputs = comp_meta.inputs;
        for (const auto& input : inputs)
        {
            std::string input_id = input.id;

            // Handle SLOT naming convention (* -> SLOT, *0 -> SLOT0, etc.)
            if (!input_id.empty() && input_id[0] == '*')
            {
                std::string suffix = input_id.substr(1);
                input_id = suffix.empty() ? "SLOT" : "SLOT" + suffix;
            }

            input_ids.push_back(input_id);

            // Map input type from IOMetaData to DataType enum
            std::string type_str = epoch_core::EnumWrapper<epoch_core::IODataType>::type::ToString(input.type);
            if (type_str == "Boolean")
                input_types[input_id] = DataType::Boolean;
            else if (type_str == "Integer")
                input_types[input_id] = DataType::Integer;
            else if (type_str == "Decimal")
                input_types[input_id] = DataType::Decimal;
            else if (type_str == "Number")
                input_types[input_id] = DataType::Number;
            else if (type_str == "String")
                input_types[input_id] = DataType::String;
            else
                input_types[input_id] = DataType::Any;
        }

        // Helper lambda to find the target node by ID
        auto find_target_node = [this](const std::string& node_id) -> epoch_script::strategy::AlgorithmNode* {
            for (auto& algo : context_.algorithms)
            {
                if (algo.id == node_id)
                {
                    return &algo;
                }
            }
            return nullptr;
        };

        // Wire keyword arguments to inputs map
        for (const auto& [name, handle] : kwargs)
        {
            // Validate input name exists
            if (std::find(input_ids.begin(), input_ids.end(), name) == input_ids.end())
            {
                ThrowError("Unknown input handle '" + name + "' for '" + target_node_id + "'");
            }

            // Type checking: get source and target types
            DataType source_type = type_checker_.GetNodeOutputType(handle);
            DataType target_type = input_types[name];

            // Check if types are compatible
            if (!type_checker_.IsTypeCompatible(source_type, target_type))
            {
                // Try to insert type cast
                auto cast_result = type_checker_.NeedsTypeCast(source_type, target_type);
                if (cast_result.has_value() && cast_result.value() != "incompatible")
                {
                    // Insert cast node (this may reallocate algorithms_ vector)
                    strategy::InputValue casted = type_checker_.InsertTypeCast(handle, source_type, target_type);
                    // Find target node by ID after potential reallocation
                    auto* target_node = find_target_node(target_node_id);
                    if (target_node)
                    {
                        target_node->inputs[name].push_back(casted);
                    }
                }
                else
                {
                    // Incompatible types - throw error
                    ThrowError("Type error calling '" + component_name + "()': " +
                               "named argument '" + name + "' must be " + TypeChecker::DataTypeToString(target_type) +
                               ", but received " + TypeChecker::DataTypeToString(source_type) +
                               " from '" + handle.GetColumnIdentifier() + "'");
                }
            }
            else
            {
                // Types are compatible - wire directly
                auto* target_node = find_target_node(target_node_id);
                if (target_node)
                {
                    target_node->inputs[name].push_back(handle);
                }
            }
        }

        // Wire positional arguments to inputs map
        if (!args.empty())
        {
            if (input_ids.empty())
            {
                // Component with 0 inputs - ignore positional args (special case)
                return;
            }

            // Check if last input allows multiple connections (variadic inputs)
            bool last_input_allows_multi = false;
            if (!inputs.empty())
            {
                last_input_allows_multi = inputs.back().allowMultipleConnections;
            }

            // Validate positional args count (allow multiple args if last input is variadic)
            if (args.size() > input_ids.size() && !last_input_allows_multi)
            {
                ThrowError(error_formatting::ArgumentCountError(
                    target_node_id, component_name,
                    input_ids.size(), args.size(),
                    input_ids, args
                ));
            }

            for (size_t i = 0; i < args.size(); ++i)
            {
                const auto& handle = args[i];
                // For variadic inputs, all extra args go to the last input slot
                std::string dst_handle = (i < input_ids.size()) ? input_ids[i] : input_ids.back();

                // Type checking: get source and target types
                DataType source_type = type_checker_.GetNodeOutputType(handle);
                DataType target_type = input_types[dst_handle];

                // Check if types are compatible
                if (!type_checker_.IsTypeCompatible(source_type, target_type))
                {
                    // Try to insert type cast
                    auto cast_result = type_checker_.NeedsTypeCast(source_type, target_type);
                    if (cast_result.has_value() && cast_result.value() != "incompatible")
                    {
                        // Insert cast node (this may reallocate algorithms_ vector)
                        strategy::InputValue casted = type_checker_.InsertTypeCast(handle, source_type, target_type);
                        // Find target node by ID after potential reallocation
                        auto* target_node = find_target_node(target_node_id);
                        if (target_node)
                        {
                            target_node->inputs[dst_handle].push_back(casted);
                        }
                    }
                    else
                    {
                        // Incompatible types - throw error with clear guidance
                        std::string error_msg = "Type error calling '" + component_name + "()': ";
                        error_msg += "argument " + std::to_string(i + 1) + " ('" + dst_handle + "') must be " + TypeChecker::DataTypeToString(target_type);
                        error_msg += ", but received " + TypeChecker::DataTypeToString(source_type);
                        error_msg += " from '" + handle.GetColumnIdentifier() + "'";
                        ThrowError(error_msg);
                    }
                }
                else
                {
                    // Types are compatible - wire directly
                    auto* target_node = find_target_node(target_node_id);
                    if (target_node)
                    {
                        target_node->inputs[dst_handle].push_back(handle);
                    }
                }
            }
        }
    }

    void ExpressionCompiler::ThrowError(const std::string& msg, int line, int col)
    {
        std::string full_msg = msg;
        // Tree-sitter provides 1-based line numbers (line 1 is the first line)
        // Only add location info if line > 0
        if (line > 0)
        {
            full_msg += " (line " + std::to_string(line) + ", col " + std::to_string(col) + ")";
        }
        throw std::runtime_error(full_msg);
    }

} // namespace epoch_script
