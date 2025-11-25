//
// Created by Claude Code
// EpochScript Type Checker Implementation
//

#include "type_checker.h"
#include <epoch_core/enum_wrapper.h>
#include <stdexcept>
#include <algorithm>

namespace epoch_script
{

    DataType TypeChecker::GetNodeOutputType(const std::string& node_id, const std::string& handle)
    {
        // Check tracked output types first
        auto tracked_it = context_.node_output_types.find(node_id);
        if (tracked_it != context_.node_output_types.end())
        {
            auto handle_it = tracked_it->second.find(handle);
            if (handle_it != tracked_it->second.end())
            {
                // If type is still Any, try to resolve it recursively
                if (handle_it->second == DataType::Any)
                {
                    // Get node type to attempt resolution
                    auto node_it = context_.node_lookup.find(node_id);
                    if (node_it != context_.node_lookup.end())
                    {
                        const auto& algo = context_.algorithms[node_it->second];
                        const std::string node_type = algo.type;

                        // Try to resolve it now
                        ResolveAnyOutputType(node_id, node_type);

                        // Check if it was resolved
                        auto updated_it = context_.node_output_types.find(node_id);
                        if (updated_it != context_.node_output_types.end())
                        {
                            auto updated_handle_it = updated_it->second.find(handle);
                            if (updated_handle_it != updated_it->second.end())
                            {
                                return updated_handle_it->second;
                            }
                        }
                    }
                }
                return handle_it->second;
            }
        }

        // Check if it's a known node in our algorithms list
        auto node_it = context_.node_lookup.find(node_id);
        if (node_it != context_.node_lookup.end())
        {
            const auto& algo = context_.algorithms[node_it->second];
            // Copy the string to avoid reference invalidation after vector reallocation
            const std::string node_type = algo.type;

            // Check registry for output types
            const auto& all_metadata = context_.GetRegistry().GetMetaData();
            if (all_metadata.contains(node_type))
            {
                const auto& comp_meta = all_metadata.at(node_type);
                for (const auto& output : comp_meta.outputs)
                {
                    if (output.id == handle)
                    {
                        // Map IOMetaData type to DataType enum using EnumWrapper
                        std::string type_str = epoch_core::EnumWrapper<epoch_core::IODataType>::type::ToString(output.type);
                        if (type_str == "Boolean")
                            return DataType::Boolean;
                        else if (type_str == "Integer")
                            return DataType::Integer;
                        else if (type_str == "Decimal")
                            return DataType::Decimal;
                        else if (type_str == "Number")
                            return DataType::Number;
                        else if (type_str == "String")
                            return DataType::String;
                        else
                            return DataType::Any;
                    }
                }
            }

            // Special cases for operators and literals
            if (node_type == "lt" || node_type == "gt" || node_type == "lte" ||
                node_type == "gte" || node_type == "eq" || node_type == "neq" ||
                node_type == "logical_and" || node_type == "logical_or" || node_type == "logical_not")
            {
                // Debug: Log that we're returning Boolean for comparison operators
                // std::cerr << "DEBUG: GetNodeOutputType returning Boolean for node_id=" << node_id << ", type=" << node_type << std::endl;
                return DataType::Boolean;
            }
            else if (node_type == "add" || node_type == "sub" ||
                     node_type == "mul" || node_type == "div")
            {
                return DataType::Decimal;
            }
            else if (node_type == "number")
            {
                return DataType::Decimal;
            }
            else if (node_type == "bool_true" || node_type == "bool_false")
            {
                return DataType::Boolean;
            }
            else if (node_type == "text")
            {
                return DataType::String;
            }
            else if (node_type == "null")
            {
                return DataType::Any;
            }
        }

        // Default to Any if unknown
        return DataType::Any;
    }

    DataType TypeChecker::GetNodeOutputType(const strategy::InputValue& input)
    {
        // If it's a NodeReference, delegate to the existing overload
        if (input.IsNodeReference())
        {
            const auto& node_ref = input.GetNodeReference();
            return GetNodeOutputType(node_ref.GetNodeId(), node_ref.GetHandle());
        }

        // If it's a Constant, return its type directly
        if (input.IsLiteral())
        {
            const auto& constant = input.GetLiteral();
            if (constant.IsDecimal()) return DataType::Decimal;
            if (constant.IsBoolean()) return DataType::Boolean;
            if (constant.IsString()) return DataType::String;
            if (constant.IsTimestamp()) return DataType::Timestamp;
            if (constant.IsNull())
            {
                // Null has specific types - map to DataType
                auto null_type = constant.GetNull().type;
                if (null_type == epoch_core::IODataType::Integer) return DataType::Integer;
                if (null_type == epoch_core::IODataType::Decimal) return DataType::Decimal;
                if (null_type == epoch_core::IODataType::Boolean) return DataType::Boolean;
                if (null_type == epoch_core::IODataType::String) return DataType::String;
                if (null_type == epoch_core::IODataType::Timestamp) return DataType::Timestamp;
            }
        }

        // Default to Any if unknown
        return DataType::Any;
    }

    bool TypeChecker::IsTypeCompatible(DataType source, DataType target)
    {
        // Any type accepts all
        if (target == DataType::Any || source == DataType::Any)
        {
            return true;
        }

        // Exact match
        if (source == target)
        {
            return true;
        }

        // Numeric type compatibility: Number, Decimal, and Integer are mutually compatible
        // This allows arithmetic operations between different numeric types
        if ((target == DataType::Number || target == DataType::Decimal || target == DataType::Integer) &&
            (source == DataType::Number || source == DataType::Decimal || source == DataType::Integer))
        {
            return true;
        }

        // Otherwise incompatible
        return false;
    }

    std::optional<std::string> TypeChecker::NeedsTypeCast(DataType source, DataType target)
    {
        // If types are already compatible, no cast needed
        if (IsTypeCompatible(source, target))
        {
            return std::nullopt;
        }

        // Boolean to Number/Decimal/Integer
        if (source == DataType::Boolean &&
            (target == DataType::Number || target == DataType::Decimal || target == DataType::Integer))
        {
            return "bool_to_num";
        }

        // Number/Decimal/Integer to Boolean
        if ((source == DataType::Number || source == DataType::Decimal || source == DataType::Integer) &&
            target == DataType::Boolean)
        {
            return "num_to_bool";
        }

        // Boolean to String
        if (source == DataType::Boolean && target == DataType::String)
        {
            return "bool_to_string";
        }

        // Incompatible types that can't be cast
        return "incompatible";
    }

    strategy::InputValue TypeChecker::InsertTypeCast(const strategy::InputValue& source, DataType source_type, DataType target_type)
    {
        // Don't insert static_cast if target type is Any - there's nothing to cast to
        if (target_type == DataType::Any)
        {
            return source;
        }

        // Check if casting is needed
        auto cast_method = NeedsTypeCast(source_type, target_type);

        if (!cast_method.has_value())
        {
            // No casting needed - types are compatible
            return source;
        }

        if (cast_method.value() == "incompatible")
        {
            // Incompatible types that can't be cast
            throw std::runtime_error("Type mismatch: Cannot convert " + DataTypeToString(source_type) +
                                     " to " + DataTypeToString(target_type));
        }

        // Special case: Boolean to String uses stringify instead of static_cast
        if (cast_method.value() == "bool_to_string")
        {
            // Extract NodeReference from source (casting only makes sense for node outputs, not constants)
            const auto& source_ref = source.GetNodeReference();
            std::string cast_node_id = InsertStringify(source_ref.GetNodeId(), source_ref.GetHandle());
            return strategy::NodeReference(cast_node_id, "result");
        }

        // All other casts use static_cast transforms
        // Insert static_cast node for the target type
        const auto& source_ref = source.GetNodeReference();
        std::string cast_node_id = InsertStaticCast(source_ref.GetNodeId(), source_ref.GetHandle(), target_type);
        return strategy::NodeReference(cast_node_id, "result");
    }

    std::string TypeChecker::DataTypeToString(DataType type)
    {
        switch (type)
        {
        case DataType::Boolean:
            return "Boolean";
        case DataType::Integer:
            return "Integer";
        case DataType::Decimal:
            return "Decimal";
        case DataType::Number:
            return "Number";
        case DataType::String:
            return "String";
        case DataType::Any:
            return "Any";
        case DataType::Timestamp:
            return "Timestamp";
        default:
            return "Unknown";
        }
    }

    void TypeChecker::ResolveAnyOutputType(const std::string& node_id, const std::string& /* node_type */)
    {
        // Only resolve if the output type is currently Any
        auto it = context_.node_output_types.find(node_id);
        if (it != context_.node_output_types.end())
        {
            auto handle_it = it->second.find("result");
            if (handle_it != it->second.end() && handle_it->second != DataType::Any)
            {
                // Already resolved or not Any
                return;
            }
        }

        // Type-specialized transforms (boolean_select_*, conditional_select_*, etc.)
        // have explicit output types in metadata, so no special handling needed.
        // Add special cases here only for transforms with genuine Any output that needs inference.
    }

    // Private helpers

    strategy::InputValue TypeChecker::MaterializeNumber(double value)
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

    strategy::InputValue TypeChecker::MaterializeString(const std::string& value)
    {
        std::string node_id = UniqueNodeId("text");

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

    std::string TypeChecker::UniqueNodeId(const std::string& base)
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

    std::string TypeChecker::JoinId(const std::string& node_id, const std::string& handle)
    {
        return node_id + "#" + handle;
    }

    std::string TypeChecker::InsertStaticCast(const std::string& source_node_id, const std::string& source_handle, DataType resolved_type)
    {
        // Determine which static_cast transform to use based on resolved type
        std::string cast_type;
        switch (resolved_type)
        {
            case DataType::Integer:
                cast_type = "static_cast_to_integer";
                break;
            case DataType::Decimal:
                cast_type = "static_cast_to_decimal";
                break;
            case DataType::Number:
                // Number is a generic numeric type - cast to Decimal which can hold Integer or Decimal
                cast_type = "static_cast_to_decimal";
                break;
            case DataType::Boolean:
                cast_type = "static_cast_to_boolean";
                break;
            case DataType::String:
                cast_type = "static_cast_to_string";
                break;
            case DataType::Timestamp:
                cast_type = "static_cast_to_timestamp";
                break;
            case DataType::Any:
            default:
                // Should not insert static_cast for Any or unknown types
                // This should be prevented by checks in InsertTypeCast and ResolveAnyOutputType
                throw std::runtime_error("Cannot insert static_cast for unresolved Any type (node: " +
                                       source_node_id + ", handle: " + source_handle +
                                       ", resolved_type: " + DataTypeToString(resolved_type) + ")");
        }

        // Create unique node ID for the static_cast node
        std::string cast_node_id = UniqueNodeId("static_cast");

        // Create the static_cast AlgorithmNode
        epoch_script::strategy::AlgorithmNode cast_node;
        cast_node.id = cast_node_id;
        cast_node.type = cast_type;

        // Wire the input from source node (static_cast transforms expect "SLOT" as input key)
        cast_node.inputs["SLOT"] = {epoch_script::strategy::InputValue{epoch_script::strategy::NodeReference{source_node_id, source_handle}}};

        // Add to algorithms list
        context_.algorithms.push_back(std::move(cast_node));
        context_.node_lookup[cast_node_id] = context_.algorithms.size() - 1;
        context_.var_to_binding[cast_node_id] = cast_type;

        // Register the output type
        context_.node_output_types[cast_node_id]["result"] = resolved_type;

        return cast_node_id;
    }

    std::string TypeChecker::InsertStringify(const std::string& source_node_id, const std::string& source_handle)
    {
        // Create unique node ID for the stringify node
        std::string stringify_node_id = UniqueNodeId("stringify");

        // Create the stringify AlgorithmNode
        epoch_script::strategy::AlgorithmNode stringify_node;
        stringify_node.id = stringify_node_id;
        stringify_node.type = "stringify";

        // Wire the input from source node (stringify expects "SLOT" as input key)
        stringify_node.inputs["SLOT"] = {epoch_script::strategy::InputValue{epoch_script::strategy::NodeReference{source_node_id, source_handle}}};

        // Add to algorithms list
        context_.algorithms.push_back(std::move(stringify_node));
        context_.node_lookup[stringify_node_id] = context_.algorithms.size() - 1;
        context_.var_to_binding[stringify_node_id] = "stringify";

        // Register the output type as String
        context_.node_output_types[stringify_node_id]["result"] = DataType::String;

        return stringify_node_id;
    }

} // namespace epoch_script
