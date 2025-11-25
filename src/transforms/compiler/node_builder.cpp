//
// Created by Claude Code
// EpochScript Node Builder Implementation
//

#include "node_builder.h"
#include "validators/special_node_validator.h"
#include "error_formatting/all_errors.h"
#include <epoch_core/enum_wrapper.h>
#include <algorithm>

namespace epoch_script
{

    void NodeBuilder::HandleConstructorAssignment(const Expr& target, const Expr& value, const Assign& assign)
    {
        auto parse_result = constructor_parser_.ParseConstructorAndFeeds(dynamic_cast<const Call&>(value));

        // Validate component exists
        if (!context_.HasComponent(parse_result.ctor_name))
        {
            ThrowError("Unknown component '" + parse_result.ctor_name + "'", assign.lineno, assign.col_offset);
        }

        const auto& comp_meta = context_.GetComponentMetadata(parse_result.ctor_name);

        // Case 1: Single name target (e.g., x = ema(period=20)(src.c))
        if (auto* name_target = dynamic_cast<const Name*>(&target))
        {
            std::string node_id = name_target->id;

            // Ensure variable is not already bound
            if (node_id != "_" && context_.var_to_binding.count(node_id))
            {
                ThrowError("Variable '" + node_id + "' already bound", assign.lineno, assign.col_offset);
            }

            // Canonicalize special parameters
            auto params = parse_result.ctor_kwargs;
            special_param_handler_.CanonicalizeTimeframe(params);
            special_param_handler_.CanonicalizeSession(params);

            // Validate and apply option defaults/clamping
            option_validator_.ValidateAndApplyOptions(node_id, comp_meta, params, dynamic_cast<const Call&>(value));

            // Create AlgorithmNode
            epoch_script::strategy::AlgorithmNode algo;
            algo.id = node_id;
            algo.type = parse_result.ctor_name;

            // Convert regular options (excluding timeframe and session)
            for (const auto& [key, val] : params)
            {
                if (key != "timeframe" && key != "session")
                {
                    algo.options[key] = epoch_script::MetaDataOptionDefinition{val};
                }
            }

            // Apply special fields (timeframe and session)
            special_param_handler_.ApplySpecialFields(algo, params);

            // Add to algorithms list first (so WireInputs can access it)
            context_.algorithms.push_back(std::move(algo));
            context_.node_lookup[node_id] = context_.algorithms.size() - 1;
            context_.var_to_binding[node_id] = parse_result.ctor_name;

            // Track executor count
            if (parse_result.ctor_name == "trade_signal_executor")
            {
                context_.executor_count++;
            }

            // Wire inputs from feed steps
            for (const auto& [args, kwargs] : parse_result.feed_steps)
            {
                WireInputs(node_id, parse_result.ctor_name, args, kwargs);
                // Resolve SLOT references in options after wiring inputs
                ResolveSlotReferencesInOptions(node_id, args);
            }

            return;
        }

        // Case 2: Tuple target (e.g., a, b = macd()(src.c))
        if (auto* tuple_target = dynamic_cast<const Tuple*>(&target))
        {
            std::vector<std::string> names;
            for (const auto& elt : tuple_target->elts)
            {
                if (auto* name_elt = dynamic_cast<const Name*>(elt.get()))
                {
                    names.push_back(name_elt->id);
                }
                else
                {
                    ThrowError("Tuple targets must be simple names", assign.lineno, assign.col_offset);
                }
            }

            // Ensure all names are not already bound
            for (const auto& name : names)
            {
                if (name != "_" && context_.var_to_binding.count(name))
                {
                    ThrowError("Variable '" + name + "' already bound", assign.lineno, assign.col_offset);
                }
            }

            // Create synthetic node ID
            std::string synthetic_id = UniqueNodeId("node");

            // Canonicalize special parameters
            auto params = parse_result.ctor_kwargs;
            special_param_handler_.CanonicalizeTimeframe(params);
            special_param_handler_.CanonicalizeSession(params);

            // Validate and apply option defaults/clamping
            option_validator_.ValidateAndApplyOptions(synthetic_id, comp_meta, params, dynamic_cast<const Call&>(value));

            // Create AlgorithmNode
            epoch_script::strategy::AlgorithmNode algo;
            algo.id = synthetic_id;
            algo.type = parse_result.ctor_name;

            // Convert regular options (excluding timeframe and session)
            for (const auto& [key, val] : params)
            {
                if (key != "timeframe" && key != "session")
                {
                    algo.options[key] = epoch_script::MetaDataOptionDefinition{val};
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
                // Resolve SLOT references in options after wiring inputs
                ResolveSlotReferencesInOptions(synthetic_id, args);
            }

            // Extract output handles and bind to tuple variables
            const auto& outputs = comp_meta.outputs;
            if (outputs.size() != names.size())
            {
                std::string output_names;
                for (size_t i = 0; i < outputs.size(); ++i) {
                    if (i > 0) output_names += ", ";
                    output_names += outputs[i].id;
                }
                ThrowError("Component '" + parse_result.ctor_name + "()' returns " + std::to_string(outputs.size()) +
                           " output" + (outputs.size() == 1 ? "" : "s") + " (" + output_names + "), " +
                           "but you're trying to unpack into " + std::to_string(names.size()) + " variable" +
                           (names.size() == 1 ? "" : "s"),
                           assign.lineno, assign.col_offset);
            }

            for (size_t i = 0; i < names.size(); ++i)
            {
                std::string handle = outputs[i].id;
                // Don't bind underscore variables - they're throwaway
                if (names[i] != "_")
                {
                    context_.var_to_binding[names[i]] = synthetic_id + "." + handle;
                }
            }

            return;
        }

        ThrowError("Unsupported assignment target", assign.lineno, assign.col_offset);
    }

    void NodeBuilder::HandleNonConstructorAssignment(const Expr& target, const Expr& value, const Assign& assign)
    {
        // Handle non-constructor assignments (operators, name references, etc.)
        if (auto* name_target = dynamic_cast<const Name*>(&target))
        {
            std::string node_id = name_target->id;

            // Ensure variable is not already bound
            if (node_id != "_" && context_.var_to_binding.count(node_id))
            {
                ThrowError("Variable '" + node_id + "' already bound", assign.lineno, assign.col_offset);
            }

            // Parse the value and resolve to a handle
            strategy::InputValue input_value = expr_compiler_.VisitExpr(value);

            // Bind the variable to the column identifier using "." format for expression compiler lookup
            // (e.g., "number_0.result" not "number_0#result")
            if (input_value.IsNodeReference()) {
                const auto& ref = input_value.GetNodeReference();
                context_.var_to_binding[node_id] = ref.GetNodeId() + "." + ref.GetHandle();
            } else if (input_value.IsLiteral()) {
                // Literal constant - store the column name for lookup
                context_.var_to_binding[node_id] = input_value.GetLiteral().GetColumnName();
            } else {
                // Null/empty value - shouldn't happen in valid code
                ThrowError("Cannot assign null value to variable '" + node_id + "'", assign.lineno, assign.col_offset);
            }
            return;
        }

        ThrowError("Unsupported non-constructor assignment target", assign.lineno, assign.col_offset);
    }

    void NodeBuilder::HandleSinkNode(const ConstructorParseResult& parse_result, const Call& call)
    {
        // Handle sink nodes (components with no outputs) that are called as statements
        // Example: trade_signal_executor(...) or export_to_csv(...)

        // Validate component exists
        if (!context_.HasComponent(parse_result.ctor_name))
        {
            ThrowError("Unknown component '" + parse_result.ctor_name + "'", call.lineno, call.col_offset);
        }

        const auto& comp_meta = context_.GetComponentMetadata(parse_result.ctor_name);

        // Create synthetic node ID using "node" as base (matches Python behavior)
        std::string synthetic_id = UniqueNodeId("node");

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
        for (const auto& [key, val] : params)
        {
            if (key != "timeframe" && key != "session")
            {
                algo.options[key] = epoch_script::MetaDataOptionDefinition{val};
            }
        }

        // Apply special fields (timeframe and session)
        special_param_handler_.ApplySpecialFields(algo, params);

        // Add to algorithms list
        context_.algorithms.push_back(std::move(algo));
        context_.node_lookup[synthetic_id] = context_.algorithms.size() - 1;

        // Track executor count
        if (parse_result.ctor_name == "trade_signal_executor")
        {
            context_.executor_count++;
        }

        // Wire inputs from feed steps
        for (const auto& [args, kwargs] : parse_result.feed_steps)
        {
            WireInputs(synthetic_id, parse_result.ctor_name, args, kwargs);
            // Resolve SLOT references in options after wiring inputs
            ResolveSlotReferencesInOptions(synthetic_id, args);
        }
    }

    void NodeBuilder::WireInputs(
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
                    // Incompatible types - throw error with detailed context
                    std::string error_msg = "Type error calling '" + component_name + "()': ";
                    error_msg += "keyword argument '" + name + "' must be " + TypeChecker::DataTypeToString(target_type);
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
                    target_node->inputs[name].push_back({handle});
                }
            }
        }

        // Check if last input allows multiple connections (variadic inputs)
        bool last_input_allows_multi = false;
        if (!inputs.empty())
        {
            last_input_allows_multi = inputs.back().allowMultipleConnections;
        }

        // Wire positional arguments to inputs map
        if (!args.empty())
        {
            if (input_ids.empty())
            {
                // Component with 0 inputs - ignore positional args (special case)
                return;
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
                        target_node->inputs[dst_handle].emplace_back(handle);
                    }
                }
            }
        }

        // Run special node validation if validator exists (even for non-variadic transforms)
        // This allows validators to check type compatibility and other constraints
        try {
            ValidationContext val_ctx{
                args, kwargs, target_node_id, component_name,
                type_checker_, context_
            };
            SpecialNodeValidatorRegistry::Instance().ValidateIfNeeded(val_ctx);
        } catch (const std::exception& e) {
            ThrowError(e.what());
        }

        // After wiring inputs, resolve Any output types based on actual inputs
        type_checker_.ResolveAnyOutputType(target_node_id, component_name);
    }

    // Private helper methods

    std::string NodeBuilder::UniqueNodeId(const std::string& base)
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

    void NodeBuilder::ResolveSlotReferencesInOptions(
        const std::string& target_node_id,
        const std::vector<strategy::InputValue>& args)
    {
        // Find the target node
        auto it = context_.node_lookup.find(target_node_id);
        if (it == context_.node_lookup.end())
        {
            return; // Node not found, nothing to resolve
        }

        auto& algo = context_.algorithms[it->second];

        // Helper lambda to recursively resolve SLOT references in a value
        std::function<void(epoch_script::MetaDataOptionDefinition::T&)> resolve_value;
        resolve_value = [&](epoch_script::MetaDataOptionDefinition::T& value) {
            // Handle string values
            if (auto* str_ptr = std::get_if<std::string>(&value))
            {
                std::string& str = *str_ptr;
                // Check if this is a SLOT reference (SLOT0, SLOT1, SLOT2, etc.)
                if (str.rfind("SLOT", 0) == 0) // starts_with("SLOT")
                {
                    // Extract slot index from SLOT0, SLOT1, etc.
                    std::string slot_suffix = str.substr(4); // after "SLOT"
                    size_t slot_idx = 0;
                    if (!slot_suffix.empty())
                    {
                        try {
                            slot_idx = std::stoull(slot_suffix);
                        } catch (const std::exception& exp) {
                            // Not a valid slot reference number
                            SPDLOG_WARN("Invalid SLOT reference suffix '{}' in string value: {}. Skipping.", slot_suffix, exp.what());
                            return;
                        } catch (...) {
                            // Not a valid slot reference number (unknown error)
                            SPDLOG_WARN("Invalid SLOT reference suffix '{}' in string value (unknown error). Skipping.", slot_suffix);
                            return;
                        }
                    }

                    // Check if slot index is valid
                    if (slot_idx >= args.size())
                    {
                        ThrowError("SLOT" + slot_suffix + " reference out of range (only " +
                                   std::to_string(args.size()) + " arguments provided)");
                    }

                    // Resolve to column identifier (handles both NodeReference and Constants)
                    const auto& arg_value = args[slot_idx];
                    str = arg_value.GetColumnIdentifier();
                }
            }
            // Handle EventMarkerSchema
            else if (auto* filter_ptr = std::get_if<epoch_script::EventMarkerSchema>(&value))
            {
                auto& filter = *filter_ptr;

                // Validate and resolve select_key
                if (filter.select_key.empty())
                {
                    ThrowError("EventMarkerSchema 'select_key' cannot be empty. It must reference a boolean column using 'SLOT' syntax (e.g., 'SLOT' or 'SLOT0' for first argument).");
                }

                if (filter.select_key.rfind("SLOT", 0) == 0)
                {
                    std::string slot_suffix = filter.select_key.substr(4);
                    size_t slot_idx = 0;
                    if (!slot_suffix.empty())
                    {
                        try {
                            slot_idx = std::stoull(slot_suffix);
                        } catch (const std::exception& exp) {
                            ThrowError("Invalid SLOT reference '" + filter.select_key + "' in select_key: " + std::string(exp.what()) + ". Use 'SLOT' for first argument, 'SLOT0', 'SLOT1', etc. for subsequent arguments.");
                        } catch (...) {
                            ThrowError("Invalid SLOT reference '" + filter.select_key + "' in select_key (unknown error). Use 'SLOT' for first argument, 'SLOT0', 'SLOT1', etc. for subsequent arguments.");
                        }
                    }

                    if (args.empty())
                    {
                        ThrowError("EventMarkerSchema references '" + filter.select_key + "' but no input arguments were provided to event_marker(). Pass at least one boolean column as an argument.");
                    }

                    if (slot_idx >= args.size())
                    {
                        ThrowError("EventMarkerSchema references '" + filter.select_key + "' but only " + std::to_string(args.size()) + " argument(s) were provided. SLOT indices are 0-based: use 'SLOT' or 'SLOT0' for first argument, 'SLOT1' for second, etc.");
                    }

                    const auto& arg_handle = args[slot_idx];
                    filter.select_key = arg_handle.GetColumnIdentifier();
                }
                else
                {
                    // select_key doesn't start with "SLOT" - this is a common user error
                    ThrowError("EventMarkerSchema 'select_key' must use 'SLOT' syntax, not column names. "
                               "Got '" + filter.select_key + "'. "
                               "Use 'SLOT0' to reference the first argument passed to event_marker(), 'SLOT1' for the second, etc. "
                               "Example: event_marker(schema=EventMarkerSchema(select_key=\"SLOT0\", schemas=[{\"column_id\":\"SLOT0\", ...}]))(my_boolean_column)");
                }

                // Resolve column_id in schemas
                for (auto& schema : filter.schemas)
                {
                    if (schema.column_id.rfind("SLOT", 0) == 0)
                    {
                        std::string slot_suffix = schema.column_id.substr(4);
                        size_t slot_idx = 0;
                        if (!slot_suffix.empty())
                        {
                            try {
                                slot_idx = std::stoull(slot_suffix);
                            } catch (const std::exception& exp) {
                                // Not a valid slot reference number
                                SPDLOG_WARN("Invalid SLOT reference suffix '{}' in EventMarkerSchema column_id: {}. Skipping.", slot_suffix, exp.what());
                                continue;
                            } catch (...) {
                                // Not a valid slot reference number (unknown error)
                                SPDLOG_WARN("Invalid SLOT reference suffix '{}' in EventMarkerSchema column_id (unknown error). Skipping.", slot_suffix);
                                continue;
                            }
                        }

                        if (slot_idx >= args.size())
                        {
                            ThrowError("SLOT" + slot_suffix + " reference in column_id out of range");
                        }

                        const auto& arg_handle = args[slot_idx];
                        schema.column_id = arg_handle.GetColumnIdentifier();

                        // Note: color_map values are auto-serialized to strings at parse time,
                        // so we accept any type here (Boolean, Integer, Decimal, Number, String, Any)
                        // No type validation needed - serialization happens in constructor_parser.cpp
                    }
                }
            }
            // Handle TableReportSchema
            else if (auto* table_ptr = std::get_if<epoch_script::TableReportSchema>(&value))
            {
                auto& table = *table_ptr;

                // Resolve select_key
                if (table.select_key.rfind("SLOT", 0) == 0)
                {
                    std::string slot_suffix = table.select_key.substr(4);
                    size_t slot_idx = 0;
                    if (!slot_suffix.empty())
                    {
                        try {
                            slot_idx = std::stoull(slot_suffix);
                        } catch (const std::exception& exp) {
                            // Not a valid slot reference number
                            SPDLOG_WARN("Invalid SLOT reference suffix '{}' in TableReportSchema select_key: {}. Skipping.", slot_suffix, exp.what());
                            return;
                        } catch (...) {
                            // Not a valid slot reference number (unknown error)
                            SPDLOG_WARN("Invalid SLOT reference suffix '{}' in TableReportSchema select_key (unknown error). Skipping.", slot_suffix);
                            return;
                        }
                    }

                    if (slot_idx >= args.size())
                    {
                        ThrowError("SLOT" + slot_suffix + " reference in select_key out of range");
                    }

                    const auto& arg_handle = args[slot_idx];
                    table.select_key = arg_handle.GetColumnIdentifier();
                }

                // Resolve column_id in columns
                for (auto& column : table.columns)
                {
                    if (column.column_id.rfind("SLOT", 0) == 0)
                    {
                        std::string slot_suffix = column.column_id.substr(4);
                        size_t slot_idx = 0;
                        if (!slot_suffix.empty())
                        {
                            try {
                                slot_idx = std::stoull(slot_suffix);
                            } catch (const std::exception& exp) {
                                // Not a valid slot reference number
                                SPDLOG_WARN("Invalid SLOT reference suffix '{}' in TableReportSchema column_id: {}. Skipping.", slot_suffix, exp.what());
                                continue;
                            } catch (...) {
                                // Not a valid slot reference number (unknown error)
                                SPDLOG_WARN("Invalid SLOT reference suffix '{}' in TableReportSchema column_id (unknown error). Skipping.", slot_suffix);
                                continue;
                            }
                        }

                        if (slot_idx >= args.size())
                        {
                            ThrowError("SLOT" + slot_suffix + " reference in column_id out of range");
                        }

                        const auto& arg_handle = args[slot_idx];
                        column.column_id = arg_handle.GetColumnIdentifier();
                    }
                }
            }
        };

        // Iterate through all options and resolve SLOT references
        for (auto& [option_name, option_def] : algo.options)
        {
            // Get the variant value from MetaDataOptionDefinition
            auto variant_value = option_def.GetVariant();
            // Resolve SLOT references in the variant
            resolve_value(variant_value);
            // Store back the modified variant (reconstruct MetaDataOptionDefinition)
            option_def = epoch_script::MetaDataOptionDefinition{variant_value};
        }
    }

    void NodeBuilder::ThrowError(const std::string& msg, int line, int col)
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
