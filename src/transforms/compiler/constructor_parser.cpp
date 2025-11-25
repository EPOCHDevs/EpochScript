//
// Created by Claude Code
// EpochScript Constructor Parser Implementation
//

#include "constructor_parser.h"
#include "expression_compiler.h"
#include "option_validator.h"
#include <algorithm>
#include <format>

namespace epoch_script
{

    bool ConstructorParser::IsConstructorCall(const Expr& expr)
    {
        // Must have at least one Call in the chain
        if (auto* call = dynamic_cast<const Call*>(&expr))
        {
            const Expr* cur = call;
            while (auto* call_node = dynamic_cast<const Call*>(cur))
            {
                cur = call_node->func.get();
            }
            // Base must be a Name (component name)
            return dynamic_cast<const Name*>(cur) != nullptr;
        }
        return false;
    }

    ConstructorParseResult ConstructorParser::ParseConstructorAndFeeds(const Call& call)
    {
        // Collect all calls in the chain
        std::vector<const Call*> calls;
        const Expr* cur = &call;
        while (auto* call_node = dynamic_cast<const Call*>(cur))
        {
            calls.push_back(call_node);
            cur = call_node->func.get();
        }

        // Base must be a Name
        auto* name_node = dynamic_cast<const Name*>(cur);
        if (!name_node)
        {
            ThrowError("Right-hand side must be a constructor call (e.g., ema(...)(...))", call.lineno, call.col_offset);
        }

        std::string ctor_name = name_node->id;
        std::reverse(calls.begin(), calls.end());

        // Get component metadata for option parsing
        if (!context_.HasComponent(ctor_name))
        {
            ThrowError("Unknown component '" + ctor_name + "'", call.lineno, call.col_offset);
        }
        const auto& comp_meta = context_.GetComponentMetadata(ctor_name);

        // Build metadata lookup map for O(1) lookups
        std::unordered_map<std::string, epoch_script::MetaDataOption> option_metadata;
        for (const auto& opt : comp_meta.options)
        {
            option_metadata[opt.id] = opt;
        }

        ConstructorParseResult result;
        result.ctor_name = ctor_name;

        // Parse constructor kwargs from first call
        for (const auto& [key, value_expr] : calls[0]->keywords)
        {
            // Skip special parameters (timeframe and session) - validated separately
            if (key == "timeframe" || key == "session")
            {
                // Extract raw string value (validated later by canonicalizeTimeframe/Session)
                if (auto* constant = dynamic_cast<const Constant*>(value_expr.get()))
                {
                    if (std::holds_alternative<std::string>(constant->value))
                    {
                        result.ctor_kwargs[key] = std::get<std::string>(constant->value);
                    }
                    else
                    {
                        ThrowError(
                            std::format("Parameter '{}' must be a string", key),
                            calls[0]->lineno, calls[0]->col_offset);
                    }
                }
                else if (auto* name = dynamic_cast<const Name*>(value_expr.get()))
                {
                    // Bare identifier like sessions(session=London)
                    result.ctor_kwargs[key] = name->id;
                }
                else
                {
                    ThrowError(
                        std::format("Parameter '{}' must be a string literal", key),
                        calls[0]->lineno, calls[0]->col_offset);
                }
                continue;
            }

            // Look up metadata for this option (throw error if not found - invalid option)
            auto it = option_metadata.find(key);
            if (it == option_metadata.end())
            {
                ThrowError(
                    std::format("Unknown option '{}' for component '{}'", key, ctor_name),
                    calls[0]->lineno, calls[0]->col_offset);
            }

            result.ctor_kwargs[key] = ParseLiteralOrPrimitive(*value_expr, it->second, comp_meta);
        }

        // Handle shorthand syntax: component(inputs) instead of component()(inputs)
        // If first call has args and component has no options, treat args as feed inputs
        std::vector<strategy::InputValue> feed_step_args;
        std::unordered_map<std::string, strategy::InputValue> feed_step_kwargs;

        if (!calls[0]->args.empty())
        {
            bool has_options = !comp_meta.options.empty();

            if (!has_options && calls.size() == 1)
            {
                // Shorthand: treat positional args as feed inputs
                for (const auto& arg_expr : calls[0]->args)
                {
                    feed_step_args.push_back(expr_compiler_.VisitExpr(*arg_expr));
                }
                result.feed_steps.push_back({feed_step_args, feed_step_kwargs});
            }
            else
            {
                ThrowError("Positional constructor arguments not supported; use keyword args", calls[0]->lineno, calls[0]->col_offset);
            }
        }

        // Parse subsequent feed steps
        for (size_t i = 1; i < calls.size(); ++i)
        {
            std::vector<strategy::InputValue> args;
            std::unordered_map<std::string, strategy::InputValue> kwargs;

            for (const auto& arg_expr : calls[i]->args)
            {
                args.push_back(expr_compiler_.VisitExpr(*arg_expr));
            }

            for (const auto& [key, value_expr] : calls[i]->keywords)
            {
                kwargs[key] = expr_compiler_.VisitExpr(*value_expr);
            }

            result.feed_steps.push_back({args, kwargs});
        }

        return result;
    }

    epoch_script::MetaDataOptionDefinition::T ConstructorParser::ParseLiteralOrPrimitive(
        const Expr& expr,
        const epoch_script::MetaDataOption& meta_option,
        const epoch_script::transforms::TransformsMetaData& comp_meta)
    {
        // Extract raw value from AST expression
        epoch_script::MetaDataOptionDefinition::T raw_value;

        // Handle custom type constructor calls
        if (auto* call = dynamic_cast<const Call*>(&expr))
        {
            // Extract constructor name
            auto* func_name = dynamic_cast<const Name*>(call->func.get());
            if (!func_name)
            {
                ThrowError("Only direct constructor calls supported for custom types", call->lineno, call->col_offset);
            }

            const std::string& ctor_name = func_name->id;

            // Dispatch to appropriate constructor parser based on name
            if (ctor_name == "Time")
            {
                return epoch_script::MetaDataOptionDefinition::T{ParseTimeConstructor(*call)};
            }
            else if (ctor_name == "EventMarkerSchema")
            {
                return epoch_script::MetaDataOptionDefinition::T{ParseEventMarkerSchemaConstructor(*call)};
            }
            else if (ctor_name == "SqlStatement")
            {
                return epoch_script::MetaDataOptionDefinition::T{ParseSqlStatementConstructor(*call)};
            }
            else if (ctor_name == "TableReportSchema")
            {
                return epoch_script::MetaDataOptionDefinition::T{ParseTableReportSchemaConstructor(*call)};
            }
            // Note: SessionRange and TimeFrame are handled as special parameters
            // in SpecialParameterHandler, not as regular options
            else
            {
                ThrowError("Unknown custom type constructor: " + ctor_name + ". Supported: Time, EventMarkerSchema, SqlStatement, TableReportSchema", call->lineno, call->col_offset);
            }
        }
        else if (auto* constant = dynamic_cast<const Constant*>(&expr))
        {
            raw_value = std::visit([](auto&& value) -> epoch_script::MetaDataOptionDefinition::T
            {
                using T = std::decay_t<decltype(value)>;
                if constexpr (std::is_same_v<T, int>) {
                    return static_cast<double>(value);
                } else if constexpr (std::is_same_v<T, double>) {
                    return value;
                } else if constexpr (std::is_same_v<T, bool>) {
                    return value;
                } else if constexpr (std::is_same_v<T, std::string>) {
                    return value;
                } else if constexpr (std::is_same_v<T, std::monostate>) {
                    return std::string{""};  // Return empty string for null/None
                }
                throw std::runtime_error("Unsupported constant type");
            }, constant->value);
        }
        else if (auto* name = dynamic_cast<const Name*>(&expr))
        {
            // Check if this name is bound to a constant value
            auto it = context_.var_to_binding.find(name->id);
            if (it != context_.var_to_binding.end() && it->second.find('.') != std::string::npos)
            {
                std::string node_id = it->second.substr(0, it->second.find('.'));

                // Check if it's a literal node (use node_lookup and algorithms)
                if (context_.node_lookup.contains(node_id))
                {
                    const auto& algo = context_.algorithms[context_.node_lookup[node_id]];
                    if (algo.type == "number")
                    {
                        if (algo.options.contains("value"))
                        {
                            raw_value = algo.options.at("value").GetVariant();
                        }
                        else
                        {
                            ThrowError("Internal error: Number node '" + node_id + "' is missing value option. This should not happen.");
                        }
                    }
                    else if (algo.type == "bool_true")
                    {
                        raw_value = true;
                    }
                    else if (algo.type == "bool_false")
                    {
                        raw_value = false;
                    }
                    else
                    {
                        ThrowError("Option '" + meta_option.id + "' in '" + comp_meta.id + "()' requires a literal value (number, string, boolean), but got variable '" + name->id + "' bound to non-literal node type '" + algo.type + "'");
                    }
                }
                else
                {
                    ThrowError("Option '" + meta_option.id + "' in '" + comp_meta.id + "()' requires a literal value (number, string, boolean), but got unbound variable '" + name->id + "'");
                }
            }
            else
            {
                // Fallback: accept bare identifiers as strings (e.g., sessions(session=London))
                raw_value = name->id;
            }
        }
        else if ([[maybe_unused]] auto* dict = dynamic_cast<const Dict*>(&expr))
        {
            // Dictionary literals are NOT supported - must use constructor call
            ThrowError("Option '" + meta_option.id + "' in '" + comp_meta.id + "()' cannot accept inline dictionary literals {...}. "
                       "You must use a constructor call instead. "
                       "For example: " + comp_meta.id + "(" + meta_option.id + "=EventMarkerSchema({\"select_key\":\"SLOT0\", ...})) "
                       "instead of " + comp_meta.id + "(" + meta_option.id + "={...})");
        }
        else if ([[maybe_unused]] auto* list = dynamic_cast<const List*>(&expr))
        {
            // List literals are supported - this shouldn't happen in normal flow
            ThrowError("Option '" + meta_option.id + "' in '" + comp_meta.id + "()' received a list literal. "
                       "Lists are supported, but this case shouldn't be reached. This may be an internal compiler issue.");
        }
        else
        {
            // Determine what type of expression was provided for better error message
            std::string expr_type = "unknown expression";
            if (dynamic_cast<const BinOp*>(&expr)) {
                expr_type = "arithmetic expression (e.g., a + b)";
            } else if (dynamic_cast<const UnaryOp*>(&expr)) {
                expr_type = "unary expression (e.g., -x)";
            } else if (dynamic_cast<const Compare*>(&expr)) {
                expr_type = "comparison expression (e.g., a > b)";
            } else if (dynamic_cast<const Attribute*>(&expr)) {
                expr_type = "attribute access (e.g., obj.attr)";
            } else if (dynamic_cast<const Subscript*>(&expr)) {
                expr_type = "subscript expression (e.g., arr[0])";
            }

            ThrowError("Option '" + meta_option.id + "' in '" + comp_meta.id + "()' requires a compile-time constant. "
                       "Got " + expr_type + ". "
                       "Supported: literals (number, string, boolean), dictionaries {...}, lists [...], or constructor calls like EventMarkerSchema({...}).");
        }

        // Delegate to OptionValidator for type-aware parsing
        // Create a dummy call for error reporting (real error locations come from the caller)
        auto dummy_func = std::make_unique<Name>("dummy");
        Call dummy_call{std::move(dummy_func)};
        dummy_call.lineno = 0;
        dummy_call.col_offset = 0;

        OptionValidator validator(context_);
        return validator.ParseOptionByMetadata(raw_value, meta_option, meta_option.id, comp_meta.id, dummy_call, comp_meta);
    }

    glz::generic ConstructorParser::CallKwargsToGeneric(const Call& call)
    {
        // Convert Call kwargs to glz::generic object
        glz::generic obj;

        for (const auto& [key, value_expr] : call.keywords)
        {
            // Handle different expression types
            if (auto* constant = dynamic_cast<const Constant*>(value_expr.get()))
            {
                // Convert Constant to generic
                std::visit([&](auto&& value) {
                    using T = std::decay_t<decltype(value)>;
                    if constexpr (std::is_same_v<T, int>) {
                        obj[key] = static_cast<double>(value);
                    } else if constexpr (std::is_same_v<T, double>) {
                        obj[key] = value;
                    } else if constexpr (std::is_same_v<T, bool>) {
                        obj[key] = value;
                    } else if constexpr (std::is_same_v<T, std::string>) {
                        obj[key] = value;
                    } else if constexpr (std::is_same_v<T, std::monostate>) {
                        obj[key] = nullptr;
                    }
                }, constant->value);
            }
            else if (auto* name = dynamic_cast<const Name*>(value_expr.get()))
            {
                // Bare identifier - treat as string (e.g., icon=Signal)
                obj[key] = name->id;
            }
            else if (auto* nested_call = dynamic_cast<const Call*>(value_expr.get()))
            {
                // Nested constructor call (e.g., sql=SqlStatement(...))
                auto* func_name = dynamic_cast<const Name*>(nested_call->func.get());
                if (!func_name)
                {
                    ThrowError("Nested constructor must be a direct call", call.lineno, call.col_offset);
                }

                const std::string& ctor_name = func_name->id;

                // Recursively convert nested constructor to generic
                if (ctor_name == "SqlStatement")
                {
                    // SqlStatement is special - just extract the SQL string
                    auto nested_obj = CallKwargsToGeneric(*nested_call);
                    if (nested_obj.contains("sql"))
                    {
                        obj[key] = nested_obj["sql"].get<std::string>();
                    }
                }
                else if (ctor_name == "CardColumnSchema")
                {
                    obj[key] = CallKwargsToGeneric(*nested_call);
                }
                else
                {
                    ThrowError("Unsupported nested constructor: " + ctor_name, call.lineno, call.col_offset);
                }
            }
            else if (auto* list = dynamic_cast<const List*>(value_expr.get()))
            {
                // Handle list of constructors (e.g., schemas=[CardColumnSchema(...), ...])
                std::vector<glz::generic> arr;

                for (const auto& elem : list->elts)
                {
                    if (auto* elem_call = dynamic_cast<const Call*>(elem.get()))
                    {
                        arr.push_back(CallKwargsToGeneric(*elem_call));
                    }
                    else if (auto* elem_const = dynamic_cast<const Constant*>(elem.get()))
                    {
                        // Handle constant list elements
                        std::visit([&](auto&& value) {
                            using T = std::decay_t<decltype(value)>;
                            glz::generic elem_gen;
                            if constexpr (std::is_same_v<T, int>) {
                                elem_gen = static_cast<double>(value);
                            } else if constexpr (std::is_same_v<T, double>) {
                                elem_gen = value;
                            } else if constexpr (std::is_same_v<T, std::string>) {
                                elem_gen = value;
                            }
                            arr.push_back(elem_gen);
                        }, elem_const->value);
                    }
                }

                obj[key] = arr;
            }
            else if (auto* dict = dynamic_cast<const Dict*>(value_expr.get()))
            {
                // Handle dictionary literal (e.g., color_map={Success: ["active"], Error: ["inactive"]})
                // Initialize as empty object (not null) to handle empty dicts like color_map={}
                glz::generic dict_obj;
                dict_obj.data = glz::generic::object_t{};

                // Process each key-value pair
                for (size_t i = 0; i < dict->keys.size(); ++i)
                {
                    const auto& key_expr = dict->keys[i];
                    const auto& value_expr = dict->values[i];

                    // Extract key (usually a Name or Constant)
                    std::string dict_key;
                    if (auto* key_name = dynamic_cast<const Name*>(key_expr.get()))
                    {
                        dict_key = key_name->id;
                    }
                    else if (auto* key_const = dynamic_cast<const Constant*>(key_expr.get()))
                    {
                        if (auto* s = std::get_if<std::string>(&key_const->value))
                        {
                            dict_key = *s;
                        }
                        else
                        {
                            ThrowError("Dictionary keys must be strings or identifiers", call.lineno, call.col_offset);
                        }
                    }
                    else
                    {
                        ThrowError("Dictionary keys must be strings or identifiers", call.lineno, call.col_offset);
                    }

                    // Extract value (can be various types)
                    if (auto* val_const = dynamic_cast<const Constant*>(value_expr.get()))
                    {
                        // Constant value
                        std::visit([&](auto&& value) {
                            using T = std::decay_t<decltype(value)>;
                            if constexpr (std::is_same_v<T, int>) {
                                dict_obj[dict_key] = static_cast<double>(value);
                            } else if constexpr (std::is_same_v<T, double>) {
                                dict_obj[dict_key] = value;
                            } else if constexpr (std::is_same_v<T, bool>) {
                                dict_obj[dict_key] = value;
                            } else if constexpr (std::is_same_v<T, std::string>) {
                                dict_obj[dict_key] = value;
                            }
                        }, val_const->value);
                    }
                    else if (auto* val_list = dynamic_cast<const List*>(value_expr.get()))
                    {
                        // List value (e.g., color_map={Success: ["active", "enabled"]})
                        std::vector<glz::generic> list_vals;
                        for (const auto& list_elem : val_list->elts)
                        {
                            if (auto* elem_const = dynamic_cast<const Constant*>(list_elem.get()))
                            {
                                std::visit([&](auto&& value) {
                                    using T = std::decay_t<decltype(value)>;
                                    glz::generic elem_gen;
                                    if constexpr (std::is_same_v<T, int>) {
                                        elem_gen = static_cast<double>(value);
                                    } else if constexpr (std::is_same_v<T, double>) {
                                        elem_gen = value;
                                    } else if constexpr (std::is_same_v<T, std::string>) {
                                        elem_gen = value;
                                    }
                                    list_vals.push_back(elem_gen);
                                }, elem_const->value);
                            }
                            else if (auto* elem_name = dynamic_cast<const Name*>(list_elem.get()))
                            {
                                list_vals.push_back(glz::generic{elem_name->id});
                            }
                        }
                        dict_obj[dict_key] = list_vals;
                    }
                    else if (auto* val_name = dynamic_cast<const Name*>(value_expr.get()))
                    {
                        // Name value (treat as string)
                        dict_obj[dict_key] = val_name->id;
                    }
                }

                obj[key] = dict_obj;
            }
            else
            {
                ThrowError("Unsupported expression type in constructor argument '" + key + "'. "
                           "Constructor arguments must be literals (strings, numbers, booleans), lists, or dictionaries. "
                           "Complex expressions are not supported.", call.lineno, call.col_offset);
            }
        }

        return obj;
    }

    epoch_frame::Time ConstructorParser::ParseTimeConstructor(const Call& call)
    {
        // Convert kwargs to glz::generic and let glaze deserialize
        glz::generic obj = CallKwargsToGeneric(call);

        epoch_frame::Time time{};
        auto error = glz::read<glz::opts{}>(time, obj);
        if (error)
        {
            ThrowError("Failed to parse Time constructor: " + glz::format_error(error, glz::write_json(obj).value_or("{}")), call.lineno, call.col_offset);
        }

        return time;
    }

    epoch_script::CardColumnSchema ConstructorParser::ParseCardColumnSchemaConstructor(const Call& call)
    {
        // Convert kwargs to glz::generic and let glaze deserialize
        glz::generic obj = CallKwargsToGeneric(call);

        epoch_script::CardColumnSchema schema{};
        auto error = glz::read<glz::opts{}>(schema, obj);
        if (error)
        {
            ThrowError("Failed to parse CardColumnSchema constructor: " + glz::format_error(error, glz::write_json(obj).value_or("{}")), call.lineno, call.col_offset);
        }

        return schema;
    }

    epoch_script::EventMarkerSchema ConstructorParser::ParseEventMarkerSchemaConstructor(const Call& call)
    {
        // Convert kwargs to glz::generic and let glaze deserialize
        glz::generic obj = CallKwargsToGeneric(call);

        epoch_script::EventMarkerSchema filter{};
        auto error = glz::read<glz::opts{}>(filter, obj);
        if (error)
        {
            ThrowError("Failed to parse EventMarkerSchema constructor: " + glz::format_error(error, glz::write_json(obj).value_or("{}")), call.lineno, call.col_offset);
        }

        return filter;
    }


    epoch_script::SqlStatement ConstructorParser::ParseSqlStatementConstructor(const Call& call)
    {
        // Convert kwargs to glz::generic and let glaze deserialize
        glz::generic obj = CallKwargsToGeneric(call);

        epoch_script::SqlStatement stmt{};
        auto error = glz::read<glz::opts{}>(stmt, obj);
        if (error)
        {
            ThrowError("Failed to parse SqlStatement constructor: " + glz::format_error(error, glz::write_json(obj).value_or("{}")), call.lineno, call.col_offset);
        }

        return stmt;
    }

    epoch_script::TableReportSchema ConstructorParser::ParseTableReportSchemaConstructor(const Call& call)
    {
        // Convert kwargs to glz::generic and let glaze deserialize
        glz::generic obj = CallKwargsToGeneric(call);

        epoch_script::TableReportSchema schema{};
        auto error = glz::read<glz::opts{}>(schema, obj);
        if (error)
        {
            ThrowError("Failed to parse TableReportSchema constructor: " + glz::format_error(error, glz::write_json(obj).value_or("{}")), call.lineno, call.col_offset);
        }

        return schema;
    }

    void ConstructorParser::ThrowError(const std::string& msg, int line, int col)
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
