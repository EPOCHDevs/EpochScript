//
// Created by Claude Code
// Common Subexpression Elimination (CSE) Optimizer - Implementation
//

#include "cse_optimizer.h"
#include <epoch_core/enum_wrapper.h>
#include <epoch_frame/datetime.h>
#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>
#include <algorithm>
#include <stdexcept>
#include <sstream>

namespace epoch_script
{

CSEOptimizer::CSEOptimizer() {}

bool CSEOptimizer::IsScalarType(const std::string& type) const
{
    // Scalar/literal types that are timeframe-agnostic
    static const std::unordered_set<std::string> scalar_types = {
        "text",
        "number",
        "bool_true",
        "bool_false",
        "null_number"
    };
    return scalar_types.count(type) > 0;
}

void CSEOptimizer::Optimize(std::vector<strategy::AlgorithmNode>& algorithms,
                           CompilationContext& context)
{
    // Map: semantic hash -> canonical node ID (first occurrence)
    std::unordered_map<size_t, std::string> hash_to_canonical;

    // Map: duplicate node ID -> canonical node ID (for redirecting references)
    std::unordered_map<std::string, std::string> redirect_map;

    // Set of node IDs to remove
    std::unordered_set<std::string> nodes_to_remove;


    // Phase 1: Identify duplicates
    for (const auto& node : algorithms)
    {
        // Skip nodes that should not be deduplicated
        if (ShouldExcludeFromCSE(node.type))
        {
            continue;
        }

        size_t h = ComputeSemanticHash(node);

        auto it = hash_to_canonical.find(h);
        if (it != hash_to_canonical.end())
        {
            // Potential duplicate found - verify with full equality check
            // (hash collision is possible, so we need to confirm)
            const std::string& canonical_id = it->second;

            // Find the canonical node
            auto canonical_it = std::find_if(algorithms.begin(), algorithms.end(),
                [&canonical_id](const strategy::AlgorithmNode& n) {
                    return n.id == canonical_id;
                });

            if (canonical_it != algorithms.end() && SemanticEquals(node, *canonical_it))
            {
                // True duplicate found
                redirect_map[node.id] = canonical_id;
                nodes_to_remove.insert(node.id);
            }
        }
        else
        {
            // First occurrence - this becomes the canonical node
            hash_to_canonical[h] = node.id;
        }
    }

    // Phase 2: Rewrite all references to point to canonical nodes
    for (auto& node : algorithms)
    {
        for (auto& [input_name, refs] : node.inputs)
        {
            for (auto& ref : refs)
            {
                // Only process node references, skip literal constants
                if (!ref.IsNodeReference())
                    continue;

                const auto& node_ref = ref.GetNodeReference();
                const std::string& node_id = node_ref.GetNodeId();
                const std::string& handle = node_ref.GetHandle();

                // Check if this node is a duplicate that needs redirection
                auto redirect_it = redirect_map.find(node_id);
                if (redirect_it != redirect_map.end())
                {
                    // Redirect to canonical node
                    ref = epoch_script::strategy::InputValue{
                        epoch_script::strategy::NodeReference{redirect_it->second, handle}
                    };
                }
                // else: external reference like "src", leave unchanged
            }
        }
    }

    // Phase 2.5: Update node references in options (schema fields)
    // Schema fields like select_key can reference node outputs and need to be redirected
    for (auto& node : algorithms)
    {
        // Check if this node has a "schema" option
        auto schema_it = node.options.find("schema");
        if (schema_it != node.options.end())
        {
            auto& option_def = schema_it->second;

            // Serialize to JSON string
            std::string json_str = glz::write_json(option_def.GetVariant()).value_or("");
            if (json_str.empty())
            {
                continue;  // Skip if serialization failed
            }

            // Replace all node references found in redirect_map
            // Look for patterns like "node_id#handle" in JSON string values
            for (const auto& [old_id, new_id] : redirect_map)
            {
                // Pattern: "old_id# (appears in select_key and other fields)
                std::string old_pattern = "\"" + old_id + "#";
                std::string new_pattern = "\"" + new_id + "#";

                size_t pos = 0;
                while ((pos = json_str.find(old_pattern, pos)) != std::string::npos)
                {
                    json_str.replace(pos, old_pattern.length(), new_pattern);
                    pos += new_pattern.length();
                }
            }

            // Deserialize back to the variant
            auto variant = option_def.GetVariant();
            auto parse_result = glz::read_json(variant, json_str);
            if (!parse_result)
            {
                // Successfully parsed - update the option
                option_def = MetaDataOptionDefinition{variant};
            }
            // If parse failed, leave option unchanged
        }
    }

    // Phase 3: Remove duplicate nodes from the algorithms vector
    algorithms.erase(
        std::remove_if(algorithms.begin(), algorithms.end(),
            [&nodes_to_remove](const strategy::AlgorithmNode& node) {
                return nodes_to_remove.count(node.id) > 0;
            }),
        algorithms.end()
    );

    // Phase 4: Update context.used_node_ids to remove deleted IDs
    for (const auto& removed_id : nodes_to_remove)
    {
        context.used_node_ids.erase(removed_id);
    }
}

size_t CSEOptimizer::ComputeSemanticHash(const strategy::AlgorithmNode& node) const
{
    size_t h = 0;

    // Hash the type
    HashCombine(h, std::hash<std::string>{}(node.type));

    // Hash the options map
    // We need to hash in a deterministic order (sorted by key)
    std::vector<std::string> option_keys;
    option_keys.reserve(node.options.size());
    for (const auto& [key, _] : node.options)
    {
        option_keys.push_back(key);
    }
    std::sort(option_keys.begin(), option_keys.end());

    for (const auto& key : option_keys)
    {
        HashCombine(h, std::hash<std::string>{}(key));
        HashCombine(h, node.options.at(key).GetHash());
    }

    // Hash the inputs map
    // Again, use sorted order for determinism
    std::vector<std::string> input_keys;
    input_keys.reserve(node.inputs.size());
    for (const auto& [key, _] : node.inputs)
    {
        input_keys.push_back(key);
    }
    std::sort(input_keys.begin(), input_keys.end());

    for (const auto& key : input_keys)
    {
        HashCombine(h, std::hash<std::string>{}(key));
        const auto& refs = node.inputs.at(key);
        for (const auto& ref : refs)
        {
            // Hash InputValue: differentiate between reference and literal
            if (ref.IsNodeReference())
            {
                HashCombine(h, 0); // Tag for node reference
                HashCombine(h, std::hash<std::string>{}(ref.GetNodeReference().GetRef()));
            }
            else
            {
                HashCombine(h, 1); // Tag for literal
                HashCombine(h, std::hash<std::string>{}(ref.GetLiteral().ToString()));
            }
        }
    }

    // Hash the timeframe if present
    // IMPORTANT: Skip timeframe hashing for scalar/literal types (text, number, bool, null)
    // These types are timeframe-agnostic and should be deduplicated regardless of timeframe
    // This fixes bugs where literals get assigned different timeframes based on usage context
    if (!IsScalarType(node.type))
    {
        if (node.timeframe)
        {
            // Use TimeFrame's ToString() method for hashing
            HashCombine(h, std::hash<std::string>{}(node.timeframe->ToString()));
        }
        else
        {
            // Hash a sentinel value for "no timeframe"
            HashCombine(h, 0);
        }
    }

    // Hash the session if present
    // Scalars are also session-agnostic
    if (!IsScalarType(node.type))
    {
        if (node.session)
        {
            HashCombine(h, HashSession(*node.session));
        }
        else
        {
            // Hash a sentinel value for "no session"
            HashCombine(h, 1);
        }
    }

    return h;
}

bool CSEOptimizer::SemanticEquals(const strategy::AlgorithmNode& a,
                                 const strategy::AlgorithmNode& b) const
{
    // Compare all fields EXCEPT the node ID
    // This matches the existing operator== but without the id comparison

    if (a.type != b.type || a.options != b.options || a.inputs != b.inputs)
    {
        return false;
    }

    // Compare timeframes (skip for scalar types - they're timeframe-agnostic)
    if (!IsScalarType(a.type))
    {
        if (a.timeframe.has_value() != b.timeframe.has_value())
        {
            return false;
        }
        if (a.timeframe && b.timeframe)
        {
            if (*a.timeframe != *b.timeframe)
            {
                return false;
            }
        }
    }

    // Compare sessions using hash-based comparison (skip for scalar types - they're session-agnostic)
    if (!IsScalarType(a.type))
    {
        if (a.session.has_value() != b.session.has_value())
        {
            return false;
        }
        if (a.session && b.session)
        {
            // Use hash comparison as a proxy for equality
            // This is safe because hash collisions are extremely rare
            if (HashSession(*a.session) != HashSession(*b.session))
            {
                return false;
            }
        }
    }

    return true;
}

bool CSEOptimizer::ShouldExcludeFromCSE(const std::string& type) const
{
    // Executor nodes have side effects (they generate trade signals)
    // and should never be deduplicated
    static const std::unordered_set<std::string> excluded_types = {
        "trade_signal_executor",
        "trade_manager_executor",
        "portfolio_executor"
    };

    // Alias nodes should never be deduplicated - each represents a distinct
    // variable assignment that needs a unique column identifier.
    // This prevents the "duplicate columns" error when the same source column
    // is referenced by multiple variables.
    if (type.starts_with("alias") || type == "alias")
    {
        return true;
    }

    return excluded_types.count(type) > 0;
}

std::string CSEOptimizer::ExtractNodeId(const std::string& ref) const
{
    auto hash_pos = ref.find('#');
    if (hash_pos != std::string::npos)
    {
        return ref.substr(0, hash_pos);
    }
    return ref; // No '#' found, return as-is
}

void CSEOptimizer::HashCombine(size_t& seed, size_t h) const
{
    // Same formula used in MetaDataOptionDefinition::GetHash()
    // This is a standard hash combination technique
    seed ^= h + 0x9e3779b9 + (seed << 6) + (seed >> 2);
}

size_t CSEOptimizer::HashSession(const strategy::SessionVariant& session) const
{
    return std::visit([](const auto& s) -> size_t {
        using T = std::decay_t<decltype(s)>;

        if constexpr (std::is_same_v<T, epoch_frame::SessionRange>)
        {
            // Hash SessionRange by converting to JSON and hashing the string
            // This is safe and doesn't require knowing the internal structure
            std::string json = glz::write_json(s).value_or("");
            return std::hash<std::string>{}(json);
        }
        else if constexpr (std::is_same_v<T, epoch_core::SessionType>)
        {
            // Hash SessionType enum by its integer value
            return std::hash<int>{}(static_cast<int>(s));
        }
        else
        {
            // Should never reach here, but return 0 if we do
            return 0;
        }
    }, session);
}

} // namespace epoch_script
