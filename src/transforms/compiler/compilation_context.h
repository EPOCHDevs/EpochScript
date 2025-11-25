//
// Created by Claude Code
// EpochScript Compilation Context
//
// Shared state structure used across all compiler components.
// Provides centralized access to compilation state and metadata registry.
//

#pragma once

#include <epoch_script/strategy/metadata.h>
#include <epoch_script/transforms/core/registry.h>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include <string>

namespace epoch_script
{

    // Type system for type checking and casting
    enum class DataType
    {
        Boolean, // B
        Integer, // I
        Decimal, // D
        Number,  // N (accepts Integer or Decimal)
        String,  // S
        Timestamp, // T
        Any      // A
    };


    // Shared compilation state accessible to all compiler components
    struct CompilationContext
    {
        // Variable bindings: variable -> "node.handle" or "component_name"
        std::unordered_map<std::string, std::string> var_to_binding;

        // Variable bindings for literals: variable -> InputValue (ConstantValue)
        // Used when a variable is assigned a constant value directly (e.g., threshold = 100.0)
        std::unordered_map<std::string, strategy::InputValue> var_to_literal;

        // Main output: topologically sorted algorithms (includes executor)
        std::vector<strategy::AlgorithmNode> algorithms;

        // Fast O(1) lookup by node ID -> index (never invalidated)
        std::unordered_map<std::string, size_t> node_lookup;

        // Track used node IDs for O(1) uniqueness checks
        std::unordered_set<std::string> used_node_ids;

        // Track output types for type checking: node_id -> handle -> DataType
        std::unordered_map<std::string, std::unordered_map<std::string, DataType>> node_output_types;

        // Track executor count for validation
        size_t executor_count = 0;

        // Access to transform registry (singleton)
        const transforms::ITransformRegistry& GetRegistry() const
        {
            return transforms::ITransformRegistry::GetInstance();
        }

        // Helper to get component metadata
        const transforms::TransformsMetaData& GetComponentMetadata(const std::string& component_name) const
        {
            const auto& all_metadata = GetRegistry().GetMetaData();
            if (!all_metadata.contains(component_name))
            {
                throw std::runtime_error("Unknown component '" + component_name + "'");
            }
            return all_metadata.at(component_name);
        }

        // Helper to check if component exists
        bool HasComponent(const std::string& component_name) const
        {
            const auto& all_metadata = GetRegistry().GetMetaData();
            return all_metadata.contains(component_name);
        }
    };

} // namespace epoch_script
