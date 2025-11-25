//
// Created by Claude Code
// EpochScript Type Checker
//
// Handles type checking and type casting for node connections.
// Manages data type compatibility and automatic type conversions.
//

#pragma once

#include "compilation_context.h"
#include "parser/ast_nodes.h"
#include <optional>
#include <string>

namespace epoch_script
{

    class TypeChecker
    {
    public:
        explicit TypeChecker(CompilationContext& context) : context_(context) {}

        // Get the output type of a node's handle
        DataType GetNodeOutputType(const std::string& node_id, const std::string& handle);

        // Get the type of an InputValue (handles both NodeReference and Constants)
        DataType GetNodeOutputType(const strategy::InputValue& input);

        // Check if source type is compatible with target type
        bool IsTypeCompatible(DataType source, DataType target);

        // Determine if type cast is needed and which cast to use
        // Returns: std::nullopt if no cast needed, "bool_to_num", "num_to_bool", or "incompatible"
        std::optional<std::string> NeedsTypeCast(DataType source, DataType target);

        // Insert a type cast node and return the casted value handle
        strategy::InputValue InsertTypeCast(const strategy::InputValue& source, DataType source_type, DataType target_type);

        // Convert DataType enum to human-readable string
        static std::string DataTypeToString(DataType type);

        // Resolve Any output types based on node inputs
        // For nodes with Any output type, this infers the actual output type from inputs
        void ResolveAnyOutputType(const std::string& node_id, const std::string& node_type);

        // Insert a static_cast node to materialize resolved Any types
        // Returns the new node ID of the inserted static_cast node
        std::string InsertStaticCast(const std::string& source_node_id, const std::string& source_handle, DataType resolved_type);

        // Insert a stringify node to convert Any type to String
        // Returns the new node ID of the inserted stringify node
        std::string InsertStringify(const std::string& source_node_id, const std::string& source_handle);

        // Specialize alias nodes based on their input types
        // Rewrites generic "alias" nodes to typed versions (alias_decimal, alias_boolean, etc.)
        void SpecializeAliasNodes();

    private:
        CompilationContext& context_;

        // Helper to create number literal nodes for casting
        strategy::InputValue MaterializeNumber(double value);

        // Helper to create string literal nodes for casting
        strategy::InputValue MaterializeString(const std::string& value);

        // Helper to generate unique node ID
        std::string UniqueNodeId(const std::string& base);

        // Helper to create "node_id#handle" format
        std::string JoinId(const std::string& node_id, const std::string& handle);
    };

} // namespace epoch_script
