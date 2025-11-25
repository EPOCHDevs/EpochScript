//
// Created by Claude Code
// EpochScript AST Compiler - Refactored Facade Implementation
//

#include "ast_compiler.h"
#include "parser/python_parser.h"
#include "validators/boolean_select_validator.h"
#include <epoch_script/transforms/core/transform_registry.h>
#include <stdexcept>
#include <queue>
#include <unordered_map>
#include <unordered_set>
#include <algorithm>
#include <format>

namespace epoch_script
{
    // Helper function to extract node_id from "node_id#handle" reference
    static std::string extractNodeId(const std::string& ref)
    {
        auto hash_pos = ref.find('#');
        if (hash_pos != std::string::npos)
        {
            return ref.substr(0, hash_pos);
        }
        return ref;
    }

    // Helper function to check if a node type is a scalar/constant
    // Scalars are timeframe-agnostic and should not require timeframe resolution
    // This checks the metadata's category field to determine if it's a Scalar type
    static bool isScalarType(const std::string& type,
                            const std::unordered_map<std::string, epoch_script::transforms::TransformsMetaData>& metadata_map)
    {
        auto it = metadata_map.find(type);
        if (it == metadata_map.end())
        {
            throw std::runtime_error("Transform type '" + type + "' not found in metadata map");
        }
        return it->second.category == epoch_core::TransformCategory::Scalar;
    }

    // Topological sort using Kahn's algorithm (BFS-based)
    // Returns nodes in dependency order: dependencies before dependents
    static std::vector<epoch_script::strategy::AlgorithmNode> TopologicalSort(
        std::vector<epoch_script::strategy::AlgorithmNode> nodes)
    {
        using AlgorithmNode = epoch_script::strategy::AlgorithmNode;

        // Build node index: node_id -> position in input vector
        std::unordered_map<std::string, size_t> node_index;
        for (size_t i = 0; i < nodes.size(); ++i)
        {
            node_index[nodes[i].id] = i;
        }

        // Build dependency graph
        std::unordered_map<std::string, int> in_degree;  // node_id -> count of dependencies
        std::unordered_map<std::string, std::vector<std::string>> dependents;  // node_id -> nodes that depend on it

        // Initialize in-degree to 0 for all nodes
        for (const auto& node : nodes)
        {
            in_degree[node.id] = 0;
        }

        // Calculate in-degrees and build adjacency list
        for (const auto& node : nodes)
        {
            const std::string& node_id = node.id;

            // Process all inputs to find dependencies
            for (const auto& [input_name, refs] : node.inputs)
            {
                for (const auto& ref : refs)
                {
                    // Skip literal values - only process node references
                    if (!ref.IsNodeReference())
                    {
                        continue;
                    }

                    std::string dep_id = extractNodeId(ref.GetNodeReference().GetRef());

                    // Only count dependency if it's an internal node (not external like "src")
                    if (node_index.find(dep_id) != node_index.end())
                    {
                        in_degree[node_id]++;
                        dependents[dep_id].push_back(node_id);
                    }
                }
            }
        }

        // Kahn's algorithm: start with nodes that have no dependencies
        std::queue<std::string> queue;
        for (const auto& [node_id, degree] : in_degree)
        {
            if (degree == 0)
            {
                queue.push(node_id);
            }
        }

        // Process nodes in topological order
        std::vector<AlgorithmNode> sorted_nodes;
        sorted_nodes.reserve(nodes.size());

        while (!queue.empty())
        {
            std::string node_id = queue.front();
            queue.pop();

            // Add this node to sorted output
            sorted_nodes.push_back(std::move(nodes[node_index[node_id]]));

            // Decrease in-degree for all dependents
            for (const std::string& dependent_id : dependents[node_id])
            {
                in_degree[dependent_id]--;
                if (in_degree[dependent_id] == 0)
                {
                    queue.push(dependent_id);
                }
            }
        }

        // Check for cycles
        if (sorted_nodes.size() != nodes.size())
        {
            // Collect nodes that are part of the cycle
            std::vector<std::string> remaining_nodes;
            for (const auto& [node_id, degree] : in_degree)
            {
                if (degree > 0)
                {
                    remaining_nodes.push_back(node_id);
                }
            }

            std::string error_msg = "Circular dependency detected in algorithm graph! Remaining nodes: ";
            for (size_t i = 0; i < remaining_nodes.size(); ++i)
            {
                if (i > 0) error_msg += ", ";
                error_msg += remaining_nodes[i];
            }
            throw std::runtime_error(error_msg);
        }

        return sorted_nodes;
    }

    AlgorithmAstCompiler::AlgorithmAstCompiler()
    {
        initializeComponents();
    }

    void AlgorithmAstCompiler::initializeComponents()
    {
        // Register validators
        RegisterBooleanSelectValidator();

        // Initialize all components in dependency order
        type_checker_ = std::make_unique<TypeChecker>(context_);
        option_validator_ = std::make_unique<OptionValidator>(context_);
        special_param_handler_ = std::make_unique<SpecialParameterHandler>(context_);

        // ExpressionCompiler and ConstructorParser have circular dependency
        // Initialize ExpressionCompiler first, then ConstructorParser, then wire them together
        expr_compiler_ = std::make_unique<ExpressionCompiler>(
            context_,
            *type_checker_,
            *option_validator_,
            *special_param_handler_);
        constructor_parser_ = std::make_unique<ConstructorParser>(context_, *expr_compiler_);

        // Wire circular dependency
        expr_compiler_->SetConstructorParser(constructor_parser_.get());

        // Initialize remaining components
        node_builder_ = std::make_unique<NodeBuilder>(
            context_,
            *type_checker_,
            *option_validator_,
            *special_param_handler_,
            *constructor_parser_,
            *expr_compiler_);

        ast_visitor_ = std::make_unique<AstVisitor>(
            context_,
            *node_builder_,
            *expr_compiler_,
            *constructor_parser_);

        // Initialize constant folder for preprocessing
        constant_folder_ = std::make_unique<ConstantFolder>(context_);

        // Initialize CSE optimizer for deduplicating identical transforms
        cse_optimizer_ = std::make_unique<CSEOptimizer>();
    }

    CompilationResult AlgorithmAstCompiler::compile(const std::string& source, bool skip_sink_validation)
    {
        // Parse Python source to AST
        PythonParser parser;
        auto module = parser.parse(source);

        // Compile AST directly to AlgorithmNode structures
        return compileAST(std::move(module), skip_sink_validation);
    }

    CompilationResult AlgorithmAstCompiler::compileAST(ModulePtr module, bool skip_sink_validation)
    {
        // Clear state for fresh compilation
        context_.algorithms.clear();
        context_.executor_count = 0;
        context_.node_lookup.clear();
        context_.var_to_binding.clear();
        context_.node_output_types.clear();
        context_.used_node_ids.clear();

        // Reserve capacity to prevent reallocations (typical algorithm has 50-500 nodes)
        context_.algorithms.reserve(500);

        // Preprocess module to fold constants (like C++ template metaprogramming)
        // This enables constant variables in subscripts: src.v[lookback_period]
        module = constant_folder_->PreprocessModule(std::move(module));

        // Visit the module - builds algorithms in AST order (source code order)
        ast_visitor_->VisitModule(*module);

        // Verify session dependencies and auto-create missing sessions nodes
        verifySessionDependencies();

        // Common Subexpression Elimination (CSE) optimization pass
        // Deduplicates semantically identical transform nodes to reduce computation
        // Runs before topological sort so we can modify the graph structure
        cse_optimizer_->Optimize(context_.algorithms, context_);

        // Debug: Check for duplicate IDs after CSE
        std::unordered_set<std::string> ids_after_cse;
        for (const auto& algo : context_.algorithms) {
            if (!ids_after_cse.insert(algo.id).second) {
                spdlog::error("[AST Compiler] DUPLICATE ID after CSE: '{}' (type: {})", algo.id, algo.type);
            }
        }

        // Specialize alias nodes based on their input types
        // Rewrites generic "alias" nodes to typed versions (alias_decimal, alias_boolean, etc.)
        type_checker_->SpecializeAliasNodes();

        // Remove orphan nodes (nodes not used by any sink/executor)
        // Must run BEFORE timeframe resolution to avoid resolving timeframes for dead code
        // Orphans occur when users write unused variables: x = 5.0 (never used)
        removeOrphanNodes(skip_sink_validation);

        // Debug: Check for duplicate IDs after orphan removal
        std::unordered_set<std::string> ids_after_orphan;
        for (const auto& algo : context_.algorithms) {
            if (!ids_after_orphan.insert(algo.id).second) {
                spdlog::error("[AST Compiler] DUPLICATE ID after orphan removal: '{}' (type: {})", algo.id, algo.type);
            }
        }

        // Sort algorithms in topological order: dependencies before dependents
        // This ensures handles are registered before they're referenced
        // IMPORTANT: Must sort BEFORE resolving timeframes so input nodes are cached first
        context_.algorithms = TopologicalSort(std::move(context_.algorithms));

        // Debug: Check for duplicate IDs after topological sort
        std::unordered_set<std::string> ids_after_topo;
        for (const auto& algo : context_.algorithms) {
            if (!ids_after_topo.insert(algo.id).second) {
                spdlog::error("[AST Compiler] DUPLICATE ID after topological sort: '{}' (type: {})", algo.id, algo.type);
            }
        }

        // Extract base timeframe from market_data_source if present, else use 1d default
        std::optional<epoch_script::TimeFrame> base_timeframe;
        for (const auto& algo : context_.algorithms) {
            if (algo.type == "market_data_source" && algo.timeframe) {
                base_timeframe = algo.timeframe;
                break;
            }
        }
        if (!base_timeframe) {
            base_timeframe = epoch_script::TimeFrame("1d");
        }

        // Resolve timeframes for all nodes using TimeframeResolver utility
        // This runs AFTER topological sort so input timeframes are available
        resolveTimeframes(base_timeframe.value(), skip_sink_validation);

        // Update node_lookup indices after reordering
        context_.node_lookup.clear();
        for (size_t i = 0; i < context_.algorithms.size(); ++i)
        {
            context_.node_lookup[context_.algorithms[i].id] = i;
        }

        // Return results - move semantics for zero-copy
        return std::move(context_.algorithms);
    }

    void AlgorithmAstCompiler::verifySessionDependencies()
    {
        special_param_handler_->VerifySessionDependencies();
    }

    void AlgorithmAstCompiler::resolveTimeframes(const epoch_script::TimeFrame& /*base_timeframe*/, bool skip_sink_validation)
    {
        // Use TimeframeResolver utility to resolve timeframes for all nodes
        // Create fresh resolver instance to avoid stale cache from previous compilations
        TimeframeResolver resolver;

        // PASS 1: Resolve timeframes for nodes with inputs or explicit timeframes
        // Literals will return nullopt and be handled in pass 2
        for (auto& algo : context_.algorithms)
        {
            auto resolved_timeframe = resolver.ResolveNodeTimeframe(algo);
            if (resolved_timeframe)
            {
                algo.timeframe = resolved_timeframe;
            }
        }

        // PASS 2: Resolve literal timeframes by finding nodes that use them
        // Literals inherit timeframes from their dependent nodes
        // EXCEPT scalar types which don't need timeframes (runtime handles them)
        const auto& metadata_map = context_.GetRegistry().GetMetaData();
        for (auto& algo : context_.algorithms)
        {
            // Skip if already resolved
            if (algo.timeframe)
            {
                continue;
            }

            // Skip scalar types - they don't need timeframes (runtime handles them)
            if (isScalarType(algo.type, metadata_map))
            {
                continue;
            }

            // Resolve literal timeframe based on usage (for non-scalar literals)
            auto literal_timeframe = resolver.ResolveLiteralTimeframe(algo.id, context_.algorithms);
            if (literal_timeframe)
            {
                algo.timeframe = literal_timeframe;
            }
        }

        // Validate that ALL nodes have timeframes after resolution
        // No node should leave compilation with timeframe=null
        // This is a strict check - if we can't resolve a timeframe, compilation fails
        // EXCEPT when skip_sink_validation=true (test scenarios with partial scripts)
        // ALSO EXCEPT for scalar/constant nodes which are timeframe-agnostic
        if (!skip_sink_validation)
        {
            const auto& metadata_map = context_.GetRegistry().GetMetaData();
            for (const auto& algo : context_.algorithms)
            {
                // Skip validation for scalar types - they don't need timeframes
                if (isScalarType(algo.type, metadata_map))
                {
                    continue;
                }

                // DEFERRED VALIDATION: Check if this transform requires a timeframe (moved from option_validator.cpp)
                // This validation now occurs AFTER timeframe inheritance has been attempted,
                // allowing transforms to inherit timeframes from their inputs as documented.
                auto meta_it = metadata_map.find(algo.type);
                if (meta_it != metadata_map.end())
                {
                    const auto& comp_meta = meta_it->second;

                    // Skip intradayOnly nodes (they default to 1Min at runtime)
                    if (comp_meta.requiresTimeFrame && !comp_meta.intradayOnly && !algo.timeframe.has_value())
                    {
                        throw std::runtime_error(
                            std::format("Data source '{}' (type '{}') requires a 'timeframe' parameter. "
                                      "Timeframe inheritance failed - the node has no inputs with resolved timeframes. "
                                      "Add an explicit timeframe option, e.g. timeframe=\"1D\"",
                                      algo.id, comp_meta.name));
                    }
                }

                if (!algo.timeframe.has_value())
                {
                    throw std::runtime_error(
                        "Could not resolve timeframe for node '" + algo.id +
                        "' (type: " + algo.type + "). " +
                        "This indicates the node has no inputs and no dependents. " +
                        "If this node should be executed, ensure it's connected to a sink (report/executor). " +
                        "If it's unused, it should have been removed as an orphan node.");
                }
            }
        }
    }

    bool AlgorithmAstCompiler::isSinkNode(const std::string& type) const
    {
        // Sink nodes are transforms with 0 outputs (terminal nodes)
        // These are outputs (reports, executors, event markers) that don't feed into other nodes
        // Using metadata-based check instead of hardcoded list for maintainability
        auto metadata = transforms::ITransformRegistry::GetInstance().GetMetaData(type);
        if (!metadata)
        {
            return false;  // Unknown transform, assume not a sink
        }

        const auto& transform = metadata->get();
        return transform.outputs.empty();  // Sink if no outputs
    }

    void AlgorithmAstCompiler::removeOrphanNodes(bool skip_sink_validation)
    {
        // Orphan nodes are nodes that are not used by any sink (report/executor)
        // These nodes are dead code and should be removed before timeframe resolution
        // to avoid trying to resolve timeframes for nodes that will never execute

        // First, check if at least one sink node exists
        bool has_sink = false;
        for (const auto& node : context_.algorithms)
        {
            if (isSinkNode(node.type))
            {
                has_sink = true;
                break;
            }
        }

        // If no sinks exist:
        // - In strict mode (skip_sink_validation=false): Error - scripts must have output
        // - In permissive mode (skip_sink_validation=true): Skip orphan removal and continue
        if (!has_sink)
        {
            if (!skip_sink_validation)
            {
                throw std::runtime_error(
                    "Script has no output. Add at least one report or executor node. "
                    "Reports: table_report, gap_report, numeric_cards_report, bar_chart_report, "
                    "pie_chart_report, lines_chart_report, candles_chart_report, etc. "
                    "Executors: trade_signal_executor, trade_manager_executor, portfolio_executor.");
            }
            return;  // Skip orphan removal for test scenarios without sinks
        }

        // Build reverse dependency graph using BFS from sink nodes
        std::unordered_set<std::string> reachable_nodes;
        std::queue<std::string> queue;

        // Phase 1: Find all sink nodes (starting points for BFS)
        for (const auto& node : context_.algorithms)
        {
            if (isSinkNode(node.type))
            {
                queue.push(node.id);
                reachable_nodes.insert(node.id);
            }
        }

        // Phase 2: BFS backwards through dependencies to mark all reachable nodes
        while (!queue.empty())
        {
            std::string node_id = queue.front();
            queue.pop();

            // Find the node in algorithms vector
            auto it = std::find_if(context_.algorithms.begin(), context_.algorithms.end(),
                [&node_id](const auto& n) { return n.id == node_id; });

            if (it != context_.algorithms.end())
            {
                // Mark all input dependencies as reachable
                for (const auto& [input_name, refs] : it->inputs)
                {
                    for (const auto& ref : refs)
                    {
                        // Skip literal values - only process node references
                        if (!ref.IsNodeReference())
                        {
                            continue;
                        }

                        // Extract node_id from "node_id#handle" format
                        std::string dep_id = extractNodeId(ref.GetNodeReference().GetRef());

                        // Only process if not already marked reachable
                        if (reachable_nodes.find(dep_id) == reachable_nodes.end())
                        {
                            reachable_nodes.insert(dep_id);
                            queue.push(dep_id);
                        }
                    }
                }
            }
        }

        // Phase 3: Remove unreachable (orphan) nodes
        size_t original_count = context_.algorithms.size();
        context_.algorithms.erase(
            std::remove_if(context_.algorithms.begin(), context_.algorithms.end(),
                [&reachable_nodes](const auto& node) {
                    return reachable_nodes.find(node.id) == reachable_nodes.end();
                }),
            context_.algorithms.end()
        );

        // Phase 4: Update context.used_node_ids to remove deleted IDs
        for (const auto& node : context_.algorithms)
        {
            if (reachable_nodes.find(node.id) == reachable_nodes.end())
            {
                context_.used_node_ids.erase(node.id);
            }
        }

        // Log if orphans were removed (useful for debugging)
        size_t removed_count = original_count - context_.algorithms.size();
        if (removed_count > 0)
        {
            // Orphan removal is expected behavior, not an error
            // Users may write unused variables like: x = 5.0 (never used)
        }
    }

    // Convenience function
    CompilationResult compileAlgorithm(const std::string& source)
    {
        AlgorithmAstCompiler compiler;
        return compiler.compile(source);
    }

} // namespace epoch_script
