#pragma once

#include <epoch_script/strategy/metadata.h>
#include <epoch_script/transforms/core/constant_value.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>

namespace epoch_script::compiler {

/**
 * @brief Optimization pass that inlines scalar transforms as literal_inputs
 *
 * This pass runs after compilation to eliminate scalar transform nodes by:
 * 1. Detecting nodes of category Scalar (number, text, bool_true, etc.)
 * 2. Extracting the constant value from their options
 * 3. Replacing references to scalar nodes with literal_inputs in consuming nodes
 * 4. Removing scalar nodes from the graph
 *
 * Example transformation:
 * Before:
 *   number_0: {type: "number", options: {value: 42.0}}
 *   gt_1: {type: "gt", inputs: {SLOT0: ["price#result"], SLOT1: ["number_0#result"]}}
 *
 * After:
 *   gt_1: {type: "gt", inputs: {SLOT0: ["price#result"]}, literal_inputs: {SLOT1: 42.0}}
 *   number_0: DELETED
 *
 * Benefits:
 * - Eliminates ~23 scalar transform types from runtime
 * - Reduces graph size (fewer nodes)
 * - Removes scalar cache complexity
 * - Improves performance (no transform instantiation for constants)
 */
class ScalarInliningPass {
public:
    /**
     * @brief Run the scalar inlining optimization on a compiled graph
     *
     * @param algorithms The list of algorithm nodes from compilation
     * @return Modified algorithm list with scalars inlined and removed
     */
    static std::vector<epoch_script::strategy::AlgorithmNode> Run(
        const std::vector<epoch_script::strategy::AlgorithmNode>& algorithms);

private:
    /**
     * @brief Check if a node is a scalar transform
     */
    static bool IsScalarNode(const epoch_script::strategy::AlgorithmNode& node);

    /**
     * @brief Extract the constant value from a scalar node
     */
    static epoch_script::transform::ConstantValue ExtractScalarValue(
        const epoch_script::strategy::AlgorithmNode& node);

    /**
     * @brief Build mapping of node_id#output -> ConstantValue for all scalars
     */
    static std::unordered_map<std::string, epoch_script::transform::ConstantValue>
    BuildScalarValueMap(const std::vector<epoch_script::strategy::AlgorithmNode>& algorithms);

    /**
     * @brief Inline scalar references in a node's inputs as literal_inputs
     * @return true if any scalar references were inlined
     */
    static bool InlineScalarsInNode(
        epoch_script::strategy::AlgorithmNode& node,
        const std::unordered_map<std::string, epoch_script::transform::ConstantValue>& scalar_values);

    /**
     * @brief Remove scalar nodes from the graph
     */
    static std::vector<epoch_script::strategy::AlgorithmNode> RemoveScalarNodes(
        const std::vector<epoch_script::strategy::AlgorithmNode>& algorithms,
        const std::unordered_set<std::string>& scalar_node_ids);
};

} // namespace epoch_script::compiler
