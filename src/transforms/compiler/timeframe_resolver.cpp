//
// Created by Claude Code
// EpochScript Timeframe Resolution Utility Implementation
//

#include "timeframe_resolver.h"
#include <epoch_script/transforms/core/transform_registry.h>
#include <algorithm>

namespace epoch_script
{

    std::vector<std::string> TimeframeResolver::extractInputNodeIds(const std::vector<std::string> &inputIds)
    {
        std::vector<std::string> nodeIds;
        nodeIds.reserve(inputIds.size());

        for (const auto &handleId : inputIds)
        {
            // Extract node ID from "node_id#handle" format
            auto hashPos = handleId.find("#");
            if (hashPos != std::string::npos)
            {
                nodeIds.push_back(handleId.substr(0, hashPos));
            }
            else
            {
                // Fallback: treat entire string as node ID
                nodeIds.push_back(handleId);
            }
        }

        return nodeIds;
    }

    std::optional<epoch_script::TimeFrame> TimeframeResolver::ResolveTimeframe(
        const std::string &nodeId,
        const std::vector<std::string> &inputIds)
    {
        // Check cache first
        if (nodeTimeframes.contains(nodeId))
        {
            return nodeTimeframes[nodeId];
        }

        std::optional<epoch_script::TimeFrame> resolvedTimeframe;

        // If we have inputs, resolve from them
        if (!inputIds.empty())
        {
            std::vector<epoch_script::TimeFrame> inputTimeframes;
            inputTimeframes.reserve(inputIds.size());

            // Extract node IDs from "node_id#handle" format
            auto nodeIds = extractInputNodeIds(inputIds);

            for (const auto &inputNodeId : nodeIds)
            {
                if (nodeTimeframes.contains(inputNodeId) && nodeTimeframes[inputNodeId])
                {
                    inputTimeframes.push_back(nodeTimeframes[inputNodeId].value());
                }
            }

            // Find the lowest resolution (highest timeframe value) using operator<
            // TimeFrame::operator< is defined such that higher resolution < lower resolution
            // So std::max_element gives us the lowest resolution timeframe
            if (!inputTimeframes.empty())
            {
                auto maxTimeframe = *std::max_element(inputTimeframes.begin(), inputTimeframes.end());
                resolvedTimeframe = maxTimeframe;
            }
        }

        // Cache the result (might be nullopt if no inputs had timeframes)
        nodeTimeframes[nodeId] = resolvedTimeframe;
        return resolvedTimeframe;
    }

    std::optional<epoch_script::TimeFrame> TimeframeResolver::ResolveNodeTimeframe(
        const epoch_script::strategy::AlgorithmNode &node)
    {
        // If node has explicit timeframe, use it and cache it
        if (node.timeframe)
        {
            nodeTimeframes[node.id] = node.timeframe;
            return node.timeframe;
        }

        // Extract input IDs from the node (flatten all input vectors)
        std::vector<std::string> inputIds;
        for (const auto &[key, values] : node.inputs)
        {
            for (const auto& input_value : values)
            {
                // Only process node references, skip literals
                if (input_value.IsNodeReference())
                {
                    inputIds.push_back(input_value.GetNodeReference().GetRef());
                }
            }
        }

        // If node has inputs, resolve timeframe from them
        if (!inputIds.empty())
        {
            auto resolved = ResolveTimeframe(node.id, inputIds);

            // If we resolved a timeframe from inputs, return it
            if (resolved)
            {
                return resolved;
            }
        }

        // Node has no explicit timeframe and no inputs (or inputs had no timeframes)
        // This means it's likely a literal - will be resolved in second pass
        return std::nullopt;
    }

    std::optional<epoch_script::TimeFrame> TimeframeResolver::ResolveLiteralTimeframe(
        const std::string &nodeId,
        const std::vector<epoch_script::strategy::AlgorithmNode> &allNodes)
    {
        // Check cache first
        if (nodeTimeframes.contains(nodeId) && nodeTimeframes[nodeId])
        {
            return nodeTimeframes[nodeId];
        }

        // Find all nodes that use this literal as an input
        std::vector<epoch_script::TimeFrame> dependentTimeframes;

        for (const auto &node : allNodes)
        {
            // Skip if this node doesn't have a resolved timeframe yet
            if (!nodeTimeframes.contains(node.id) || !nodeTimeframes[node.id])
            {
                continue;
            }

            // Check if this node uses our literal
            for (const auto &[inputName, inputRefs] : node.inputs)
            {
                for (const auto &ref : inputRefs)
                {
                    // Skip literal values - only process node references
                    if (!ref.IsNodeReference())
                    {
                        continue;
                    }

                    // Extract node ID from NodeReference
                    const auto& node_ref = ref.GetNodeReference();
                    std::string inputNodeId = node_ref.GetNodeId();

                    if (inputNodeId == nodeId)
                    {
                        // This node uses our literal - add its timeframe
                        dependentTimeframes.push_back(nodeTimeframes[node.id].value());
                        break;
                    }
                }
            }
        }

        // If we found dependent nodes, inherit their timeframe
        // Use the maximum (lowest resolution) if multiple dependents exist
        if (!dependentTimeframes.empty())
        {
            auto maxTimeframe = *std::max_element(dependentTimeframes.begin(), dependentTimeframes.end());
            nodeTimeframes[nodeId] = maxTimeframe;
            return maxTimeframe;
        }

        // No dependents found - cannot resolve timeframe
        // Note: This will be caught during validation for transforms that require explicit timeframes
        return std::nullopt;
    }

} // namespace epoch_script
