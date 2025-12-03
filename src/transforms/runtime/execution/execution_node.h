//
// Created by adesola on 12/28/24.
//

#pragma once
#include "execution_context.h"
#include <epoch_script/transforms/core/itransform.h>
#include <functional>
#include <memory>
#include <tbb/flow_graph.h>

namespace epoch_script::runtime {
using execution_context_t = const tbb::flow::continue_msg &;

// Execution node types
enum class ExecutionNodeType {
  Default,
  CrossSectional,
  AssetRefPassthrough,
  IsAssetRef
};

// Apply a regular transform
void ApplyDefaultTransform(const epoch_script::transform::ITransformBase &transformer,
                           ExecutionContext &msg);

// Apply a cross-sectional transform
void ApplyCrossSectionTransform(const epoch_script::transform::ITransformBase &transformer,
                                ExecutionContext &msg);

// Apply an asset_ref_passthrough transform
// Filters data by asset matching criteria, passes through for matching assets,
// skips entirely for non-matching assets
void ApplyAssetRefPassthroughTransform(const epoch_script::transform::ITransformBase &transformer,
                                        ExecutionContext &msg);

// Apply an is_asset_ref transform
// Returns boolean series: true for matching assets, false for non-matching
// Outputs for ALL assets (scalar-optimized, no timeframe management)
void ApplyIsAssetRefTransform(const epoch_script::transform::ITransformBase &transformer,
                               ExecutionContext &msg);

// Create a node function for a transform
// Pass the transformer, assets, and message by reference to avoid dangling references
template <ExecutionNodeType node_type>
std::function<void(execution_context_t)>
MakeExecutionNode(const epoch_script::transform::ITransformBase &transformer,
                 ExecutionContext &msg) {

  return [&](execution_context_t /*unused*/) {
    if constexpr (node_type == ExecutionNodeType::CrossSectional) {
      ApplyCrossSectionTransform(transformer, msg);
    } else if constexpr (node_type == ExecutionNodeType::AssetRefPassthrough) {
      ApplyAssetRefPassthroughTransform(transformer, msg);
    } else if constexpr (node_type == ExecutionNodeType::IsAssetRef) {
      ApplyIsAssetRefTransform(transformer, msg);
    } else {
      ApplyDefaultTransform(transformer, msg);
    }
  };
}

// Backward compatibility: bool template for existing code
template <bool is_cross_sectional>
std::function<void(execution_context_t)>
MakeExecutionNode(const epoch_script::transform::ITransformBase &transformer,
                 ExecutionContext &msg) {
  if constexpr (is_cross_sectional) {
    return MakeExecutionNode<ExecutionNodeType::CrossSectional>(transformer, msg);
  } else {
    return MakeExecutionNode<ExecutionNodeType::Default>(transformer, msg);
  }
}

} // namespace epoch_script::runtime