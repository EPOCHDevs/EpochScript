#include "scalar_inlining_pass.h"
#include <epoch_script/transforms/core/itransform.h>
#include <spdlog/spdlog.h>
#include <functional>

namespace epoch_script::compiler {

std::vector<epoch_script::strategy::AlgorithmNode> ScalarInliningPass::Run(
    const std::vector<epoch_script::strategy::AlgorithmNode>& algorithms) {

    SPDLOG_DEBUG("Running scalar inlining optimization pass on {} nodes", algorithms.size());

    // Step 1: Build map of scalar node outputs to their constant values
    auto scalar_values = BuildScalarValueMap(algorithms);

    if (scalar_values.empty()) {
        SPDLOG_DEBUG("No scalar nodes found, skipping inlining");
        return algorithms;
    }

    SPDLOG_DEBUG("Found {} scalar values to inline", scalar_values.size());

    // Step 2: For each non-scalar node, replace scalar inputs with literal_inputs
    std::vector<epoch_script::strategy::AlgorithmNode> modified_algorithms;
    std::unordered_set<std::string> scalar_node_ids;

    // Collect scalar node IDs
    for (const auto& [output_ref, value] : scalar_values) {
        size_t hash_pos = output_ref.find('#');
        if (hash_pos != std::string::npos) {
            scalar_node_ids.insert(output_ref.substr(0, hash_pos));
        }
    }

    // Process each node
    for (auto node : algorithms) {
        if (IsScalarNode(node)) {
            // Skip scalar nodes - they'll be removed
            SPDLOG_DEBUG("Skipping scalar node: {} (type: {})", node.id, node.type);
            continue;
        }

        // Inline scalar references in this node
        bool inlined = InlineScalarsInNode(node, scalar_values);
        if (inlined) {
            SPDLOG_DEBUG("Inlined scalars in node: {} (type: {})", node.id, node.type);
        }

        modified_algorithms.push_back(std::move(node));
    }

    SPDLOG_DEBUG("Scalar inlining complete: removed {} scalar nodes, kept {} regular nodes",
                scalar_node_ids.size(), modified_algorithms.size());

    return modified_algorithms;
}

bool ScalarInliningPass::IsScalarNode(const epoch_script::strategy::AlgorithmNode& node) {
    // Check if this node type is registered as a scalar transform
    auto metadata = epoch_script::transforms::ITransformRegistry::GetInstance().GetMetaData(node.type);
    if (!metadata) {
        return false;
    }

    return metadata->get().category == epoch_core::TransformCategory::Scalar;
}

epoch_script::transform::ConstantValue ScalarInliningPass::ExtractScalarValue(
    const epoch_script::strategy::AlgorithmNode& node) {

    // Static map of scalar type names to extraction functors
    using ExtractorFunc = std::function<epoch_script::transform::ConstantValue(const epoch_script::strategy::AlgorithmNode&)>;

    static const std::unordered_map<std::string, ExtractorFunc> scalar_extractors = {
        // Value-based scalars
        {"number", [](const auto& n) {
            auto it = n.options.find("value");
            if (it == n.options.end()) {
                throw std::runtime_error("number node missing value option: " + n.id);
            }
            return epoch_script::transform::ConstantValue(it->second.GetDecimal());
        }},
        {"text", [](const auto& n) {
            auto it = n.options.find("value");
            if (it == n.options.end()) {
                throw std::runtime_error("text node missing value option: " + n.id);
            }
            return epoch_script::transform::ConstantValue(it->second.GetString());
        }},

        // Boolean scalars
        {"bool_true",  [](const auto&) { return epoch_script::transform::ConstantValue(true); }},
        {"bool_false", [](const auto&) { return epoch_script::transform::ConstantValue(false); }},

        // Numeric constants
        {"zero",         [](const auto&) { return epoch_script::transform::ConstantValue(0.0); }},
        {"one",          [](const auto&) { return epoch_script::transform::ConstantValue(1.0); }},
        {"negative_one", [](const auto&) { return epoch_script::transform::ConstantValue(-1.0); }},

        // Mathematical constants
        {"pi",     [](const auto&) { return epoch_script::transform::ConstantValue(3.141592653589793); }},
        {"e",      [](const auto&) { return epoch_script::transform::ConstantValue(2.718281828459045); }},
        {"phi",    [](const auto&) { return epoch_script::transform::ConstantValue(1.618033988749895); }},
        {"sqrt2",  [](const auto&) { return epoch_script::transform::ConstantValue(1.414213562373095); }},
        {"sqrt3",  [](const auto&) { return epoch_script::transform::ConstantValue(1.732050807568877); }},
        {"sqrt5",  [](const auto&) { return epoch_script::transform::ConstantValue(2.236067977499790); }},
        {"ln2",    [](const auto&) { return epoch_script::transform::ConstantValue(0.693147180559945); }},
        {"ln10",   [](const auto&) { return epoch_script::transform::ConstantValue(2.302585092994046); }},
        {"log2e",  [](const auto&) { return epoch_script::transform::ConstantValue(1.442695040888963); }},
        {"log10e", [](const auto&) { return epoch_script::transform::ConstantValue(0.434294481903252); }},

        // Typed nulls
        {"null_number",    [](const auto&) { return epoch_script::transform::ConstantValue::MakeNull(epoch_core::IODataType::Decimal); }},
        {"null_string",    [](const auto&) { return epoch_script::transform::ConstantValue::MakeNull(epoch_core::IODataType::String); }},
        {"null_boolean",   [](const auto&) { return epoch_script::transform::ConstantValue::MakeNull(epoch_core::IODataType::Boolean); }},
        {"null_timestamp", [](const auto&) { return epoch_script::transform::ConstantValue::MakeNull(epoch_core::IODataType::Timestamp); }},
    };

    auto it = scalar_extractors.find(node.type);
    if (it != scalar_extractors.end()) {
        return it->second(node);
    }

    throw std::runtime_error("Unknown scalar type: " + node.type);
}

std::unordered_map<std::string, epoch_script::transform::ConstantValue>
ScalarInliningPass::BuildScalarValueMap(
    const std::vector<epoch_script::strategy::AlgorithmNode>& algorithms) {

    std::unordered_map<std::string, epoch_script::transform::ConstantValue> scalar_values;

    for (const auto& node : algorithms) {
        if (IsScalarNode(node)) {
            try {
                auto value = ExtractScalarValue(node);
                // Scalar transforms have single output named "result"
                std::string output_ref = node.id + "#result";
                scalar_values[output_ref] = value;

                SPDLOG_DEBUG("Mapped scalar: {} -> {}", output_ref, value.ToString());
            } catch (const std::exception& e) {
                SPDLOG_ERROR("Failed to extract value from scalar node {}: {}", node.id, e.what());
            }
        }
    }

    return scalar_values;
}

bool ScalarInliningPass::InlineScalarsInNode(
    epoch_script::strategy::AlgorithmNode& node,
    const std::unordered_map<std::string, epoch_script::transform::ConstantValue>& scalar_values) {

    bool any_inlined = false;

    // Process each input slot
    for (auto& [slot_id, input_refs] : node.inputs) {
        // Check each InputValue in this slot
        for (auto& input_value : input_refs) {
            // Skip if this is already a literal
            if (!input_value.IsNodeReference()) {
                continue;
            }

            // Check if this node reference points to a scalar
            std::string input_ref = input_value.GetNodeReference().GetRef();
            auto scalar_it = scalar_values.find(input_ref);

            if (scalar_it != scalar_values.end()) {
                // This input references a scalar - replace with literal!
                input_value = epoch_script::strategy::InputValue{scalar_it->second};

                SPDLOG_DEBUG("Inlined scalar {} in node {} slot {}",
                            input_ref, node.id, slot_id);

                any_inlined = true;
            }
        }
    }

    return any_inlined;
}

std::vector<epoch_script::strategy::AlgorithmNode> ScalarInliningPass::RemoveScalarNodes(
    const std::vector<epoch_script::strategy::AlgorithmNode>& algorithms,
    const std::unordered_set<std::string>& scalar_node_ids) {

    std::vector<epoch_script::strategy::AlgorithmNode> result;
    result.reserve(algorithms.size());

    for (const auto& node : algorithms) {
        if (scalar_node_ids.find(node.id) == scalar_node_ids.end()) {
            result.push_back(node);
        }
    }

    return result;
}

} // namespace epoch_script::compiler
