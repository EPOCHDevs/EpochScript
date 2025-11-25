//
// Created by dewe on 9/10/24.
//
#include <epoch_script/strategy/metadata.h>
#include "../core/doc_deserialization_helper.h"
#include <epoch_script/core/metadata_options.h>
#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/metadata.h>
#include <epoch_script/transforms/core/registration.h>
#include <epoch_script/strategy/registration.h>
#include "transforms/compiler/ast_compiler.h"
#include "transforms/compiler/scalar_inlining_pass.h"
#include <epoch_core/macros.h>
#include <glaze/core/reflect.hpp>
#include <glaze/json/json_concepts.hpp>
#include <glaze/json/generic.hpp>
#include <glaze/json/write.hpp>
#include <string>
#include <unordered_set>
#include <ranges>
#include <algorithm>

using namespace epoch_script;
using namespace epoch_script::strategy;
using epoch_core::EpochOffsetType;

// Helper function to determine base timeframe from compilation result
static bool IsIntraday_(const std::vector<AlgorithmNode> &compilationResult) {
  std::unordered_set<EpochOffsetType> types;

  for (const auto &node : compilationResult)
  {
    // Check if node type requires intraday data or has a session
    if (epoch_script::transforms::kIntradayOnlyIds.contains(node.type) ||
        node.session)
    {
      types.emplace(EpochOffsetType::Minute);
      continue;
    }

    // Check node's timeframe
    if (!node.timeframe)
      continue;

    types.emplace(node.timeframe->GetOffset()->type());
  }

  if (types.empty())
  {
    return false;
  }

  return std::ranges::any_of(types, [](EpochOffsetType t)
                             { return epoch_script::IsIntraday(t); });
}

// PythonSource constructor implementation
PythonSource::PythonSource(std::string src, bool skip_sink_validation) : source_(std::move(src)) {
  if (source_.empty())
  {
    return;
  }

  // Compile Python source to get algorithm nodes
  epoch_script::AlgorithmAstCompiler compiler;
  compilationResult_ = compiler.compile(source_, skip_sink_validation);
  m_executor_count = compiler.getExecutorCount();

  // Run scalar inlining optimization pass
  // This eliminates scalar transform nodes by converting them to literal_inputs
  compilationResult_ = epoch_script::compiler::ScalarInliningPass::Run(compilationResult_);

  isIntraday_ = IsIntraday_(compilationResult_);
}

namespace epoch_script::strategy {

// NodeReference implementations
std::string NodeReference::GetColumnName() const {
  return node_id_ + "#" + handle_;
}

std::string NodeReference::GetRef() const {
  return GetColumnName();
}

bool NodeReference::operator==(const NodeReference& other) const {
  return node_id_ == other.node_id_ && handle_ == other.handle_;
}

std::ostream& operator<<(std::ostream& os, const NodeReference& node_ref) {
  return os << node_ref.GetNodeId() << "." << node_ref.GetHandle();
}

// InputValue implementations
std::string InputValue::GetColumnIdentifier() const {
  if (IsNodeReference()) {
    return GetNodeReference().GetColumnName();
  }
  if (IsLiteral()) {
    return GetLiteral().GetColumnName();
  }
  throw std::runtime_error("Cannot get column identifier from null InputValue");
}

bool InputValue::operator==(const InputValue& other) const {
  return value == other.value;
}

std::ostream& operator<<(std::ostream& os, const InputValue& input_val) {
  input_val.visit([&](auto const& val) {
    using T = std::decay_t<decltype(val)>;
    if constexpr (std::is_same_v<T, std::monostate>) {
       os << "null";
    }
    else if constexpr (std::is_same_v<T, NodeReference>) {
       os << val.GetNodeId() << "." << val.GetHandle();
    }
    else {
       os << val.ToString();
    }
  });
  return os;
}

} // namespace epoch_script::strategy

namespace YAML
{
  // InputValue YAML serialization - matches JSON format with type/value structure
  Node convert<epoch_script::strategy::InputValue>::encode(const epoch_script::strategy::InputValue &rhs)
  {
    Node result;
    if (rhs.IsNodeReference()) {
      // Serialize as: {type: ref, value: {node_id: ..., handle: ...}}
      result["type"] = "ref";
      Node value_node;
      value_node["node_id"] = rhs.GetNodeReference().GetNodeId();
      value_node["handle"] = rhs.GetNodeReference().GetHandle();
      result["value"] = value_node;
    } else if (rhs.IsLiteral()) {
      // Serialize as: {type: literal, value: {...}}
      result["type"] = "literal";
      result["value"] = rhs.GetLiteral();
    } else {
      // Serialize as: {type: null}
      result["type"] = "null";
    }
    return result;
  }

  bool convert<epoch_script::strategy::InputValue>::decode(const Node &node, epoch_script::strategy::InputValue &rhs)
  {
    if (!node["type"]) {
      throw std::runtime_error("InputValue must have 'type' field (ref, literal, or null)");
    }

    std::string type = node["type"].as<std::string>();

    if (type == "ref") {
      // Parse node reference from value field - always expects a map with node_id and handle
      if (!node["value"]) {
        throw std::runtime_error("InputValue with type 'ref' must have 'value' field");
      }
      auto value_node = node["value"];
      auto node_id = value_node["node_id"].as<std::string>("");
      auto handle = value_node["handle"].as<std::string>();
      rhs = epoch_script::strategy::InputValue{
        epoch_script::strategy::NodeReference{node_id, handle}
      };
      return true;
    } else if (type == "literal") {
      // Parse constant value from value field
      if (!node["value"]) {
        throw std::runtime_error("InputValue with type 'literal' must have 'value' field");
      }
      rhs = epoch_script::strategy::InputValue{node["value"].as<epoch_script::transform::ConstantValue>()};
      return true;
    } else if (type == "null") {
      // Empty/null value
      rhs = epoch_script::strategy::InputValue{};
      return true;
    } else {
      throw std::runtime_error("Invalid InputValue type: " + type + " (expected ref, literal, or null)");
    }
  }

  bool convert<SessionVariant>::decode(YAML::Node const &node,
                                       SessionVariant &metadata)
  {
    if (node.IsScalar())
    {
      auto session = node.as<std::string>();
      metadata = epoch_core::SessionTypeWrapper::FromString(session);
    }
    else if (node["start"] && node["end"])
    {
      auto start = node["start"].as<std::string>();
      auto end = node["end"].as<std::string>();
      metadata = epoch_frame::SessionRange{epoch_script::TimeFromString(start),
                                           epoch_script::TimeFromString(end)};
    }
    else
    {
      throw std::runtime_error("Invalid session variant, must be a scalar or a "
                               "map with start and end keys, not " +
                               YAML::Dump(node));
    }
    return true;
  }

  bool convert<AlgorithmNode>::decode(YAML::Node const &node,
                                      AlgorithmNode &metadata)
  {

    metadata.type = node["type"].as<std::string>();
    metadata.id = node["id"].as<std::string>(metadata.type);

    auto expectedTransform =
        transforms::ITransformRegistry::GetInstance().GetMetaData(metadata.type);
    if (!expectedTransform)
    {
      throw std::runtime_error("Unknown transform type: " + metadata.type);
    }

    const auto &transform = expectedTransform->get();
    auto options = node["options"];
    if (!options && transform.options.size() > 0)
    {
      throw std::runtime_error(
          fmt::format("Missing options for transform {}", metadata.type));
    }

    for (auto const &option : transform.options)
    {
      auto arg = options[option.id];
      if (option.isRequired && !arg)
      {
        throw std::runtime_error("Missing required option: " + option.id +
                                 " for transform " + metadata.type);
      }

      auto serialized = YAML::Dump(arg);
      if (serialized.starts_with("."))
      {
        metadata.options.emplace(
            option.id,
            MetaDataOptionDefinition{MetaDataArgRef{serialized.substr(1)}});
      }
      else
      {
        metadata.options.emplace(option.id,
                                 CreateMetaDataArgDefinition(arg, option));
      }
      options.remove(option.id);
    }

    if (options && options.size() != 0)
    {
      throw std::runtime_error("Unknown options: " + Dump(options));
    }

    // Decode inputs - now unified to handle both node references and literal constants
    auto nodeInputs = node["inputs"];
    for (auto const &input : transform.inputs)
    {
      auto inputs = nodeInputs[input.id];
      if (!inputs)
      {
        continue;
      }

      if (input.allowMultipleConnections)
      {
        AssertFromFormat(inputs.IsSequence(), "Input {} is not a sequence while allowMultipleConnections is set",
                         input.id);
        metadata.inputs[input.id] = inputs.as<std::vector<epoch_script::strategy::InputValue>>();
      }
      else
      {
        AssertFromFormat(!inputs.IsSequence(), "Input {} is a sequence while allowMultipleConnections is false",
                         input.id);
        // Single input - wrap in vector
        auto single_input = inputs.as<epoch_script::strategy::InputValue>();
        metadata.inputs[input.id] = std::vector{single_input};
      }
    }

    // If this transform requires a timeframe/session, set a default session
    // to signal session context to clients. Use SessionType by default so
    // clients can override with custom ranges in the UI if desired.
    if (auto sessionNode = node["session"])
    {
      auto session = sessionNode.as<SessionVariant>();
      AssertFromStream(transform.requiresTimeFrame,
                       "requiresTimeFrame is required for session");
      metadata.session = session;
    }

    return true;
  }

  bool convert<AlgorithmBaseMetaData>::decode(YAML::Node const &node,
                                              AlgorithmBaseMetaData &metadata)
  {
    metadata.id = node["id"].as<std::string>();
    metadata.name = node["name"].as<std::string>("");
    metadata.options =
        node["options"].as<MetaDataOptionList>(MetaDataOptionList{});
    metadata.desc = MakeDescLink(node["desc"].as<std::string>(""));
    metadata.tags =
        node["tags"].as<std::vector<std::string>>(std::vector<std::string>{});
    return true;
  }

  bool convert<AlgorithmMetaData>::decode(YAML::Node const &node,
                                          AlgorithmMetaData &metadata)
  {
    metadata.id = node["id"].as<std::string>();
    metadata.name = node["name"].as<std::string>("");
    metadata.options =
        node["options"].as<MetaDataOptionList>(MetaDataOptionList{});
    metadata.desc = MakeDescLink(node["desc"].as<std::string>(""));
    metadata.requiresTimeframe = node["requiresTimeframe"].as<bool>(true);
    metadata.tags =
        node["tags"].as<std::vector<std::string>>(std::vector<std::string>{});
    return true;
  }
} // namespace YAML