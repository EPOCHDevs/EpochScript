//
// Created by dewe on 9/10/24.
//

#pragma once
#include "epoch_script/core/metadata_options.h"
#include "epoch_script/core/time_frame.h"
#include "epoch_script/transforms/core/constant_value.h"
#include "enums.h"
#include "session_variant.h"
#include <glaze/json/generic.hpp>
#include <variant>
#include <optional>

// including here ensure all transforms have been serialized
namespace epoch_script::strategy
{
  // Forward declaration
  struct AlgorithmNode;

  // PythonSource - encapsulates EpochScript source code with pre-compiled metadata
  class PythonSource
  {
  private:
    std::string source_;
    std::vector<AlgorithmNode> compilationResult_;
    bool isIntraday_{false};
    size_t m_executor_count{};

  public:
    // Default constructor
    PythonSource() = default;

    // Constructor that compiles source and extracts metadata
    explicit PythonSource(std::string src, bool skip_sink_validation = false);

    // Const getters
    const std::string &GetSource() const { return source_; }
    const std::vector<AlgorithmNode> &GetCompilationResult() const { return compilationResult_; }
    bool IsIntraday() const { return isIntraday_; }

    // Equality operator for comparison
    bool operator==(const PythonSource &other) const
    {
      return source_ == other.source_;
    }

    size_t getExecutorCount() const { return m_executor_count; }

    // Glaze serialization support - friend for custom serialization
    friend struct glz::meta<PythonSource>;
  };

  struct AlgorithmBaseMetaData
  {
    std::string id;
    std::string name;
    MetaDataOptionList options{};
    std::string desc{};
    std::vector<std::string> tags{};
  };

  struct AlgorithmMetaData
  {
    std::string id;
    std::string name;
    MetaDataOptionList options{};
    std::string desc{};
    bool requiresTimeframe{true};
    std::vector<std::string> tags{};
  };

  class NodeReference {
  public:
    NodeReference() = default;
    NodeReference(std::string node_id, std::string handle) :
    node_id_(std::move(node_id)),
    handle_(std::move(handle)) {}

    // Explicit copy/move to ensure proper copying
    NodeReference(const NodeReference&) = default;
    NodeReference(NodeReference&&) = default;
    NodeReference& operator=(const NodeReference&) = default;
    NodeReference& operator=(NodeReference&&) = default;

    // Accessors for node_id and handle
    const std::string& GetNodeId() const { return node_id_; }
    const std::string& GetHandle() const { return handle_; }

    // Get combined "node_id#handle" string - ONLY for DataFrame column names!
    std::string GetColumnName() const;

    // Backward compatibility alias for GetColumnName()
    std::string GetRef() const;

    bool operator==(const NodeReference& other) const;

    friend std::ostream& operator<<(std::ostream& os, const NodeReference& node_ref);

  private:
    std::string node_id_;
    std::string handle_;

    // Glaze serialization as object
    friend struct glz::meta<NodeReference>;
  };

  // InputValue - unified representation for both node references and literal constants
  // Replaces the dual-field design (inputs + literal_inputs) with a single variant-based approach
  struct InputValue {
    explicit InputValue(): value(std::monostate{}) {}
    InputValue(NodeReference node_ref) : value(std::move(node_ref)) {}
    InputValue(epoch_script::transform::ConstantValue const_val) : value(std::move(const_val)) {}

    // Explicit copy/move to ensure variant is properly copied
    InputValue(const InputValue&) = default;
    InputValue(InputValue&&) = default;
    InputValue& operator=(const InputValue&) = default;
    InputValue& operator=(InputValue&&) = default;

    // Type checkers
    bool IsNodeReference() const { return std::holds_alternative<NodeReference>(value); }
    bool IsLiteral() const { return std::holds_alternative<epoch_script::transform::ConstantValue>(value); }

    // Accessors with type safety - return concrete types
    const NodeReference& GetNodeReference() const {
      return std::get<NodeReference>(value);
    }
    const epoch_script::transform::ConstantValue& GetLiteral() const {
      return std::get<epoch_script::transform::ConstantValue>(value);
    }
    epoch_script::transform::ConstantValue& GetLiteral() {
      return std::get<epoch_script::transform::ConstantValue>(value);
    }

    // Get the column identifier that this InputValue will produce in the DataFrame
    // - For node references: returns the reference string "node_id#handle"
    // - For literals: returns unique name generated from the constant's value (e.g., "num_42", "text_hello")
    // ONLY place where NodeReference is converted to string!
    std::string GetColumnIdentifier() const;

    // Equality operator
    bool operator==(const InputValue& other) const;

    friend std::ostream& operator<<(std::ostream& os, const InputValue& input_val);

    // Visitor pattern for serialization (keeps variant private)
    template<typename Visitor>
    auto visit(Visitor&& visitor) const {
      return std::visit(std::forward<Visitor>(visitor), value);
    }

    // Factory for deserialization
    static InputValue from_node_ref(std::string node_id, std::string handle) {
      return InputValue{NodeReference{std::move(node_id), std::move(handle)}};
    }
    static InputValue from_literal(epoch_script::transform::ConstantValue val) {
      return InputValue{std::move(val)};
    }
    static InputValue make_empty() {
      return InputValue{};
    }

    // Glaze serialization support
    friend struct glz::meta<InputValue>;

  private:
    std::variant<NodeReference, epoch_script::transform::ConstantValue, std::monostate> value;
  };

  // InputMapping now contains vectors of InputValue (can be node references OR literals)
  using InputMapping = std::unordered_map<std::string, std::vector<InputValue>>;

  struct AlgorithmNode
  {
    std::string type;
    std::string id{};
    epoch_script::MetaDataArgDefinitionMapping options{};
    InputMapping inputs{};  // Now contains InputValue variants (node references OR literals)
    std::optional<TimeFrame> timeframe{};
    std::optional<SessionVariant> session{};
    bool operator==(const AlgorithmNode &other) const
    {
      return type == other.type && id == other.id && options == other.options &&
             inputs == other.inputs &&
             timeframe == other.timeframe && (session == other.session);
    }
  };

  struct TradeSignalMetaData
  {
    std::string id;
    std::string name;
    MetaDataOptionList options{};
    std::string desc{};
    bool requiresTimeframe{true};
    PythonSource source;
    std::vector<std::string> tags{};
  };

  struct PartialTradeSignalMetaData
  {
    MetaDataOptionList options;
    std::vector<AlgorithmNode> algorithm;
    AlgorithmNode executor;
  };

  // Copy member variables to support glaze serialization form decomposition
} // namespace epoch_script::strategy

namespace YAML
{
  template <>
  struct convert<epoch_script::strategy::InputValue>
  {
    static Node encode(const epoch_script::strategy::InputValue &rhs);
    static bool decode(const Node &node, epoch_script::strategy::InputValue &rhs);
  };

  template <>
  struct convert<epoch_script::strategy::SessionVariant>
  {
    static bool decode(const Node &node,
                       epoch_script::strategy::SessionVariant &out);
  };

  template <>
  struct convert<epoch_script::strategy::AlgorithmNode>
  {
    static bool decode(YAML::Node const &,
                       epoch_script::strategy::AlgorithmNode &);
  };

  template <>
  struct convert<epoch_script::strategy::AlgorithmBaseMetaData>
  {
    static bool decode(YAML::Node const &,
                       epoch_script::strategy::AlgorithmBaseMetaData &);
  };

  template <>
  struct convert<epoch_script::strategy::AlgorithmMetaData>
  {
    static bool decode(YAML::Node const &,
                       epoch_script::strategy::AlgorithmMetaData &);
  };

  epoch_script::strategy::TradeSignalMetaData decode(glz::generic const &);

  glz::generic encode(epoch_script::strategy::TradeSignalMetaData const &);

} // namespace YAML

// Helper struct for NodeReference JSON serialization
namespace epoch_script::strategy::detail {
  struct NodeRefJson {
    std::string node_id;
    std::string handle;
  };
}

// Glaze serialization
namespace glz {
  template <>
  struct meta<epoch_script::strategy::detail::NodeRefJson>
  {
    using T = epoch_script::strategy::detail::NodeRefJson;
    static constexpr auto value = object(
      "node_id", &T::node_id,
      "handle", &T::handle
    );
  };

  // Serialize NodeReference as object {"node_id": "...", "handle": "..."}
  template <>
  struct to<JSON, epoch_script::strategy::NodeReference>
  {
    template <auto Opts>
    static void op(const epoch_script::strategy::NodeReference &x, auto &&...args) noexcept
    {
      epoch_script::strategy::detail::NodeRefJson obj{x.GetNodeId(), x.GetHandle()};
      serialize<JSON>::op<Opts>(obj, args...);
    }
  };

  template <>
  struct from<JSON, epoch_script::strategy::NodeReference>
  {
    template <auto Opts>
    static void op(epoch_script::strategy::NodeReference &value, auto &&...args)
    {
      epoch_script::strategy::detail::NodeRefJson data;
      parse<JSON>::op<Opts>(data, args...);
      value = epoch_script::strategy::NodeReference{std::move(data.node_id), std::move(data.handle)};
    }
  };

  // InputValue serialization using to/from for proper tagged variant handling
  // Format: {"type": "ref", "ref": {...}} or {"type": "literal", "literal": {...}}
  template <>
  struct to<JSON, epoch_script::strategy::InputValue>
  {
    template <auto Opts>
    static void op(const epoch_script::strategy::InputValue &x, auto &&...args) noexcept
    {
      x.visit([&](auto const& val) {
        using T = std::decay_t<decltype(val)>;
        if constexpr (std::is_same_v<T, epoch_script::strategy::NodeReference>) {
          auto obj = glz::obj{"type", "ref", "value", val};
          serialize<JSON>::op<Opts>(obj, args...);
        }
        else if constexpr (std::is_same_v<T, epoch_script::transform::ConstantValue>) {
          auto obj = glz::obj{"type", "literal", "value", val};
          serialize<JSON>::op<Opts>(obj, args...);
        }
        else {
          auto obj = glz::obj{"type", "null"};
          serialize<JSON>::op<Opts>(obj, args...);
        }
      });
    }
  };

  template <>
  struct from<JSON, epoch_script::strategy::InputValue>
  {
    template <auto Opts>
    static void op(epoch_script::strategy::InputValue &value, auto &&...args)
    {
      glz::generic json_obj;
      parse<JSON>::op<Opts>(json_obj, args...);

      auto type_it = json_obj.get_object().find("type");
      if (type_it == json_obj.get_object().end()) {
        value = epoch_script::strategy::InputValue::make_empty();
        return;
      }

      const auto& type_str = type_it->second.get_string();
      if (type_str == "ref") {
        auto ref_it = json_obj.get_object().find("value");
        if (ref_it != json_obj.get_object().end()) {
          auto json_str = glz::write_json(ref_it->second);
          if (json_str) {
            epoch_script::strategy::NodeReference node_ref;
            auto ec = glz::read_json(node_ref, json_str.value());
            if (!ec) {
              value = epoch_script::strategy::InputValue{std::move(node_ref)};
              return;
            }
          }
        }
      }
      else if (type_str == "literal") {
        auto lit_it = json_obj.get_object().find("value");
        if (lit_it != json_obj.get_object().end()) {
          auto json_str = glz::write_json(lit_it->second);
          if (json_str) {
            epoch_script::transform::ConstantValue const_val;
            auto ec = glz::read_json(const_val, json_str.value());
            if (!ec) {
              value = epoch_script::strategy::InputValue{std::move(const_val)};
              return;
            }
          }
        }
      }

      value = epoch_script::strategy::InputValue::make_empty();
    }
  };

  // Custom serialization for PythonSource - serialize/deserialize as string
  template <>
  struct to<JSON, epoch_script::strategy::PythonSource>
  {
    template <auto Opts>
    static void op(const epoch_script::strategy::PythonSource &x, auto &&...args) noexcept
    {
      serialize<JSON>::op<Opts>(x.GetSource(), args...);
    }
  };

  template <>
  struct from<JSON, epoch_script::strategy::PythonSource>
  {
    template <auto Opts>
    static void op(epoch_script::strategy::PythonSource &value, auto &&...args)
    {
      std::string source_code;
      parse<JSON>::op<Opts>(source_code, args...);
      value = epoch_script::strategy::PythonSource(source_code);
    }
  };
} // namespace glz