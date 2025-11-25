#pragma once
//
// Created by dewe on 4/6/23.
//
#include "transform_definition.h"
#include "string"
#include <epoch_core/common_utils.h>
#include <epoch_frame/datetime.h>
#include <epoch_script/core/glaze_custom_types.h>
#include <epoch_script/core/id_sequence.h>

namespace epoch_script::transform {
class TransformConfiguration {

public:
  explicit TransformConfiguration(TransformDefinition def)
      : m_transformDefinition(std::move(def)) {
    for (auto const &output : m_transformDefinition.GetMetadata().outputs) {
      m_globalOutputMapping[output.id] =
          strategy::NodeReference{GetId(), output.id};
    }
  }

  std::string GetId() const { return m_transformDefinition.GetId(); }

  std::string GetTransformName() const {
    return m_transformDefinition.GetType();
  }

  epoch_script::TimeFrame GetTimeframe() const {
    return m_transformDefinition.GetTimeframe();
  }

  std::vector<epoch_script::transforms::IOMetaData> GetOutputs() const {
    return m_transformDefinition.GetMetadata().outputs;
  }

  const InputMapping& GetInputs() const { return m_transformDefinition.GetInputs(); }

  epoch_script::strategy::InputValue GetInput() const {
    AssertFromStream(m_transformDefinition.GetInputs().size() == 1,
                     "Expected only one input\n"
                         << ToString());
    const auto& input_values = m_transformDefinition.GetInputs().begin()->second;
    AssertFromStream(input_values.size() == 1,
                     "Expected only one input\n"
                         << ToString());
    return input_values.front();
  }

  epoch_script::strategy::InputValue GetInput(std::string const &parameter) const {
    auto inputs = epoch_core::lookup(GetInputs(), parameter);
    AssertFromStream(inputs.size() == 1, "Expected only one input\n"
                                             << ToString());
    return inputs.front();
  }

  std::vector<epoch_script::strategy::InputValue> GetInputs(std::string const &parameter) const {
    auto inputs = GetInputs();
    if (auto iter = inputs.find(parameter); iter != inputs.end()) {
      return iter->second;
    }
    return {};
  }

  epoch_script::MetaDataOptionDefinition
  GetOptionValue(const std::string &key) const {
    return epoch_script::MetaDataOptionDefinition{
        epoch_core::lookup(GetOptions(), key)};
  }

  epoch_script::MetaDataOptionDefinition GetOptionValue(
      const std::string &key,
      epoch_script::MetaDataOptionDefinition const &defaultValue) const {
    return epoch_script::MetaDataOptionDefinition{
        epoch_core::lookupDefault(GetOptions(), key, defaultValue)};
  }

  const epoch_script::MetaDataArgDefinitionMapping& GetOptions() const {
    return m_transformDefinition.GetOptions();
  }

  bool IsCrossSectional() const {
    return m_transformDefinition.GetMetadata().isCrossSectional;
  }

  strategy::NodeReference GetOutputId() const {
    AssertFromStream(m_globalOutputMapping.size() == 1,
                     "Expected exactly 1 output, but transform has "
                         << m_globalOutputMapping.size() << " outputs\n"
                         << ToString());
    return m_globalOutputMapping.cbegin()->second;
  }

  strategy::NodeReference GetOutputId(std::string const &transformOutputId) const {
    return epoch_core::lookup(m_globalOutputMapping, transformOutputId);
  }

  bool ContainsOutputId(std::string const &transformOutputId) const {
    return m_globalOutputMapping.contains(transformOutputId);
  }

  auto GetOutputIds() const {
    return m_globalOutputMapping | std::views::values;
  }

  const TransformDefinition& GetTransformDefinition() const {
    return m_transformDefinition;
  }

  std::optional<epoch_frame::SessionRange> GetSessionRange() const {
    return m_transformDefinition.GetSessionRange();
  }

  std::string ToString() const {
    return glz::prettify("TransformConfiguration",
                         m_transformDefinition.GetData());
  }

  ~TransformConfiguration() = default;

private:
  TransformDefinition m_transformDefinition;
  std::unordered_map<std::string, strategy::NodeReference> m_globalOutputMapping;
};

using TransformConfigurationPtrList =
    std::vector<std::unique_ptr<transform::TransformConfiguration>>;
using TransformConfigurationList =
    std::vector<transform::TransformConfiguration>;

} // namespace epoch_script::transform