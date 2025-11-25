//
// Created by adesola on 9/26/24.
//

#pragma once
#include <epoch_core/common_utils.h>
#include <epoch_frame/datetime.h>
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/transforms/core/metadata.h>
#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/constant_value.h>
#include <yaml-cpp/yaml.h>

namespace epoch_script {
using epoch_script::strategy::InputMapping;

struct TransformDefinitionData {
  std::string type;
  std::string id{};
  epoch_script::MetaDataArgDefinitionMapping options{};
  std::optional<epoch_script::TimeFrame> timeframe;
  InputMapping inputs{};  // Unified: contains InputValue variants (node references OR literals)
  epoch_script::transforms::TransformsMetaData metaData{};
  std::optional<epoch_frame::SessionRange> sessionRange{};
};

struct TransformDefinition {
public:
  explicit TransformDefinition(TransformDefinitionData data)
      : m_data(std::move(data)) {
    // Auto-fill metadata from registry if not provided
    if (m_data.metaData.id.empty()) {
      auto metaDataPtr = epoch_script::transforms::ITransformRegistry::GetInstance().GetMetaData(m_data.type);
      AssertFromStream(metaDataPtr, "Invalid Transform: " << m_data.type);
      m_data.metaData = *metaDataPtr;
    }
  }
  explicit TransformDefinition(YAML::Node const &node);
  TransformDefinition(epoch_script::strategy::AlgorithmNode const &algorithm,
                      std::optional<epoch_script::TimeFrame> timeframe);

  TransformDefinition &
  SetOption(std::string const &key,
            epoch_script::MetaDataOptionDefinition const &value) {
    m_data.options.emplace(key, value.GetVariant());
    return *this;
  }

  TransformDefinition &SetPeriod(int64_t value) {
    return SetOption("period", epoch_script::MetaDataOptionDefinition{
                                   static_cast<double>(value)});
  }

  TransformDefinition &SetPeriods(int64_t value) {
    return SetOption("periods", epoch_script::MetaDataOptionDefinition{
                                    static_cast<double>(value)});
  }

  TransformDefinition &SetType(std::string const &value) {
    m_data.type = value;
    return *this;
  }

  TransformDefinition SetTypeCopy(std::string const &new_type) const noexcept {
    auto clone = *this;
    return clone.SetType(new_type);
  }

  TransformDefinition &SetTypeIfEmpty(std::string const &value) {
    m_data.type = m_data.type.empty() ? value : m_data.type;
    return *this;
  }

  TransformDefinition SetInput(InputMapping const &newInputs) const noexcept {
    auto clone = *this;
    clone.m_data.inputs = newInputs;
    return clone;
  }

  double GetOptionAsDouble(std::string const &key, double fallback) const {
    return epoch_core::lookupDefault(
               m_data.options, key,
               epoch_script::MetaDataOptionDefinition{fallback})
        .GetDecimal();
  }

  double GetOptionAsDouble(std::string const &key) const {
    return epoch_script::MetaDataOptionDefinition{
        epoch_core::lookup(m_data.options, key)}
        .GetDecimal();
  }

  std::string GetType() const { return m_data.type; }

  epoch_script::TimeFrame GetTimeframe() const {
    AssertFromStream(m_data.timeframe.has_value(), "Timeframe is not set");
    return m_data.timeframe.value();
  }

  std::string GetId() const { return m_data.id; }

  const InputMapping& GetInputs() const { return m_data.inputs; }

  const epoch_script::MetaDataArgDefinitionMapping& GetOptions() const {
    return m_data.options;
  }

  const epoch_script::transforms::TransformsMetaData& GetMetadata() const {
    return m_data.metaData;
  }

  std::optional<epoch_frame::SessionRange> GetSessionRange() const {
    return m_data.sessionRange;
  }

  TransformDefinitionData GetData() const { return m_data; }

private:
  TransformDefinitionData m_data;
};
} // namespace epoch_script

// namespace YAML {
// template <> struct convert<epoch_script::TransformDefinition> {
//   static bool decode(Node const &node,
//                      epoch_script::TransformDefinition &def) {
//     try {
//       def = epoch_script::TransformDefinition{node};
//       return true;
//     } catch (...) {
//       return false;
//     }
//   }
// };
// }