//
// Created by adesola on 8/7/24.
//

#include "transform_manager.h"
#include <epoch_script/transforms/core/transform_registry.h>

namespace epoch_script::runtime {
  ITransformManagerPtr CreateTransformManager() {
    return std::make_unique<TransformManager>();
  }

  ITransformManagerPtr CreateTransformManager(
      epoch_script::strategy::PythonSource const& source) {
    return std::make_unique<TransformManager>(source);
  }

  void TransformManager::BuildTransformManager(
      std::vector<epoch_script::strategy::AlgorithmNode> const& algorithms) {
    for (auto const& algorithm : algorithms) {
      // Check if this node is a scalar type (constants, literals)
      // Scalars are timeframe-agnostic and don't require timeframes
      auto metadata = epoch_script::transforms::ITransformRegistry::GetInstance().GetMetaData(algorithm.type);
      bool isScalar = metadata.has_value() &&
                      metadata->get().category == epoch_core::TransformCategory::Scalar;

      // Assert timeframe is present (compiler should have resolved it)
      // EXCEPT for scalar types which are timeframe-agnostic
      AssertFromFormat(
          algorithm.timeframe.has_value() || isScalar,
          "TransformManager received node '{}' (type: '{}') without timeframe. "
          "This indicates a compiler bug - all non-scalar nodes must have timeframes "
          "resolved during compilation (see ast_compiler.cpp::resolveTimeframes).",
          algorithm.id, algorithm.type);

      // For scalar types without timeframes, use a dummy timeframe (won't be used at runtime)
      auto timeframe = algorithm.timeframe.has_value()
          ? algorithm.timeframe.value()
          : epoch_script::TimeFrame("1d");

      this->Insert(
          epoch_script::TransformDefinition{algorithm, timeframe});

      if (algorithm.type == epoch_script::transforms::TRADE_SIGNAL_EXECUTOR_ID) {
        m_executorId = algorithm.id;
      }
    }
  }

  TransformManager::TransformManager(
      epoch_script::strategy::PythonSource const& source) {
    BuildTransformManager(source.GetCompilationResult());
  }

  const epoch_script::transform::TransformConfiguration *
  TransformManager::Insert(TransformConfigurationPtr info) {
    const auto name = info->GetId();

    if (m_configurationsById.contains(name)) {
      return m_configurationsById[name];
    }

    auto ptr =
        m_configurationsById
            .emplace(name, m_configurations.emplace_back(std::move(info)).get())
            .first->second;

    for (auto const &outputMetadata : ptr->GetOutputs()) {
      m_configurationsByOutput.emplace(ptr->GetOutputId(outputMetadata.id).GetColumnName(), ptr);
    }
    return ptr;
  }

  const epoch_script::transform::TransformConfiguration *
  TransformManager::Insert(const std::string &name,
                           TransformConfigurationPtr info) {
    AssertFromStream(!m_configurationsById.contains(name),
                     "Transform is already registered as " << name << ".");
    return Insert(std::move(info));
  }

  void TransformManager::Merge(const ITransformManager *transformManager) {
    if (transformManager) {
      for (auto const &transformInfo : *transformManager->GetTransforms()) {
        Insert(
            std::make_unique<epoch_script::transform::TransformConfiguration>(
                *transformInfo));
      }
    }
  }

  std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>>
  TransformManager::BuildTransforms() const {
    std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms;
    transforms.reserve(m_configurations.size());

    for (const auto& config : m_configurations) {
      transforms.push_back(MAKE_TRANSFORM(*config));
    }

    return transforms;
  }
} // namespace epoch_script::runtime