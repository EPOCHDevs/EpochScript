#pragma once

#include <epoch_script/transforms/runtime/transform_manager/itransform_manager.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <memory>
#include <vector>

namespace epoch_script::runtime::test {

/**
 * @brief Simple mock transform manager for testing
 *
 * This is a basic implementation of ITransformManager that holds a vector
 * of mock transforms. It doesn't need Trompeloeil since we're just wrapping
 * existing mocks.
 *
 * Example usage:
 * @code
 * auto manager = std::make_unique<MockTransformManager>();
 * auto mock1 = CreateSimpleMockTransform("transform1", dailyTF);
 * auto mock2 = CreateSimpleMockTransform("transform2", dailyTF);
 * manager->AddTransform(std::move(mock1));
 * manager->AddTransform(std::move(mock2));
 *
 * auto orchestrator = DataFlowRuntimeOrchestrator(assets, std::move(manager));
 * @endcode
 */
class MockTransformManager : public epoch_script::runtime::ITransformManager {
public:
    MockTransformManager() = default;

    /**
     * @brief Add a transform to the manager
     * @param transform Transform instance to add
     */
    void AddTransform(std::unique_ptr<epoch_script::transform::ITransformBase> transform) {
        m_transforms.push_back(std::move(transform));
    }

    // ITransformManager interface implementation
    [[nodiscard]] const epoch_script::transform::TransformConfiguration*
    GetExecutor() const override {
        throw std::runtime_error("GetExecutor() not used in tests - orchestrator uses interface methods");
    }

    [[nodiscard]] const std::vector<epoch_script::runtime::TransformConfigurationPtr>*
    GetTransforms() const override {
        throw std::runtime_error("GetTransforms() not used in tests - orchestrator uses interface methods");
    }

    [[nodiscard]] const epoch_script::transform::TransformConfiguration*
    GetTransformConfigurationById(const std::string&) const override {
        throw std::runtime_error("GetTransformConfigurationById() not used in tests - orchestrator uses interface methods");
    }

    [[nodiscard]] std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>>
    BuildTransforms() const override {
        return std::move(m_transforms);
    }

    const epoch_script::transform::TransformConfiguration *
            Insert(const epoch_script::transform::TransformConfiguration& ) override {
        throw std::runtime_error("Insert() not used in tests - orchestrator uses interface methods");
    }

private:
    mutable std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> m_transforms;
};

/**
 * @brief Helper to create a MockTransformManager from a vector of transforms
 *
 * @param transforms Vector of transform unique_ptrs to add to the manager
 * @return Ready-to-use mock transform manager
 */
inline std::unique_ptr<MockTransformManager> CreateMockTransformManager(
    std::vector<std::unique_ptr<epoch_script::transform::ITransformBase>> transforms) {
    auto manager = std::make_unique<MockTransformManager>();
    for (auto& transform : transforms) {
        manager->AddTransform(std::move(transform));
    }
    return manager;
}

} // namespace epoch_script::runtime::test
