#pragma once

#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_protos/tearsheet.pb.h>
#include <trompeloeil.hpp>
#include <memory>
#include <vector>
#include <format>
#include <optional>
#include <unordered_map>

namespace epoch_script::runtime::test {

// Forward declare - we'll use a wrapper approach instead of inheritance
// to avoid object slicing issues with return-by-value

/**
 * @brief Fully mockable transform for testing DataFlowRuntimeOrchestrator
 *
 * ALL methods are mockable via Trompeloeil REQUIRE_CALL/ALLOW_CALL.
 * Supports two modes:
 * 1. With real config (backward compatible) - uses CreateSimpleMockTransform()
 * 2. Fully mocked (no real config) - uses CreateFullyMockedTransform()
 *
 * Example usage (fully mocked):
 * @code
 * auto mock = CreateFullyMockedTransform("test_id", dailyTF);
 *
 * // Direct configuration behavior
 * REQUIRE_CALL(*mock, GetId())
 *     .RETURN("custom_id");
 *
 * // Direct execution behavior with verification
 * REQUIRE_CALL(*mock, TransformData(trompeloeil::_))
 *     .WITH(_1.column_names().size() > 0)
 *     .RETURN(expectedDataFrame);
 *
 * // Verify execution order
 * trompeloeil::sequence seq;
 * REQUIRE_CALL(*mockA, TransformData(trompeloeil::_))
 *     .IN_SEQUENCE(seq);
 * REQUIRE_CALL(*mockB, TransformData(trompeloeil::_))
 *     .IN_SEQUENCE(seq);
 * @endcode
 */
class MockTransform : public epoch_script::transform::ITransformBase {
public:
    explicit MockTransform() = default;
    ~MockTransform() override = default;

    // Mock stateless transform interface methods
    MAKE_CONST_MOCK1(TransformData, epoch_frame::DataFrame(const epoch_frame::DataFrame&), override);
    MAKE_CONST_MOCK1(GetDashboard, std::optional<epoch_tearsheet::DashboardBuilder>(const epoch_frame::DataFrame&), override);
    MAKE_CONST_MOCK1(GetEventMarkers, std::optional<epoch_script::transform::EventMarkerData>(const epoch_frame::DataFrame&), override);
    MAKE_CONST_MOCK1(TransformDataWithMetadata, epoch_script::runtime::TransformResult(const epoch_frame::DataFrame&), override);

    MAKE_MOCK1(SetProgressEmitter, void(runtime::events::TransformProgressEmitterPtr), override);
    MAKE_CONST_MOCK0(GetProgressEmitter, runtime::events::TransformProgressEmitterPtr(), override);

    // Note: GetConfiguration is stubbed below, not mocked, to avoid complexity

    // Use STUBS for simple data accessors - no Trompeloeil complexity, no lifetime issues
    [[nodiscard]] std::string GetId() const override { return m_id; }
    [[nodiscard]] std::string GetName() const override { return m_name; }
    [[nodiscard]] epoch_script::TimeFrame GetTimeframe() const override { return *m_timeframe; }
    [[nodiscard]] std::vector<std::string> GetInputIds() const override { return m_inputIds; }
    [[nodiscard]] std::vector<epoch_script::transforms::IOMetaData> GetOutputMetaData() const override {
        return m_outputMetadata;
    }

    // GetOutputId overloads
    [[nodiscard]] std::string GetOutputId() const override {
        return m_outputIds.empty() ? "" : m_id + "#" + m_outputIds[0];
    }
    [[nodiscard]] std::string GetOutputId(const std::string& outputId) const override {
        return m_id + "#" + outputId;
    }

    // GetInputId overloads
    [[nodiscard]] std::string GetInputId() const override {
        return m_inputIds.empty() ? "" : m_inputIds[0];
    }
    [[nodiscard]] std::string GetInputId(const std::string& inputId) const override {
        return inputId;
    }

    // Unused methods - throw if called
    [[nodiscard]] epoch_script::MetaDataOptionDefinition GetOption(std::string const&) const override {
        throw std::runtime_error("GetOption() not implemented in MockTransform stub");
    }
    [[nodiscard]] epoch_script::MetaDataOptionList GetOptionsMetaData() const override {
        return {};  // Return empty list
    }

    [[nodiscard]] std::vector<std::string> GetRequiredDataSources() const override {
        return m_requiredDataSources;
    }

    [[nodiscard]] const epoch_script::transform::TransformConfiguration& GetConfiguration() const override {
        // Lazy initialization of configuration using config_helper pattern (no YAML)
        if (!m_cachedConfig) {
            using namespace epoch_script::transform;
            using InputVal = epoch_script::strategy::InputValue;
            using NodeRef = epoch_script::strategy::NodeReference;

            // Default timeframe for mocks - use string constructor
            auto mockTimeframe = m_timeframe.value_or(epoch_script::TimeFrame("1D"));

            // Helper to parse "node#col" format into NodeReference
            auto parseInputRef = [](const std::string& ref) -> NodeRef {
                auto hashPos = ref.find('#');
                if (hashPos != std::string::npos) {
                    return NodeRef{ref.substr(0, hashPos), ref.substr(hashPos + 1)};
                }
                return NodeRef{"", ref};  // Just column name, no node
            };

            // Build input mapping from actual m_inputIds
            epoch_script::strategy::InputMapping inputMapping;
            for (size_t i = 0; i < m_inputIds.size(); ++i) {
                std::string slotName = "SLOT" + std::to_string(i);
                inputMapping[slotName] = {InputVal{parseInputRef(m_inputIds[i])}};
            }

            if (m_isSelector) {
                // Use card_selector_filter for selector transforms
                // Create event marker schema
                epoch_script::EventMarkerSchema schema;
                schema.title = "Test Selector";
                schema.select_key = "filter";
                schema.schemas.push_back(epoch_script::CardColumnSchema{
                    .column_id = "c",
                    .slot = epoch_core::CardSlot::Hero,
                    .render_type = epoch_core::CardRenderType::Decimal,
                    .color_map = {},
                    .label = ""
                });

                // Use actual inputs if provided, otherwise use SLOT
                if (inputMapping.empty()) {
                    inputMapping["SLOT"] = {};
                }

                TransformDefinitionData data{
                    .type = "card_selector_filter",
                    .id = m_id,
                    .options = {{"event_marker_schema",
                        epoch_script::MetaDataOptionDefinition{epoch_script::MetaDataOptionDefinition::T{schema}}}},
                    .timeframe = mockTimeframe,
                    .inputs = inputMapping
                };
                m_cachedConfig = std::make_unique<TransformConfiguration>(TransformDefinition{std::move(data)});

            } else if (m_isCrossSectional) {
                // Use top_k for cross-sectional (requires SLOT input)
                // Use actual inputs if provided, otherwise use SLOT
                if (inputMapping.empty()) {
                    inputMapping["SLOT"] = {};
                }

                TransformDefinitionData data{
                    .type = "top_k",
                    .id = m_id,
                    .options = {{"k", epoch_script::MetaDataOptionDefinition{5.0}}},
                    .timeframe = mockTimeframe,
                    .inputs = inputMapping
                };
                m_cachedConfig = std::make_unique<TransformConfiguration>(TransformDefinition{std::move(data)});

            } else {
                // Use gt for regular mocks (category: Math, not Scalar)
                // This ensures mocks go through normal execution path in execution_node.cpp
                // Mocks override TransformData anyway, so actual transform logic doesn't matter
                TransformDefinitionData data{
                    .type = "gt",
                    .id = m_id,
                    .timeframe = mockTimeframe,
                    .inputs = inputMapping  // Use actual inputs from m_inputIds
                };
                m_cachedConfig = std::make_unique<TransformConfiguration>(TransformDefinition{std::move(data)});
            }
        }
        return *m_cachedConfig;
    }

    // Storage for expectations - needs to be able to hold any expectation type
    // We use a type-erased container by storing pointers to the abstract base
    std::vector<std::shared_ptr<void>> m_expectations;

    // Stub data storage - populated by helper functions
    std::string m_id;
    std::string m_name = "MockTransform";
    std::optional<epoch_script::TimeFrame> m_timeframe;  // Optional since TimeFrame has no default ctor
    std::vector<std::string> m_inputIds;
    std::vector<std::string> m_outputIds;
    std::vector<epoch_script::transforms::IOMetaData> m_outputMetadata;
    std::vector<std::string> m_requiredDataSources;
    bool m_isCrossSectional = false;  // Controls execution path selection
    bool m_isSelector = false;  // Controls whether this is a selector transform
    mutable std::unique_ptr<epoch_script::transform::TransformConfiguration> m_cachedConfig;  // Lazy-initialized config
};

/**
 * @brief Helper to create a mock transform with pre-configured default behaviors
 *
 * This creates a mock that has default ALLOW_CALL expectations set up.
 * Tests can override these with REQUIRE_CALL for strict verification.
 *
 * Note: Default expectations are stored in the returned object, so they persist.
 *
 * @param id Transform ID
 * @param timeframe Timeframe for the transform
 * @param inputIds List of input handle IDs
 * @param outputIds List of output IDs
 * @param isCrossSectional Whether this transform should use cross-sectional execution
 * @param isSelector Whether this is a selector transform
 * @return Ready-to-use mock transform with default behaviors
 */
inline std::unique_ptr<MockTransform> CreateSimpleMockTransform(
    const std::string& id,
    const epoch_script::TimeFrame& timeframe,
    const std::vector<std::string>& inputIds = {},
    const std::vector<std::string>& outputIds = {"result"},
    bool isCrossSectional = false,
    bool isSelector = false) {

    auto mock = std::make_unique<MockTransform>();

    // Populate stub data members - simple and reliable
    mock->m_id = id;
    mock->m_name = "MockTransform";
    mock->m_timeframe = timeframe;
    mock->m_inputIds = inputIds;
    mock->m_outputIds = outputIds;
    mock->m_isCrossSectional = isCrossSectional;
    mock->m_isSelector = isSelector;

    // Populate output metadata
    for (const auto& outId : outputIds) {
        mock->m_outputMetadata.push_back({epoch_core::IODataType::Decimal, outId, outId, false, false});
    }

    // Set default expectation for GetEventMarkers - orchestrator always calls this
    // Store the expectation in the mock to keep it alive (type-erased with shared_ptr<void>)
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, GetEventMarkers(trompeloeil::_)).RETURN(std::nullopt))>(
            NAMED_ALLOW_CALL(*mock, GetEventMarkers(trompeloeil::_))
                .RETURN(std::nullopt)
        )
    );

    // Set default expectations for progress emitter - orchestrator calls SetProgressEmitter on all transforms
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, SetProgressEmitter(trompeloeil::_)))>(
            NAMED_ALLOW_CALL(*mock, SetProgressEmitter(trompeloeil::_))
        )
    );
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, GetProgressEmitter()).RETURN(nullptr))>(
            NAMED_ALLOW_CALL(*mock, GetProgressEmitter())
                .RETURN(nullptr)
        )
    );

    // Set default expectation for TransformData - returns empty DataFrame
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, TransformData(trompeloeil::_)).RETURN(epoch_frame::DataFrame{}))>(
            NAMED_ALLOW_CALL(*mock, TransformData(trompeloeil::_))
                .RETURN(epoch_frame::DataFrame{})
        )
    );

    // Set default expectation for TransformDataWithMetadata - returns empty result
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, TransformDataWithMetadata(trompeloeil::_)).RETURN(epoch_script::runtime::TransformResult{}))>(
            NAMED_ALLOW_CALL(*mock, TransformDataWithMetadata(trompeloeil::_))
                .RETURN(epoch_script::runtime::TransformResult{})
        )
    );

    return mock;
}

/**
 * @brief Helper to create a fully mocked transform for strict testing
 *
 * This creates a mock with minimal default expectations.
 * Tests should add REQUIRE_CALL on top to verify and direct behavior.
 *
 * @param id Transform ID
 * @param timeframe Timeframe for the transform
 * @param inputIds List of input handle IDs
 * @param outputIds List of output IDs
 * @param isCrossSectional Whether this transform should use cross-sectional execution
 * @param isSelector Whether this is a selector transform
 * @return Fully mockable transform ready for REQUIRE_CALL
 */
inline std::unique_ptr<MockTransform> CreateFullyMockedTransform(
    const std::string& id,
    const epoch_script::TimeFrame& timeframe,
    const std::vector<std::string>& inputIds = {},
    const std::vector<std::string>& outputIds = {"result"},
    bool isCrossSectional = false,
    bool isSelector = false) {

    auto mock = std::make_unique<MockTransform>();

    // Populate stub data members - simple and reliable
    mock->m_id = id;
    mock->m_name = "FullyMockedTransform";
    mock->m_timeframe = timeframe;
    mock->m_inputIds = inputIds;
    mock->m_outputIds = outputIds;
    mock->m_isCrossSectional = isCrossSectional;
    mock->m_isSelector = isSelector;

    // Populate output metadata
    for (const auto& outId : outputIds) {
        mock->m_outputMetadata.push_back({epoch_core::IODataType::Decimal, outId, outId, false, false});
    }

    // Set default expectation for GetEventMarkers - orchestrator always calls this
    // Store the expectation in the mock to keep it alive (type-erased with shared_ptr<void>)
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, GetEventMarkers(trompeloeil::_)).RETURN(std::nullopt))>(
            NAMED_ALLOW_CALL(*mock, GetEventMarkers(trompeloeil::_))
                .RETURN(std::nullopt)
        )
    );

    return mock;
}

/**
 * @brief Helper to create a reporter/sink mock transform for testing
 *
 * Reporter transforms have:
 * - category = Reporter
 * - outputs = {} (no outputs - they generate tearsheets instead)
 * - TransformData() is called but results are not distributed
 *
 * @param id Transform ID
 * @param timeframe Timeframe for the transform
 * @param inputIds List of input handle IDs
 * @param outputIds Should be empty {} for reporters
 * @return Reporter mock transform
 */
inline std::unique_ptr<MockTransform> CreateReporterMockTransform(
    const std::string& id,
    const epoch_script::TimeFrame& timeframe,
    const std::vector<std::string>& inputIds = {},
    const std::vector<std::string>& outputIds = {}) {

    auto mock = std::make_unique<MockTransform>();

    // Populate stub data members
    mock->m_id = id;
    mock->m_name = "ReporterMockTransform";
    mock->m_timeframe = timeframe;
    mock->m_inputIds = inputIds;
    mock->m_outputIds = outputIds;
    mock->m_isCrossSectional = true;  // Most reporters are cross-sectional
    mock->m_isSelector = false;

    // Reporters have NO output metadata (they generate tearsheets)
    mock->m_outputMetadata = {};

    // Create configuration with Reporter category using TransformDefinitionData (no YAML)
    using namespace epoch_script::transform;
    using InputVal = epoch_script::strategy::InputValue;
    using NodeRef = epoch_script::strategy::NodeReference;

    std::vector<InputVal> slotInputs;
    if (!inputIds.empty()) {
        // Parse the first inputId (format: "node#col" or just "col")
        auto& firstInput = inputIds[0];
        auto hashPos = firstInput.find('#');
        if (hashPos != std::string::npos) {
            slotInputs.push_back(InputVal{NodeRef{
                firstInput.substr(0, hashPos),
                firstInput.substr(hashPos + 1)
            }});
        } else {
            slotInputs.push_back(InputVal{NodeRef{"", firstInput}});
        }
    }

    TransformDefinitionData data{
        .type = "cs_numeric_cards_report",
        .id = id,
        .options = {{"title", epoch_script::MetaDataOptionDefinition{"Test Report"}}},
        .timeframe = timeframe,
        .inputs = {{"SLOT", slotInputs}}
    };
    mock->m_cachedConfig = std::make_unique<TransformConfiguration>(TransformDefinition{std::move(data)});

    // Set default expectation for GetEventMarkers - orchestrator always calls this
    // Store the expectation in the mock to keep it alive (type-erased with shared_ptr<void>)
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, GetEventMarkers(trompeloeil::_)).RETURN(std::nullopt))>(
            NAMED_ALLOW_CALL(*mock, GetEventMarkers(trompeloeil::_))
                .RETURN(std::nullopt)
        )
    );

    // Set default expectations for progress emitter - orchestrator calls SetProgressEmitter on all transforms
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, SetProgressEmitter(trompeloeil::_)))>(
            NAMED_ALLOW_CALL(*mock, SetProgressEmitter(trompeloeil::_))
        )
    );
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, GetProgressEmitter()).RETURN(nullptr))>(
            NAMED_ALLOW_CALL(*mock, GetProgressEmitter())
                .RETURN(nullptr)
        )
    );

    // Set default expectation for TransformData - returns empty DataFrame
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, TransformData(trompeloeil::_)).RETURN(epoch_frame::DataFrame{}))>(
            NAMED_ALLOW_CALL(*mock, TransformData(trompeloeil::_))
                .RETURN(epoch_frame::DataFrame{})
        )
    );

    // Set default expectation for TransformDataWithMetadata - returns empty result
    mock->m_expectations.push_back(
        std::make_shared<decltype(NAMED_ALLOW_CALL(*mock, TransformDataWithMetadata(trompeloeil::_)).RETURN(epoch_script::runtime::TransformResult{}))>(
            NAMED_ALLOW_CALL(*mock, TransformDataWithMetadata(trompeloeil::_))
                .RETURN(epoch_script::runtime::TransformResult{})
        )
    );

    return mock;
}

} // namespace epoch_script::runtime::test
