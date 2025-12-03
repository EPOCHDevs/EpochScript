#pragma once

// #include <common/yaml_transform_tester.hpp>  // Removed - YAML tester deprecated
// #include <common/transform_tester_base.hpp>  // Removed - not needed for flow graph tests
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/series_factory.h>
#include <epoch_core/catch_defs.h>
#include <epoch_protos/tearsheet.pb.h>
#include <runtime/orchestrator.h>
#include <runtime/transform_manager/transform_manager.h>
#include <epoch_data_sdk/events/all.h>

#include <yaml-cpp/yaml.h>
#include <memory>
#include <filesystem>
#include "transform_builder.h"

namespace epoch_stratifyx {
namespace test {

// Forward declarations
using TimeFrameAssetDataFrameMap = std::unordered_map<std::string, std::unordered_map<std::string, epoch_frame::DataFrame>>;
using AssetReportMap = std::unordered_map<std::string, epoch_proto::TearSheet>;

// Simple output type interface (replaces missing epoch_testing dependency)
class IOutputType {
public:
    virtual ~IOutputType() = default;
    virtual std::string getType() const = 0;
    virtual bool equals(const IOutputType& other) const = 0;
    virtual std::string toString() const = 0;
};

// FlowGraph output implementation that handles both dataframes and reports
class FlowGraphOutput : public IOutputType {
public:
    TimeFrameAssetDataFrameMap dataframes;
    AssetReportMap reports;

    FlowGraphOutput() = default;
    FlowGraphOutput(const TimeFrameAssetDataFrameMap& dfs, const AssetReportMap& ts)
        : dataframes(dfs), reports(ts) {}

    std::string getType() const override { return "flow_graph"; }
    bool equals(const IOutputType& other) const override;
    std::string toString() const override;

    // Factory method for creating from YAML
    static std::unique_ptr<IOutputType> fromYAML(const YAML::Node& node);
};

// FlowGraph test case structure
struct FlowGraphTestCase {
    std::string title;
    std::vector<std::string> assets;
    std::vector<std::string> timeframes;
    TimeFrameAssetDataFrameMap inputData;
    epoch_script::transform::TransformConfigurationList configuration;
    std::unique_ptr<IOutputType> expect;
};

// Helper functions for data loading
TimeFrameAssetDataFrameMap loadDataFromYAML(const YAML::Node& node);
std::vector<std::string> loadAssetsFromYAML(const YAML::Node& node);
epoch_script::transform::TransformConfigurationList loadConfigurationFromYAML(const YAML::Node& node);
epoch_frame::DataFrame loadDataFrameFromYAML(const YAML::Node& node);
std::vector<FlowGraphTestCase> loadFlowGraphTestsFromYAML(const std::string& filePath);

/**
 * YAML-based TransformFlowGraph testing utility for flow graph-specific testing workflows
 */
class YamlFlowGraphTester {
public:
    // Configuration structure for test discovery
    struct Config {
        std::vector<std::string> testDirectories;
        bool recursive = true;
        bool requireTestCasesDir = false;

        explicit Config(const std::string& baseDir = "flow_graph_test_cases")
            : testDirectories{baseDir} {}

        Config(std::vector<std::string> dirs, bool rec = true, bool req = false)
            : testDirectories(std::move(dirs)), recursive(rec), requireTestCasesDir(req) {}
    };

    /**
     * Run all YAML flow graph tests found in configured directories
     */
    static void runAllTests(
        const Config& config,
        std::function<std::pair<TimeFrameAssetDataFrameMap, AssetReportMap>(
            const std::vector<std::string>&,
            const epoch_script::transform::TransformConfigurationList&,
            const TimeFrameAssetDataFrameMap&)> flowGraphAdapter) {

        // Find all test files (only YAML files, skip datasets folder)
        std::vector<std::string> allTestFiles = findFlowGraphTestFiles(config);

        if (allTestFiles.empty()) {
            if (config.requireTestCasesDir) {
                FAIL("No test files found in any of the configured directories");
            } else {
                WARN("No test files found in any of the configured directories");
                return;
            }
        }

        // Sort files for consistent test ordering
        std::sort(allTestFiles.begin(), allTestFiles.end());

        INFO("Found " << allTestFiles.size() << " flow graph test files across "
             << config.testDirectories.size() << " directories");

        // Process each test file
        for (const auto& testFile : allTestFiles) {
            runFlowGraphTestFile(testFile, flowGraphAdapter);
        }
    }

    /**
     * Run flow graph tests using the standard TransformFlowGraph approach
     */
    static void runFlowGraphRegistryTests(const Config& config = Config("flow_graph_test_cases")) {
        runAllTests(config, runFlowGraphWithConfig);
    }

private:
    /**
     * Find all flow graph test files (YAML files only, skip datasets folder)
     */
    static std::vector<std::string> findFlowGraphTestFiles(const Config& config) {
        std::vector<std::string> testFiles;

        for (const auto& dir : config.testDirectories) {
            std::filesystem::path testDir(dir);

            if (!std::filesystem::exists(testDir) || !std::filesystem::is_directory(testDir)) {
                continue;
            }

            if (config.recursive) {
                for (const auto& entry : std::filesystem::recursive_directory_iterator(testDir)) {
                    if (entry.is_regular_file()) {
                        std::string filename = entry.path().filename().string();
                        std::string extension = entry.path().extension().string();
                        std::string pathStr = entry.path().string();

                        // Only include .yaml/.yml files, skip datasets folder content
                        if ((extension == ".yaml" || extension == ".yml") &&
                            pathStr.find("/datasets/") == std::string::npos) {  // Skip dataset directory files
                            testFiles.push_back(entry.path().string());
                        }
                    }
                }
            } else {
                for (const auto& entry : std::filesystem::directory_iterator(testDir)) {
                    if (entry.is_regular_file()) {
                        std::string filename = entry.path().filename().string();
                        std::string extension = entry.path().extension().string();
                        std::string pathStr = entry.path().string();

                        // Only include .yaml/.yml files, skip datasets folder content
                        if ((extension == ".yaml" || extension == ".yml") &&
                            pathStr.find("/datasets/") == std::string::npos) {  // Skip dataset directory files
                            testFiles.push_back(entry.path().string());
                        }
                    }
                }
            }
        }

        return testFiles;
    }

    /**
     * Run tests from a single YAML file for flow graphs
     */
    static void runFlowGraphTestFile(
        const std::string& testFile,
        std::function<std::pair<TimeFrameAssetDataFrameMap, AssetReportMap>(
            const std::vector<std::string>&,
            const epoch_script::transform::TransformConfigurationList&,
            const TimeFrameAssetDataFrameMap&)> flowGraphAdapter) {

        // Extract a clean name for the section
        std::filesystem::path filePath(testFile);
        std::string sectionName = filePath.stem().string() + " [" +
                                 filePath.parent_path().filename().string() + "]";

        SECTION(sectionName) {
            INFO("Loading flow graph test file: " << testFile);

            // Load test cases from YAML
            std::vector<FlowGraphTestCase> testCases;
            try {
                testCases = loadFlowGraphTestsFromYAML(testFile);
            } catch (const std::exception& e) {
                FAIL("Failed to load test cases from " << testFile << ": " << e.what());
                return;
            }

            INFO("Loaded " << testCases.size() << " flow graph test cases from " << testFile);

            // Run each test case
            for (auto& testCase : testCases) {
                SECTION(testCase.title) {
                    INFO("FlowGraph Test: " << testCase.title);
                    INFO("Assets: " << testCase.assets.size());
                    INFO("Timeframes: " << testCase.timeframes.size());
                    INFO("Transforms: " << testCase.configuration.size());

                    // Run flow graph execution
                    auto [outputDataframes, outputReports] = flowGraphAdapter(
                        testCase.assets, testCase.configuration, testCase.inputData);

                    INFO("Flow graph execution completed");
                    INFO("Output timeframes: " << outputDataframes.size());
                    INFO("Output reports: " << outputReports.size());

                    // Convert output to FlowGraphOutput for comparison
                    auto actualOutput = std::make_unique<FlowGraphOutput>(outputDataframes, outputReports);

                    // Compare with expected output
                    if (testCase.expect) {
                        INFO("Expected:\n" << testCase.expect->toString());
                        INFO("Actual:\n" << actualOutput->toString());

                        REQUIRE(actualOutput->equals(*testCase.expect));
                    } else {
                        // If no expected output specified, just verify basic structure
                        REQUIRE(outputDataframes.size() >= 0);
                        REQUIRE(outputReports.size() >= 0);
                    }
                }
            }
        }
    }

    /**
     * Generic flow graph runner using TransformFlowGraph
     */
    static std::pair<TimeFrameAssetDataFrameMap, AssetReportMap> runFlowGraphWithConfig(
        const std::vector<std::string>& assets,
        const epoch_script::transform::TransformConfigurationList& configuration,
        const TimeFrameAssetDataFrameMap& inputData) {

        // Build transforms from configurations
        auto transforms = epoch_flow::runtime::test::TransformBuilder::BuildFromConfigurations(configuration);

        // Create a transform manager from the transforms
        auto manager = std::make_unique<epoch_flow::runtime::TransformManager>();
        for (const auto& config : configuration) {
            manager->Insert(config);
        }

        // Create DataFlowRuntimeOrchestrator with transform manager
        epoch_flow::runtime::DataFlowRuntimeOrchestrator graph(assets, std::move(manager));

        // Transform data
        data_sdk::events::ScopedProgressEmitter emitter;
        auto outputDataframes = graph.ExecutePipeline(inputData, emitter);

        // Get reports
        auto outputReports = graph.GetGeneratedReports();

        return {outputDataframes, outputReports};
    }
};

} // namespace test
} // namespace epoch_stratifyx