#include "flow_source_tester.h"

#include <catch2/catch_test_macros.hpp>
#include <fstream>
#include <glaze/glaze.hpp>
#include <spdlog/spdlog.h>

#include "event_marker_comparator.h"
#include "tearsheet_comparator.h"
#include "runtime/orchestrator.h"
#include <runtime/transform_manager/transform_manager.h>
#include <epoch_data_sdk/events/all.h>

namespace epoch_script::runtime::test {

void FlowSourceTestRunAllTests(const FlowSourceTestRunner::Config& config) {
    auto testCases = FlowSourceTestRunner::DiscoverTestCases(config);

    SPDLOG_INFO("Discovered {} test case(s)", testCases.size());

    for (const auto& testCase : testCases) {
        // Create dynamic test section for each test case
        DYNAMIC_SECTION(testCase.name) {
            FlowSourceTestRunner::RunTestCase(testCase, config.updateMode);
        }
    }
}

std::vector<FlowSourceTestRunner::TestCase>
FlowSourceTestRunner::DiscoverTestCases(const Config& config) {
    std::vector<TestCase> testCases;

    for (const auto& testDir : config.testDirectories) {
        std::filesystem::path basePath(testDir);

        if (!std::filesystem::exists(basePath)) {
            SPDLOG_WARN("Test directory does not exist: {}", testDir);
            continue;
        }

        // Recursively find directories containing source.py
        for (const auto& entry : std::filesystem::recursive_directory_iterator(basePath)) {
            if (!entry.is_directory()) continue;

            std::filesystem::path sourceFile = entry.path() / "source.py";
            if (!std::filesystem::exists(sourceFile)) continue;

            // Found a test case
            TestCase testCase;
            testCase.name = std::filesystem::relative(entry.path(), basePath).string();
            testCase.directory = entry.path();
            testCase.sourceFile = sourceFile;
            testCase.inputDir = entry.path() / "input";
            testCase.expectedDir = entry.path() / "expected";

            // Load config if exists
            std::filesystem::path configPath = entry.path() / "config.yaml";
            if (std::filesystem::exists(configPath)) {
                testCase.config = LoadConfig(configPath);
            }

            testCases.push_back(testCase);
        }
    }

    // Sort for consistent ordering
    std::sort(testCases.begin(), testCases.end(),
             [](const TestCase& a, const TestCase& b) { return a.name < b.name; });

    return testCases;
}

void FlowSourceTestRunner::RunTestCase(const TestCase& testCase, bool updateMode) {
    SPDLOG_INFO("Running test case: {}", testCase.name);

    // Skip if pending review and not in update mode
    if (testCase.config.status == "PENDING_REVIEW" && !updateMode) {
        SKIP("Test pending review - run with --update to regenerate");
    }

    // 1. Load and compile source
    auto source = LoadSource(testCase.sourceFile);

    // 2. Load input data
    auto inputData = CsvDataLoader::LoadFromDirectory(testCase.inputDir);
    REQUIRE(!inputData.empty());  // Must have input data

    // 3. Detect required assets
    std::vector<std::string> assets;
    if (!testCase.config.assets.empty()) {
        // Use configured assets
        assets = testCase.config.assets;
    } else {
        // Auto-detect
        assets = DetectRequiredAssets(source, testCase.inputDir);
    }
    REQUIRE(!assets.empty());

    // 4. Determine base timeframe
    std::optional<epoch_script::TimeFrame> baseTimeframe;
    if (!testCase.config.timeframes.empty()) {
        baseTimeframe = epoch_script::TimeFrame{testCase.config.timeframes[0]};
    } else {
        // Use first timeframe from input data
        if (!inputData.empty()) {
            baseTimeframe = epoch_script::TimeFrame{inputData.begin()->first};
        }
    }

    // 5. Execute test
    auto outputs = ExecuteTest(source, inputData, assets, baseTimeframe);

    // 6. Validate or generate expected outputs
    if (updateMode) {
        GenerateExpectedOutputs(testCase, outputs);
        // Update config status
        auto config = testCase.config;
        config.status = "PENDING_REVIEW";
        SaveConfig(testCase.directory / "config.yaml", config);
        SKIP("Generated expected outputs - review and set status to APPROVED");
    } else {
        ValidateOutputs(testCase, outputs);
    }
}

epoch_script::strategy::PythonSource
FlowSourceTestRunner::LoadSource(const std::filesystem::path& sourceFile) {
    std::ifstream file(sourceFile);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open source file: " + sourceFile.string());
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string sourceCode = buffer.str();

    // Compile source
    return epoch_script::strategy::PythonSource(sourceCode);
}

std::vector<std::string> FlowSourceTestRunner::DetectRequiredAssets(
    const epoch_script::strategy::PythonSource& source,
    const std::filesystem::path& inputDir) {

    // Load all assets from input directory
    auto inputData = CsvDataLoader::LoadFromDirectory(inputDir);

    // Extract unique assets
    std::unordered_set<std::string, std::hash<std::string>> assetSet;
    for (const auto& [timeframe, assetMap] : inputData) {
        for (const auto& [asset, df] : assetMap) {
            assetSet.insert(asset);
        }
    }

    std::vector<std::string> assets(assetSet.begin(), assetSet.end());

    // If cross-sectional, use all assets
    if (IsCrossSectional(source)) {
        SPDLOG_INFO("Detected cross-sectional transforms - using all {} assets", assets.size());
        return assets;
    }

    // Otherwise, use first asset (or all if only one)
    if (!assets.empty()) {
        SPDLOG_INFO("Using single asset: {}", assets[0]);
        return {assets[0]};
    }

    throw std::runtime_error("No assets found in input directory");
}

// Normalize column types for comparison (convert int64 to double for numeric columns)
static epoch_frame::DataFrame NormalizeTypesForComparison(const epoch_frame::DataFrame& df) {
    if (df.empty()) return df;

    auto table = df.table();
    auto schema = table->schema();

    std::vector<std::shared_ptr<arrow::Field>> new_fields;
    std::vector<std::shared_ptr<arrow::ChunkedArray>> new_columns;
    bool needs_cast = false;

    for (int i = 0; i < schema->num_fields(); i++) {
        auto field = schema->field(i);
        auto column = table->column(i);

        // Convert int64 columns to double for consistent comparison
        if (field->type()->id() == arrow::Type::INT64) {
            auto cast_result = arrow::compute::Cast(column, arrow::float64());
            if (!cast_result.ok()) {
                throw std::runtime_error("Failed to cast column to double: " + cast_result.status().ToString());
            }
            new_columns.push_back(cast_result.ValueOrDie().chunked_array());
            new_fields.push_back(arrow::field(field->name(), arrow::float64()));
            needs_cast = true;
        } else {
            new_columns.push_back(column);
            new_fields.push_back(field);
        }
    }

    if (!needs_cast) {
        return df;
    }

    auto new_schema = arrow::schema(new_fields);
    auto new_table = arrow::Table::Make(new_schema, new_columns);
    return epoch_frame::DataFrame(new_table);
}

bool FlowSourceTestRunner::IsCrossSectional(
    const epoch_script::strategy::PythonSource& source) {

    auto algorithms = source.GetCompilationResult();

    for (const auto& node : algorithms) {
        // Check for cross-sectional transform types
        if (node.type.starts_with("cs_")) {
            return true;
        }
        if (node.type == "top_k" || node.type == "bottom_k") {
            return true;
        }
        if (node.type.starts_with("portfolio_")) {
            return true;
        }

        // Could also check transform metadata
        // auto metadata = TransformRegistry::instance().GetMetadata(node.type);
        // if (metadata.isCrossSectional) return true;
    }

    return false;
}

FlowSourceTestRunner::TestOutputs FlowSourceTestRunner::ExecuteTest(
    const epoch_script::strategy::PythonSource& source,
    const TimeFrameAssetDataFrameMap& inputData,
    const std::vector<std::string>& assets,
    const std::optional<epoch_script::TimeFrame>& baseTimeframe) {

    // Build TransformManager from compiled source
    epoch_flow::runtime::TransformManagerOptions options{
        .source = source,
        .strict = false,
        .timeframeIsBase = true,
        .timeframe = baseTimeframe
    };

    auto transformManager = std::make_unique<epoch_flow::runtime::TransformManager>(options);

    // Create DataFlowRuntimeOrchestrator
    auto orchestrator = std::make_unique<epoch_flow::runtime::DataFlowRuntimeOrchestrator>(
        assets,
        std::move(transformManager)
    );

    // Execute pipeline and collect outputs
    data_sdk::events::ScopedProgressEmitter emitter;
    TestOutputs outputs;
    outputs.dataframes = orchestrator->ExecutePipeline(inputData, emitter);
    outputs.tearsheets = orchestrator->GetGeneratedReports();
    outputs.selectors = orchestrator->GetGeneratedEventMarkers();

    return outputs;
}

void FlowSourceTestRunner::ValidateOutputs(
    const TestCase& testCase,
    const TestOutputs& outputs) {

    // Validate DataFrames
    for (const auto& [timeframe, assetMap] : outputs.dataframes) {
        for (const auto& [asset, actualDf] : assetMap) {
            // Construct expected file path
            std::string filename = timeframe + "_" + asset + ".csv";
            std::filesystem::path expectedPath = testCase.expectedDir / "dataframe" / filename;

            if (!std::filesystem::exists(expectedPath)) {
                FAIL("Expected DataFrame file not found: " << expectedPath);
            }

            auto expectedDf = CsvDataLoader::LoadCsvFile(expectedPath);

            // Normalize by round-tripping actual through CSV to match expected behavior
            auto tempPath = std::filesystem::temp_directory_path() / "actual_normalized.csv";
            CsvDataLoader::WriteCsvFile(actualDf, tempPath, true);
            auto normalizedActualDf = CsvDataLoader::LoadCsvFile(tempPath);
            std::filesystem::remove(tempPath);

            INFO("Comparing DataFrame for " << timeframe << " " << asset);
            INFO("Expected shape: " << expectedDf.num_rows() << " x " << expectedDf.num_cols());
            INFO("Actual shape: " << normalizedActualDf.num_rows() << " x " << normalizedActualDf.num_cols());

            // Compare tables without metadata checking (CSV loses type info)
            auto expected_table = expectedDf.table();
            auto actual_table = normalizedActualDf.table();
            REQUIRE(actual_table->Equals(*expected_table, /*check_metadata=*/false));
        }
    }

    // Validate TearSheets
    for (const auto& [asset, actualTearsheet] : outputs.tearsheets) {
        std::string filename = asset + ".json";
        std::filesystem::path expectedPath = testCase.expectedDir / "tearsheet" / filename;

        if (!std::filesystem::exists(expectedPath)) {
            FAIL("Expected TearSheet file not found: " << expectedPath);
        }

        std::string expectedJson = TearSheetComparator::LoadJson(expectedPath);
        std::string actualJson = TearSheetComparator::ToJson(actualTearsheet);

        std::string diff;
        if (!TearSheetComparator::Compare(expectedJson, actualJson, diff)) {
            FAIL("TearSheet mismatch for " << asset << ":\n" << diff);
        }
    }

    // Validate Selectors
    for (const auto& [asset, actualSelectors] : outputs.selectors) {
        std::string filename = asset + ".json";
        std::filesystem::path expectedPath = testCase.expectedDir / "selector" / filename;

        if (!std::filesystem::exists(expectedPath)) {
            // Selectors are optional - skip if not present
            continue;
        }

        std::string expectedJson = SelectorComparator::LoadJson(expectedPath);
        std::string actualJson = SelectorComparator::ToJson(actualSelectors);

        std::string diff;
        if (!SelectorComparator::Compare(expectedJson, actualJson, diff)) {
            FAIL("Selector mismatch for " << asset << ":\n" << diff);
        }
    }

    SPDLOG_INFO("Test case passed: {}", testCase.name);
}

void FlowSourceTestRunner::GenerateExpectedOutputs(
    const TestCase& testCase,
    const TestOutputs& outputs) {

    SPDLOG_INFO("Generating expected outputs for: {}", testCase.name);

    // Create expected directory structure
    std::filesystem::create_directories(testCase.expectedDir / "dataframe");
    std::filesystem::create_directories(testCase.expectedDir / "tearsheet");
    std::filesystem::create_directories(testCase.expectedDir / "selector");

    // Write DataFrames
    for (const auto& [timeframe, assetMap] : outputs.dataframes) {
        for (const auto& [asset, df] : assetMap) {
            std::string filename = timeframe + "_" + asset + ".csv";
            std::filesystem::path outputPath = testCase.expectedDir / "dataframe" / filename;
            CsvDataLoader::WriteCsvFile(df, outputPath, true);
        }
    }

    // Write TearSheets
    for (const auto& [asset, tearsheet] : outputs.tearsheets) {
        std::string filename = asset + ".json";
        std::filesystem::path outputPath = testCase.expectedDir / "tearsheet" / filename;
        TearSheetComparator::SaveJson(tearsheet, outputPath);
    }

    // Write Selectors
    for (const auto& [asset, selectors] : outputs.selectors) {
        if (selectors.empty()) continue;

        std::string filename = asset + ".json";
        std::filesystem::path outputPath = testCase.expectedDir / "selector" / filename;
        SelectorComparator::SaveJson(selectors, outputPath);
    }

    SPDLOG_INFO("Generated expected outputs for: {}", testCase.name);
}

FlowSourceTestRunner::TestCaseConfig
FlowSourceTestRunner::LoadConfig(const std::filesystem::path& configPath) {
    TestCaseConfig config;

    // Read YAML file (using glaze for JSON/YAML parsing)
    std::ifstream file(configPath);
    if (!file.is_open()) {
        return config;  // Return default config
    }

    std::stringstream buffer;
    buffer << file.rdbuf();
    std::string content = buffer.str();

    // Parse as JSON (glaze supports YAML-like JSON)
    glz::generic configJson;
    if (auto ec = glz::read_json(configJson, content); ec) {
        SPDLOG_WARN("Failed to parse config file {}: {}", configPath.string(), glz::format_error(ec));
        return config;
    }

    // Extract fields
    if (configJson.contains("title")) {
        config.title = configJson["title"].get<std::string>();
    }
    if (configJson.contains("status")) {
        config.status = configJson["status"].get<std::string>();
    }
    if (configJson.contains("strict")) {
        config.strict = configJson["strict"].get<bool>();
    }

    return config;
}

void FlowSourceTestRunner::SaveConfig(
    const std::filesystem::path& configPath,
    const TestCaseConfig& config) {

    glz::generic configJson;
    configJson["title"] = config.title;
    configJson["status"] = config.status;
    configJson["strict"] = config.strict;

    std::string buffer;
    if (auto ec = glz::write_file_json(configJson, configPath.string(), buffer); !ec) {
        SPDLOG_INFO("Saved config to {}", configPath.string());
    } else {
        SPDLOG_ERROR("Failed to write config: {}", glz::format_error(ec));
    }
}

} // namespace epoch_script::runtime::test
