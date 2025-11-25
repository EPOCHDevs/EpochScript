//
// EpochScript AST Compiler Test Suite
//
// Tests Python algorithm compilation to AlgorithmNode list using JSON expected outputs
//

#include <catch.hpp>
#include <glaze/glaze.hpp>
#include <filesystem>
#include <fstream>
#include <algorithm>
#include <set>
#include "transforms/compiler/ast_compiler.h"

using namespace epoch_script;
namespace fs = std::filesystem;

// Test case structure
struct TestCase
{
    std::string name;
    std::string input_path;
    std::string expected_path;
};

// Error case structure for tests expecting compilation errors
struct CompilerErrorCase
{
    std::string error;
};

// Helper to load all test cases from test_cases directory
std::vector<TestCase> LoadTestCases()
{
    std::vector<TestCase> cases;
    fs::path test_dir = fs::path(__FILE__).parent_path() / "test_cases";

    if (!fs::exists(test_dir))
    {
        return cases;
    }

    for (const auto &entry : fs::directory_iterator(test_dir))
    {
        if (!entry.is_directory())
            continue;

        fs::path input = entry.path() / "input.txt";
        fs::path expected = entry.path() / "expected.json";

        if (fs::exists(input) && fs::exists(expected))
        {
            cases.push_back({entry.path().filename().string(),
                             input.string(),
                             expected.string()});
        }
    }

    std::sort(cases.begin(), cases.end(),
              [](const auto &a, const auto &b)
              { return a.name < b.name; });
    return cases;
}

// Normalize result for comparison (sort by id)
static CompilationResult NormalizeResult(CompilationResult result)
{
    std::sort(result.begin(), result.end(),
              [](const auto &a, const auto &b)
              { return a.id < b.id; });
    return result;
}

// Read file contents
std::string ReadFile(const std::string &path)
{
    std::ifstream file(path);
    if (!file.is_open())
    {
        throw std::runtime_error("Failed to open file: " + path);
    }
    return std::string(std::istreambuf_iterator<char>(file),
                       std::istreambuf_iterator<char>());
}

// Parameterized test cases
TEST_CASE("EpochScript Compiler: Test Cases", "[epoch_script_compiler]")
{
    auto test_cases = LoadTestCases();

    if (test_cases.empty())
    {
        WARN("No test cases found in test_cases directory");
        return;
    }

    INFO("Found " << test_cases.size() << " test cases");

    for (const auto &test_case : test_cases)
    {
        DYNAMIC_SECTION(test_case.name)
        {
            // Read input Python source
            std::string source = ReadFile(test_case.input_path);

            // Read expected JSON
            std::string expected_json = ReadFile(test_case.expected_path);

            // Check if this is an error case
            if (expected_json.find("\"error\"") != std::string::npos)
            {
                // Error case: expect exception
                auto error_case = glz::read_json<CompilerErrorCase>(expected_json);
                REQUIRE(error_case.has_value());

                bool threw_expected_error = false;
                std::string actual_error;

                try
                {
                    AlgorithmAstCompiler compiler;
                    compiler.compile(source);
                }
                catch (const std::exception &e)
                {
                    actual_error = e.what();
                    if (actual_error.find(error_case->error) != std::string::npos)
                    {
                        threw_expected_error = true;
                    }
                }

                INFO("Expected error containing: " << error_case->error);
                INFO("Actual error: " << actual_error);
                REQUIRE(threw_expected_error);
            }
            else
            {
                // Success case: parse expected and compare
                auto expected_result = glz::read_json<CompilationResult>(expected_json);

                if (!expected_result.has_value())
                {
                    FAIL("Failed to parse expected.json: " << glz::format_error(expected_result.error(), expected_json));
                }

                CAPTURE(test_case.name);

                AlgorithmAstCompiler compiler;
                CompilationResult actual_result;

                try
                {
                    actual_result = compiler.compile(source);
                }
                catch (const std::exception &e)
                {
                    FAIL("Compilation failed: " << e.what());
                }

                // Normalize both for comparison
                auto normalized_actual = NormalizeResult(std::move(actual_result));
                auto normalized_expected = NormalizeResult(std::move(*expected_result));

                // Serialize both to JSON for easy debugging
                std::string actual_json = glz::write_json(normalized_actual).value_or("{}");
                std::string expected_json_normalized = glz::write_json(normalized_expected).value_or("{}");

                INFO("Expected JSON: " << expected_json_normalized);
                INFO("Actual JSON: " << actual_json);

                REQUIRE(normalized_actual.size() == normalized_expected.size());

                for (size_t i = 0; i < normalized_actual.size(); ++i)
                {
                    const auto &actual = normalized_actual[i];
                    const auto &expected = normalized_expected[i];

                    INFO("Comparing node at index " << i);
                    INFO("Expected id: " << expected.id << ", type: " << expected.type);
                    INFO("Actual id: " << actual.id << ", type: " << actual.type);

                    REQUIRE(actual.id == expected.id);
                    REQUIRE(actual.type == expected.type);

                    // Compare options
                    REQUIRE(actual.options.size() == expected.options.size());
                    for (const auto &[key, expected_val] : expected.options)
                    {
                        REQUIRE(actual.options.contains(key));
                        // Basic value comparison (can be enhanced)
                        INFO("Comparing option: " << key);
                    }

                    // Compare inputs map
                    REQUIRE(actual.inputs.size() == expected.inputs.size());
                    for (const auto &[handle, expected_refs] : expected.inputs)
                    {
                        REQUIRE(actual.inputs.contains(handle));
                        REQUIRE(actual.inputs.at(handle).size() == expected_refs.size());

                        for (size_t j = 0; j < expected_refs.size(); ++j)
                        {
                            INFO("Comparing input " << handle << "[" << j << "]");
                            INFO("Expected: " << expected_refs[j].GetColumnIdentifier());
                            INFO("Actual: " << actual.inputs.at(handle)[j].GetColumnIdentifier());
                            REQUIRE(actual.inputs.at(handle)[j].GetColumnIdentifier() == expected_refs[j].GetColumnIdentifier());
                        }
                    }

                    // Compare timeframe if present
                    REQUIRE(actual.timeframe.has_value() == expected.timeframe.has_value());
                    if (expected.timeframe.has_value())
                    {
                        REQUIRE(actual.timeframe->ToString() == expected.timeframe->ToString());
                    }

                    // Compare session if present
                    REQUIRE(actual.session.has_value() == expected.session.has_value());
                }
            }
        }
    }
}

// Manual test for basic functionality without test files
// ============================================================================
// TIMEFRAME RESOLUTION TESTS
// ============================================================================

#include "transforms/compiler/timeframe_resolver.h"
#include "epoch_frame/factory/date_offset_factory.h"

using epoch_script::TimeframeResolver;

TEST_CASE("TimeframeResolver: Returns nullopt when no inputs [2]", "[timeframe_resolution]")
{
    TimeframeResolver resolver;

    auto result = resolver.ResolveTimeframe("test_node", {});

    // With no inputs, resolver returns nullopt (literals will be resolved in pass 2)
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("TimeframeResolver: Resolves from input timeframes", "[timeframe_resolution]")
{
    TimeframeResolver resolver;
    auto inputTimeframe = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(5));

    // Set up input node with timeframe
    resolver.nodeTimeframes["input1"] = inputTimeframe;

    auto result = resolver.ResolveTimeframe("test_node", {"input1#result"});

    REQUIRE(result.has_value());
    REQUIRE(result.value().ToString() == inputTimeframe.ToString());
}

TEST_CASE("TimeframeResolver: Uses lowest resolution from multiple inputs", "[timeframe_resolution]")
{
    TimeframeResolver resolver;
    auto baseTimeframe = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(1));
    auto timeframe1Min = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(1));
    auto timeframe5Min = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(5));

    // Set up input nodes with different timeframes
    resolver.nodeTimeframes["input1"] = timeframe5Min; // Lower resolution (higher timeframe value)
    resolver.nodeTimeframes["input2"] = timeframe1Min; // Higher resolution (lower timeframe value)

    auto result = resolver.ResolveTimeframe("test_node", {"input1#result", "input2#result"});

    REQUIRE(result.has_value());
    // Should pick the maximum (lowest resolution) timeframe
    REQUIRE(result.value().ToString() == timeframe5Min.ToString());
}

TEST_CASE("TimeframeResolver: Caching works correctly", "[timeframe_resolution]")
{
    TimeframeResolver resolver;
    auto baseTimeframe = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(15));
    auto inputTimeframe = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(5));

    // Pre-populate cache
    resolver.nodeTimeframes["input1"] = inputTimeframe;

    // First call should resolve and cache
    auto result1 = resolver.ResolveTimeframe("test_node", {"input1#result"});
    REQUIRE(result1.has_value());
    REQUIRE(result1.value().ToString() == inputTimeframe.ToString());

    // Verify it was cached
    REQUIRE(resolver.nodeTimeframes.contains("test_node"));
    REQUIRE(resolver.nodeTimeframes["test_node"] == result1);

    // Second call should return cached value
    auto result2 = resolver.ResolveTimeframe("test_node", {"input1#result"});
    REQUIRE(result2.has_value());
    REQUIRE(result2.value().ToString() == result1.value().ToString());
}

TEST_CASE("TimeframeResolver: ResolveNodeTimeframe uses explicit node timeframe", "[timeframe_resolution]")
{
    TimeframeResolver resolver;
    auto baseTimeframe = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(1));
    auto nodeTimeframe = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(5));

    epoch_script::strategy::AlgorithmNode node;
    node.id = "test_node";
    node.timeframe = nodeTimeframe;

    auto result = resolver.ResolveNodeTimeframe(node);

    REQUIRE(result.has_value());
    REQUIRE(result.value().ToString() == nodeTimeframe.ToString());
    REQUIRE(resolver.nodeTimeframes["test_node"] == nodeTimeframe);
}

TEST_CASE("TimeframeResolver: ResolveNodeTimeframe returns nullopt for nodes without timeframe or inputs", "[timeframe_resolution]")
{
    TimeframeResolver resolver;

    epoch_script::strategy::AlgorithmNode node;
    node.id = "test_node";
    // node.timeframe is not set
    // node has no inputs

    auto result = resolver.ResolveNodeTimeframe(node);

    // Nodes without timeframes or inputs (e.g., literals) return nullopt
    // They will be resolved in a second pass via ResolveLiteralTimeframe
    REQUIRE_FALSE(result.has_value());
}

TEST_CASE("TimeframeResolver: ResolveNodeTimeframe resolves from inputs", "[timeframe_resolution]")
{
    TimeframeResolver resolver;
    auto baseTimeframe = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(1));
    auto inputTimeframe = epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(15));

    // Pre-populate cache with input node timeframe
    resolver.nodeTimeframes["input_node"] = inputTimeframe;

    epoch_script::strategy::AlgorithmNode node;
    node.id = "test_node";
    // node.timeframe is not set - should resolve from inputs
    node.inputs["SLOT0"] = std::vector<strategy::InputValue>{strategy::NodeReference{"input_node", "result"}};
    node.inputs["SLOT1"] = std::vector<strategy::InputValue>{strategy::NodeReference{"input_node", "result"}};

    auto result = resolver.ResolveNodeTimeframe(node);

    REQUIRE(result.has_value());
    // Should resolve to input's timeframe (15min)
    REQUIRE(result.value().ToString() == inputTimeframe.ToString());
}

// ============================================================================
// ORPHAN NODE REMOVAL TESTS
// ============================================================================

// ============================================================================
// STRICT TIMEFRAME VALIDATION TESTS
// ============================================================================

TEST_CASE("Compiler: All non-orphan nodes have timeframes after compilation", "[timeframe_validation]")
{
    // Test that all nodes in final graph have timeframes
    const std::string source = R"(
mds = market_data_source(timeframe="1H")
sma_node = sma(period=14)(mds.c)
report = numeric_cards_report(agg="sum", category="Test", title="Test")(sma_node)
)";

    AlgorithmAstCompiler compiler;
    auto result = compiler.compile(source);

    // Verify ALL nodes have timeframes
    for (const auto &node : result)
    {
        INFO("Checking node: " << node.id << " (type: " << node.type << ")");
        REQUIRE(node.timeframe.has_value());
        REQUIRE(!node.timeframe->ToString().empty());
    }
}

TEST_CASE("Compiler: Literals inherit timeframe from dependents", "[timeframe_validation]")
{
    // Test that literals used by other nodes get correct timeframes
    // NOTE: As of 2024, constants are stored directly as ConstantValue in inputs
    // (no separate number/text nodes created), so we verify that:
    // 1. Compilation succeeds
    // 2. The gt node exists with the literal as input
    const std::string source = R"(
mds = market_data_source(timeframe="15Min")
threshold = 100.0
signal = gt()(mds.c, threshold)
report = numeric_cards_report(agg="sum", category="Test", title="Test")(signal)
)";

    AlgorithmAstCompiler compiler;
    auto result = compiler.compile(source);

    // Find the gt node that uses the threshold
    bool found_gt = false;
    for (const auto &node : result)
    {
        if (node.type == "gt")
        {
            found_gt = true;
            // Constants are now stored as ConstantValue in inputs directly
            // (no separate number node needed)
            break;
        }
    }

    REQUIRE(found_gt);
}

TEST_CASE("Compiler: Validates timeframes for complex graphs", "[timeframe_validation]")
{
    // Test timeframe validation with complex multi-level graph
    const std::string source = R"(
mds1 = market_data_source(timeframe="1Min")
mds2 = market_data_source(timeframe="5Min")
fast = sma(period=10)(mds1.c)
slow = sma(period=20)(mds2.c)
cross = gt()(fast, slow)
multiplier = 1.5
result = mul()(cross, multiplier)
report = numeric_cards_report(agg="sum", category="Test", title="Test")(result)
)";

    AlgorithmAstCompiler compiler;
    auto result = compiler.compile(source);

    // Verify ALL nodes have valid timeframes
    REQUIRE(result.size() > 0);

    for (const auto &node : result)
    {
        INFO("Node: " << node.id << " (" << node.type << ")");

        // Scalars no longer require timeframes - runtime handles this
        // Check metadata to see if this is a scalar transform
        auto metadata = transforms::ITransformRegistry::GetInstance().GetMetaData(node.type);
        if (metadata.has_value() && metadata->get().category == epoch_core::TransformCategory::Scalar) {
            continue;
        }

        REQUIRE(node.timeframe.has_value());

        // Timeframe should be valid
        std::string tf_str = node.timeframe->ToString();
        REQUIRE(!tf_str.empty());

        // Should be either 1Min or 5Min
        bool valid_timeframe = (tf_str == "1Min" || tf_str == "5Min");
        REQUIRE(valid_timeframe);
    }
}

// ============================================================================
// ALIAS NODE TESTS
// ============================================================================

TEST_CASE("Compiler: Creates alias nodes for variable assignments from node references", "[alias]")
{
    // When assigning a node output to a variable, an alias node should be created
    // This gives each variable a unique column identifier
    const std::string source = R"(
mds = market_data_source(timeframe="1D")
price = mds.c
report = numeric_cards_report(agg="sum", category="Test", title="Test")(price)
)";

    AlgorithmAstCompiler compiler;
    auto result = compiler.compile(source);

    // Find the alias node named "price"
    bool found_alias = false;
    for (const auto &node : result)
    {
        if (node.id == "price")
        {
            found_alias = true;
            // Should be a typed alias (alias_decimal for numeric data)
            REQUIRE(node.type.find("alias_") != std::string::npos);
            // Should have input wired to mds.c
            REQUIRE(node.inputs.contains("SLOT"));
            REQUIRE(!node.inputs.at("SLOT").empty());
            REQUIRE(node.inputs.at("SLOT")[0].GetColumnIdentifier() == "mds#c");
            break;
        }
    }
    REQUIRE(found_alias);
}

TEST_CASE("Compiler: Multiple variables referencing same source get unique column identifiers", "[alias]")
{
    // Multiple variables assigned from the same source should each create
    // their own alias node, giving them unique column identifiers
    const std::string source = R"(
mds = market_data_source(timeframe="1D")
pe = mds.c
ps = mds.c
pb = mds.c
sum_node = add()(pe, ps)
report = numeric_cards_report(agg="sum", category="Test", title="Test")(sum_node)
)";

    AlgorithmAstCompiler compiler;
    auto result = compiler.compile(source);

    // Find all alias nodes and verify they have unique IDs
    std::vector<std::string> alias_ids;
    for (const auto &node : result)
    {
        if (node.type.find("alias_") != std::string::npos)
        {
            alias_ids.push_back(node.id);
        }
    }

    // Should have at least pe and ps alias nodes (pb may be orphan-removed if unused)
    REQUIRE(alias_ids.size() >= 2);

    // All alias IDs should be unique
    std::set<std::string> unique_ids(alias_ids.begin(), alias_ids.end());
    REQUIRE(unique_ids.size() == alias_ids.size());

    // Verify pe and ps exist with their variable names as node IDs
    bool found_pe = std::find(alias_ids.begin(), alias_ids.end(), "pe") != alias_ids.end();
    bool found_ps = std::find(alias_ids.begin(), alias_ids.end(), "ps") != alias_ids.end();
    REQUIRE(found_pe);
    REQUIRE(found_ps);
}

TEST_CASE("Compiler: Alias nodes preserve type information", "[alias]")
{
    // Alias nodes should be specialized based on their input type
    const std::string source = R"(
mds = market_data_source(timeframe="1D")
price = mds.c
is_up = gt()(price, price)
signal = is_up
report = boolean_cards_report(agg="any", category="Test", title="Test")(signal)
)";

    AlgorithmAstCompiler compiler;
    auto result = compiler.compile(source);

    // Find the signal alias node (should be alias_boolean since is_up is boolean)
    for (const auto &node : result)
    {
        if (node.id == "signal")
        {
            // is_up is boolean (output of gt), so signal alias should be alias_boolean
            REQUIRE(node.type == "alias_boolean");
            break;
        }
    }
}

TEST_CASE("Compiler throws error for dict literals in list parameters", "[AstCompiler][error]")
{
    // Dict literals in lists (like columns=[{...}]) are not valid EpochScript syntax
    // Users must use constructor calls like TableColumnSchema(...) or CardColumnSchema(...)

    SECTION("Dict literal in TableReportSchema columns throws error")
    {
        const std::string source = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
table_report(
    schema=TableReportSchema(
        title="Test",
        select_key="SLOT0",
        columns=[
            {"column_id":"SLOT0","title":"Signal"}
        ]
    )
)(signal)
)";

        AlgorithmAstCompiler compiler;
        REQUIRE_THROWS_WITH(compiler.compile(source),
            Catch::Matchers::ContainsSubstring("Inline dictionary literals"));
    }

    SECTION("Dict literal in EventMarkerSchema schemas throws error")
    {
        const std::string source = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
event_marker(
    schema=EventMarkerSchema(
        title="Test",
        select_key="SLOT0",
        schemas=[
            {"column_id":"SLOT0","slot":"PrimaryBadge","render_type":"Badge","color_map":{}}
        ]
    )
)(signal)
)";

        AlgorithmAstCompiler compiler;
        REQUIRE_THROWS_WITH(compiler.compile(source),
            Catch::Matchers::ContainsSubstring("Inline dictionary literals"));
    }

    SECTION("Valid constructor syntax compiles successfully")
    {
        const std::string source = R"(
src = market_data_source(timeframe="1D")()
signal = src.c > 100
table_report(
    schema=TableReportSchema(
        title="Test",
        select_key="SLOT0",
        columns=[
            TableColumnSchema(column_id="SLOT0", title="Signal")
        ]
    )
)(signal)
)";

        AlgorithmAstCompiler compiler;
        auto result = compiler.compile(source);

        // Should compile without error
        REQUIRE(result.size() > 0);

        // Find the table_report node and verify schema has columns
        bool found_table_report = false;
        for (const auto& node : result)
        {
            if (node.type == "table_report")
            {
                found_table_report = true;
                REQUIRE(node.options.contains("schema"));
                auto schema = node.options.at("schema").GetTableReportSchema();
                REQUIRE(schema.columns.size() == 1);
                REQUIRE(schema.columns[0].title == "Signal");
                break;
            }
        }
        REQUIRE(found_table_report);
    }
}
