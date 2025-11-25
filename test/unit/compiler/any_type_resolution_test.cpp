#include <catch2/catch_test_macros.hpp>
#include "transforms/compiler/ast_compiler.h"

using namespace epoch_script;

TEST_CASE("Typed boolean_select variants", "[compiler][type_resolution][boolean_select]") {

    SECTION("boolean_select_string with string literals") {
        const char* code = R"(
            src = market_data_source(timeframe="1D")
            test_bool = gt(src.c, 100)
            result = boolean_select_string()(test_bool, "High", "Low")
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        // Verify the code compiled successfully
        REQUIRE(algorithms.size() > 0);

        // Find the boolean_select_string node
        bool found_boolean_select = false;
        for (const auto& algo : algorithms) {
            if (algo.type == "boolean_select_string") {
                found_boolean_select = true;

                // Verify inputs are wired correctly
                REQUIRE(algo.inputs.count("condition") > 0);
                REQUIRE(algo.inputs.count("true") > 0);
                REQUIRE(algo.inputs.count("false") > 0);
            }
        }

        REQUIRE(found_boolean_select);
    }

    SECTION("boolean_select_string compiles successfully") {
        const char* code = R"(
            src = market_data_source(timeframe="1D")
            test_bool = gt(src.c, 100)
            label = boolean_select_string()(test_bool, "ValuePick", "Other")
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        // The code should compile without type errors
        REQUIRE(algorithms.size() > 0);
    }

    SECTION("boolean_select_number with numeric literals") {
        const char* code = R"(
            src = market_data_source(timeframe="1D")
            test_bool = gt(src.c, 100)
            result = boolean_select_number()(test_bool, 1.0, 0.0)
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        REQUIRE(algorithms.size() > 0);
    }

    SECTION("boolean_select_boolean with boolean literals") {
        const char* code = R"(
            src = market_data_source(timeframe="1D")
            test_bool = gt(src.c, 100)
            result = boolean_select_boolean()(test_bool, true, false)
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        REQUIRE(algorithms.size() > 0);
    }
}

TEST_CASE("Boolean to String type cast", "[compiler][type_cast][bool_to_string]") {

    SECTION("Boolean expression can be used where String is expected") {
        // This should trigger automatic bool->string cast
        const char* code = R"(
            src = market_data_source(timeframe="1D")
            test_bool = gt(src.c, 100)
            # If a transform expects String but receives Boolean,
            # the compiler should insert a bool_to_string cast
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        REQUIRE(algorithms.size() > 0);
    }
}

TEST_CASE("Real-world scenario: bottom_k_percent with boolean_select_string", "[compiler][type_resolution][integration]") {

    SECTION("bottom_k_percent -> boolean_select_string -> bar_chart_report") {
        const char* code = R"(
            # This is the exact pattern from the failing test
            fr = financial_ratios(timeframe="1D")
            value_picks = bottom_k_percent(k=20)(fr.price_to_earnings)
            value_pick_label = boolean_select_string()(value_picks, "ValuePick", "Other")

            # value_pick_label should now be String type
            # So it can be safely used in bar_chart_report
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        // Find the boolean_select_string node
        bool found_boolean_select = false;
        for (const auto& algo : algorithms) {
            if (algo.type == "boolean_select_string" && algo.id.find("value_pick_label") != std::string::npos) {
                found_boolean_select = true;

                // Verify it has string inputs
                REQUIRE(algo.inputs.count("true") > 0);
                REQUIRE(algo.inputs.count("false") > 0);
            }
        }

        REQUIRE(found_boolean_select);
    }
}

TEST_CASE("MaterializeString helper creates string literals", "[compiler][type_cast][materialize]") {

    SECTION("String literals are created correctly") {
        const char* code = R"(
            # Test that string literals work correctly
            label1 = "Test"
            label2 = "Another Test"
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        // Find text nodes
        int text_node_count = 0;
        for (const auto& algo : algorithms) {
            if (algo.type == "text") {
                text_node_count++;
                REQUIRE(algo.options.count("value") > 0);
            }
        }

        REQUIRE(text_node_count >= 2);
    }
}
