//
// Test that Python boolean operators (and, or, not) compile correctly
//

#include <catch.hpp>
#include "transforms/compiler/ast_compiler.h"

using namespace epoch_script;

TEST_CASE("Python boolean operators compile correctly", "[python_operators]")
{
    SECTION("not operator with numeric constant") {
        std::string python_code = R"(
result = not 1
)";

        AlgorithmAstCompiler compiler;
        REQUIRE_NOTHROW(compiler.compile(python_code, true));

        auto result = compiler.compile(python_code, true);

        // Should contain logical_not transform
        bool has_logical_not = false;
        for (const auto& node : result) {
            if (node.type == "logical_not") {
                has_logical_not = true;
                break;
            }
        }
        REQUIRE(has_logical_not);
    }

    SECTION("and operator with mixed types") {
        std::string python_code = R"(
result = 1 and True
)";

        AlgorithmAstCompiler compiler;
        REQUIRE_NOTHROW(compiler.compile(python_code, true));

        auto result = compiler.compile(python_code, true);

        // Should contain logical_and transform
        bool has_logical_and = false;
        for (const auto& node : result) {
            if (node.type == "logical_and") {
                has_logical_and = true;
                break;
            }
        }
        REQUIRE(has_logical_and);
    }

    SECTION("or operator with numeric constants") {
        std::string python_code = R"(
result = 1 or 0
)";

        AlgorithmAstCompiler compiler;
        REQUIRE_NOTHROW(compiler.compile(python_code, true));

        auto result = compiler.compile(python_code, true);

        // Should contain logical_or transform
        bool has_logical_or = false;
        for (const auto& node : result) {
            if (node.type == "logical_or") {
                has_logical_or = true;
                break;
            }
        }
        REQUIRE(has_logical_or);
    }

    SECTION("combined operators with mixed types") {
        std::string python_code = R"(
result = 1 and False or not 0
)";

        AlgorithmAstCompiler compiler;
        REQUIRE_NOTHROW(compiler.compile(python_code, true));

        auto result = compiler.compile(python_code, true);

        // Should contain all three logical operators
        bool has_and = false, has_or = false, has_not = false;
        for (const auto& node : result) {
            if (node.type == "logical_and") has_and = true;
            if (node.type == "logical_or") has_or = true;
            if (node.type == "logical_not") has_not = true;
        }
        REQUIRE(has_and);
        REQUIRE(has_or);
        REQUIRE(has_not);
    }
}
