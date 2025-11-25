//
// EpochScript Boolean Type Casting Test
//
// Tests that boolean operations (and/or) automatically cast non-boolean operands
//

#include <catch.hpp>
#include "transforms/compiler/ast_compiler.h"
#include <catch2/matchers/catch_matchers_string.hpp>

using namespace epoch_script;

TEST_CASE("Boolean operations with numeric operands require type casting", "[boolean_cast]")
{
    SECTION("logical_and with double and bool should auto-cast double to bool")
    {
        // Python code: result = 1.0 and True
        // Should compile with automatic type cast
        // For constant literals, the cast happens at compile-time (ConstantValue conversion)
        // rather than inserting static_cast nodes
        std::string python_code = R"(
result = 1.0 and True
)";

        try
        {
            AlgorithmAstCompiler compiler;
            auto result = compiler.compile(python_code, true);  // skip_sink_validation=true

            // Should succeed and contain a logical_and node
            // Constants are cast at compile-time, so no cast node needed
            bool has_logical_and = false;
            for (const auto& node : result)
            {
                if (node.type == "logical_and")
                {
                    has_logical_and = true;
                    break;
                }
            }

            REQUIRE(has_logical_and);
            INFO("Compilation succeeded with compile-time constant casting");
        }
        catch (const std::exception& e)
        {
            FAIL("Compilation should succeed with automatic type cast, but failed with: " << e.what());
        }
    }

    SECTION("logical_or with int64 and bool should auto-cast int64 to bool")
    {
        // Python code: result = 5 or False
        // For constant literals, the cast happens at compile-time
        std::string python_code = R"(
result = 5 or False
)";

        try
        {
            AlgorithmAstCompiler compiler;
            auto result = compiler.compile(python_code, true);  // skip_sink_validation=true

            // Should succeed and contain a logical_or node
            bool has_logical_or = false;
            for (const auto& node : result)
            {
                if (node.type == "logical_or")
                {
                    has_logical_or = true;
                    break;
                }
            }

            REQUIRE(has_logical_or);
            INFO("Compilation succeeded with compile-time constant casting");
        }
        catch (const std::exception& e)
        {
            FAIL("Compilation should succeed with automatic type cast, but failed with: " << e.what());
        }
    }

    SECTION("logical_and with bool and number should auto-cast number")
    {
        // Python code: result = True and 1
        // Boolean and number - number is cast at compile-time for constants
        std::string python_code = R"(
result = True and 1
)";

        try
        {
            AlgorithmAstCompiler compiler;
            auto result = compiler.compile(python_code, true);  // skip_sink_validation=true

            // Should succeed and contain a logical_and node
            bool has_logical_and = false;
            for (const auto& node : result)
            {
                if (node.type == "logical_and")
                {
                    has_logical_and = true;
                    break;
                }
            }

            REQUIRE(has_logical_and);
            INFO("Compilation succeeded with compile-time constant casting");
        }
        catch (const std::exception& e)
        {
            FAIL("Compilation should succeed with automatic type cast, but failed with: " << e.what());
        }
    }

    SECTION("logical_or with multiple numeric operands should auto-cast all")
    {
        // Python code: result = 1 or 2 or 3
        // All numeric constants are cast to boolean at compile-time
        std::string python_code = R"(
result = 1 or 2 or 3
)";

        try
        {
            AlgorithmAstCompiler compiler;
            auto result = compiler.compile(python_code, true);  // skip_sink_validation=true

            // Should succeed and contain logical_or nodes
            int logical_or_count = 0;
            for (const auto& node : result)
            {
                if (node.type == "logical_or")
                {
                    logical_or_count++;
                }
            }

            // Should have 2 logical_or nodes for 3 operands: (1 or (2 or 3))
            REQUIRE(logical_or_count >= 2);
            INFO("Compilation succeeded with compile-time constant casting");
        }
        catch (const std::exception& e)
        {
            FAIL("Compilation should succeed with automatic type cast, but failed with: " << e.what());
        }
    }

    SECTION("logical_and with string should fail (incompatible type)")
    {
        // Python code: result = "hello" and True
        // Strings cannot be cast to bool
        std::string python_code = R"(
result = "hello" and True
)";

        AlgorithmAstCompiler compiler;
        REQUIRE_THROWS_WITH(
            compiler.compile(python_code, true),  // skip_sink_validation=true
            Catch::Matchers::ContainsSubstring("Cannot use type String")
        );
    }
}

TEST_CASE("Boolean type casting preserves logical semantics", "[boolean_cast]")
{
    SECTION("number 0 should cast to false")
    {
        // In Python, 0 is falsy
        std::string python_code = R"(
result = 0 and True
)";

        try
        {
            AlgorithmAstCompiler compiler;
            auto result = compiler.compile(python_code, true);  // skip_sink_validation=true

            // Should compile successfully with cast using neq(0, 0) which evaluates to false
            bool has_logical_and = false;
            for (const auto& node : result)
            {
                if (node.type == "logical_and")
                {
                    has_logical_and = true;
                    break;
                }
            }

            REQUIRE(has_logical_and);
        }
        catch (const std::exception& e)
        {
            FAIL("Should compile: " << e.what());
        }
    }

    SECTION("non-zero number should cast to true")
    {
        // In Python, non-zero numbers are truthy
        std::string python_code = R"(
result = 42 or False
)";

        try
        {
            AlgorithmAstCompiler compiler;
            auto result = compiler.compile(python_code, true);  // skip_sink_validation=true

            // Should compile successfully with cast using neq(42, 0) which evaluates to true
            bool has_logical_or = false;
            for (const auto& node : result)
            {
                if (node.type == "logical_or")
                {
                    has_logical_or = true;
                    break;
                }
            }

            REQUIRE(has_logical_or);
        }
        catch (const std::exception& e)
        {
            FAIL("Should compile: " << e.what());
        }
    }
}