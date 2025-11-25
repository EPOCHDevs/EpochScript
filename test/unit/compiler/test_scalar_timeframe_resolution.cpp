#include <catch.hpp>
#include "transforms/compiler/ast_compiler.h"

TEST_CASE("Scalars do not require timeframe resolution", "[compiler][scalars]")
{

    SECTION("Literal in boolean_select_number should compile without timeframe error")
    {
        std::string code = R"(
src = market_data_source(timeframe="1d")()
ret = intraday_returns(timeframe="1d", return_type="simple")()
cond = src.c > src.o

# boolean_select_number with literal 0 - this previously failed
result = boolean_select_number()(cond, ret, 0)

numeric_cards_report(agg="mean", category="Test", title="Result")(result)
)";

        epoch_script::AlgorithmAstCompiler compiler;
        REQUIRE_NOTHROW(compiler.compile(code, false));

        auto result = compiler.compile(code, false);
        REQUIRE(!result.empty());

        // Constants are now stored directly as ConstantValue in inputs (no scalar nodes created)
        // This is more efficient than creating number/text/bool nodes
        // The main test is that compilation succeeds without timeframe resolution errors

        // Verify boolean_select_number is present
        bool has_boolean_select = false;
        for (const auto& node : result)
        {
            if (node.type == "boolean_select_number")
            {
                has_boolean_select = true;
                break;
            }
        }
        REQUIRE(has_boolean_select);
    }

    SECTION("Multiple scalar literals in complex expression")
    {
        std::string code = R"(
src = market_data_source(timeframe="1h")()
result1 = boolean_select_number()(src.c > src.o, 1, 0)
result2 = boolean_select_number()(src.h > src.l, 100, -100)
numeric_cards_report(agg="sum", category="Test", title="R1")(result1)
numeric_cards_report(agg="sum", category="Test", title="R2")(result2)
)";

        epoch_script::AlgorithmAstCompiler compiler;
        REQUIRE_NOTHROW(compiler.compile(code, false));

        auto result = compiler.compile(code, false);

        // Constants are now stored directly as ConstantValue in inputs (no scalar nodes created)
        // Count boolean_select_number nodes to verify compilation worked
        int boolean_select_count = 0;
        for (const auto& node : result)
        {
            if (node.type == "boolean_select_number")
            {
                boolean_select_count++;
            }
        }

        // Should have 2 boolean_select_number nodes
        REQUIRE(boolean_select_count == 2);
    }
}
