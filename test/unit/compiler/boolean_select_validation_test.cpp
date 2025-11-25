//
// Unit tests for typed boolean_select transforms
// Tests that typed boolean_select variants work correctly
//

#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <catch2/matchers/catch_matchers_string.hpp>
#include "transforms/compiler/ast_compiler.h"
#include "transforms/compiler/compilation_context.h"
#include <stdexcept>

using namespace epoch_script;

TEST_CASE("Typed boolean_select compilation", "[compiler][validation][boolean_select]") {

    SECTION("boolean_select_string with string literals compiles successfully") {
        const char* compatible_string_code = R"(
fr = financial_ratios(timeframe="1D")
value_picks = bottom_k_percent(k=20)(fr.price_to_earnings)
value_pick_label = boolean_select_string()(value_picks, "ValuePick", "Other")
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW(compiler.compile(compatible_string_code, true));
    }

    SECTION("boolean_select_number with numeric literals compiles successfully") {
        const char* compatible_numeric_code = R"(
fr = financial_ratios(timeframe="1D")
high_pe = gt()(fr.price_to_earnings, 20)
signal = boolean_select_number()(high_pe, 1, 0)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW(compiler.compile(compatible_numeric_code, true));
    }

    SECTION("boolean_select_boolean with boolean variables compiles successfully") {
        const char* compatible_bool_code = R"(
fr = financial_ratios(timeframe="1D")
high_pe = gt()(fr.price_to_earnings, 20)
low_roe = lt()(fr.return_on_equity, 0.1)
signal = boolean_select_boolean()(high_pe, low_roe, high_pe)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW(compiler.compile(compatible_bool_code, true));
    }

    SECTION("Nested boolean_select_string works correctly") {
        const char* nested_code = R"(
src = market_data_source(timeframe="1D")
ret = roc(period=1)(src.c)
z_ret = zscore(window=20)(ret)

a_top10 = top_k_percent(k=10)(z_ret)
a_bot10 = bottom_k_percent(k=10)(z_ret)

# a_l0 will be String
a_l0 = boolean_select_string()(a_top10, "ALPHA_TOP10", "OTHER")

# This should succeed because both are using boolean_select_string
alpha_label = boolean_select_string()(a_bot10, "ALPHA_BOT10", a_l0)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW(compiler.compile(nested_code, true));
    }

    SECTION("Mixed typed boolean_select calls compile successfully") {
        const char* mixed_code = R"(
src = market_data_source(timeframe="1D")
ret = roc(period=1)(src.c)

high_ret = gt()(ret, 0.01)

# Numeric result
numeric_result = boolean_select_number()(high_ret, 1, 0)

# String result
string_label = boolean_select_string()(high_ret, "High", "Low")
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW(compiler.compile(mixed_code, true));
    }
}
