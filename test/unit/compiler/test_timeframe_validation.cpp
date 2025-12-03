//
// Unit test for requiresTimeFrame validation
// Bug: Data sources with requiresTimeFrame=true should reject missing timeframe parameter
//

#include <catch2/catch_all.hpp>
#include "transforms/compiler/ast_compiler.h"

using namespace epoch_script;

TEST_CASE("Data Source Timeframe Validation", "[compiler][validation][timeframe]")
{
    SECTION("economic_indicator without timeframe should fail")
    {
        std::string source = R"(
fed_funds = economic_indicator(category="FedFunds")()
numeric_cards_report(agg="mean", category="Test", title="Value")(fed_funds.value)
)";

        AlgorithmAstCompiler compiler;

        // Should throw with helpful error message
        REQUIRE_THROWS_WITH(
            compiler.compile(source),
            Catch::Matchers::ContainsSubstring("requires a 'timeframe' parameter")
        );
    }

    SECTION("economic_indicator with timeframe should succeed")
    {
        std::string source = R"(
fed_funds = economic_indicator(category="FedFunds", timeframe="1D")()
numeric_cards_report(agg="mean", category="Test", title="Value")(fed_funds.value)
)";

        AlgorithmAstCompiler compiler;

        // Should compile successfully
        REQUIRE_NOTHROW([&]() {
            auto result = compiler.compile(source);

            // Find fed_funds node
            auto fed_funds_it = std::find_if(result.begin(), result.end(),
                [](const auto& n) { return n.id == "fed_funds"; });

            REQUIRE(fed_funds_it != result.end());
            REQUIRE(fed_funds_it->timeframe.has_value());
            REQUIRE(fed_funds_it->timeframe->ToString() == "1D");
        }());
    }

    SECTION("indices() without timeframe should fail")
    {
        std::string source = R"(
vix = indices(ticker="VIX")()
numeric_cards_report(agg="mean", category="Test", title="VIX")(vix.c)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_THROWS_WITH(
            compiler.compile(source),
            Catch::Matchers::ContainsSubstring("requires a 'timeframe' parameter")
        );
    }

    SECTION("indices() with timeframe should succeed")
    {
        std::string source = R"(
vix = indices(ticker="VIX", timeframe="1D")()
numeric_cards_report(agg="mean", category="Test", title="VIX")(vix.c)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW([&]() {
            auto result = compiler.compile(source);

            auto vix_it = std::find_if(result.begin(), result.end(),
                [](const auto& n) { return n.id == "vix"; });

            REQUIRE(vix_it != result.end());
            REQUIRE(vix_it->timeframe.has_value());
            REQUIRE(vix_it->timeframe->ToString() == "1D");
        }());
    }

    SECTION("is_asset_ref without timeframe should succeed - runtime scalar")
    {
        // is_asset_ref is a runtime scalar that returns true/false based on current asset
        // It should NOT require a timeframe parameter since it's asset-based, not time-based
        std::string source = R"(
is_aapl = is_asset_ref(ticker="AAPL")()
signal = conditional_select()(is_aapl, src.c, -src.c)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW([&]() {
            auto result = compiler.compile(source);

            auto is_aapl_it = std::find_if(result.begin(), result.end(),
                [](const auto& n) { return n.id == "is_aapl"; });

            REQUIRE(is_aapl_it != result.end());
            REQUIRE(is_aapl_it->type == "is_asset_ref");
            // is_asset_ref should NOT have a timeframe - it's a runtime scalar
            REQUIRE_FALSE(is_aapl_it->timeframe.has_value());
        }());
    }

    SECTION("is_asset_ref used in boolean expressions without timeframe")
    {
        // More complex test: is_asset_ref used with boolean operations
        std::string source = R"(
is_gld = is_asset_ref(ticker="GLD")()
is_gdx = is_asset_ref(ticker="GDX")()
momentum = sma(period=20)(src.c) - sma(period=50)(src.c)
long_signal = conditional_select_boolean()(
    is_gld & (momentum > 0), bool_true()(),
    is_gdx & (momentum < 0), bool_true()(),
    bool_false()()
)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW([&]() {
            auto result = compiler.compile(source);

            // Verify both is_asset_ref nodes compiled without timeframe
            auto is_gld_it = std::find_if(result.begin(), result.end(),
                [](const auto& n) { return n.id == "is_gld"; });
            auto is_gdx_it = std::find_if(result.begin(), result.end(),
                [](const auto& n) { return n.id == "is_gdx"; });

            REQUIRE(is_gld_it != result.end());
            REQUIRE(is_gdx_it != result.end());
            REQUIRE(is_gld_it->type == "is_asset_ref");
            REQUIRE(is_gdx_it->type == "is_asset_ref");
            REQUIRE_FALSE(is_gld_it->timeframe.has_value());
            REQUIRE_FALSE(is_gdx_it->timeframe.has_value());
        }());
    }
}
