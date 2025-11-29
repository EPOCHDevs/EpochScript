//
// Unit test for intradayOnly default timeframe behavior
// Verifies that nodes with intradayOnly=true default to 1Min when no timeframe is specified
//

#include <catch2/catch_all.hpp>
#include "transforms/compiler/ast_compiler.h"

using namespace epoch_script;

TEST_CASE("IntradayOnly Default Timeframe", "[compiler][validation][timeframe][intradayOnly]")
{
    SECTION("session_time_window without timeframe should default to 1Min")
    {
        std::string source = R"(
window = session_time_window(session="NewYork")()
numeric_cards_report(agg="mean", category="Test", title="Window")(window.value)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW([&]() {
            auto result = compiler.compile(source);

            auto window_it = std::find_if(result.begin(), result.end(),
                [](const auto& n) { return n.id == "window"; });

            REQUIRE(window_it != result.end());
            REQUIRE(window_it->timeframe.has_value());
            // intradayOnly transforms default to 1Min when no timeframe specified
            REQUIRE(window_it->timeframe->ToString() == "1Min");
        }());
    }

    SECTION("session_time_window with explicit timeframe should use that timeframe")
    {
        std::string source = R"(
window = session_time_window(session="NewYork", timeframe="5Min")()
numeric_cards_report(agg="mean", category="Test", title="Window")(window.value)
)";

        AlgorithmAstCompiler compiler;

        REQUIRE_NOTHROW([&]() {
            auto result = compiler.compile(source);

            auto window_it = std::find_if(result.begin(), result.end(),
                [](const auto& n) { return n.id == "window"; });

            REQUIRE(window_it != result.end());
            REQUIRE(window_it->timeframe.has_value());
            // Should use explicit 5Min
            REQUIRE(window_it->timeframe->ToString() == "5Min");
        }());
    }

    SECTION("Non-intradayOnly node (market_data_source) should require explicit timeframe")
    {
        std::string source = R"(
mds = market_data_source()()
numeric_cards_report(agg="mean", category="Test", title="MDS")(mds.c)
)";

        AlgorithmAstCompiler compiler;

        // Should throw error about missing timeframe (market_data_source is not intradayOnly)
        REQUIRE_THROWS_WITH(
            compiler.compile(source),
            Catch::Matchers::ContainsSubstring("requires a 'timeframe' parameter")
        );
    }
}
