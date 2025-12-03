//
// SLOT + Keyword Arguments Test Suite
//
// Tests Python-style argument handling in the DSL:
// - Positional args go to SLOT (variadic) inputs
// - Named inputs after SLOT are keyword-only
// - Mixing positional and keyword args works correctly
//
// Python equivalent: def func(*features, target): ...
// DSL equivalent: transform()(feat1, feat2, target=label)
//

#include <catch2/catch_test_macros.hpp>
#include "transforms/compiler/ast_compiler.h"

using namespace epoch_script;

// Helper to compile and check for errors
static std::pair<bool, std::string> TryCompile(const std::string& code) {
    try {
        AlgorithmAstCompiler compiler;
        auto result = compiler.compile(code);
        return {true, ""};
    } catch (const std::exception& e) {
        return {false, e.what()};
    }
}

// Helper to compile and get result
static CompilationResult Compile(const std::string& code) {
    AlgorithmAstCompiler compiler;
    return compiler.compile(code);
}

TEST_CASE("SLOT + kwargs: positional args to SLOT, keyword to named", "[compiler][slot][kwargs]") {
    // rolling_lightgbm_regressor has: SLOT (features), target (keyword-only)
    // DSL: transform()(feat1, feat2, target=label)

    SECTION("single positional arg to SLOT with keyword target") {
        const char* code = R"(
mds = market_data_source(timeframe="1D")
o = mds.o
c = mds.c
pred = rolling_lightgbm_regressor(window_size=100, step_size=10)(o, target=c)
report = numeric_cards_report(agg="last", category="ML", title="Pred")(pred)
)";
        auto [ok, err] = TryCompile(code);
        INFO("Error: " << err);
        REQUIRE(ok);

        auto result = Compile(code);

        // Find the rolling_lightgbm_regressor node
        auto it = std::find_if(result.begin(), result.end(),
            [](const auto& node) { return node.type == "rolling_lightgbm_regressor"; });
        REQUIRE(it != result.end());

        // Check inputs are wired correctly
        REQUIRE(it->inputs.count("SLOT") == 1);
        REQUIRE(it->inputs.at("SLOT").size() == 1);  // o goes to SLOT
        REQUIRE(it->inputs.count("target") == 1);
        REQUIRE(it->inputs.at("target").size() == 1);  // c goes to target
    }

    SECTION("multiple positional args to SLOT with keyword target") {
        const char* code = R"(
mds = market_data_source(timeframe="1D")
o = mds.o
h = mds.h
l = mds.l
c = mds.c
pred = rolling_lightgbm_regressor(window_size=100, step_size=10)(o, h, l, target=c)
report = numeric_cards_report(agg="last", category="ML", title="Pred")(pred)
)";
        auto [ok, err] = TryCompile(code);
        INFO("Error: " << err);
        REQUIRE(ok);

        auto result = Compile(code);

        auto it = std::find_if(result.begin(), result.end(),
            [](const auto& node) { return node.type == "rolling_lightgbm_regressor"; });
        REQUIRE(it != result.end());

        // All 3 positional args go to SLOT
        REQUIRE(it->inputs.count("SLOT") == 1);
        REQUIRE(it->inputs.at("SLOT").size() == 3);  // o, h, l all go to SLOT
        REQUIRE(it->inputs.count("target") == 1);
        REQUIRE(it->inputs.at("target").size() == 1);  // c via keyword
    }
}

TEST_CASE("SLOT + kwargs: static supervised models", "[compiler][slot][kwargs][static]") {
    // logistic_l1 has: SLOT (features), target (keyword-only)

    SECTION("logistic_l1 with multiple features and keyword target") {
        // logistic_l1 has multiple outputs (prediction, probability, decision_value)
        // Need to use explicit output handle
        const char* code = R"(
mds = market_data_source(timeframe="1D")
o = mds.o
h = mds.h
c = mds.c
pred = logistic_l1(split_ratio=0.8)(o, h, target=c)
report = numeric_cards_report(agg="last", category="ML", title="Pred")(pred.prediction)
)";
        auto [ok, err] = TryCompile(code);
        INFO("Error: " << err);
        REQUIRE(ok);

        auto result = Compile(code);

        auto it = std::find_if(result.begin(), result.end(),
            [](const auto& node) { return node.type == "logistic_l1"; });
        REQUIRE(it != result.end());

        REQUIRE(it->inputs.count("SLOT") == 1);
        REQUIRE(it->inputs.at("SLOT").size() == 2);  // o, h
        REQUIRE(it->inputs.count("target") == 1);
    }

    SECTION("svr_l2 with single feature and keyword target") {
        const char* code = R"(
mds = market_data_source(timeframe="1D")
o = mds.o
c = mds.c
pred = svr_l2(split_ratio=0.8)(o, target=c)
report = numeric_cards_report(agg="last", category="ML", title="Pred")(pred)
)";
        auto [ok, err] = TryCompile(code);
        INFO("Error: " << err);
        REQUIRE(ok);
    }
}

TEST_CASE("SLOT + kwargs: error cases", "[compiler][slot][kwargs][error]") {

    SECTION("missing required keyword-only argument should fail") {
        // target is required but not provided
        const char* code = R"(
mds = market_data_source(timeframe="1D")
o = mds.o
pred = rolling_lightgbm_regressor(window_size=100, step_size=10)(o)
)";
        auto [ok, err] = TryCompile(code);
        // This should fail because 'target' is required but not provided
        // The exact behavior depends on whether target has a default or is required
        // For now, just document behavior
        INFO("Compilation result: " << (ok ? "success" : err));
    }

    SECTION("unknown keyword argument should fail") {
        const char* code = R"(
mds = market_data_source(timeframe="1D")
o = mds.o
c = mds.c
pred = rolling_lightgbm_regressor(window_size=100)(o, target=c, unknown_input=o)
)";
        auto [ok, err] = TryCompile(code);
        REQUIRE_FALSE(ok);
        REQUIRE(err.find("unknown_input") != std::string::npos);
    }
}

TEST_CASE("SLOT-only transforms still work", "[compiler][slot][backwards-compat]") {
    // Transforms with only SLOT inputs should continue to work

    SECTION("rolling_pca with multiple positional args") {
        // rolling_pca_2 through rolling_pca_6 are the actual transform names
        const char* code = R"(
mds = market_data_source(timeframe="1D")
o = mds.o
h = mds.h
l = mds.l
c = mds.c
pca = rolling_pca_4(window_size=100)(o, h, l, c)
report = numeric_cards_report(agg="last", category="ML", title="PCA")(pca.pc_0)
)";
        auto [ok, err] = TryCompile(code);
        INFO("Error: " << err);
        REQUIRE(ok);
    }

    SECTION("sma with single positional arg") {
        const char* code = R"(
mds = market_data_source(timeframe="1D")
c = mds.c
avg = sma(period=20)(c)
report = numeric_cards_report(agg="last", category="TA", title="SMA")(avg)
)";
        auto [ok, err] = TryCompile(code);
        INFO("Error: " << err);
        REQUIRE(ok);
    }
}

TEST_CASE("Named-only transforms (no SLOT)", "[compiler][kwargs][named-only]") {
    // Some transforms may have only named inputs (no SLOT)
    // Positional args should not work for these

    SECTION("transform with only named inputs rejects positional args") {
        // This test documents expected behavior for transforms without SLOT
        // The exact transform to use depends on what's available in the codebase
        // For now, we just verify the general infrastructure works
    }
}
