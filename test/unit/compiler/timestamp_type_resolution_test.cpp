//
// Unit test for Timestamp type resolution
// Bug fix: IODataType::Timestamp was not being mapped to DataType::Timestamp
// in type_checker.cpp, causing alias_timestamp to be incorrectly typed as alias_decimal
//

#include <catch2/catch_test_macros.hpp>
#include "transforms/compiler/ast_compiler.h"

using namespace epoch_script;

TEST_CASE("Timestamp type resolution for economic_indicator.observation_date", "[compiler][type_resolution][timestamp]")
{
    SECTION("observation_date should be aliased as alias_timestamp not alias_decimal")
    {
        const char* code = R"(
            src = market_data_source(timeframe="1D")
            fed_funds = economic_indicator(category="FedFunds")()
            obs = fed_funds.observation_date
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        // Find the alias node for 'obs'
        bool found_alias_timestamp = false;
        bool found_alias_decimal = false;

        for (const auto& algo : algorithms)
        {
            if (algo.id == "obs")
            {
                if (algo.type == "alias_timestamp")
                {
                    found_alias_timestamp = true;
                }
                else if (algo.type == "alias_decimal")
                {
                    found_alias_decimal = true;
                }
            }
        }

        // Should be alias_timestamp, NOT alias_decimal
        REQUIRE(found_alias_timestamp);
        REQUIRE_FALSE(found_alias_decimal);
    }

    SECTION("timestamp column should be usable with datetime extraction")
    {
        const char* code = R"(
            src = market_data_source(timeframe="1D")
            econ = economic_indicator(category="CPI")()
            obs = econ.observation_date
            month = column_datetime_extract(component="month")(obs)
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        // Verify compilation succeeds
        REQUIRE(algorithms.size() > 0);

        // Find the datetime_extract node
        bool found_datetime_extract = false;
        for (const auto& algo : algorithms)
        {
            if (algo.type == "column_datetime_extract")
            {
                found_datetime_extract = true;
                // Verify input is connected to obs
                REQUIRE(algo.inputs.count("SLOT") > 0);
            }
        }

        REQUIRE(found_datetime_extract);
    }
}

TEST_CASE("Timestamp type propagation through alias nodes", "[compiler][type_resolution][timestamp]")
{
    SECTION("Multiple timestamp aliases should all be alias_timestamp")
    {
        const char* code = R"(
            src = market_data_source(timeframe="1D")
            econ = economic_indicator(category="GDP")()
            obs1 = econ.observation_date
            obs2 = obs1
        )";

        AlgorithmAstCompiler compiler;
        auto algorithms = compiler.compile(code, true);

        int timestamp_alias_count = 0;
        for (const auto& algo : algorithms)
        {
            if (algo.type == "alias_timestamp" &&
                (algo.id == "obs1" || algo.id == "obs2"))
            {
                timestamp_alias_count++;
            }
        }

        // Both obs1 and obs2 should be alias_timestamp
        REQUIRE(timestamp_alias_count == 2);
    }
}
