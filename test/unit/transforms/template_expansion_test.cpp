#include <catch.hpp>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include "transforms/components/data_sources/parametric_data_source.h"

using namespace epoch_script;
using namespace epoch_script::transform;

TEST_CASE("Template Expansion - FRED Transform") {

    SECTION("FREDTransform expands {category} placeholder in requiredDataSources") {
        // Create metadata with template placeholders in requiredDataSources
        epoch_script::transforms::TransformsMetaData metadata;
        metadata.id = fred::ECONOMIC_INDICATOR;
        metadata.requiredDataSources = {
            "ECON:{category}:observation_date",
            "ECON:{category}:value",
            "ECON:{category}:published_at"
        };
        // Outputs use simple IDs (no prefix) for AST compiler validation
        metadata.outputs = {
            {epoch_core::IODataType::Timestamp, "observation_date", "Observation Date"},
            {epoch_core::IODataType::Decimal, "value", "Value"},
            {epoch_core::IODataType::Timestamp, "published_at", "Published At"}
        };

        // Create transform configuration with category="CPI"
        TransformDefinitionData data{
            .type = fred::ECONOMIC_INDICATOR,
            .id = "test_fred_cpi",
            .options = {{"category", MetaDataOptionDefinition{std::string("CPI")}}},
            .timeframe = TimeFrame("1D"),
            .inputs = {},
            .metaData = metadata
        };

        TransformDefinition def(data);
        TransformConfiguration config(def);

        // Create FRED transform
        FREDTransform transform(config);

        // Get required data sources - should have placeholders expanded
        auto requiredDataSources = transform.GetRequiredDataSources();

        REQUIRE(requiredDataSources.size() == 3);
        REQUIRE(requiredDataSources[0] == "ECON:CPI:observation_date");
        REQUIRE(requiredDataSources[1] == "ECON:CPI:value");
        REQUIRE(requiredDataSources[2] == "ECON:CPI:published_at");
    }

    SECTION("FREDTransform uses simple output IDs for AST compiler validation") {
        // Create metadata with simple output IDs (no placeholders)
        epoch_script::transforms::TransformsMetaData metadata;
        metadata.id = fred::ECONOMIC_INDICATOR;
        metadata.requiredDataSources = {
            "ECON:{category}:observation_date",
            "ECON:{category}:value"
        };
        // Outputs use simple IDs (no prefix) for AST compiler validation
        metadata.outputs = {
            {epoch_core::IODataType::Timestamp, "observation_date", "Observation Date"},
            {epoch_core::IODataType::Decimal, "value", "Value"}
        };

        // Create transform configuration with category="GDP"
        TransformDefinitionData data{
            .type = fred::ECONOMIC_INDICATOR,
            .id = "test_fred_gdp",
            .options = {{"category", MetaDataOptionDefinition{std::string("GDP")}}},
            .timeframe = TimeFrame("1D"),
            .inputs = {},
            .metaData = metadata
        };

        TransformDefinition def(data);
        TransformConfiguration config(def);
        FREDTransform transform(config);

        // Get output IDs - should use simple handles (for graph wiring)
        auto outputIds = transform.GetOutputIds();

        REQUIRE(outputIds.size() == 2);
        REQUIRE(outputIds[0] == "test_fred_gdp#observation_date");
        REQUIRE(outputIds[1] == "test_fred_gdp#value");
    }

    SECTION("FREDTransform expands different categories in requiredDataSources with simple output IDs") {
        std::vector<std::string> categories = {"GDP", "Unemployment", "FedFunds", "CorePCE"};

        for (const auto& category : categories) {
            epoch_script::transforms::TransformsMetaData metadata;
            metadata.id = fred::ECONOMIC_INDICATOR;
            metadata.requiredDataSources = {"ECON:{category}:value"};
            // Outputs use simple IDs (no prefix) for AST compiler validation
            metadata.outputs = {
                {epoch_core::IODataType::Decimal, "value", "Value"}
            };

            TransformDefinitionData data{
                .type = fred::ECONOMIC_INDICATOR,
                .id = "test_fred_" + category,
                .options = {{"category", MetaDataOptionDefinition{category}}},
                .timeframe = TimeFrame("1D"),
                .inputs = {},
                .metaData = metadata
            };

            TransformDefinition def(data);
            TransformConfiguration config(def);
            FREDTransform transform(config);

            auto requiredDataSources = transform.GetRequiredDataSources();
            REQUIRE(requiredDataSources.size() == 1);
            REQUIRE(requiredDataSources[0] == "ECON:" + category + ":value");

            auto outputIds = transform.GetOutputIds();
            REQUIRE(outputIds.size() == 1);
            REQUIRE(outputIds[0] == "test_fred_" + category + "#value");
        }
    }
}

TEST_CASE("Template Expansion - Indices Transform (common_indices)") {

    SECTION("Common Indices expands {ticker} placeholder in requiredDataSources") {
        // Create metadata with template placeholders in requiredDataSources
        epoch_script::transforms::TransformsMetaData metadata;
        metadata.id = polygon::COMMON_INDICES;
        metadata.requiredDataSources = {
            "IDX:{ticker}:c",
            "IDX:{ticker}:o",
            "IDX:{ticker}:h",
            "IDX:{ticker}:l",
            "IDX:{ticker}:v"
        };
        // Outputs use simple IDs (no prefix) for AST compiler validation
        metadata.outputs = {
            {epoch_core::IODataType::Decimal, "c", "Close"},
            {epoch_core::IODataType::Decimal, "o", "Open"},
            {epoch_core::IODataType::Decimal, "h", "High"},
            {epoch_core::IODataType::Decimal, "l", "Low"},
            {epoch_core::IODataType::Integer, "v", "Volume"}
        };

        // Create transform configuration with ticker="SPX" (SelectOption)
        TransformDefinitionData data{
            .type = polygon::COMMON_INDICES,
            .id = "test_common_indices_spx",
            .options = {{"ticker", MetaDataOptionDefinition{std::string("SPX")}}},
            .timeframe = TimeFrame("1D"),
            .inputs = {},
            .metaData = metadata
        };

        TransformDefinition def(data);
        TransformConfiguration config(def);

        // Create Polygon transform
        PolygonDataSourceTransform transform(config);

        // Get required data sources - should have placeholders expanded
        auto requiredDataSources = transform.GetRequiredDataSources();

        REQUIRE(requiredDataSources.size() == 5);
        REQUIRE(requiredDataSources[0] == "IDX:SPX:c");
        REQUIRE(requiredDataSources[1] == "IDX:SPX:o");
        REQUIRE(requiredDataSources[2] == "IDX:SPX:h");
        REQUIRE(requiredDataSources[3] == "IDX:SPX:l");
        REQUIRE(requiredDataSources[4] == "IDX:SPX:v");
    }

    SECTION("Common Indices uses simple output IDs for AST compiler validation") {
        // Create metadata with simple output IDs (no placeholders)
        epoch_script::transforms::TransformsMetaData metadata;
        metadata.id = polygon::COMMON_INDICES;
        metadata.requiredDataSources = {"IDX:{ticker}:c"};
        // Outputs use simple IDs (no prefix) for AST compiler validation
        metadata.outputs = {
            {epoch_core::IODataType::Decimal, "c", "Close"}
        };

        // Create transform configuration with ticker="VIX"
        TransformDefinitionData data{
            .type = polygon::COMMON_INDICES,
            .id = "test_common_indices_vix",
            .options = {{"ticker", MetaDataOptionDefinition{std::string("VIX")}}},
            .timeframe = TimeFrame("1D"),
            .inputs = {},
            .metaData = metadata
        };

        TransformDefinition def(data);
        TransformConfiguration config(def);
        PolygonDataSourceTransform transform(config);

        // Get output IDs - should use simple handles (for graph wiring)
        auto outputIds = transform.GetOutputIds();

        REQUIRE(outputIds.size() == 1);
        REQUIRE(outputIds[0] == "test_common_indices_vix#c");
    }

    SECTION("Common Indices expands multiple tickers in requiredDataSources with simple output IDs") {
        std::vector<std::string> tickers = {"SPX", "DJI", "NDX", "VIX"};

        for (const auto& ticker : tickers) {
            epoch_script::transforms::TransformsMetaData metadata;
            metadata.id = polygon::COMMON_INDICES;
            metadata.requiredDataSources = {"IDX:{ticker}:c"};
            // Outputs use simple IDs (no prefix) for AST compiler validation
            metadata.outputs = {
                {epoch_core::IODataType::Decimal, "c", "Close"}
            };

            TransformDefinitionData data{
                .type = polygon::COMMON_INDICES,
                .id = "test_common_indices_" + ticker,
                .options = {{"ticker", MetaDataOptionDefinition{ticker}}},
                .timeframe = TimeFrame("1D"),
                .inputs = {},
                .metaData = metadata
            };

            TransformDefinition def(data);
            TransformConfiguration config(def);
            PolygonDataSourceTransform transform(config);

            auto requiredDataSources = transform.GetRequiredDataSources();
            REQUIRE(requiredDataSources.size() == 1);
            REQUIRE(requiredDataSources[0] == "IDX:" + ticker + ":c");

            auto outputIds = transform.GetOutputIds();
            REQUIRE(outputIds.size() == 1);
            REQUIRE(outputIds[0] == "test_common_indices_" + ticker + "#c");
        }
    }
}

TEST_CASE("Template Expansion - Indices Transform (indices)") {

    SECTION("Indices expands {ticker} placeholder in requiredDataSources with simple output IDs") {
        // Create metadata with template placeholders in requiredDataSources
        epoch_script::transforms::TransformsMetaData metadata;
        metadata.id = polygon::INDICES;
        metadata.requiredDataSources = {
            "IDX:{ticker}:c",
            "IDX:{ticker}:o",
            "IDX:{ticker}:h",
            "IDX:{ticker}:l",
            "IDX:{ticker}:v"
        };
        // Outputs use simple IDs (no prefix) for AST compiler validation
        metadata.outputs = {
            {epoch_core::IODataType::Decimal, "c", "Close"},
            {epoch_core::IODataType::Decimal, "o", "Open"},
            {epoch_core::IODataType::Decimal, "h", "High"},
            {epoch_core::IODataType::Decimal, "l", "Low"},
            {epoch_core::IODataType::Integer, "v", "Volume"}
        };

        // Create transform configuration with ticker="FTSE" (String option)
        TransformDefinitionData data{
            .type = polygon::INDICES,
            .id = "test_indices_ftse",
            .options = {{"ticker", MetaDataOptionDefinition{std::string("FTSE")}}},
            .timeframe = TimeFrame("1D"),
            .inputs = {},
            .metaData = metadata
        };

        TransformDefinition def(data);
        TransformConfiguration config(def);

        // Create Polygon transform
        PolygonDataSourceTransform transform(config);

        // Get required data sources - should have placeholders expanded
        auto requiredDataSources = transform.GetRequiredDataSources();

        REQUIRE(requiredDataSources.size() == 5);
        REQUIRE(requiredDataSources[0] == "IDX:FTSE:c");
        REQUIRE(requiredDataSources[1] == "IDX:FTSE:o");
        REQUIRE(requiredDataSources[2] == "IDX:FTSE:h");
        REQUIRE(requiredDataSources[3] == "IDX:FTSE:l");
        REQUIRE(requiredDataSources[4] == "IDX:FTSE:v");

        // Get output IDs - should use simple handles (for graph wiring)
        auto outputIds = transform.GetOutputIds();

        REQUIRE(outputIds.size() == 5);
        REQUIRE(outputIds[0] == "test_indices_ftse#c");
        REQUIRE(outputIds[1] == "test_indices_ftse#o");
        REQUIRE(outputIds[2] == "test_indices_ftse#h");
        REQUIRE(outputIds[3] == "test_indices_ftse#l");
        REQUIRE(outputIds[4] == "test_indices_ftse#v");
    }
}

TEST_CASE("Template Expansion - Non-template transforms") {

    SECTION("Regular transform returns metadata requiredDataSources unchanged") {
        // Create metadata without template placeholders (e.g., balance sheet)
        epoch_script::transforms::TransformsMetaData metadata;
        metadata.id = "balance_sheet";
        metadata.requiredDataSources = {
            "assets",
            "liabilities",
            "equity",
            "current_assets",
            "current_liabilities"
        };
        metadata.outputs = {
            {epoch_core::IODataType::Decimal, "assets", "Total Assets"},
            {epoch_core::IODataType::Decimal, "liabilities", "Total Liabilities"},
            {epoch_core::IODataType::Decimal, "equity", "Shareholders Equity"}
        };

        TransformDefinitionData data{
            .type = "balance_sheet",
            .id = "test_balance_sheet",
            .options = {},
            .timeframe = TimeFrame("1D"),
            .inputs = {},
            .metaData = metadata
        };

        TransformDefinition def(data);
        TransformConfiguration config(def);

        // Create Polygon transform (which handles balance sheet)
        PolygonDataSourceTransform transform(config);

        // Get required data sources - should be unchanged (no ticker option)
        auto requiredDataSources = transform.GetRequiredDataSources();

        REQUIRE(requiredDataSources.size() == 5);
        REQUIRE(requiredDataSources[0] == "assets");
        REQUIRE(requiredDataSources[1] == "liabilities");
        REQUIRE(requiredDataSources[2] == "equity");
        REQUIRE(requiredDataSources[3] == "current_assets");
        REQUIRE(requiredDataSources[4] == "current_liabilities");
    }

    SECTION("Empty requiredDataSources returns empty") {
        epoch_script::transforms::TransformsMetaData metadata;
        metadata.id = "test_empty";
        metadata.requiredDataSources = {};
        metadata.outputs = {};

        TransformDefinitionData data{
            .type = "test_empty",
            .id = "test_empty_1",
            .options = {},
            .timeframe = TimeFrame("1D"),
            .inputs = {},
            .metaData = metadata
        };

        TransformDefinition def(data);
        TransformConfiguration config(def);
        PolygonDataSourceTransform transform(config);

        auto requiredDataSources = transform.GetRequiredDataSources();

        REQUIRE(requiredDataSources.empty());
    }
}
