#include <catch2/catch_test_macros.hpp>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/registry.h>
#include "transforms/components/data_sources/polygon_indices_metadata.h"

using namespace epoch_script::transforms;
using namespace epoch_script::transform;

TEST_CASE("Polygon Indices Metadata Registration", "[polygon_indices]") {
  SECTION("MakePolygonIndicesDataSources returns two nodes") {
    auto metadataList = MakePolygonIndicesDataSources();
    REQUIRE(metadataList.size() == 2);
  }

  SECTION("Common Indices node has correct basic properties") {
    auto metadataList = MakePolygonIndicesDataSources();
    auto& commonIndices = metadataList[0];

    REQUIRE(commonIndices.id == "common_indices");
    REQUIRE(commonIndices.name == "Common Indices");
    REQUIRE(commonIndices.category == epoch_core::TransformCategory::DataSource);
    REQUIRE(commonIndices.plotKind == epoch_core::TransformPlotKind::close_line);
    REQUIRE(commonIndices.requiresTimeFrame == true);
  }

  SECTION("Dynamic Indices node has correct basic properties") {
    auto metadataList = MakePolygonIndicesDataSources();
    auto& indices = metadataList[1];

    REQUIRE(indices.id == "indices");
    REQUIRE(indices.name == "Indices");
    REQUIRE(indices.category == epoch_core::TransformCategory::DataSource);
    REQUIRE(indices.plotKind == epoch_core::TransformPlotKind::close_line);
    REQUIRE(indices.requiresTimeFrame == true);
  }
}

TEST_CASE("Common Indices Configuration", "[polygon_indices][common_indices]") {
  auto metadataList = MakePolygonIndicesDataSources();
  auto& commonIndices = metadataList[0];

  SECTION("Has index SelectOption parameter") {
    REQUIRE(commonIndices.options.size() == 1);
    auto& indexOption = commonIndices.options[0];

    REQUIRE(indexOption.id == "ticker");
    REQUIRE(indexOption.name == "Index Ticker");
    REQUIRE(indexOption.type == epoch_core::MetaDataOptionType::Select);
    REQUIRE(indexOption.desc == "Select the market index");
  }

  SECTION("SelectOption contains common indices") {
    auto& indexOption = commonIndices.options[0];
    REQUIRE(indexOption.selectOption.size() == 10);

    // Verify a few key indices are present
    bool hasSPX = false;
    bool hasDJI = false;
    bool hasVIX = false;

    for (const auto& opt : indexOption.selectOption) {
      if (opt.name == "S&P 500" && opt.value == "SPX") hasSPX = true;
      if (opt.name == "Dow Jones Industrial Average" && opt.value == "DJI") hasDJI = true;
      if (opt.name == "CBOE Volatility Index" && opt.value == "VIX") hasVIX = true;
    }

    REQUIRE(hasSPX);
    REQUIRE(hasDJI);
    REQUIRE(hasVIX);
  }

  SECTION("Has correct output fields") {
    // SDK returns 4 OHLC outputs (v is in requiredDataSources)
    REQUIRE(commonIndices.outputs.size() == 4);

    // Verify core OHLC outputs are present (SDK returns them as o, h, l, c)
    REQUIRE(commonIndices.outputs[0].id == "o");
    REQUIRE(commonIndices.outputs[0].name == "Open");
    REQUIRE(commonIndices.outputs[0].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonIndices.outputs[1].id == "h");
    REQUIRE(commonIndices.outputs[1].name == "High");
    REQUIRE(commonIndices.outputs[1].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonIndices.outputs[2].id == "l");
    REQUIRE(commonIndices.outputs[2].name == "Low");
    REQUIRE(commonIndices.outputs[2].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonIndices.outputs[3].id == "c");
    REQUIRE(commonIndices.outputs[3].name == "Close");
    REQUIRE(commonIndices.outputs[3].type == epoch_core::IODataType::Decimal);
  }

  SECTION("Has no input fields") {
    REQUIRE(commonIndices.inputs.empty());
  }

  SECTION("Has requiredDataSources for OHLC") {
    // Indices load data internally using template patterns (no volume)
    REQUIRE(commonIndices.requiredDataSources.size() == 4);
    REQUIRE(commonIndices.requiredDataSources[0] == "IDX:{ticker}:c");
    REQUIRE(commonIndices.requiredDataSources[1] == "IDX:{ticker}:o");
    REQUIRE(commonIndices.requiredDataSources[2] == "IDX:{ticker}:h");
    REQUIRE(commonIndices.requiredDataSources[3] == "IDX:{ticker}:l");
  }

  SECTION("Has strategy metadata") {
    REQUIRE(!commonIndices.strategyTypes.empty());
    REQUIRE(!commonIndices.assetRequirements.empty());
    REQUIRE(!commonIndices.usageContext.empty());
    REQUIRE(!commonIndices.limitations.empty());

    // Verify description contains OHLC information from SDK metadata
    REQUIRE(commonIndices.desc.find("OHLC") != std::string::npos);
  }
}

TEST_CASE("Dynamic Indices Configuration", "[polygon_indices][indices]") {
  auto metadataList = MakePolygonIndicesDataSources();
  auto& indices = metadataList[1];

  SECTION("Has ticker String parameter") {
    REQUIRE(indices.options.size() == 1);
    auto& tickerOption = indices.options[0];

    REQUIRE(tickerOption.id == "ticker");
    REQUIRE(tickerOption.name == "Index Ticker");
    REQUIRE(tickerOption.type == epoch_core::MetaDataOptionType::String);
    REQUIRE(tickerOption.desc == "Index ticker symbol (e.g., SPX, DJI, NDX, DAX, FTSE)");
  }

  SECTION("Has same output fields as common_indices") {
    // SDK returns 4 OHLC outputs
    REQUIRE(indices.outputs.size() == 4);

    // Verify core OHLC outputs are present (SDK returns them as o, h, l, c)
    REQUIRE(indices.outputs[0].id == "o");
    REQUIRE(indices.outputs[0].name == "Open");
    REQUIRE(indices.outputs[0].type == epoch_core::IODataType::Decimal);

    REQUIRE(indices.outputs[1].id == "h");
    REQUIRE(indices.outputs[1].name == "High");
    REQUIRE(indices.outputs[1].type == epoch_core::IODataType::Decimal);

    REQUIRE(indices.outputs[2].id == "l");
    REQUIRE(indices.outputs[2].name == "Low");
    REQUIRE(indices.outputs[2].type == epoch_core::IODataType::Decimal);

    REQUIRE(indices.outputs[3].id == "c");
    REQUIRE(indices.outputs[3].name == "Close");
    REQUIRE(indices.outputs[3].type == epoch_core::IODataType::Decimal);
  }

  SECTION("Has no input fields") {
    REQUIRE(indices.inputs.empty());
  }

  SECTION("Has requiredDataSources for OHLC") {
    // Indices load data internally using template patterns (no volume)
    REQUIRE(indices.requiredDataSources.size() == 4);
    REQUIRE(indices.requiredDataSources[0] == "IDX:{ticker}:c");
    REQUIRE(indices.requiredDataSources[1] == "IDX:{ticker}:o");
    REQUIRE(indices.requiredDataSources[2] == "IDX:{ticker}:h");
    REQUIRE(indices.requiredDataSources[3] == "IDX:{ticker}:l");
  }

  SECTION("Has comprehensive descriptions") {
    REQUIRE(!indices.desc.empty());
    REQUIRE(!indices.usageContext.empty());
    REQUIRE(!indices.limitations.empty());

    // Verify description contains OHLC information from SDK metadata
    REQUIRE(indices.desc.find("OHLC") != std::string::npos);
  }
}
