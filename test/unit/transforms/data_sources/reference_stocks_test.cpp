#include <catch2/catch_test_macros.hpp>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/registry.h>
#include "transforms/components/data_sources/reference_stocks_metadata.h"

using namespace epoch_script::transforms;
using namespace epoch_script::transform;

TEST_CASE("Reference Stocks Metadata Registration", "[reference_stocks]") {
  SECTION("MakeReferenceStocksDataSources returns two nodes") {
    auto metadataList = MakeReferenceStocksDataSources();
    REQUIRE(metadataList.size() == 2);
  }

  SECTION("Common Reference Stocks node has correct basic properties") {
    auto metadataList = MakeReferenceStocksDataSources();
    auto& commonStocks = metadataList[0];

    REQUIRE(commonStocks.id == "common_reference_stocks");
    REQUIRE(commonStocks.name == "Common Reference Stocks");
    REQUIRE(commonStocks.category == epoch_core::TransformCategory::DataSource);
    REQUIRE(commonStocks.plotKind == epoch_core::TransformPlotKind::close_line);
    REQUIRE(commonStocks.requiresTimeFrame == true);
  }

  SECTION("Dynamic Reference Stocks node has correct basic properties") {
    auto metadataList = MakeReferenceStocksDataSources();
    auto& stocks = metadataList[1];

    REQUIRE(stocks.id == "reference_stocks");
    REQUIRE(stocks.name == "Reference Stocks");
    REQUIRE(stocks.category == epoch_core::TransformCategory::DataSource);
    REQUIRE(stocks.plotKind == epoch_core::TransformPlotKind::close_line);
    REQUIRE(stocks.requiresTimeFrame == true);
  }
}

TEST_CASE("Common Reference Stocks Configuration", "[reference_stocks][common_reference_stocks]") {
  auto metadataList = MakeReferenceStocksDataSources();
  auto& commonStocks = metadataList[0];

  SECTION("Has ticker SelectOption parameter") {
    REQUIRE(commonStocks.options.size() == 1);
    auto& tickerOption = commonStocks.options[0];

    REQUIRE(tickerOption.id == "ticker");
    REQUIRE(tickerOption.name == "Reference Stock");
    REQUIRE(tickerOption.type == epoch_core::MetaDataOptionType::Select);
    REQUIRE(tickerOption.desc == "Select the reference stock");
  }

  SECTION("SelectOption contains common reference stocks") {
    auto& tickerOption = commonStocks.options[0];
    REQUIRE(tickerOption.selectOption.size() == 8);

    // Verify key stocks are present
    bool hasSPY = false;
    bool hasQQQ = false;
    bool hasGLD = false;

    for (const auto& opt : tickerOption.selectOption) {
      if (opt.value == "SPY") hasSPY = true;
      if (opt.value == "QQQ") hasQQQ = true;
      if (opt.value == "GLD") hasGLD = true;
    }

    REQUIRE(hasSPY);
    REQUIRE(hasQQQ);
    REQUIRE(hasGLD);
  }

  SECTION("Has correct output fields") {
    REQUIRE(commonStocks.outputs.size() == 4);

    REQUIRE(commonStocks.outputs[0].id == "o");
    REQUIRE(commonStocks.outputs[0].name == "Open");
    REQUIRE(commonStocks.outputs[0].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonStocks.outputs[1].id == "h");
    REQUIRE(commonStocks.outputs[1].name == "High");
    REQUIRE(commonStocks.outputs[1].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonStocks.outputs[2].id == "l");
    REQUIRE(commonStocks.outputs[2].name == "Low");
    REQUIRE(commonStocks.outputs[2].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonStocks.outputs[3].id == "c");
    REQUIRE(commonStocks.outputs[3].name == "Close");
    REQUIRE(commonStocks.outputs[3].type == epoch_core::IODataType::Decimal);
  }

  SECTION("Has no input fields") {
    REQUIRE(commonStocks.inputs.empty());
  }

  SECTION("Has requiredDataSources with STK prefix") {
    REQUIRE(commonStocks.requiredDataSources.size() == 4);
    REQUIRE(commonStocks.requiredDataSources[0] == "STK:{ticker}:c");
    REQUIRE(commonStocks.requiredDataSources[1] == "STK:{ticker}:o");
    REQUIRE(commonStocks.requiredDataSources[2] == "STK:{ticker}:h");
    REQUIRE(commonStocks.requiredDataSources[3] == "STK:{ticker}:l");
  }

  SECTION("Has strategy metadata") {
    REQUIRE(!commonStocks.strategyTypes.empty());
    REQUIRE(!commonStocks.assetRequirements.empty());
    REQUIRE(!commonStocks.usageContext.empty());
    REQUIRE(!commonStocks.limitations.empty());
  }
}

TEST_CASE("Dynamic Reference Stocks Configuration", "[reference_stocks][reference_stocks_dynamic]") {
  auto metadataList = MakeReferenceStocksDataSources();
  auto& stocks = metadataList[1];

  SECTION("Has ticker String parameter") {
    REQUIRE(stocks.options.size() == 1);
    auto& tickerOption = stocks.options[0];

    REQUIRE(tickerOption.id == "ticker");
    REQUIRE(tickerOption.name == "Reference Ticker");
    REQUIRE(tickerOption.type == epoch_core::MetaDataOptionType::String);
  }

  SECTION("Has same output fields as common_reference_stocks") {
    REQUIRE(stocks.outputs.size() == 4);

    REQUIRE(stocks.outputs[0].id == "o");
    REQUIRE(stocks.outputs[1].id == "h");
    REQUIRE(stocks.outputs[2].id == "l");
    REQUIRE(stocks.outputs[3].id == "c");
  }

  SECTION("Has no input fields") {
    REQUIRE(stocks.inputs.empty());
  }

  SECTION("Has requiredDataSources with STK prefix") {
    REQUIRE(stocks.requiredDataSources.size() == 4);
    REQUIRE(stocks.requiredDataSources[0] == "STK:{ticker}:c");
    REQUIRE(stocks.requiredDataSources[1] == "STK:{ticker}:o");
    REQUIRE(stocks.requiredDataSources[2] == "STK:{ticker}:h");
    REQUIRE(stocks.requiredDataSources[3] == "STK:{ticker}:l");
  }
}
