#include <catch2/catch_test_macros.hpp>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/registry.h>
#include "transforms/components/data_sources/reference_fx_metadata.h"

using namespace epoch_script::transforms;
using namespace epoch_script::transform;

TEST_CASE("Reference FX Metadata Registration", "[reference_fx]") {
  SECTION("MakeReferenceFXDataSources returns two nodes") {
    auto metadataList = MakeReferenceFXDataSources();
    REQUIRE(metadataList.size() == 2);
  }

  SECTION("Common FX Pairs node has correct basic properties") {
    auto metadataList = MakeReferenceFXDataSources();
    auto& commonFX = metadataList[0];

    REQUIRE(commonFX.id == "common_fx_pairs");
    REQUIRE(commonFX.name == "Common FX Pairs");
    REQUIRE(commonFX.category == epoch_core::TransformCategory::DataSource);
    REQUIRE(commonFX.plotKind == epoch_core::TransformPlotKind::close_line);
    REQUIRE(commonFX.requiresTimeFrame == true);
  }

  SECTION("Dynamic FX Pairs node has correct basic properties") {
    auto metadataList = MakeReferenceFXDataSources();
    auto& fx = metadataList[1];

    REQUIRE(fx.id == "fx_pairs");
    REQUIRE(fx.name == "FX Pairs");
    REQUIRE(fx.category == epoch_core::TransformCategory::DataSource);
    REQUIRE(fx.plotKind == epoch_core::TransformPlotKind::close_line);
    REQUIRE(fx.requiresTimeFrame == true);
  }
}

TEST_CASE("Common FX Pairs Configuration", "[reference_fx][common_fx_pairs]") {
  auto metadataList = MakeReferenceFXDataSources();
  auto& commonFX = metadataList[0];

  SECTION("Has ticker SelectOption parameter") {
    REQUIRE(commonFX.options.size() == 1);
    auto& tickerOption = commonFX.options[0];

    REQUIRE(tickerOption.id == "ticker");
    REQUIRE(tickerOption.name == "Currency Pair");
    REQUIRE(tickerOption.type == epoch_core::MetaDataOptionType::Select);
    REQUIRE(tickerOption.desc == "Select the FX currency pair");
  }

  SECTION("SelectOption contains common FX pairs") {
    auto& tickerOption = commonFX.options[0];
    REQUIRE(tickerOption.selectOption.size() == 10);

    // Verify key currency pairs are present
    bool hasEURUSD = false;
    bool hasGBPUSD = false;
    bool hasUSDJPY = false;

    for (const auto& opt : tickerOption.selectOption) {
      if (opt.value == "EURUSD") hasEURUSD = true;
      if (opt.value == "GBPUSD") hasGBPUSD = true;
      if (opt.value == "USDJPY") hasUSDJPY = true;
    }

    REQUIRE(hasEURUSD);
    REQUIRE(hasGBPUSD);
    REQUIRE(hasUSDJPY);
  }

  SECTION("Has correct output fields") {
    REQUIRE(commonFX.outputs.size() == 4);

    REQUIRE(commonFX.outputs[0].id == "o");
    REQUIRE(commonFX.outputs[0].name == "Open");
    REQUIRE(commonFX.outputs[0].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonFX.outputs[1].id == "h");
    REQUIRE(commonFX.outputs[1].name == "High");
    REQUIRE(commonFX.outputs[1].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonFX.outputs[2].id == "l");
    REQUIRE(commonFX.outputs[2].name == "Low");
    REQUIRE(commonFX.outputs[2].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonFX.outputs[3].id == "c");
    REQUIRE(commonFX.outputs[3].name == "Close");
    REQUIRE(commonFX.outputs[3].type == epoch_core::IODataType::Decimal);
  }

  SECTION("Has no input fields") {
    REQUIRE(commonFX.inputs.empty());
  }

  SECTION("Has requiredDataSources with FX prefix") {
    REQUIRE(commonFX.requiredDataSources.size() == 4);
    REQUIRE(commonFX.requiredDataSources[0] == "FX:{ticker}:c");
    REQUIRE(commonFX.requiredDataSources[1] == "FX:{ticker}:o");
    REQUIRE(commonFX.requiredDataSources[2] == "FX:{ticker}:h");
    REQUIRE(commonFX.requiredDataSources[3] == "FX:{ticker}:l");
  }

  SECTION("Has strategy metadata") {
    REQUIRE(!commonFX.strategyTypes.empty());
    REQUIRE(!commonFX.assetRequirements.empty());
    REQUIRE(!commonFX.usageContext.empty());
    REQUIRE(!commonFX.limitations.empty());
  }
}

TEST_CASE("Dynamic FX Pairs Configuration", "[reference_fx][fx_pairs]") {
  auto metadataList = MakeReferenceFXDataSources();
  auto& fx = metadataList[1];

  SECTION("Has ticker String parameter") {
    REQUIRE(fx.options.size() == 1);
    auto& tickerOption = fx.options[0];

    REQUIRE(tickerOption.id == "ticker");
    REQUIRE(tickerOption.name == "Currency Pair");
    REQUIRE(tickerOption.type == epoch_core::MetaDataOptionType::String);
  }

  SECTION("Has same output fields as common_fx_pairs") {
    REQUIRE(fx.outputs.size() == 4);

    REQUIRE(fx.outputs[0].id == "o");
    REQUIRE(fx.outputs[1].id == "h");
    REQUIRE(fx.outputs[2].id == "l");
    REQUIRE(fx.outputs[3].id == "c");
  }

  SECTION("Has no input fields") {
    REQUIRE(fx.inputs.empty());
  }

  SECTION("Has requiredDataSources with FX prefix") {
    REQUIRE(fx.requiredDataSources.size() == 4);
    REQUIRE(fx.requiredDataSources[0] == "FX:{ticker}:c");
    REQUIRE(fx.requiredDataSources[1] == "FX:{ticker}:o");
    REQUIRE(fx.requiredDataSources[2] == "FX:{ticker}:h");
    REQUIRE(fx.requiredDataSources[3] == "FX:{ticker}:l");
  }
}
