#include <catch2/catch_test_macros.hpp>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/registry.h>
#include "transforms/components/data_sources/reference_crypto_metadata.h"

using namespace epoch_script::transforms;
using namespace epoch_script::transform;

TEST_CASE("Reference Crypto Metadata Registration", "[reference_crypto]") {
  SECTION("MakeReferenceCryptoDataSources returns two nodes") {
    auto metadataList = MakeReferenceCryptoDataSources();
    REQUIRE(metadataList.size() == 2);
  }

  SECTION("Common Crypto Pairs node has correct basic properties") {
    auto metadataList = MakeReferenceCryptoDataSources();
    auto& commonCrypto = metadataList[0];

    REQUIRE(commonCrypto.id == "common_crypto_pairs");
    REQUIRE(commonCrypto.name == "Common Crypto Pairs");
    REQUIRE(commonCrypto.category == epoch_core::TransformCategory::DataSource);
    REQUIRE(commonCrypto.plotKind == epoch_core::TransformPlotKind::close_line);
    REQUIRE(commonCrypto.requiresTimeFrame == true);
  }

  SECTION("Dynamic Crypto Pairs node has correct basic properties") {
    auto metadataList = MakeReferenceCryptoDataSources();
    auto& crypto = metadataList[1];

    REQUIRE(crypto.id == "crypto_pairs");
    REQUIRE(crypto.name == "Crypto Pairs");
    REQUIRE(crypto.category == epoch_core::TransformCategory::DataSource);
    REQUIRE(crypto.plotKind == epoch_core::TransformPlotKind::close_line);
    REQUIRE(crypto.requiresTimeFrame == true);
  }
}

TEST_CASE("Common Crypto Pairs Configuration", "[reference_crypto][common_crypto_pairs]") {
  auto metadataList = MakeReferenceCryptoDataSources();
  auto& commonCrypto = metadataList[0];

  SECTION("Has ticker SelectOption parameter") {
    REQUIRE(commonCrypto.options.size() == 1);
    auto& tickerOption = commonCrypto.options[0];

    REQUIRE(tickerOption.id == "ticker");
    REQUIRE(tickerOption.name == "Crypto Pair");
    REQUIRE(tickerOption.type == epoch_core::MetaDataOptionType::Select);
    REQUIRE(tickerOption.desc == "Select the cryptocurrency pair");
  }

  SECTION("SelectOption contains common crypto pairs") {
    auto& tickerOption = commonCrypto.options[0];
    REQUIRE(tickerOption.selectOption.size() == 10);

    // Verify key cryptocurrencies are present
    bool hasBTCUSD = false;
    bool hasETHUSD = false;
    bool hasSOLUSD = false;

    for (const auto& opt : tickerOption.selectOption) {
      if (opt.value == "BTCUSD") hasBTCUSD = true;
      if (opt.value == "ETHUSD") hasETHUSD = true;
      if (opt.value == "SOLUSD") hasSOLUSD = true;
    }

    REQUIRE(hasBTCUSD);
    REQUIRE(hasETHUSD);
    REQUIRE(hasSOLUSD);
  }

  SECTION("Has correct output fields") {
    REQUIRE(commonCrypto.outputs.size() == 4);

    REQUIRE(commonCrypto.outputs[0].id == "o");
    REQUIRE(commonCrypto.outputs[0].name == "Open");
    REQUIRE(commonCrypto.outputs[0].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonCrypto.outputs[1].id == "h");
    REQUIRE(commonCrypto.outputs[1].name == "High");
    REQUIRE(commonCrypto.outputs[1].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonCrypto.outputs[2].id == "l");
    REQUIRE(commonCrypto.outputs[2].name == "Low");
    REQUIRE(commonCrypto.outputs[2].type == epoch_core::IODataType::Decimal);

    REQUIRE(commonCrypto.outputs[3].id == "c");
    REQUIRE(commonCrypto.outputs[3].name == "Close");
    REQUIRE(commonCrypto.outputs[3].type == epoch_core::IODataType::Decimal);
  }

  SECTION("Has no input fields") {
    REQUIRE(commonCrypto.inputs.empty());
  }

  SECTION("Has requiredDataSources with CRYPTO prefix") {
    REQUIRE(commonCrypto.requiredDataSources.size() == 4);
    REQUIRE(commonCrypto.requiredDataSources[0] == "CRYPTO:{ticker}:c");
    REQUIRE(commonCrypto.requiredDataSources[1] == "CRYPTO:{ticker}:o");
    REQUIRE(commonCrypto.requiredDataSources[2] == "CRYPTO:{ticker}:h");
    REQUIRE(commonCrypto.requiredDataSources[3] == "CRYPTO:{ticker}:l");
  }

  SECTION("Has strategy metadata") {
    REQUIRE(!commonCrypto.strategyTypes.empty());
    REQUIRE(!commonCrypto.assetRequirements.empty());
    REQUIRE(!commonCrypto.usageContext.empty());
    REQUIRE(!commonCrypto.limitations.empty());
  }
}

TEST_CASE("Dynamic Crypto Pairs Configuration", "[reference_crypto][crypto_pairs]") {
  auto metadataList = MakeReferenceCryptoDataSources();
  auto& crypto = metadataList[1];

  SECTION("Has ticker String parameter") {
    REQUIRE(crypto.options.size() == 1);
    auto& tickerOption = crypto.options[0];

    REQUIRE(tickerOption.id == "ticker");
    REQUIRE(tickerOption.name == "Crypto Pair");
    REQUIRE(tickerOption.type == epoch_core::MetaDataOptionType::String);
  }

  SECTION("Has same output fields as common_crypto_pairs") {
    REQUIRE(crypto.outputs.size() == 4);

    REQUIRE(crypto.outputs[0].id == "o");
    REQUIRE(crypto.outputs[1].id == "h");
    REQUIRE(crypto.outputs[2].id == "l");
    REQUIRE(crypto.outputs[3].id == "c");
  }

  SECTION("Has no input fields") {
    REQUIRE(crypto.inputs.empty());
  }

  SECTION("Has requiredDataSources with CRYPTO prefix") {
    REQUIRE(crypto.requiredDataSources.size() == 4);
    REQUIRE(crypto.requiredDataSources[0] == "CRYPTO:{ticker}:c");
    REQUIRE(crypto.requiredDataSources[1] == "CRYPTO:{ticker}:o");
    REQUIRE(crypto.requiredDataSources[2] == "CRYPTO:{ticker}:h");
    REQUIRE(crypto.requiredDataSources[3] == "CRYPTO:{ticker}:l");
  }
}
