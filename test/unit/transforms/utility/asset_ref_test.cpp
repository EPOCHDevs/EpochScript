//
// Asset Reference Transform Unit Tests
// Tests EvaluateAssetRefTicker, EvaluateAssetRefClass helper functions
// and AssetRef transform behavior
//

#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>

#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/config_helper.h>

#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"

#include "transforms/components/utility/asset_ref_passthrough.h"

#include "epoch_script/core/bar_attribute.h"

using namespace epoch_core;
using namespace epoch_frame;
using namespace epoch_script;
using namespace epoch_script::transform;

// ============================================================================
// EvaluateAssetRefTicker tests
// ============================================================================

TEST_CASE("EvaluateAssetRefTicker exact match", "[asset_ref][ticker]") {
  REQUIRE(EvaluateAssetRefTicker("AAPL", "AAPL") == true);
  REQUIRE(EvaluateAssetRefTicker("SPY", "SPY") == true);
  REQUIRE(EvaluateAssetRefTicker("MSFT", "MSFT") == true);
}

TEST_CASE("EvaluateAssetRefTicker case insensitive", "[asset_ref][ticker]") {
  REQUIRE(EvaluateAssetRefTicker("AAPL", "aapl") == true);
  REQUIRE(EvaluateAssetRefTicker("aapl", "AAPL") == true);
  REQUIRE(EvaluateAssetRefTicker("AaPl", "aApL") == true);
  REQUIRE(EvaluateAssetRefTicker("spy", "SPY") == true);
  REQUIRE(EvaluateAssetRefTicker("SPY", "spy") == true);
}

TEST_CASE("EvaluateAssetRefTicker non-match", "[asset_ref][ticker]") {
  REQUIRE(EvaluateAssetRefTicker("AAPL", "MSFT") == false);
  REQUIRE(EvaluateAssetRefTicker("SPY", "QQQ") == false);
  REQUIRE(EvaluateAssetRefTicker("GOOG", "GOOGL") == false);
}

TEST_CASE("EvaluateAssetRefTicker empty filter matches all", "[asset_ref][ticker]") {
  REQUIRE(EvaluateAssetRefTicker("AAPL", "") == true);
  REQUIRE(EvaluateAssetRefTicker("SPY", "") == true);
  REQUIRE(EvaluateAssetRefTicker("anything", "") == true);
}

TEST_CASE("EvaluateAssetRefTicker empty asset returns false", "[asset_ref][ticker]") {
  REQUIRE(EvaluateAssetRefTicker("", "SPY") == false);
  REQUIRE(EvaluateAssetRefTicker("", "AAPL") == false);
}

TEST_CASE("EvaluateAssetRefTicker both empty", "[asset_ref][ticker]") {
  // Empty filter matches all, so even empty asset matches
  REQUIRE(EvaluateAssetRefTicker("", "") == true);
}

TEST_CASE("EvaluateAssetRefTicker special characters", "[asset_ref][ticker]") {
  // Test tickers with dots (e.g., BRK.B)
  REQUIRE(EvaluateAssetRefTicker("BRK.B", "BRK.B") == true);
  REQUIRE(EvaluateAssetRefTicker("BRK.B", "brk.b") == true);
  REQUIRE(EvaluateAssetRefTicker("BRK.B", "BRK.A") == false);

  // Test tickers with hyphens
  REQUIRE(EvaluateAssetRefTicker("ES-FUT", "ES-FUT") == true);
  REQUIRE(EvaluateAssetRefTicker("ES-FUT", "es-fut") == true);
}

TEST_CASE("EvaluateAssetRefTicker partial match is false", "[asset_ref][ticker]") {
  // Partial matches should NOT match
  REQUIRE(EvaluateAssetRefTicker("AAPL", "AAP") == false);
  REQUIRE(EvaluateAssetRefTicker("AAPL", "AAPLX") == false);
  REQUIRE(EvaluateAssetRefTicker("SPY", "SP") == false);
  REQUIRE(EvaluateAssetRefTicker("SPY", "SPYG") == false);
}

// ============================================================================
// EvaluateAssetRefClass tests
// ============================================================================

TEST_CASE("EvaluateAssetRefClass empty filter matches all", "[asset_ref][class]") {
  REQUIRE(EvaluateAssetRefClass("AAPL", "") == true);
  REQUIRE(EvaluateAssetRefClass("BTC", "") == true);
  REQUIRE(EvaluateAssetRefClass("ES", "") == true);
}

TEST_CASE("EvaluateAssetRefClass with filter returns false when asset not in database", "[asset_ref][class]") {
  // When asset class filter is specified but asset isn't in the database,
  // the function returns false since it can't verify the asset class.
  // In production, assets would be loaded into AssetSpecificationDatabase.
  REQUIRE(EvaluateAssetRefClass("AAPL", "Stocks") == false);
  REQUIRE(EvaluateAssetRefClass("BTC", "Crypto") == false);
  REQUIRE(EvaluateAssetRefClass("ES", "Futures") == false);
  REQUIRE(EvaluateAssetRefClass("EURUSD", "FX") == false);
  REQUIRE(EvaluateAssetRefClass("SPX", "Indices") == false);
}

// ============================================================================
// AssetRefPassthrough transform tests
// ============================================================================

TEST_CASE("AssetRefPassthroughNumber transform throws on direct call", "[asset_ref][transform]") {
  const auto tf = EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto cfg = run_op("asset_ref_passthrough", "asset_ref_test",
      {{ARG, {input_ref("src", "close")}}},
      {{"ticker", MetaDataOptionDefinition{std::string("SPY")}}},
      tf);

  AssetRefPassthroughNumber transform{cfg};

  // Create minimal dataframe
  std::vector<int64_t> ticks{0, 1, 2};
  auto idx_arr = factory::array::make_contiguous_array(ticks);
  auto index = factory::index::make_index(idx_arr, MonotonicDirection::Increasing, "i");
  auto df = make_dataframe<double>(index, {{1.0, 2.0, 3.0}}, {"src#close"});

  // AssetRefPassthrough::TransformData should throw
  REQUIRE_THROWS_WITH(transform.TransformData(df),
      Catch::Matchers::ContainsSubstring("should not be called directly"));
}

TEST_CASE("AssetRefPassthroughNumber construction with ticker option", "[asset_ref][transform]") {
  const auto tf = EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto cfg = run_op("asset_ref_passthrough", "asset_ref_ticker",
      {{ARG, {input_ref("src", "close")}}},
      {{"ticker", MetaDataOptionDefinition{std::string("AAPL")}}},
      tf);

  // Should construct without throwing
  REQUIRE_NOTHROW(AssetRefPassthroughNumber{cfg});
}

TEST_CASE("AssetRefPassthroughNumber construction with asset_class option", "[asset_ref][transform]") {
  const auto tf = EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto cfg = run_op("asset_ref_passthrough", "asset_ref_class",
      {{ARG, {input_ref("src", "close")}}},
      {{"asset_class", MetaDataOptionDefinition{std::string("Stocks")}}},
      tf);

  // Should construct without throwing
  REQUIRE_NOTHROW(AssetRefPassthroughNumber{cfg});
}

TEST_CASE("AssetRefPassthroughNumber construction with both options", "[asset_ref][transform]") {
  const auto tf = EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto cfg = run_op("asset_ref_passthrough", "asset_ref_both",
      {{ARG, {input_ref("src", "close")}}},
      {{"ticker", MetaDataOptionDefinition{std::string("SPY")}},
       {"asset_class", MetaDataOptionDefinition{std::string("Stocks")}}},
      tf);

  // Should construct without throwing
  REQUIRE_NOTHROW(AssetRefPassthroughNumber{cfg});
}
