#include "../../../../include/epoch_script/data/database/database.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "mocks.h"
#include <epoch_script/data/common/constants.h>
#include <catch2/catch_test_macros.hpp>
#include <trompeloeil/mock.hpp>
#include <epoch_data_sdk/events/all.h>

using namespace epoch_script::data;
using namespace epoch_script;
using namespace epoch_frame;
using namespace epoch_frame::factory;

TEST_CASE("Database wraps impl", "[Database]") {
  auto mock_impl = std::make_unique<MockDatabaseImpl>();
  const auto &C = EpochScriptAssetConstants::instance();

  SECTION("Calling RunPipeline calls impl") {
    REQUIRE_CALL(*mock_impl, RunPipeline(trompeloeil::_));
    data_sdk::events::ScopedProgressEmitter emitter;
    Database(std::move(mock_impl)).RunPipeline(emitter);
  }

  SECTION("Calling GetTransformedData returns impl's transformed data") {
      TransformedDataType empty;
      REQUIRE_CALL(*mock_impl, GetTransformedData())
          .LR_RETURN(empty);

      Database db(std::move(mock_impl));
      REQUIRE(db.GetTransformedData().empty());
  }

  SECTION("Calling GetDataCategory returns impl's data category") {
    REQUIRE_CALL(*mock_impl, GetDataCategory()).RETURN(DataCategory::DailyBars);
    REQUIRE(Database(std::move(mock_impl)).GetDataCategory() ==
            DataCategory::DailyBars);
  }

  SECTION("Calling GetAssets returns impl's assets") {
    asset::AssetHashSet assets{C.AAPL};
    REQUIRE_CALL(*mock_impl, GetAssets()).RETURN(assets);
    REQUIRE(Database(std::move(mock_impl)).GetAssets() == assets);
  }

  SECTION("Calling GetBaseTimeframe returns impl's base timeframe") {
    REQUIRE_CALL(*mock_impl, GetBaseTimeframe()).RETURN("1D");
    REQUIRE(Database(std::move(mock_impl)).GetBaseTimeframe() == "1D");
  }

  SECTION("Calling GetFrontContract returns impl's front contract") {
    REQUIRE_CALL(*mock_impl, GetFrontContract(C.AAPL, epoch_frame::DateTime()))
        .RETURN(std::optional<std::string>("AAPL"));
    REQUIRE(Database(std::move(mock_impl))
                .GetFrontContract(C.AAPL, epoch_frame::DateTime())
                .value() == "AAPL");
  }
}

TEST_CASE("Database handles indexer data correctly", "[Database]") {
  auto mock_impl = std::make_unique<MockDatabaseImpl>();
  const auto &C = EpochScriptAssetConstants::instance();

  SECTION("GetTimestampIndex returns empty when no data") {
    TimestampIndex emptyIndex{};
    REQUIRE_CALL(*mock_impl, GetTimestampIndex()).LR_RETURN(emptyIndex);
    Database db(std::move(mock_impl));
    bool handlerWasCalled = false;
    auto dataHandler = [&](auto &&...) { handlerWasCalled = true; };
    db.HandleData(dataHandler, epoch_frame::DateTime("2021-01-01"__date));
    REQUIRE_FALSE(handlerWasCalled);
  }

  SECTION("GetTimestampIndex processes single timeframe-asset pair") {
    epoch_frame::DateTime dt = "2021-01-01"__date;
    // Mock data setup with O(1) timestamp index
    TimestampIndex mockIndex;
    mockIndex[dt.timestamp().value] = {{TimeFrameNotation("1D"), C.AAPL, {0, 0}}};

    TransformedDataType transformedData;
    auto df = make_dataframe<double>(factory::index::make_datetime_index({dt}),
                                     {std::vector<double>{100}}, {"col"});
    transformedData[TimeFrameNotation("1D")][C.AAPL] = df;

    ALLOW_CALL(*mock_impl, GetTimestampIndex()).LR_RETURN(mockIndex);
    ALLOW_CALL(*mock_impl, GetTransformedData()).LR_RETURN(transformedData);

    Database db(std::move(mock_impl));
    bool handlerWasCalled = false;

    // Test data retrieval for specific timestamp
    auto dataHandler = [&](TimeFrameNotation const &expectedTimeframe,
                           asset::Asset const &expectedAsset,
                           epoch_frame::DataFrame const &expectedDf,
                           epoch_frame::DateTime const &expectedDt) {
      handlerWasCalled = true;
      INFO("expectedTimeframe: " << expectedTimeframe);
      INFO("expectedAsset: " << expectedAsset);
      INFO("expectedDf: " << expectedDf);
      INFO("expectedDt: " << expectedDt);

      REQUIRE(expectedTimeframe == "1D");
      REQUIRE(expectedAsset == C.AAPL);
      REQUIRE(expectedDf.equals(df));
      REQUIRE(expectedDt == dt);
    };
    db.HandleData(dataHandler, dt);
    REQUIRE(handlerWasCalled);
  }

  SECTION("GetTimestampIndex handles multiple timeframe-asset pairs") {
    epoch_frame::DateTime dt1 = "2021-01-01"__date;
    epoch_frame::DateTime dt2 = "2021-01-02"__date;

    // Create O(1) timestamp index with multiple entries
    TimestampIndex mockIndex;
    mockIndex[dt1.timestamp().value] = {{TimeFrameNotation("1D"), C.AAPL, {0, 0}}};
    mockIndex[dt2.timestamp().value] = {{TimeFrameNotation("1H"), C.MSFT, {0, 0}}};

    TransformedDataType transformedData;
    auto df1 =
        make_dataframe<double>(factory::index::make_datetime_index({dt1}),
                               {std::vector<double>{100}}, {"col"});
    auto df2 =
        make_dataframe<double>(factory::index::make_datetime_index({dt2}),
                               {std::vector<double>{200}}, {"col"});
    transformedData[TimeFrameNotation("1D")][C.AAPL] = df1;
    transformedData[TimeFrameNotation("1H")][C.MSFT] = df2;

    ALLOW_CALL(*mock_impl, GetTimestampIndex()).LR_RETURN(mockIndex);
    ALLOW_CALL(*mock_impl, GetTransformedData()).LR_RETURN(transformedData);

    Database db(std::move(mock_impl));
    std::vector<std::tuple<TimeFrameNotation, asset::Asset,
                           epoch_frame::DataFrame, epoch_frame::DateTime>>
        calls;
    auto dataHandler =
        [&](TimeFrameNotation const &tf, asset::Asset const &asset,
            epoch_frame::DataFrame const &df, epoch_frame::DateTime const &dt) {
          calls.emplace_back(tf, asset, df, dt);
        };
    db.HandleData(dataHandler, dt1);
    db.HandleData(dataHandler, dt2);
    REQUIRE(calls.size() == 2);
    REQUIRE(std::get<0>(calls[0]) == "1D");
    REQUIRE(std::get<1>(calls[0]) == C.AAPL);
    REQUIRE(std::get<2>(calls[0]).equals(df1));
    REQUIRE(std::get<3>(calls[0]) == dt1);
    REQUIRE(std::get<0>(calls[1]) == "1H");
    REQUIRE(std::get<1>(calls[1]) == C.MSFT);
    REQUIRE(std::get<2>(calls[1]).equals(df2));
    REQUIRE(std::get<3>(calls[1]) == dt2);
  }

  SECTION("Handles timestamp not found in timestamp index") {
    epoch_frame::DateTime dt = "2021-01-01"__date;
    TimestampIndex mockIndex; // Empty index - no entry for dt.timestamp().value
    TransformedDataType transformedData;
    auto df = make_dataframe<double>(factory::index::make_datetime_index({dt}),
                                     {std::vector<double>{100}}, {"col"});
    transformedData[TimeFrameNotation("1D")][C.AAPL] = df;
    ALLOW_CALL(*mock_impl, GetTimestampIndex()).LR_RETURN(mockIndex);
    ALLOW_CALL(*mock_impl, GetTransformedData()).LR_RETURN(transformedData);
    Database db(std::move(mock_impl));
    bool handlerWasCalled = false;
    auto dataHandler = [&](auto &&...) { handlerWasCalled = true; };
    db.HandleData(dataHandler, dt);
    REQUIRE_FALSE(handlerWasCalled);
  }

  SECTION("Handles data retrieval with valid timestamp") {
    epoch_frame::DateTime dt = "2021-01-01"__date;
    TimestampIndex mockIndex;
    mockIndex[dt.timestamp().value] = {{TimeFrameNotation("1D"), C.AAPL, {0, 0}}};
    TransformedDataType transformedData;
    auto df = make_dataframe<double>(factory::index::make_datetime_index({dt}),
                                     {std::vector<double>{100}}, {"col"});
    transformedData[TimeFrameNotation("1D")][C.AAPL] = df;
    ALLOW_CALL(*mock_impl, GetTimestampIndex()).LR_RETURN(mockIndex);
    ALLOW_CALL(*mock_impl, GetTransformedData()).LR_RETURN(transformedData);
    Database db(std::move(mock_impl));
    bool handlerWasCalled = false;
    auto dataHandler = [&](TimeFrameNotation const &expectedTimeframe,
                           asset::Asset const &expectedAsset,
                           epoch_frame::DataFrame const &expectedDf,
                           epoch_frame::DateTime const &expectedDt) {
      handlerWasCalled = true;
      REQUIRE(expectedTimeframe == "1D");
      REQUIRE(expectedAsset == C.AAPL);
      REQUIRE(expectedDf.equals(df));
      REQUIRE(expectedDt == dt);
    };
    db.HandleData(dataHandler, dt);
    REQUIRE(handlerWasCalled);
  }

  SECTION("Handles futures with duplicate index and different contract") {
    epoch_frame::DateTime dt = "2021-01-01"__date;
    // Create two futures contracts with the same timestamp but different
    // contracts
    auto GC = C.GC;
    auto contract1 = GC.MakeContract(data_sdk::Symbol{"GCF24"}); // e.g., Feb 2024
    auto contract2 = GC.MakeContract(data_sdk::Symbol{"GCM24"}); // e.g., June 2024

    // Create O(1) timestamp index with multiple entries for same timestamp
    TimestampIndex mockIndex;
    mockIndex[dt.timestamp().value] = std::vector<TimestampIndexEntry>{
        TimestampIndexEntry{TimeFrameNotation("1D"), contract1, IndexRange{0, 0}},
        TimestampIndexEntry{TimeFrameNotation("1D"), contract2, IndexRange{0, 0}}
    };

    TransformedDataType transformedData;
    auto df1 = make_dataframe<double>(factory::index::make_datetime_index({dt}),
                                      {std::vector<double>{100}}, {"col"});
    auto df2 = make_dataframe<double>(factory::index::make_datetime_index({dt}),
                                      {std::vector<double>{200}}, {"col"});
    transformedData[TimeFrameNotation("1D")][contract1] = df1;
    transformedData[TimeFrameNotation("1D")][contract2] = df2;
    ALLOW_CALL(*mock_impl, GetTimestampIndex()).LR_RETURN(mockIndex);
    ALLOW_CALL(*mock_impl, GetTransformedData()).LR_RETURN(transformedData);
    Database db(std::move(mock_impl));
    std::vector<std::tuple<asset::Asset, epoch_frame::DataFrame>> calls;
    auto dataHandler = [&](TimeFrameNotation const &tf,
                           asset::Asset const &asset,
                           epoch_frame::DataFrame const &df,
                           epoch_frame::DateTime const &dt_) {
      (void)tf;
      (void)dt_;
      calls.emplace_back(asset, df);
    };
    db.HandleData(dataHandler, dt);
    REQUIRE(calls.size() == 2);
    REQUIRE((std::get<0>(calls[0]) == contract1 ||
             std::get<0>(calls[1]) == contract1));
    REQUIRE((std::get<0>(calls[0]) == contract2 ||
             std::get<0>(calls[1]) == contract2));
    // Check that both dataframes are present
    REQUIRE((std::get<1>(calls[0]).equals(df1) ||
             std::get<1>(calls[1]).equals(df1)));
    REQUIRE((std::get<1>(calls[0]).equals(df2) ||
             std::get<1>(calls[1]).equals(df2)));
  }
}
