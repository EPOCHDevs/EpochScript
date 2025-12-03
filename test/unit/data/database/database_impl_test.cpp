#include "data/database/database_impl.h"
#include "mocks.h"
#include <catch2/catch_all.hpp>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/scalar_factory.h>

#include "epoch_frame/common.h"
#include <epoch_script/data/common/frame_utils.h>
#include <epoch_script/data/common/constants.h>
#include <epoch_data_sdk/events/all.h>

using namespace epoch_script::data;
using namespace epoch_script;
using namespace epoch_frame::factory;
using namespace epoch_frame::factory::scalar;
using namespace epoch_frame;

// Provide comparison operators for DataFrame to satisfy trompeloeil requirements
// These are dummy implementations since we don't actually use comparison in tests
namespace epoch_frame {
  inline bool operator==(const DataFrame&, const DataFrame&) {
    return true;  // Dummy implementation for trompeloeil
  }
}

// Provide comparison for AssetDataFrameMap
namespace epoch_script::data {
  inline bool operator==(const AssetDataFrameMap& a, const AssetDataFrameMap& b) {
    return a.size() == b.size();  // Simple check for trompeloeil
  }
}


TEST_CASE("DatabaseImpl throws on null dataloader", "[DatabaseImpl]") {
  DatabaseImplOptions opts;
  opts.dataloader = nullptr;
  opts.dataTransform = nullptr;
  // Other options can be left as default/null
  REQUIRE_THROWS_AS(DatabaseImpl(std::move(opts)), std::exception);
}

TEST_CASE("DatabaseImpl constructs with valid dataloader", "[DatabaseImpl]") {
  auto mock_loader = std::make_unique<MockDataloader>();
  auto category = GENERATE(DataCategory::DailyBars, DataCategory::MinuteBars,
                           DataCategory::Null);
  REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(category);

  DatabaseImplOptions opts;
  opts.dataloader = std::move(mock_loader);
  // Other options can be left as default/null
  if (category == DataCategory::Null) {
    REQUIRE_THROWS(DatabaseImpl(std::move(opts)));
  } else {
    REQUIRE_NOTHROW(DatabaseImpl(std::move(opts)));
  }
}

TEST_CASE("LoadData: single/multi asset, multi asset class, daily/minute",
          "[.][DatabaseImpl][LoadData]") {
  struct TestCase {
    std::string name;
    DataCategory category;
    std::vector<std::pair<asset::Asset, AssetClass>> assets;
    std::string expected_timeframe;
  };

  const auto AAPL =
      epoch_script::EpochScriptAssetConstants::instance().AAPL;
  const auto MSFT =
      epoch_script::EpochScriptAssetConstants::instance().MSFT;
  const auto BTC =
      epoch_script::EpochScriptAssetConstants::instance().BTC_USD;

  const auto cases = GENERATE_REF(
      TestCase{"Single asset daily",
               DataCategory::DailyBars,
               {{AAPL, AssetClass::Stocks}},
               "1D"},
      TestCase{"Multi asset daily",
               DataCategory::DailyBars,
               {{AAPL, AssetClass::Stocks}, {MSFT, AssetClass::Stocks}},
               "1D"},
      TestCase{"Multi asset class daily",
               DataCategory::DailyBars,
               {{AAPL, AssetClass::Stocks}, {BTC, AssetClass::Crypto}},
               "1D"},
      TestCase{"Single asset minute",
               DataCategory::MinuteBars,
               {{AAPL, AssetClass::Stocks}},
               "1Min"},
      TestCase{"Multi asset minute",
               DataCategory::MinuteBars,
               {{AAPL, AssetClass::Stocks}, {MSFT, AssetClass::Stocks}},
               "1Min"},
      TestCase{"Multi assetclass minute",
               DataCategory::MinuteBars,
               {{AAPL, AssetClass::Stocks}, {BTC, AssetClass::Crypto}},
               "1Min"});

  SECTION(cases.name) {
    // Build index
    auto index =
        cases.category == DataCategory::DailyBars
            ? index::date_range({.start = "2000-01-01"_date,
                                 .periods = 3,
                                 .offset = offset::days(1)})
            : index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                                 .periods = 3,
                                 .offset = offset::minutes(1)});

    data_sdk::IDataLoader::DataMap input;
    for (const auto &asset : cases.assets | std::views::keys) {
      input[asset] = make_random_ohlcv(index);
    }

    auto mock_loader = std::make_unique<MockDataloader>();
    REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(cases.category);
    REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(input);
    REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

    DatabaseImplOptions opts;
    opts.dataloader = std::move(mock_loader);

    auto db = DatabaseImpl(std::move(opts));
    data_sdk::events::ScopedProgressEmitter emitter;
    db.RunPipeline(emitter);

    for (const auto &asset : cases.assets | std::views::keys) {
      auto data = db.GetCurrentData(cases.expected_timeframe, asset);
      INFO("data: \n" << data);
      REQUIRE(data.equals(input.at(asset)));

      auto transformedData = db.GetTransformedData();
      REQUIRE(transformedData.size() == 1);
      REQUIRE_FALSE(transformedData.at(cases.expected_timeframe).empty());
      REQUIRE(transformedData.at(cases.expected_timeframe)
                  .at(asset)
                  .equals(input.at(asset)));

      // Should throw for a non-existent timeframe
      std::string wrong_tf = cases.expected_timeframe == "1D" ? "1Min" : "1D";
      REQUIRE_THROWS(db.GetCurrentData(wrong_tf, asset));
    }
  }
}

TEST_CASE("LoadData + Resampler: happy path for Minute and Daily",
          "[.][DatabaseImpl][Resampler]") {
  struct TestCase {
    std::string name;
    DataCategory category;
    std::string base_timeframe;
    std::string resampled_timeframe;
    DateOffsetHandlerPtr resample_offset;
  };

  const auto AAPL =
      epoch_script::EpochScriptAssetConstants::instance().AAPL;

  const auto cases =
      GENERATE_REF(TestCase{"MinuteBars to 1h", DataCategory::MinuteBars,
                            "1Min", "1h", offset::hours(1)},
                   TestCase{"DailyBars to 1w", DataCategory::DailyBars, "1D",
                            "1w", offset::weeks(1)});

  SECTION(cases.name) {
    // Build base index and data
    auto base_index =
        cases.category == DataCategory::MinuteBars
            ? index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                                 .periods = 60,
                                 .offset = offset::minutes(1)})
            : index::date_range({.start = "2000-01-01"_date,
                                 .periods = 5,
                                 .offset = offset::days(1)});
    data_sdk::IDataLoader::DataMap input;
    input[AAPL] = make_random_ohlcv(base_index);

    // Build resampled index and data (just 1 row for simplicity)
    auto resampled_index =
        index::date_range({.start = cases.category == DataCategory::MinuteBars
                                        ? "2000-01-01 09:30:00"_datetime
                                        : "2000-01-01"_date,
                           .periods = 1,
                           .offset = cases.resample_offset});
    auto resampled_df = make_random_ohlcv(resampled_index);

    // Mock loader
    auto mock_loader = std::make_unique<MockDataloader>();
    REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(cases.category);
    REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(input);
    REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

    // Mock resampler with input check
    auto mock_resampler = std::make_unique<MockResampler>();
    MockResampler::T resampled_vec = {
        std::make_tuple(cases.resampled_timeframe, AAPL, resampled_df)};
    REQUIRE_CALL(*mock_resampler, Build(trompeloeil::_, trompeloeil::_))
        .TIMES(1)
        .RETURN(resampled_vec);

    DatabaseImplOptions opts;
    opts.dataloader = std::move(mock_loader);
    opts.resampler = std::move(mock_resampler);

    auto db = DatabaseImpl(std::move(opts));
    data_sdk::events::ScopedProgressEmitter emitter;
    db.RunPipeline(emitter);

    // Check base timeframe
    auto base_data = db.GetCurrentData(cases.base_timeframe, AAPL);
    INFO("base_data: \n" << base_data);
    REQUIRE(base_data.equals(input.at(AAPL)));

    // Check resampled timeframe
    auto resampled_data = db.GetCurrentData(cases.resampled_timeframe, AAPL);
    INFO("resampled_data: \n" << resampled_data);
    REQUIRE(resampled_data.equals(resampled_df));

    // Check transformed data structure
    auto transformedData = db.GetTransformedData();
    REQUIRE(transformedData.count(cases.base_timeframe) == 1);
    REQUIRE(transformedData.count(cases.resampled_timeframe) == 1);
    REQUIRE(transformedData.at(cases.base_timeframe)
                .at(AAPL)
                .equals(input.at(AAPL)));
    REQUIRE(transformedData.at(cases.resampled_timeframe)
                .at(AAPL)
                .equals(resampled_df));
  }
}

TEST_CASE("LoadData + Resampler + Transform: transform base only",
          "[DatabaseImpl][Resampler][Transform]") {
  const auto AAPL =
      epoch_script::EpochScriptAssetConstants::instance().AAPL;
  const DataCategory category = DataCategory::MinuteBars;
  const std::string base_timeframe = "1Min";
  const std::string resampled_timeframe = "1h";

  // Build base index and data
  auto base_index = index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                                       .periods = 60,
                                       .offset = offset::minutes(1)});
  data_sdk::IDataLoader::DataMap input;
  input[AAPL] = make_random_ohlcv(base_index);

  // Build resampled index and data (just 1 row for simplicity)
  auto resampled_index =
      index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                         .periods = 1,
                         .offset = offset::hours(1)});
  auto resampled_df = make_random_ohlcv(resampled_index);

  // What the transform should return for each timeframe
  auto add_vwap = [](const DataFrame &df) {
    std::vector<double> vwap(df.num_rows(), 42.0);
    return df.assign("vwap", make_series(df.index(), vwap));
  };
  DataFrame expected_base = add_vwap(input.at(AAPL));
  DataFrame expected_resampled = resampled_df;

  // Prepare the static transform result
  epoch_script::runtime::TimeFrameAssetDataFrameMap transform_result;
  transform_result[base_timeframe][AAPL.GetID()] = expected_base;
  transform_result[resampled_timeframe][AAPL.GetID()] = expected_resampled;

  // Mock loader
  auto mock_loader = std::make_unique<MockDataloader>();
  REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(category);
  REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(input);
  REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

  // Mock resampler with input check
  auto mock_resampler = std::make_unique<MockResampler>();
  MockResampler::T resampled_vec = {
      std::make_tuple(resampled_timeframe, AAPL, resampled_df)};
  REQUIRE_CALL(*mock_resampler, Build(trompeloeil::_, trompeloeil::_))
      .TIMES(1)
      .RETURN(resampled_vec);

  // Mock transform: only base gets vwap (static)
  auto mock_transform = std::make_unique<MockTransformGraph>();
  REQUIRE_CALL(*mock_transform, ExecutePipeline(trompeloeil::_, trompeloeil::_))
      .RETURN(transform_result);
    REQUIRE_CALL(*mock_transform, GetGeneratedReports())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetReportMap{});  // Return empty report map
  REQUIRE_CALL(*mock_transform, GetGeneratedEventMarkers())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetEventMarkerMap{});  // Return empty event marker map

  DatabaseImplOptions opts;
  opts.dataloader = std::move(mock_loader);
  opts.resampler = std::move(mock_resampler);
  opts.dataTransform = std::move(mock_transform);

  auto db = DatabaseImpl(std::move(opts));
  data_sdk::events::ScopedProgressEmitter emitter;
  db.RunPipeline(emitter);

  {
    // Check base timeframe
    auto base_data = db.GetCurrentData(base_timeframe, AAPL);
    INFO("base_data: \n" << base_data);
    REQUIRE(base_data.equals(expected_base));
    REQUIRE(base_data.contains("vwap"));
  }

  {
    // Check resampled timeframe
    auto resampled_data = db.GetCurrentData(resampled_timeframe, AAPL);
    INFO("resampled_data: \n" << resampled_data);
    INFO("expected_resampled_data: \n" << expected_resampled);
    REQUIRE(resampled_data.equals(expected_resampled));
    REQUIRE_FALSE(resampled_data.contains("vwap"));
  }
}

TEST_CASE("LoadData + Resampler + Transform: transform resampled only",
          "[DatabaseImpl][Resampler][Transform]") {
  const auto AAPL =
      epoch_script::EpochScriptAssetConstants::instance().AAPL;
  const DataCategory category = DataCategory::MinuteBars;
  const std::string base_timeframe = "1Min";
  const std::string resampled_timeframe = "1h";

  // Build base index and data
  auto base_index = index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                                       .periods = 60,
                                       .offset = offset::minutes(1)});
  data_sdk::IDataLoader::DataMap input;
  input[AAPL] = make_random_ohlcv(base_index);

  // Build resampled index and data (just 1 row for simplicity)
  auto resampled_index =
      index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                         .periods = 1,
                         .offset = offset::hours(1)});
  auto resampled_df = make_random_ohlcv(resampled_index);

  // What the transform should return for each timeframe
  auto add_vwap = [](const DataFrame &df) {
    std::vector<double> vwap(df.num_rows(), 42.0);
    return df.assign("vwap", make_series(df.index(), vwap));
  };
  DataFrame expected_base = input.at(AAPL);
  DataFrame expected_resampled = add_vwap(resampled_df);

  // Prepare the static transform result
  epoch_script::runtime::TimeFrameAssetDataFrameMap transform_result;
  transform_result[base_timeframe][AAPL.GetID()] = expected_base;
  transform_result[resampled_timeframe][AAPL.GetID()] = expected_resampled;

  // Mock loader
  auto mock_loader = std::make_unique<MockDataloader>();
  REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(category);
  REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(input);
  REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

  // Mock resampler with input check
  auto mock_resampler = std::make_unique<MockResampler>();
  MockResampler::T resampled_vec = {
      std::make_tuple(resampled_timeframe, AAPL, resampled_df)};
  REQUIRE_CALL(*mock_resampler, Build(trompeloeil::_, trompeloeil::_))
      .TIMES(1)
      .RETURN(resampled_vec);

  // Mock transform: only resampled gets vwap (static)
  auto mock_transform = std::make_unique<MockTransformGraph>();
  REQUIRE_CALL(*mock_transform, ExecutePipeline(trompeloeil::_, trompeloeil::_))
      .RETURN(transform_result);
  REQUIRE_CALL(*mock_transform, GetGeneratedReports())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetReportMap{});  // Return empty report map
  REQUIRE_CALL(*mock_transform, GetGeneratedEventMarkers())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetEventMarkerMap{});  // Return empty event marker map

  DatabaseImplOptions opts;
  opts.dataloader = std::move(mock_loader);
  opts.resampler = std::move(mock_resampler);
  opts.dataTransform = std::move(mock_transform);

  auto db = DatabaseImpl(std::move(opts));
  data_sdk::events::ScopedProgressEmitter emitter;
  db.RunPipeline(emitter);

  // Check base timeframe
  auto base_data = db.GetCurrentData(base_timeframe, AAPL);
  INFO("base_data: \n" << base_data);
  REQUIRE(base_data.equals(expected_base));
  REQUIRE_FALSE(base_data.contains("vwap"));

  // Check resampled timeframe
  auto resampled_data = db.GetCurrentData(resampled_timeframe, AAPL);
  INFO("resampled_data: \n" << resampled_data);
  REQUIRE(resampled_data.equals(expected_resampled));
  REQUIRE(resampled_data.contains("vwap"));
}

TEST_CASE("LoadData + Resampler + Transform: transform both",
          "[DatabaseImpl][Resampler][Transform]") {
  const auto AAPL =
      epoch_script::EpochScriptAssetConstants::instance().AAPL;
  const DataCategory category = DataCategory::MinuteBars;
  const std::string base_timeframe = "1Min";
  const std::string resampled_timeframe = "1h";

  // Build base index and data
  auto base_index = index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                                       .periods = 60,
                                       .offset = offset::minutes(1)});
  data_sdk::IDataLoader::DataMap input;
  input[AAPL] = make_random_ohlcv(base_index);

  // Build resampled index and data (just 1 row for simplicity)
  auto resampled_index =
      index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                         .periods = 1,
                         .offset = offset::hours(1)});
  auto resampled_df = make_random_ohlcv(resampled_index);

  // What the transform should return for each timeframe
  auto add_vwap = [](const DataFrame &df) {
    std::vector<double> vwap(df.num_rows(), 42.0);
    return df.assign("vwap", make_series(df.index(), vwap));
  };
  DataFrame expected_base = add_vwap(input.at(AAPL));
  DataFrame expected_resampled = add_vwap(resampled_df);

  // Prepare the static transform result
  epoch_script::runtime::TimeFrameAssetDataFrameMap transform_result;
  transform_result[base_timeframe][AAPL.GetID()] = expected_base;
  transform_result[resampled_timeframe][AAPL.GetID()] = expected_resampled;

  // Mock loader
  auto mock_loader = std::make_unique<MockDataloader>();
  REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(category);
  REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(input);
  REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

  // Mock resampler with input check
  auto mock_resampler = std::make_unique<MockResampler>();
  MockResampler::T resampled_vec = {
      std::make_tuple(resampled_timeframe, AAPL, resampled_df)};
  REQUIRE_CALL(*mock_resampler, Build(trompeloeil::_, trompeloeil::_))
      .TIMES(1)
      .RETURN(resampled_vec);

  // Mock transform: both get vwap (static)
  auto mock_transform = std::make_unique<MockTransformGraph>();
  REQUIRE_CALL(*mock_transform, ExecutePipeline(trompeloeil::_, trompeloeil::_))
      .RETURN(transform_result);
  REQUIRE_CALL(*mock_transform, GetGeneratedReports())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetReportMap{});  // Return empty report map
  REQUIRE_CALL(*mock_transform, GetGeneratedEventMarkers())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetEventMarkerMap{});  // Return empty event marker map

  DatabaseImplOptions opts;
  opts.dataloader = std::move(mock_loader);
  opts.resampler = std::move(mock_resampler);
  opts.dataTransform = std::move(mock_transform);

  auto db = DatabaseImpl(std::move(opts));
  data_sdk::events::ScopedProgressEmitter emitter;
  db.RunPipeline(emitter);

  // Check base timeframe
  auto base_data = db.GetCurrentData(base_timeframe, AAPL);
  INFO("base_data: \n" << base_data);
  REQUIRE(base_data.equals(expected_base));
  REQUIRE(base_data.contains("vwap"));

  // Check resampled timeframe
  auto resampled_data = db.GetCurrentData(resampled_timeframe, AAPL);
  INFO("resampled_data: \n" << resampled_data);
  REQUIRE(resampled_data.equals(expected_resampled));
  REQUIRE(resampled_data.contains("vwap"));
}

TEST_CASE("LoadData + Resampler + Transform: unique transforms per timeframe",
          "[DatabaseImpl][Resampler][Transform]") {
  const auto AAPL =
      epoch_script::EpochScriptAssetConstants::instance().AAPL;
  const DataCategory category = DataCategory::MinuteBars;
  const std::string base_timeframe = "1Min";
  const std::string resampled_timeframe = "1h";

  // Build base index and data
  auto base_index = index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                                       .periods = 60,
                                       .offset = offset::minutes(1)});
  data_sdk::IDataLoader::DataMap input;
  input[AAPL] = make_random_ohlcv(base_index);

  // Build resampled index and data (just 1 row for simplicity)
  auto resampled_index =
      index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                         .periods = 1,
                         .offset = offset::hours(1)});
  auto resampled_df = make_random_ohlcv(resampled_index);

  // Build expected DataFrames
  auto add_vwap = [](const DataFrame &df) {
    std::vector<double> vwap(df.num_rows(), 42.0);
    return df.assign("vwap", make_series(df.index(), vwap));
  };
  auto add_zscore = [](const DataFrame &df) {
    std::vector<double> zscore(df.num_rows(), 1.23);
    return df.assign("zscore", make_series(df.index(), zscore));
  };
  DataFrame expected_base = add_vwap(input.at(AAPL));
  DataFrame expected_resampled = add_zscore(resampled_df);

  // Prepare the static transform result
  epoch_script::runtime::TimeFrameAssetDataFrameMap transform_result;
  transform_result[base_timeframe][AAPL.GetID()] = expected_base;
  transform_result[resampled_timeframe][AAPL.GetID()] = expected_resampled;

  // Mock loader
  auto mock_loader = std::make_unique<MockDataloader>();
  REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(category);
  REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(input);
  REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

  // Mock resampler with input check
  auto mock_resampler = std::make_unique<MockResampler>();
  MockResampler::T resampled_vec = {
      std::make_tuple(resampled_timeframe, AAPL, resampled_df)};
  REQUIRE_CALL(*mock_resampler, Build(trompeloeil::_, trompeloeil::_))
      .TIMES(1)
      .RETURN(resampled_vec);

  // Mock transform: static result
  auto mock_transform = std::make_unique<MockTransformGraph>();
  REQUIRE_CALL(*mock_transform, GetGeneratedReports())
    .TIMES(AT_LEAST(0))
      .RETURN(epoch_script::runtime::AssetReportMap{});  // Return empty report map
  REQUIRE_CALL(*mock_transform, GetGeneratedEventMarkers())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetEventMarkerMap{});  // Return empty event marker map
  epoch_script::runtime::TimeFrameAssetDataFrameMap transform_result_static;
  transform_result_static[base_timeframe][AAPL.GetID()] = expected_base;
  transform_result_static[resampled_timeframe][AAPL.GetID()] = expected_resampled;
  REQUIRE_CALL(*mock_transform, ExecutePipeline(trompeloeil::_, trompeloeil::_))
      .RETURN(transform_result_static);

  DatabaseImplOptions opts;
  opts.dataloader = std::move(mock_loader);
  opts.resampler = std::move(mock_resampler);
  opts.dataTransform = std::move(mock_transform);

  auto db = DatabaseImpl(std::move(opts));
  data_sdk::events::ScopedProgressEmitter emitter;
  db.RunPipeline(emitter);

  // Check base timeframe
  auto base_data = db.GetCurrentData(base_timeframe, AAPL);
  INFO("base_data: \n" << base_data);
  REQUIRE(base_data.equals(expected_base));
  REQUIRE(base_data.contains("vwap"));
  REQUIRE_FALSE(base_data.contains("zscore"));

  // Check resampled timeframe
  auto resampled_data = db.GetCurrentData(resampled_timeframe, AAPL);
  INFO("resampled_data: \n" << resampled_data);
  REQUIRE(resampled_data.equals(expected_resampled));
  REQUIRE(resampled_data.contains("zscore"));
  REQUIRE_FALSE(resampled_data.contains("vwap"));
}

TEST_CASE("LoadData + FuturesContinuation: creates continuous contracts",
          "[DatabaseImpl][FuturesContinuation]") {

  const auto ES_F = EpochScriptAssetConstants::instance().ES;
  const DataCategory category = DataCategory::MinuteBars;
  const std::string base_timeframe = "1Min";

  // Build base index and data for multiple contracts
  auto base_index = index::date_range({.start = "2000-01-01 09:30:00"_datetime,
                                       .periods = 10,
                                       .offset = offset::minutes(1)});

  // Create mock data for two contracts with correct contract naming
  data_sdk::IDataLoader::DataMap input;
  input[ES_F] = make_random_ohlcv(base_index, "ESM23");
  input[ES_F] =
      concat({.frames = {input[ES_F], make_random_ohlcv(base_index, "ESU23")},
              .sort = true});

  // Create expected continuous contract DataFrame
  auto continuous_index = base_index; // Same index
  auto continuous_df = make_random_ohlcv(continuous_index);

  // Add contract column to continuous DF with correct contract names
  std::vector<std::string> contract_column(5, "ESM23");
  contract_column.resize(10,
                         "ESU23"); // First 5 from June, next 5 from September

  // Add the CONTRACT column for GetFrontContract functionality
  continuous_df = continuous_df.assign(
      epoch_script::EpochStratifyXConstants::instance().CONTRACT(),
      make_series(continuous_index, contract_column));

  // Update the 's' column (symbol column) if it exists, otherwise add it
  if (!continuous_df.contains("s")) {
    continuous_df = continuous_df.assign(
        "s", make_series(continuous_index, contract_column));
  } else {
    // Replace the existing 's' column with contract identifiers
    continuous_df = continuous_df.drop("s").assign(
        "s", make_series(continuous_index, contract_column));
  }

  // Mock loader
  auto mock_loader = std::make_unique<MockDataloader>();
  REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(category);
  REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(input);
  REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

  // Mock futures continuation constructor
  auto mock_continuation = std::make_unique<MockFuturesContinuation>();
  AssetDataFrameMap continuation_result = {{ES_F, continuous_df}};

  REQUIRE_CALL(*mock_continuation, Build(trompeloeil::_))
      .TIMES(1)
      .RETURN(continuation_result);

  DatabaseImplOptions opts;
  opts.dataloader = std::move(mock_loader);
  opts.futuresContinuationConstructor = std::move(mock_continuation);

  auto db = DatabaseImpl(std::move(opts));
  data_sdk::events::ScopedProgressEmitter emitter;
  db.RunPipeline(emitter);

  // Check continuous contract exists
  auto continuous_data = db.GetCurrentData(base_timeframe, ES_F);
  INFO("continuous_data: \n" << continuous_data);
  REQUIRE(continuous_data.equals(continuous_df));
  REQUIRE(continuous_data.contains(
      epoch_script::EpochStratifyXConstants::instance().CONTRACT()));
  REQUIRE(continuous_data.contains("s"));

  // Check GetFrontContract returns correct contract for a timestamp
  auto mid_time = base_index->at(5).to_datetime();
  auto front_contract = db.GetFrontContract(ES_F, mid_time);
  REQUIRE(front_contract.has_value());
  REQUIRE(front_contract.value() == "ESU23");
}

TEST_CASE("RefreshPipeline: updates with websocket data",
          "[DatabaseImpl][Updates]") {
  const auto BTC_USD =
      epoch_script::EpochScriptAssetConstants::instance().BTC_USD;
  const DataCategory category = DataCategory::MinuteBars;
  const std::string base_timeframe = "1Min";

  // Build initial index and data
  auto initial_index =
      index::date_range({.start = "2025-04-21 10:00:00"_datetime,
                         .periods = 5,
                         .offset = offset::minutes(1),
                         .tz = "UTC"});
  data_sdk::IDataLoader::DataMap initial_data;
  initial_data[BTC_USD] = make_random_ohlcv(initial_index);

  // Create update data (will be returned by websocket)
  auto update_time = "2025-04-21 10:05:00"__dt.tz_localize("UTC");
  BarList update_bars = {
      BarMessage{.s = "^BTCUSD",
                 .o = 150.5,
                 .h = 151.2,
                 .l = 150.1,
                 .c = 150.8,
                 .v = 1000.0,
                 .t_utc = update_time.timestamp().value},
      BarMessage{
          .s = "^BTCUSD",
          .o = 150.8,
          .h = 151.5,
          .l = 150.3,
          .c = 151.0,
          .v = 1200.0,
          .t_utc =
              (update_time + chrono_minutes(1)).timestamp().value} // +1 min
  };

  // Create expected final dataframe after updates
  auto expected_index =
      index::date_range({.start = "2025-04-21 10:00:00"_datetime,
                         .periods = 7,
                         .offset = offset::minutes(1),
                         .tz = "UTC"});

  DataFrame expected_df = initial_data[BTC_USD];
  // Create a new dataframe with just the update data
  auto update_index = index::make_datetime_index(
      {update_time, update_time + chrono_minutes(1)}, "", "UTC");
  DataFrame update_df = make_dataframe<double>(update_index,
                                               {{150.5, 150.8},
                                                {151.2, 151.5},
                                                {150.1, 150.3},
                                                {150.8, 151.0},
                                                {1000.0, 1200.0}},
                                               {"o", "h", "l", "c", "v"});

  // Concatenate the original dataframe with the update dataframe
  expected_df = concat({.frames = {expected_df, update_df}, .sort = true});

  // Mock loader
  auto mock_loader = std::make_unique<MockDataloader>();
  REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(category);
  REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(initial_data);
  REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

  // Mock websocket manager
  auto mock_ws_manager = std::make_unique<MockWebSocketManager>();
  REQUIRE_CALL(*mock_ws_manager, HandleNewMessage(trompeloeil::_))
      .SIDE_EFFECT(_1(update_bars))
      .TIMES(AT_LEAST(0)); // Allow any number of calls

  // Create the database options
  DatabaseImplOptions opts;
  opts.dataloader = std::move(mock_loader);

  // Set up the websocket manager for the appropriate asset class
  asset::AssetClassMap<IWebSocketManagerPtr> ws_managers;
  ws_managers[AssetClass::Crypto] = std::move(mock_ws_manager);
  opts.websocketManager = std::move(ws_managers);

  // Create the database and run initial pipeline
  auto db = DatabaseImpl(std::move(opts));
  data_sdk::events::ScopedProgressEmitter emitter;
  db.RunPipeline(emitter);

  // Initial data should match what we loaded
  auto initial_db_data = db.GetCurrentData(base_timeframe, BTC_USD);
  INFO("Initial database data: \n" << initial_db_data);
  REQUIRE(initial_db_data.equals(initial_data[BTC_USD]));

  // Now refresh the pipeline to process websocket updates
  db.RefreshPipeline(emitter);

  // Check that the data was updated correctly
  auto updated_db_data = db.GetCurrentData(base_timeframe, BTC_USD);
  INFO("Updated database data: \n" << updated_db_data);
  INFO("Expected data: \n" << expected_df);

  // The update should have appended the new data
  REQUIRE(updated_db_data.num_rows() == expected_df.num_rows());

  // Check that the last two rows match our update data and include VWAP
  auto last_rows = updated_db_data.tail(2);
  INFO("Last rows: \n" << last_rows);

  // Verify each value in the last two rows, including the calculated VWAP
  REQUIRE_THAT(last_rows["o"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(150.5, 1e-2));
  REQUIRE_THAT(last_rows["h"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(151.2, 1e-2));
  REQUIRE_THAT(last_rows["l"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(150.1, 1e-2));
  REQUIRE_THAT(last_rows["c"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(150.8, 1e-2));
  REQUIRE_THAT(last_rows["v"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(1000.0, 1e-2));

  REQUIRE_THAT(last_rows["o"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(150.8, 1e-2));
  REQUIRE_THAT(last_rows["h"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(151.5, 1e-2));
  REQUIRE_THAT(last_rows["l"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(150.3, 1e-2));
  REQUIRE_THAT(last_rows["c"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(151.0, 1e-2));
  REQUIRE_THAT(last_rows["v"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(1200.0, 1e-2));
}

TEST_CASE("RefreshPipeline: updates with websocket data and transformations",
          "[DatabaseImpl][Updates][Transform]") {
  const auto BTC_USD =
      epoch_script::EpochScriptAssetConstants::instance().BTC_USD;
  const DataCategory category = DataCategory::MinuteBars;
  const std::string base_timeframe = "1Min";

  // Build initial index and data
  auto initial_index =
      index::date_range({.start = "2025-04-21 10:00:00"_datetime,
                         .periods = 5,
                         .offset = offset::minutes(1),
                         .tz = "UTC"});
  data_sdk::IDataLoader::DataMap initial_data;
  initial_data[BTC_USD] = make_random_ohlcv(initial_index);

  // Create update data (will be returned by websocket)
  auto update_time = "2025-04-21 10:05:00"__dt.tz_localize("UTC");
  BarList update_bars = {
      BarMessage{.s = "^BTCUSD",
                 .o = 150.5,
                 .h = 151.2,
                 .l = 150.1,
                 .c = 150.8,
                 .v = 1000.0,
                 .t_utc = update_time.timestamp().value},
      BarMessage{
          .s = "^BTCUSD",
          .o = 150.8,
          .h = 151.5,
          .l = 150.3,
          .c = 151.0,
          .v = 1200.0,
          .t_utc =
              (update_time + chrono_minutes(1)).timestamp().value} // +1 min
  };

  // Create expected dataframe before transformation
  DataFrame expected_df = initial_data[BTC_USD];
  auto update_index = index::make_datetime_index(
      {update_time, update_time + chrono_minutes(1)}, "", "UTC");
  DataFrame update_df = make_dataframe<double>(update_index,
                                               {{150.5, 150.8},
                                                {151.2, 151.5},
                                                {150.1, 150.3},
                                                {150.8, 151.0},
                                                {1000.0, 1200.0}},
                                               {"o", "h", "l", "c", "v"});

  // Concatenate the original dataframe with the update dataframe
  expected_df = concat({.frames = {expected_df, update_df}, .sort = true});

  // Build the transform function
  auto add_vwap = [](const DataFrame &df) {
    // Calculate a very simple VWAP: (close * volume) / volume = close
    std::vector<double> vwap(df.num_rows());
    for (size_t i = 0; i < df.num_rows(); i++) {
      vwap[i] =
          df["c"]
              .iloc(i)
              .as_double(); // Simplified - just use close price for this test
    }
    return df.assign("vwap", make_series(df.index(), vwap));
  };

  // Expected DataFrame after transformation
  DataFrame expected_transformed = add_vwap(expected_df);

  // Mock loader
  auto mock_loader = std::make_unique<MockDataloader>();
  REQUIRE_CALL(*mock_loader, GetDataCategory()).RETURN(category);
  REQUIRE_CALL(*mock_loader, GetStoredData()).RETURN(initial_data);
  REQUIRE_CALL(*mock_loader, LoadData(trompeloeil::_)).TIMES(1);

  // Mock websocket manager
  auto mock_ws_manager = std::make_unique<MockWebSocketManager>();
  REQUIRE_CALL(*mock_ws_manager, HandleNewMessage(trompeloeil::_))
      .SIDE_EFFECT(_1(update_bars))
      .TIMES(AT_LEAST(0)); // Allow any number of calls

  // Mock transform
  auto mock_transform = std::make_unique<MockTransformGraph>();
  REQUIRE_CALL(*mock_transform, GetGeneratedReports())
    .TIMES(AT_LEAST(0))
      .RETURN(epoch_script::runtime::AssetReportMap{});  // Return empty report map
  REQUIRE_CALL(*mock_transform, GetGeneratedEventMarkers())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetEventMarkerMap{});  // Return empty event marker map

  // Prepare initial static transform result (for RunPipeline)
  epoch_script::runtime::TimeFrameAssetDataFrameMap initial_transform_result_static;
  DataFrame initial_transformed = add_vwap(initial_data[BTC_USD]);
  initial_transform_result_static[base_timeframe][BTC_USD.GetID()] =
      initial_transformed;

  // Prepare updated static transform result (for RefreshPipeline)
  epoch_script::runtime::TimeFrameAssetDataFrameMap updated_transform_result_static;
  updated_transform_result_static[base_timeframe][BTC_USD.GetID()] =
      expected_transformed;

  // First transform call happens during initial RunPipeline
  trompeloeil::sequence seq;
  REQUIRE_CALL(*mock_transform, ExecutePipeline(trompeloeil::_, trompeloeil::_))
      .TIMES(1)
      .IN_SEQUENCE(seq)
      .RETURN(initial_transform_result_static);

  // Second transform call happens during RefreshPipeline
  REQUIRE_CALL(*mock_transform, ExecutePipeline(trompeloeil::_, trompeloeil::_))
      .TIMES(1)
      .IN_SEQUENCE(seq)
      .RETURN(updated_transform_result_static);

  REQUIRE_CALL(*mock_transform, GetGeneratedReports())
    .TIMES(AT_LEAST(0))
   .RETURN(epoch_script::runtime::AssetReportMap{});  // Return empty report map
  REQUIRE_CALL(*mock_transform, GetGeneratedEventMarkers())
    .TIMES(AT_LEAST(0))
    .RETURN(epoch_script::runtime::AssetEventMarkerMap{});  // Return empty event marker map

  // Create the database options
  DatabaseImplOptions opts;
  opts.dataloader = std::move(mock_loader);
  opts.dataTransform = std::move(mock_transform);

  // Set up the websocket manager for the appropriate asset class
  asset::AssetClassMap<IWebSocketManagerPtr> ws_managers;
  ws_managers[AssetClass::Crypto] = std::move(mock_ws_manager);
  opts.websocketManager = std::move(ws_managers);

  // Create the database and run initial pipeline
  auto db = DatabaseImpl(std::move(opts));
  data_sdk::events::ScopedProgressEmitter emitter;
  db.RunPipeline(emitter);

  // Initial data should include the VWAP column
  auto initial_db_data = db.GetCurrentData(base_timeframe, BTC_USD);
  INFO("Initial database data: \n" << initial_db_data);
  REQUIRE(initial_db_data.contains("vwap"));

  // Now refresh the pipeline to process websocket updates
  db.RefreshPipeline(emitter);

  // Check that the data was updated correctly and transformed
  auto updated_db_data = db.GetCurrentData(base_timeframe, BTC_USD);
  INFO("Updated database data: \n" << updated_db_data);
  INFO("Expected data: \n" << expected_transformed);

  // The update should have appended the new data and include VWAP
  REQUIRE(updated_db_data.num_rows() == expected_transformed.num_rows());
  REQUIRE(updated_db_data.contains("vwap"));

  // Check that the last two rows match our update data and include VWAP
  auto last_rows = updated_db_data.tail(2);
  INFO("Last rows: \n" << last_rows);

  // Verify each value in the last two rows, including the calculated VWAP
  REQUIRE_THAT(last_rows["o"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(150.5, 1e-2));
  REQUIRE_THAT(last_rows["h"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(151.2, 1e-2));
  REQUIRE_THAT(last_rows["l"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(150.1, 1e-2));
  REQUIRE_THAT(last_rows["c"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(150.8, 1e-2));
  REQUIRE_THAT(last_rows["v"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(1000.0, 1e-2));
  REQUIRE_THAT(last_rows["vwap"].iloc(0).as_double(),
               Catch::Matchers::WithinAbs(
                   150.8, 1e-2)); // VWAP = close price in our simplified case

  REQUIRE_THAT(last_rows["o"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(150.8, 1e-2));
  REQUIRE_THAT(last_rows["h"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(151.5, 1e-2));
  REQUIRE_THAT(last_rows["l"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(150.3, 1e-2));
  REQUIRE_THAT(last_rows["c"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(151.0, 1e-2));
  REQUIRE_THAT(last_rows["v"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(1200.0, 1e-2));
  REQUIRE_THAT(last_rows["vwap"].iloc(1).as_double(),
               Catch::Matchers::WithinAbs(
                   151.0, 1e-2)); // VWAP = close price in our simplified case
}