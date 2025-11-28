//
// Created by adesola on 2025-04-19.
//

#include "catch.hpp"
#include "data/database/resample.h"
#include "epoch_frame/day_of_week.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include <epoch_script/data/common/constants.h>
#include <catch2/generators/catch_generators.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/datetime.h>
#include <epoch_frame/factory/calendar_factory.h>
#include <epoch_frame/factory/date_offset_factory.h>
#include <epoch_frame/scalar.h>
#include <epoch_frame/series.h>

#include <format>
#include <vector>

using namespace epoch_script;
using namespace epoch_script;
using namespace epoch_script::data;
using namespace epoch_script::asset;
using namespace epoch_frame;
using namespace std::chrono_literals;

// Helper functions to create test data
DataFrame createTestOHLCVData(const std::vector<DateTime> &dates,
                               const std::string &tz = "UTC") {
  // Create OHLCV data with increasing values
  std::vector<double> open, high, low, close, volume;
  for (size_t i = 0; i < dates.size(); i++) {
    double base = static_cast<double>(i);
    open.push_back(100.0 + base);
    high.push_back(105.0 + base);
    low.push_back(95.0 + base);
    close.push_back(102.0 + base);
    volume.push_back(1000.0 + base * 100);
  }

  // Create a DataFrame with the test data
  return epoch_frame::make_dataframe<double>(
      factory::index::make_datetime_index(dates, "", tz),
      {open, high, low, close, volume}, BarsConstants::instance().all);
}

TEST_CASE("Resampler::Build correctly resamples data for multiple timeframes "
          "and assets") {
  // Create test data with one-minute intervals for multiple hours
  std::vector<DateTime> dates1;

  // 3 hours of minute data (3 * 60 = 180 data points)
  for (int hour = 9; hour < 12; hour++) {
    for (int minute = 0; minute < 60; minute++) {
      // Format a datetime string with the proper hour and minute using
      // std::format
      std::string dt_str =
          std::format("2022-01-01 {:02d}:{:02d}:00", hour, minute);
      dates1.push_back(DateTime::from_str(dt_str).replace_tz("UTC"));
    }
  }

  // Create test data for two assets
  auto df1 = createTestOHLCVData(dates1);
  auto df2 = createTestOHLCVData(dates1); // Same timestamps but different asset

  // Setup assets and data
  Asset asset1 = EpochScriptAssetConstants::instance().AAPL;
  Asset asset2 = EpochScriptAssetConstants::instance().MSFT;

  AssetDataFrameMap assetData;
  assetData[asset1] = df1;
  assetData[asset2] = df2;

  SECTION("Resample to hourly timeframe") {
    // Create a resampler with hourly timeframe
    std::vector<epoch_script::TimeFrame> timeframes = {
        epoch_script::TimeFrame(epoch_frame::factory::offset::hours(1))};

    Resampler resampler(timeframes, true);

    // Build the resampled data
    auto result = resampler.Build(assetData);

    // We should have 2 assets * 1 timeframe = 2 results
    REQUIRE(result.size() == 2);

    // For each result tuple (timeframe, asset, dataframe)
    for (const auto &[timeframe, asset, df] : result) {
      INFO(timeframe << "\n" << asset << "\n" << df);

      // Verify timeframe string
      REQUIRE(timeframe == "1H");

      // Verify either asset1 or asset2
      REQUIRE((asset == asset1 || asset == asset2));

      // Verify the dataframe has 3 rows (3 hours)
      REQUIRE(df.num_rows() == 4);

      // Verify it has the expected columns
      REQUIRE_THAT(df.column_names(), Catch::Matchers::UnorderedRangeEquals(
                                          BarsConstants::instance().all));

      // Verify the index is hourly
      const auto timestamps = df.index()->array().to_timestamp_view();
      DateTime expected1 = "2022-01-01 09:00:00"__dt.replace_tz("UTC");
      DateTime expected2 = "2022-01-01 10:00:00"__dt.replace_tz("UTC");
      DateTime expected3 = "2022-01-01 11:00:00"__dt.replace_tz("UTC");
      DateTime expected4 = "2022-01-01 12:00:00"__dt.replace_tz("UTC");

      REQUIRE(DateTime::fromtimestamp(timestamps->Value(0), "UTC") ==
              expected1);
      REQUIRE(DateTime::fromtimestamp(timestamps->Value(1), "UTC") ==
              expected2);
      REQUIRE(DateTime::fromtimestamp(timestamps->Value(2), "UTC") ==
              expected3);
      REQUIRE(DateTime::fromtimestamp(timestamps->Value(3), "UTC") ==
              expected4);
    }
  }

  SECTION("Resample to minutes/hours timeframes") {
    // Create a resampler with multiple timeframes
    std::vector<epoch_script::TimeFrame> timeframes = {
        epoch_script::TimeFrame(epoch_frame::factory::offset::minutes(5)),
        epoch_script::TimeFrame(epoch_frame::factory::offset::hours(1))};

    Resampler resampler(timeframes, true);

    // Build the resampled data
    auto result = resampler.Build(assetData);

    // We should have 2 assets * 2 timeframes = 4 results
    REQUIRE(result.size() == 4);

    // Count the number of each timeframe
    int fiveMinCount = 0;
    int hourlyCount = 0;

    for (const auto &[timeframe, asset, df] : result) {
      INFO(timeframe << "\n" << asset << "\n" << df);

      // Verify either timeframe
      if (timeframe == "5Min") {
        fiveMinCount++;

        // 3 hours with 5-minute bars should give us 3*60/5 = 36 bars
        REQUIRE(df.num_rows() == 37);
      } else if (timeframe == "1H") {
        hourlyCount++;

        // 3 hours with hourly bars should give us 3 bars
        REQUIRE(df.num_rows() == 4);
      } else {
        FAIL("Unexpected timeframe: " + timeframe);
      }

      // Verify either asset1 or asset2
      REQUIRE((asset == asset1 || asset == asset2));

      // Verify it has the expected columns
      REQUIRE_THAT(df.column_names(), Catch::Matchers::UnorderedRangeEquals(
                                          BarsConstants::instance().all));
    }

    // We should have 2 of each timeframe (one for each asset)
    REQUIRE(fiveMinCount == 2);
    REQUIRE(hourlyCount == 2);
  }

  SECTION("Handles daily resampling correctly") {
    // Create a resampler with daily timeframe
    std::vector<epoch_script::TimeFrame> timeframes = {
        epoch_script::TimeFrame(factory::offset::days(1))};

    Resampler resampler(timeframes, true);

    // Build the resampled data
    auto result = resampler.Build(assetData);

    // We should have 2 assets * 1 timeframe = 2 results
    REQUIRE(result.size() == 2);

    // For each result tuple (timeframe, asset, dataframe)
    for (const auto &[timeframe, asset, df] : result) {
      // Verify timeframe string
      REQUIRE(timeframe == "1D");

      // Since all data is from the same day, we should have 1 row
      REQUIRE(df.num_rows() == 1);

      // Verify the timestamp
      const auto timestamps = df.index()->array().to_timestamp_view();
      DateTime expected = "2022-01-02 00:00:00"__dt.replace_tz("UTC");

      REQUIRE(DateTime::fromtimestamp(timestamps->Value(0), "UTC").date() ==
              expected.date());
    }
  }
}

TEST_CASE("Resampler handles edge cases correctly") {
  SECTION("Empty input data") {
    // Create empty input data
    AssetDataFrameMap emptyData;

    // Create a resampler with hourly timeframe
    std::vector<epoch_script::TimeFrame> timeframes = {
        epoch_script::TimeFrame(epoch_frame::factory::offset::hours(1))};

    Resampler resampler(timeframes, true);

    // Build the resampled data
    auto result = resampler.Build(emptyData);

    // Should return empty result
    REQUIRE(result.empty());
  }

  SECTION("Single data point") {
    // Create test data with single data point
    std::vector dates = {"2022-01-01 10:00:00"__dt.replace_tz("UTC")};

    auto df = createTestOHLCVData(dates);

    // Setup asset and data
    Asset asset = EpochScriptAssetConstants::instance().AAPL;

    AssetDataFrameMap assetData;
    assetData[asset] = df;

    // Create a resampler with hourly timeframe
    std::vector<epoch_script::TimeFrame> timeframes = {
        epoch_script::TimeFrame(epoch_frame::factory::offset::hours(1))};

    Resampler resampler(timeframes, true);

    // Build the resampled data
    auto result = resampler.Build(assetData);

    // Should have 1 result
    REQUIRE(result.size() == 1);

    // The result should have 1 row
    const auto &[timeframe, resultAsset, resultDf] = result[0];
    REQUIRE(resultDf.num_rows() == 1);

    // Values should be preserved
    REQUIRE(
        resultDf
            .iloc(0, epoch_script::EpochStratifyXConstants::instance().OPEN())
            .as_double() ==
        df.iloc(0, epoch_script::EpochStratifyXConstants::instance().OPEN())
            .as_double());
    REQUIRE(
        resultDf
            .iloc(0, epoch_script::EpochStratifyXConstants::instance().HIGH())
            .as_double() ==
        df.iloc(0, epoch_script::EpochStratifyXConstants::instance().HIGH())
            .as_double());
    REQUIRE(
        resultDf
            .iloc(0, epoch_script::EpochStratifyXConstants::instance().LOW())
            .as_double() ==
        df.iloc(0, epoch_script::EpochStratifyXConstants::instance().LOW())
            .as_double());
    REQUIRE(
        resultDf
            .iloc(0,
                  epoch_script::EpochStratifyXConstants::instance().CLOSE())
            .as_double() ==
        df.iloc(0, epoch_script::EpochStratifyXConstants::instance().CLOSE())
            .as_double());
    REQUIRE(
        resultDf
            .iloc(0,
                  epoch_script::EpochStratifyXConstants::instance().VOLUME())
            .as_double() ==
        df.iloc(0, epoch_script::EpochStratifyXConstants::instance().VOLUME())
            .as_double());
  }

  SECTION("Multiple data points && tz") {
    auto tz = GENERATE("America/New_York", "Europe/London");
    DYNAMIC_SECTION("Timezone: " << tz) {
      // Create test data with non-UTC timezones
      std::vector dates = {"2022-01-01 10:00:00"__dt.replace_tz(tz),
                           "2022-01-01 10:01:00"__dt.replace_tz(tz),
                           "2022-01-01 10:02:00"__dt.replace_tz(tz)};

      auto df = createTestOHLCVData(dates, tz);

      // Setup asset and data
      Asset asset = EpochScriptAssetConstants::instance().AAPL;

      AssetDataFrameMap assetData;
      assetData[asset] = df;

      // Create a resampler with hourly timeframe
      std::vector<epoch_script::TimeFrame> timeframes = {
          epoch_script::TimeFrame(factory::offset::hours(1))};

      Resampler resampler(timeframes, true);

      // Build the resampled data - should throw because only UTC is supported
      REQUIRE_THROWS(resampler.Build(assetData));
    }
  }
}

struct TestCase {
  std::shared_ptr<OffsetHandler> offset;
  std::vector<std::string> output;
};

TEST_CASE("Resampling to Calendar Offsets") {
  using namespace epoch_frame;
  std::vector<DateTime> intraday;

  std::vector timeframes{
      epoch_script::TimeFrame(factory::offset::days(1)),
      epoch_script::TimeFrame(
          factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Monday)),
      epoch_script::TimeFrame(
          factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Sunday)),
      epoch_script::TimeFrame(
          factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Tuesday)),
      epoch_script::TimeFrame(
          factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Wednesday)),
      epoch_script::TimeFrame(
          factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Thursday)),
      epoch_script::TimeFrame(
          factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Friday)),
      epoch_script::TimeFrame(
          factory::offset::weeks(1, epoch_core::EpochDayOfWeek::Saturday)),
      epoch_script::TimeFrame(
          factory::offset::weeks(2, epoch_core::EpochDayOfWeek::Monday)),
      epoch_script::TimeFrame(factory::offset::week_of_month(
          1, 0, epoch_core::EpochDayOfWeek::Monday)),
      epoch_script::TimeFrame(factory::offset::week_of_month(
          1, 1, epoch_core::EpochDayOfWeek::Monday)),
      epoch_script::TimeFrame(factory::offset::week_of_month(
          1, 2, epoch_core::EpochDayOfWeek::Monday)),
      epoch_script::TimeFrame(factory::offset::last_week_of_month(
          1, epoch_core::EpochDayOfWeek::Monday)),
      epoch_script::TimeFrame(factory::offset::week_of_month(
          2, 1, epoch_core::EpochDayOfWeek::Tuesday)),
      epoch_script::TimeFrame(factory::offset::month_start(1)),
      epoch_script::TimeFrame(factory::offset::month_end(1)),
      epoch_script::TimeFrame(
          factory::offset::quarter_start(1, std::chrono::March)),
      epoch_script::TimeFrame(
          factory::offset::quarter_end(1, std::chrono::March)),
      epoch_script::TimeFrame(
          factory::offset::quarter_end(2, std::chrono::March)),
      epoch_script::TimeFrame(
          factory::offset::year_start(1, std::chrono::January)),
      epoch_script::TimeFrame(
          factory::offset::year_end(1, std::chrono::December))};

  std::vector<IndexPtr> expectedIndex(timeframes.size());
  for (bool isIntraday : {true, false}) {
    std::string fullPath =
        std::filesystem::path(isIntraday ? std::string{RESAMPLE_FILES} + "/intraday"
                                         : std::string{RESAMPLE_FILES} + "/daily")
            .string();
    for (const auto &file : std::filesystem::directory_iterator(fullPath)) {
      std::ifstream f(file.path());
      std::string line;

      std::vector<DateTime> timestamps;
      while (std::getline(f, line)) {
        timestamps.push_back(DateTime::from_str(line).replace_tz("UTC"));
      }

      auto file_name = file.path().stem().string();
      if (file_name == "DATASOURCE") {
        intraday = timestamps;
      } else {
        expectedIndex[std::stoi(file_name.substr(0, file_name.find("_")))] =
            factory::index::make_datetime_index(timestamps);
      }
    }

    if (!isIntraday) {
      auto timestamps_vec = expectedIndex[0]->array().to_vector<DateTime>();
      intraday.clear();
      for (auto& dt : timestamps_vec) {
        intraday.push_back(dt.replace_tz("UTC"));
      }
    }

    auto dataFrame = createTestOHLCVData(intraday);

    auto result = Resampler(timeframes, isIntraday)
                      .Build({{EpochScriptAssetConstants::instance().AAPL,
                               dataFrame}});

    SECTION(isIntraday ? "Intraday" : "Daily") {
      REQUIRE(result.size() == timeframes.size());
      for (size_t j = 0; j < timeframes.size(); j++) {
        INFO(timeframes[j].ToString());
        auto [timeframe, asset, df] = result[j];

        auto expected =
            isIntraday ? expectedIndex[j] : expectedIndex[j]->normalize();

        REQUIRE(asset == EpochScriptAssetConstants::instance().AAPL);
        REQUIRE(timeframe == timeframes[j].ToString());

        INFO(df.index()->repr() << "\n-------- !=  --------\n"
                                << expected->repr());
        REQUIRE(df.index()->equals(expected));
      }
    }
  }
}

TEST_CASE("Generic Resampler handles different column types correctly", "[Resampler]") {
  // Create test data with various column types
  std::vector<DateTime> dates;
  for (int i = 0; i < 10; i++) {
    dates.push_back(
        DateTime::from_str(std::format("2022-01-01 10:{:02d}:00", i))
            .replace_tz("UTC"));
  }

  // Create OHLCV data
  std::vector<double> open, high, low, close, volume;
  std::vector<double> vw;    // volume-weighted price (float)
  std::vector<int64_t> n;    // trade count (integer)
  std::vector<double> custom_float;  // custom float column
  std::vector<int64_t> custom_int;   // custom integer column
  std::vector<std::string> custom_str; // custom string column

  for (size_t i = 0; i < dates.size(); i++) {
    double base = static_cast<double>(i);
    open.push_back(100.0 + base);
    high.push_back(105.0 + base);
    low.push_back(95.0 + base);
    close.push_back(102.0 + base);
    volume.push_back(1000.0 + base * 100);
    vw.push_back(101.0 + base);  // volume-weighted average price
    n.push_back(10 + i);         // trade count
    custom_float.push_back(50.0 + base);
    custom_int.push_back(200 + i);
    custom_str.push_back("val" + std::to_string(i));
  }

  // Create DataFrame with all columns
  auto index = factory::index::make_datetime_index(dates);

  // Build schema with various types
  arrow::FieldVector fields = {
      arrow::field("o", arrow::float64()),
      arrow::field("h", arrow::float64()),
      arrow::field("l", arrow::float64()),
      arrow::field("c", arrow::float64()),
      arrow::field("v", arrow::float64()),
      arrow::field("vw", arrow::float64()),
      arrow::field("n", arrow::int64()),
      arrow::field("custom_float", arrow::float64()),
      arrow::field("custom_int", arrow::int64()),
      arrow::field("custom_str", arrow::utf8())
  };

  arrow::ArrayVector arrays;
  arrays.push_back(factory::array::make_array(open)->chunk(0));
  arrays.push_back(factory::array::make_array(high)->chunk(0));
  arrays.push_back(factory::array::make_array(low)->chunk(0));
  arrays.push_back(factory::array::make_array(close)->chunk(0));
  arrays.push_back(factory::array::make_array(volume)->chunk(0));
  arrays.push_back(factory::array::make_array(vw)->chunk(0));
  arrays.push_back(factory::array::make_array(n)->chunk(0));
  arrays.push_back(factory::array::make_array(custom_float)->chunk(0));
  arrays.push_back(factory::array::make_array(custom_int)->chunk(0));
  arrays.push_back(factory::array::make_array(custom_str)->chunk(0));

  auto table = arrow::Table::Make(arrow::schema(fields), arrays);
  auto df = DataFrame(index, table);

  SECTION("Resample to 5-minute intervals") {
    Asset asset = EpochScriptAssetConstants::instance().AAPL;
    AssetDataFrameMap assetData;
    assetData[asset] = df;

    std::vector<epoch_script::TimeFrame> timeframes = {
        epoch_script::TimeFrame(factory::offset::minutes(5))};

    Resampler resampler(timeframes, true);
    auto result = resampler.Build(assetData);

    REQUIRE(result.size() == 1);
    auto [timeframe, resultAsset, resultDf] = result[0];

    INFO(resultDf);

    // Should have 3 rows: (minute 0 at 10:00, minutes 1-5 at 10:05, minutes 6-9 at 10:10)
    REQUIRE(resultDf.num_rows() == 3);

    // Check OHLCV aggregation - test the second bar (minutes 1-5)
    SECTION("OHLCV columns are correctly aggregated") {
      // Second bar (minutes 1-5 at index 1): 5 rows
      // Open should be first value (101.0)
      REQUIRE(resultDf.iloc(1, "o").as_double() == Catch::Approx(101.0));
      // High should be max (110.0 = 105+5)
      REQUIRE(resultDf.iloc(1, "h").as_double() == Catch::Approx(110.0));
      // Low should be min (96.0 = 95+1)
      REQUIRE(resultDf.iloc(1, "l").as_double() == Catch::Approx(96.0));
      // Close should be last value (105.0)
      REQUIRE(resultDf.iloc(1, "c").as_double() == Catch::Approx(107.0));
      // Volume should be sum (1100 + 1200 + 1300 + 1400 + 1500 = 6500)
      REQUIRE(resultDf.iloc(1, "v").as_double() == Catch::Approx(6500.0));
    }

    SECTION("vw column is averaged") {
      // vw should be average: (102 + 103 + 104 + 105 + 106) / 5 = 104
      REQUIRE(resultDf.iloc(1, "vw").as_double() == Catch::Approx(104.0));
    }

    SECTION("n column is summed") {
      // n should be sum: 11 + 12 + 13 + 14 + 15 = 65
      REQUIRE(resultDf.iloc(1, "n").as_int64() == 65);
    }

    SECTION("Custom float column is last") {
      REQUIRE(resultDf.iloc(1, "custom_float").as_double() ==
              Catch::Approx(55.0));
    }

    SECTION("Custom integer column is last") {
      REQUIRE(resultDf.iloc(1, "custom_int").as_int64() == 205);
    }

    SECTION("Custom string column is last") {
      // custom_str should be concatenated: "val1,val2,val3,val4,val5"
      auto str_value = resultDf.iloc(1, "custom_str").value<std::string>().value();
      REQUIRE(str_value == "val5");
    }
  }
}

TEST_CASE("Generic Resampler takes last non-null value for sparse data", "[Resampler][sparse]") {
  // This test verifies the fix for economic indicators where sparse monthly data
  // (e.g., CPI published mid-month) was being lost when resampling to month-end
  // because the default "last" aggregation would pick up null values.

  std::vector<DateTime> dates;
  // Create 10 minutes of data
  for (int i = 0; i < 10; i++) {
    dates.push_back(
        DateTime::from_str(std::format("2022-01-01 10:{:02d}:00", i))
            .replace_tz("UTC"));
  }

  // Create OHLCV data (required columns)
  std::vector<double> open, high, low, close, volume;
  for (size_t i = 0; i < dates.size(); i++) {
    double base = static_cast<double>(i);
    open.push_back(100.0 + base);
    high.push_back(105.0 + base);
    low.push_back(95.0 + base);
    close.push_back(102.0 + base);
    volume.push_back(1000.0 + base * 100);
  }

  // Create a sparse column simulating economic indicator data
  // Only has values at indices 1 and 6 (like CPI published mid-month)
  std::vector<std::optional<double>> sparse_econ(10, std::nullopt);
  sparse_econ[1] = 234.5;  // First "publication"
  sparse_econ[6] = 236.7;  // Second "publication"

  // Create DataFrame with sparse column
  auto index = factory::index::make_datetime_index(dates);

  arrow::FieldVector fields = {
      arrow::field("o", arrow::float64()),
      arrow::field("h", arrow::float64()),
      arrow::field("l", arrow::float64()),
      arrow::field("c", arrow::float64()),
      arrow::field("v", arrow::float64()),
      arrow::field("ECON:CPI:value", arrow::float64())  // Sparse economic data
  };

  // Build sparse array with nulls
  arrow::DoubleBuilder sparse_builder;
  for (const auto& val : sparse_econ) {
    if (val.has_value()) {
      REQUIRE(sparse_builder.Append(val.value()).ok());
    } else {
      REQUIRE(sparse_builder.AppendNull().ok());
    }
  }
  std::shared_ptr<arrow::Array> sparse_array;
  REQUIRE(sparse_builder.Finish(&sparse_array).ok());

  arrow::ArrayVector arrays;
  arrays.push_back(factory::array::make_array(open)->chunk(0));
  arrays.push_back(factory::array::make_array(high)->chunk(0));
  arrays.push_back(factory::array::make_array(low)->chunk(0));
  arrays.push_back(factory::array::make_array(close)->chunk(0));
  arrays.push_back(factory::array::make_array(volume)->chunk(0));
  arrays.push_back(sparse_array);

  auto table = arrow::Table::Make(arrow::schema(fields), arrays);
  auto df = DataFrame(index, table);

  SECTION("Resample sparse data to 5-minute intervals - last non-null is preserved") {
    Asset asset = EpochScriptAssetConstants::instance().AAPL;
    AssetDataFrameMap assetData;
    assetData[asset] = df;

    std::vector<epoch_script::TimeFrame> timeframes = {
        epoch_script::TimeFrame(factory::offset::minutes(5))};

    Resampler resampler(timeframes, true);
    auto result = resampler.Build(assetData);

    REQUIRE(result.size() == 1);
    auto [timeframe, resultAsset, resultDf] = result[0];

    INFO(resultDf);

    // Should have 3 rows: 10:00, 10:05, 10:10
    // With closed=Right, label=Right:
    //   - Bar 0 (10:00): Contains only minute 0
    //   - Bar 1 (10:05): Contains minutes 1-5
    //   - Bar 2 (10:10): Contains minutes 6-9
    REQUIRE(resultDf.num_rows() == 3);

    // First bar (minute 0 only): sparse_econ[0] is null, no other values in range
    // With last_non_null and no non-null values, should be null
    auto first_bar_value = resultDf["ECON:CPI:value"].iloc(0);
    INFO("First bar (minute 0 only): Expected null (no values in range)");
    REQUIRE(first_bar_value.is_null());

    // Second bar (minutes 1-5): sparse_econ[1] has value 234.5
    // indices 2,3,4,5 are null, index 1 has value
    // With last_non_null, this should be 234.5 (not null)
    auto second_bar_value = resultDf["ECON:CPI:value"].iloc(1);
    INFO("Second bar (minutes 1-5): Expected 234.5 from index 1");
    REQUIRE_FALSE(second_bar_value.is_null());
    REQUIRE(second_bar_value.as_double() == Catch::Approx(234.5));

    // Third bar (minutes 6-9): sparse_econ[6] has value 236.7
    // indices 7,8,9 are null, index 6 has value
    // With last_non_null, this should be 236.7 (not null)
    auto third_bar_value = resultDf["ECON:CPI:value"].iloc(2);
    INFO("Third bar (minutes 6-9): Expected 236.7 from index 6");
    REQUIRE_FALSE(third_bar_value.is_null());
    REQUIRE(third_bar_value.as_double() == Catch::Approx(236.7));
  }

  SECTION("All-null sparse column remains null after resample") {
    // Create a column that's entirely null
    std::vector<std::optional<double>> all_null_data(10, std::nullopt);

    arrow::DoubleBuilder all_null_builder;
    for (const auto& val : all_null_data) {
      REQUIRE(all_null_builder.AppendNull().ok());
    }
    std::shared_ptr<arrow::Array> all_null_array;
    REQUIRE(all_null_builder.Finish(&all_null_array).ok());

    arrow::FieldVector fields2 = {
        arrow::field("o", arrow::float64()),
        arrow::field("h", arrow::float64()),
        arrow::field("l", arrow::float64()),
        arrow::field("c", arrow::float64()),
        arrow::field("v", arrow::float64()),
        arrow::field("all_null_col", arrow::float64())
    };

    arrow::ArrayVector arrays2;
    arrays2.push_back(factory::array::make_array(open)->chunk(0));
    arrays2.push_back(factory::array::make_array(high)->chunk(0));
    arrays2.push_back(factory::array::make_array(low)->chunk(0));
    arrays2.push_back(factory::array::make_array(close)->chunk(0));
    arrays2.push_back(factory::array::make_array(volume)->chunk(0));
    arrays2.push_back(all_null_array);

    auto table2 = arrow::Table::Make(arrow::schema(fields2), arrays2);
    auto df2 = DataFrame(index, table2);

    Asset asset = EpochScriptAssetConstants::instance().AAPL;
    AssetDataFrameMap assetData;
    assetData[asset] = df2;

    std::vector<epoch_script::TimeFrame> timeframes = {
        epoch_script::TimeFrame(factory::offset::minutes(5))};

    Resampler resampler(timeframes, true);
    auto result = resampler.Build(assetData);

    REQUIRE(result.size() == 1);
    auto [timeframe, resultAsset, resultDf] = result[0];

    // All values should still be null
    for (int64_t i = 0; i < resultDf.num_rows(); i++) {
      auto val = resultDf["all_null_col"].iloc(i);
      INFO("Row " << i << " should be null");
      REQUIRE(val.is_null());
    }
  }
}