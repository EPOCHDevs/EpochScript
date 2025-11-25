//
// Created by adesola on 5/23/25.
//
#include "epoch_frame/array.h"
#include <epoch_script/core/constants.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/cross_sectional/rank.h"
#include "transforms/components/cross_sectional/returns.h"
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/index_factory.h>

#include <epoch_script/transforms/core/registry.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// Helper to create InputValue YAML node in the new format: {type: ref, value: {node_id: "", handle: "col"}}
// Uses empty node_id - column name in DataFrame matches the handle directly
YAML::Node makeInputRef(const std::string& col) {
  YAML::Node node;
  node["type"] = "ref";
  YAML::Node value;
  value["node_id"] = "";
  value["handle"] = col;
  node["value"] = value;
  return node;
}

// Helper to create a sequence of InputValue YAML nodes
YAML::Node makeInputRefSeq(const std::vector<std::string>& cols) {
  YAML::Node seq;
  for (const auto& col : cols) {
    seq.push_back(makeInputRef(col));
  }
  return seq;
}

// Virtual data generator for creating appropriate test data based on transform requirements
class VirtualDataGenerator {
public:
  static constexpr size_t DEFAULT_NUM_BARS = 100;  // Increased for statistical transforms like HMM
  static constexpr size_t DEFAULT_NUM_ASSETS = 5;

  // Generate varied price data with realistic patterns
  static std::vector<double> generatePricePattern(size_t numBars, double basePrice, double volatility) {
    std::vector<double> prices;
    prices.reserve(numBars);
    double price = basePrice;

    for (size_t i = 0; i < numBars; ++i) {
      // Create oscillating pattern with trend
      double trend = i * 0.1;
      double oscillation = std::sin(i * 0.3) * volatility;
      price = basePrice + trend + oscillation;
      prices.push_back(price);
    }
    return prices;
  }

  // Generate single-asset OHLCV data
  static std::unordered_map<std::string, arrow::ChunkedArrayPtr> generateSingleAssetData(size_t numBars = DEFAULT_NUM_BARS) {
    auto closePrices = generatePricePattern(numBars, 100.0, 5.0);
    std::vector<double> openPrices;
    std::vector<double> highPrices;
    std::vector<double> lowPrices;

    openPrices.reserve(numBars);
    highPrices.reserve(numBars);
    lowPrices.reserve(numBars);

    for (size_t i = 0; i < numBars; ++i) {
      double close = closePrices[i];
      double open = (i > 0) ? closePrices[i-1] : close - 1.0;
      openPrices.push_back(open);
      highPrices.push_back(std::max(open, close) + 2.0);
      lowPrices.push_back(std::min(open, close) - 2.0);
    }

    std::vector<double> volume(numBars, 1000000.0);
    std::vector<double> vwap(numBars, 100.0);
    std::vector<int64_t> tradeCount(numBars, 500);  // Integer type for trade count

    // Raw column names (o, h, l, c, v) - transforms access these directly
    return {
      {"o", factory::array::make_array(openPrices)},
      {"c", factory::array::make_array(closePrices)},
      {"h", factory::array::make_array(highPrices)},
      {"l", factory::array::make_array(lowPrices)},
      {"v", factory::array::make_array(volume)},
      {"vw", factory::array::make_array(vwap)},
      {"n", factory::array::make_array(tradeCount)}  // Now produces INT64 Arrow type
    };
  }

  // Generate multi-asset cross-sectional data
  // Returns DataFrame with asset symbols as column names
  static DataFrame generateCrossSectionalData(
      IODataType dataType,
      const IndexPtr& index,
      size_t numAssets = DEFAULT_NUM_ASSETS,
      size_t numBars = DEFAULT_NUM_BARS) {

    std::vector<std::string> assetNames = {"AAPL", "MSFT", "TSLA", "GOOGL", "AMZN"};
    assetNames.resize(numAssets);

    std::vector<arrow::ChunkedArrayPtr> assetData;
    assetData.reserve(numAssets);

    for (size_t i = 0; i < numAssets; ++i) {
      double basePrice = 100.0 + (i * 50.0);  // Different price levels
      double volatility = 5.0 + (i * 2.0);     // Different volatilities

      switch (dataType) {
        case IODataType::Decimal:
        case IODataType::Number: {
          auto prices = generatePricePattern(numBars, basePrice, volatility);
          assetData.push_back(factory::array::make_array(prices));
          break;
        }
        case IODataType::Integer: {
          auto prices = generatePricePattern(numBars, basePrice, volatility);
          std::vector<int64_t> intPrices;
          intPrices.reserve(prices.size());
          for (auto p : prices) intPrices.push_back(static_cast<int64_t>(p));
          assetData.push_back(factory::array::make_array(intPrices));
          break;
        }
        case IODataType::Boolean: {
          std::vector<bool> values(numBars);
          for (size_t j = 0; j < numBars; ++j) {
            values[j] = (j + i) % 2 == 0;  // Alternating pattern, different per asset
          }
          assetData.push_back(factory::array::make_array(values));
          break;
        }
        default: {
          std::vector<std::string> values(numBars, "Asset" + std::to_string(i));
          assetData.push_back(factory::array::make_array(values));
          break;
        }
      }
    }

    return make_dataframe(index, assetData, assetNames);
  }

  // Get array from IODataType for non-cross-sectional transforms
  static arrow::ChunkedArrayPtr getArrayFromType(IODataType type, size_t numBars = DEFAULT_NUM_BARS, int64_t maxValue = -1) {
    switch (type) {
      case IODataType::Any:
      case IODataType::Decimal:
      case IODataType::Number:
        return factory::array::make_array(generatePricePattern(numBars, 100.0, 5.0));
      case IODataType::Integer: {
        std::vector<int64_t> values(numBars);
        if (maxValue > 0) {
          // Generate values in range [0, maxValue] using modulo for select_N transforms
          for (size_t i = 0; i < numBars; ++i) values[i] = static_cast<int64_t>(i % (maxValue + 1));
        } else {
          // Default: sequential values
          for (size_t i = 0; i < numBars; ++i) values[i] = static_cast<int64_t>(i);
        }
        return factory::array::make_array(values);
      }
      case IODataType::Boolean: {
        std::vector<bool> values(numBars);
        for (size_t i = 0; i < numBars; ++i) values[i] = i % 2 == 0;
        return factory::array::make_array(values);
      }
      case IODataType::Timestamp: {
        // Generate timestamp values starting from 2022-01-01 with hourly increments
        auto start = DateTime::from_date_str("2022-01-01").m_nanoseconds;
        arrow::TimestampBuilder builder(arrow::timestamp(arrow::TimeUnit::NANO, "UTC"),
                                        arrow::default_memory_pool());
        (void)builder.Reserve(numBars);
        for (int64_t i = 0; i < static_cast<int64_t>(numBars); ++i) {
          builder.UnsafeAppend((start + chrono_hours(1)).count()); // Add hours in nanoseconds
        }
        std::shared_ptr<arrow::Array> array_temp;
       (void) builder.Finish(&array_temp);
        return std::make_shared<arrow::ChunkedArray>(array_temp);
      }
      default:
        return factory::array::make_array(std::vector<std::string>(numBars, "test_string"));
    }
  }
};

TEST_CASE("Transform Metadata Factory") {
  auto metadataMap =
      epoch_script::transforms::ITransformRegistry::GetInstance()
          .GetMetaData();
  const auto transformMap = TransformRegistry::GetInstance().GetAll();

  auto getTransformNames = [](auto const &keysA, auto const &keysB) {
    std::set<std::string> sortedKeysA(keysA.begin(), keysA.end());
    std::set<std::string> sortedKeysB(keysB.begin(), keysB.end());
    std::stringstream result;
    result << "MetaData - Transforms.\n";
    std::ranges::set_difference(
        sortedKeysA, sortedKeysB,
        std::ostream_iterator<std::string>(result, "\n"));
    result << "\n\nTransforms - MetaData.\n";
    std::ranges::set_difference(
        sortedKeysB, sortedKeysA,
        std::ostream_iterator<std::string>(result, "\n"));
    return result.str();
  };

  SECTION("All transforms are registered") {
    INFO("Diff:\n"
         << getTransformNames(metadataMap | std::views::keys,
                              transformMap | std::views::keys));

    // Count transforms with outputs (excludes reporters and selectors which don't produce outputs)
    auto non_reporter_count_metadata = std::ranges::count_if(metadataMap, [](const auto& pair) {
      return !pair.second.outputs.empty();
    });

    auto non_reporter_count_transforms = std::ranges::count_if(transformMap, [&](const auto& pair) {
      return !metadataMap.contains(pair.first) || !metadataMap.at(pair.first).outputs.empty();
    });

    REQUIRE(non_reporter_count_metadata == non_reporter_count_transforms);
  }

  // Generate test data using VirtualDataGenerator (50 bars for better statistical coverage)
  constexpr size_t NUM_TEST_BARS = VirtualDataGenerator::DEFAULT_NUM_BARS;
  const auto dataSources = VirtualDataGenerator::generateSingleAssetData(NUM_TEST_BARS);
  const auto index = factory::index::date_range(
      {.start = DateTime::from_date_str("2022-01-01").timestamp(),
       .periods = static_cast<int64_t>(NUM_TEST_BARS),
       .offset = factory::offset::hours(6)});

  // Use VirtualDataGenerator for creating arrays from types
  auto getArrayFromType = [&](auto const &type, int64_t maxValue = -1) {
    return VirtualDataGenerator::getArrayFromType(type, NUM_TEST_BARS, maxValue);
  };

  auto makeConfig = [&](auto const &id) {
    std::vector<arrow::ChunkedArrayPtr> inputs_vec;
    std::vector<std::string> fields_vec;
    YAML::Node config;
    config["type"] = id;
    config["id"] = "1";
    config["timeframe"]["interval"] = 1;
    config["timeframe"]["type"] = "day";

    const epoch_script::transforms::TransformsMetaData metadata =
        metadataMap.at(id);

    // Handle cross-sectional transforms with multi-asset data
    if (metadata.isCrossSectional) {
      // Cross-sectional transforms receive DataFrame with asset symbols as columns
      auto cs_data = VirtualDataGenerator::generateCrossSectionalData(
          metadata.inputs.empty() ? IODataType::Decimal : metadata.inputs.front().type,
          index,
          VirtualDataGenerator::DEFAULT_NUM_ASSETS,
          NUM_TEST_BARS);

      auto col_names = cs_data.column_names();

      if (metadata.inputs.size() == 1 &&
          metadata.inputs.front().allowMultipleConnections) {
        // Single input accepting multiple connections - provide multi-asset data
        config["inputs"][epoch_script::ARG] = makeInputRefSeq(col_names);
        // Column names need # prefix to match GetInputId() format
        for (const auto& col : col_names) {
          fields_vec.push_back("#" + col);
          inputs_vec.push_back(cs_data[col].array());
        }
      } else if (metadata.inputs.size() == 1) {
        // Single non-multi-connection input (edge case for cross-sectional)
        config["inputs"][epoch_script::ARG] = makeInputRef(col_names.front());
        // Column name needs # prefix to match GetInputId() format
        fields_vec = {"#" + col_names.front()};
        inputs_vec.push_back(cs_data[col_names.front()].array());
      } else {
        // Transforms with multiple inputs (e.g., beta with asset_returns and market_returns)
        // Each input gets its own multi-asset DataFrame
        for (auto const &[i, inputMetadata] :
             metadata.inputs | std::views::enumerate) {
          auto input_cs_data = VirtualDataGenerator::generateCrossSectionalData(
              inputMetadata.type, index,
              VirtualDataGenerator::DEFAULT_NUM_ASSETS, NUM_TEST_BARS);

          auto input_col_names = input_cs_data.column_names();
          config["inputs"][inputMetadata.id] = makeInputRefSeq(input_col_names);
          for (const auto& col : input_col_names) {
            // Column names need # prefix to match GetInputId() format
            fields_vec.push_back("#" + col);
            inputs_vec.push_back(input_cs_data[col].array());
          }
        }
      }
    } else if (metadata.inputs.size() == 1 &&
               metadata.inputs.front().allowMultipleConnections) {
      // Non-cross-sectional transform accepting multiple connections

      // Special handling for conditional_select_* transforms: provide at least 2 inputs (condition + value)
      // conditional_select requires: condition0, value0, [condition1, value1, ...], [default_value]
      if (id.starts_with("conditional_select_")) {
        // Provide 2 inputs: condition (Boolean) + value (type-specific)
        config["inputs"][epoch_script::ARG] = makeInputRefSeq({"condition", "value"});
        // Column names need # prefix to match GetInputId() format
        fields_vec = {"#condition", "#value"};
        inputs_vec.emplace_back(getArrayFromType(IODataType::Boolean));  // condition

        // Determine value type from transform name suffix
        IODataType valueType = IODataType::Any;
        if (id == "conditional_select_boolean") {
          valueType = IODataType::Boolean;
        } else if (id == "conditional_select_number") {
          valueType = IODataType::Number;
        } else if (id == "conditional_select_string") {
          valueType = IODataType::String;
        } else if (id == "conditional_select_timestamp") {
          valueType = IODataType::Timestamp;
        } else {
          valueType = metadata.inputs.front().type;  // fallback to metadata type
        }
        inputs_vec.emplace_back(getArrayFromType(valueType));
      } else {
        // Default: provide single input for other VARARG transforms
        config["inputs"][epoch_script::ARG] = makeInputRefSeq({"result"});
        // Column name needs # prefix to match GetInputId() format
        fields_vec = {"#result"};
        inputs_vec.emplace_back(getArrayFromType(metadata.inputs.front().type));
      }
    } else {
      // Regular transforms with one or more single-connection inputs
      for (auto const &[i, inputMetadata] :
           metadata.inputs | std::views::enumerate) {
        std::string col = std::to_string(i);
        config["inputs"][inputMetadata.id] = makeInputRef(col);
        // Column names need # prefix to match GetInputId() format (node_id + "#" + handle)
        fields_vec.emplace_back("#" + col);

        // Special handling for select_N transforms: limit index values to valid range [0, N-1]
        if (id.starts_with("select_") && inputMetadata.id == "index") {
          // Extract N from "select_N" (e.g., "select_2" -> N=2)
          size_t N = std::stoull(id.substr(7)); // Skip "select_" prefix
          inputs_vec.emplace_back(getArrayFromType(inputMetadata.type, N - 1));
        // Special handling for switchN_* transforms: limit index values to valid range [0, N-1]
        } else if (id.starts_with("switch") && inputMetadata.id == "index") {
          // Extract N from "switchN_type" (e.g., "switch3_timestamp" -> N=3)
          size_t underscorePos = id.find('_');
          size_t N = std::stoull(id.substr(6, underscorePos - 6)); // Skip "switch" prefix
          inputs_vec.emplace_back(getArrayFromType(inputMetadata.type, N - 1));
        // Special handling for static_cast_* transforms: provide input matching output type
        } else if (id == "static_cast_to_integer") {
          // Provide integer input for integer type materializer
          inputs_vec.emplace_back(getArrayFromType(IODataType::Integer));
        } else if (id == "static_cast_to_decimal") {
          // Provide decimal input for decimal type materializer
          inputs_vec.emplace_back(getArrayFromType(IODataType::Decimal));
        } else if (id == "static_cast_to_boolean") {
          // Provide boolean input for boolean type materializer
          inputs_vec.emplace_back(getArrayFromType(IODataType::Boolean));
        } else if (id == "static_cast_to_string") {
          // Provide string input for string type materializer
          inputs_vec.emplace_back(getArrayFromType(IODataType::String));
        } else if (id == "static_cast_to_timestamp") {
          // Provide timestamp input for timestamp type materializer
          inputs_vec.emplace_back(getArrayFromType(IODataType::Timestamp));
        // Special handling for groupby_* transforms: provide String for group_key, proper type for value
        } else if ((id == "groupby_numeric_agg" || id == "groupby_boolean_agg" || id == "groupby_any_agg")
                   && inputMetadata.id == "group_key") {
          // Provide String input for groupby key column (common groupby use case)
          inputs_vec.emplace_back(getArrayFromType(IODataType::String));
        } else {
          inputs_vec.emplace_back(getArrayFromType(inputMetadata.type));
        }
      }
    }

    // Get required data sources from metadata and provide OHLCV data
    // Column names need # prefix to match GetInputId() format (node_id + "#" + handle)
    auto requiredDataSources = metadata.requiredDataSources;

    for (auto const &[i, dataSource] :
         requiredDataSources | std::views::enumerate) {
      config["inputs"][dataSource] = makeInputRef(dataSource);
      // Column names need # prefix to match GetInputId() format
      fields_vec.emplace_back("#" + dataSource);
      inputs_vec.emplace_back(dataSources.at(dataSource));
    }

    // Configure options from metadata
    for (epoch_script::MetaDataOption const &optionMetadata :
         metadata.options) {
        auto optionId = optionMetadata.id;
        if (optionMetadata.type == MetaDataOptionType::Integer) {
          // Use metadata defaults directly
          config["options"][optionId] =
              optionMetadata.defaultValue
                  .value_or(epoch_script::MetaDataOptionDefinition{2.0})
                  .GetInteger();
        } else if (optionMetadata.type ==
                   epoch_core::MetaDataOptionType::Decimal) {
          config["options"][optionId] =
              optionMetadata.defaultValue
                  .value_or(epoch_script::MetaDataOptionDefinition{0.2})
                  .GetDecimal();
        } else if (optionMetadata.type ==
                   epoch_core::MetaDataOptionType::Boolean) {
          config["options"][optionId] =
              optionMetadata.defaultValue
                  .value_or(epoch_script::MetaDataOptionDefinition{true})
                  .GetBoolean();
        } else if (optionMetadata.type ==
                   epoch_core::MetaDataOptionType::Select) {
          REQUIRE(optionMetadata.selectOption.size() > 0);
          config["options"][optionId] =
              optionMetadata.defaultValue
                  .value_or(epoch_script::MetaDataOptionDefinition{
                      optionMetadata.selectOption[0].value})
                  .GetSelectOption();
        } else if (optionMetadata.type ==
                   epoch_core::MetaDataOptionType::String) {
          config["options"][optionId] =
              optionMetadata.defaultValue
                  .value_or(epoch_script::MetaDataOptionDefinition{""})
                  .GetString();
        } else if (optionMetadata.type ==
                   epoch_core::MetaDataOptionType::EventMarkerSchema) {
          // Generate minimal valid CardSchema JSON for testing
          if (id == "card_selector_filter") {
            // Use a boolean column from test data
            config["options"][optionId] = R"({
              "title": "Test Selector",
              "select_key": "0",
              "schemas": [{
                "column_id": "0",
                "slot": "Hero",
                "render_type": "Number",
                "color_map": {}
              }]
            })";
          } else if (id == "card_selector_sql") {
            // Use simple SQL query
            config["options"][optionId] = R"({
              "title": "Test SQL Selector",
              "sql": "SELECT * FROM self",
              "schemas": [{
                "column_id": "SLOT0",
                "slot": "Hero",
                "render_type": "Number",
                "color_map": {}
              }]
            })";
          }
        }
      }

    return std::tuple{TransformDefinition{config}, std::move(fields_vec),
                      std::move(inputs_vec)};
  };

  // =============================================================================
  // LEGITIMATE TRANSFORM SKIPS
  // The following transforms are intentionally skipped from this generic test
  // because they have special requirements that can't be auto-generated.
  // Each should have dedicated tests elsewhere in the test suite.
  // =============================================================================

  for (auto const &[id, factory] : transformMap) {
    // SKIP: Trade signal executor - special execution logic for strategy evaluation
    // Dedicated test: test/unit/transforms/signals/* (if exists)
    if (id == epoch_script::transforms::TRADE_SIGNAL_EXECUTOR_ID) {
      continue;
    }

    // SKIP: Reporters and selectors - visualization/selection transforms with no column outputs
    // These transforms produce tearsheets, reports, or UI selections, not data columns
    // Dedicated test: test/unit/transforms/reports/* or test/integration/
    if (metadataMap.contains(id) && metadataMap.at(id).outputs.empty()) {
      continue;
    }

    // SKIP: SQL transforms - deregistered, require custom SQL query strings
    // All sql_query* transforms have been removed from registration
    if (id.starts_with("sql_query")) {
      continue;
    }

    // SKIP: External data source transforms - require live API access
    // These fetch data from Polygon, FRED, SEC, etc. and can't work with synthetic test data
    // Dedicated test: test/integration/external_data_sources/* (if exists)
    const std::unordered_set<std::string> externalDataSources = {
      "economic_indicator", "form13f_holdings", "insider_trading"
    };
    if (polygon::ALL_POLYGON_TRANSFORMS.contains(id) || externalDataSources.contains(id)) {
      continue;
    }

    // SKIP: Conditional select - complex input configuration
    // Requires alternating condition/value pairs that can't be auto-generated
    // Dedicated test: test/unit/transforms/operators/comparative_test.cpp
    if (id == "conditional_select") {
      continue;
    }

    // TODO: Typed conditional_select variants should declare proper metadata
    // If they fail, it means metadata.inputs doesn't match the transform's requirements

    // NOTE: flexible_pivot_detector previously skipped - trying without skip now that
    // requiredDataSources mechanism is working properly. Dedicated test exists at
    // test/unit/transforms/pivot_detector_test.cpp

    // NOTE: groupby_* transforms now have special input overrides (lines 320-323)
    // They receive String input for group_key column instead of default Decimal/Any type

    // NOTE: market_data_source requiredDataSources fixed - now only lists {"o", "h", "l", "c", "v"}
    // to match the 5 outputs. "vw" and "n" moved to separate vwap/trade_count transforms.

    // NOTE: static_cast_* transforms now have special input overrides (lines 304-318)
    // They receive inputs matching their output types instead of Any type

    // SKIP: Scalar category transforms - they now run as inlined constant values at compile time
    // Scalar transforms (number, string, pi, e, etc.) are handled by scalar inlining pass
    // They no longer need to run as runtime transforms
    if (metadataMap.contains(id) &&
        metadataMap.at(id).category == epoch_core::TransformCategory::Scalar) {
      continue;
    }

    // =============================================================================
    // TEST EXECUTION - All other transforms should work with auto-generated config
    // The test intelligently provides data sources based on metadata.requiredDataSources
    // If a transform fails here, it indicates a metadata bug that needs fixing.
    // =============================================================================

    INFO("Transform: " << id);
    REQUIRE(metadataMap.contains(id));

    auto [config, inputIds, inputValues] = makeConfig(id);
    auto transform = factory(TransformConfiguration(config));

    auto df = make_dataframe(index, inputValues, inputIds);

    // Execute transform - if it fails, the INFO above will show which transform
    epoch_frame::DataFrame result;
    try {
      INFO("DataFrame columns: " << [&]() {
        std::string cols;
        for (const auto& col : df.column_names()) cols += col + ", ";
        return cols;
      }());
      result = transform->TransformData(df);
    } catch (const std::exception& e) {
      FAIL("Transform '" << id << "' failed with error: " << e.what()
           << "\nDataFrame had columns: " << [&]() {
             std::string cols;
             for (const auto& col : df.column_names()) cols += col + ", ";
             return cols;
           }()
           << "\nThis indicates a metadata bug - either:"
           << "\n  1. metadata.requiredDataSources is incomplete/incorrect, OR"
           << "\n  2. transform accesses data in non-standard way not reflected in metadata");
    }

    const auto& transformMetadata = metadataMap.at(id);
    auto outputs = transformMetadata.outputs;

    // Cross-sectional transforms output one column per asset, not per metadata output
    if (transformMetadata.isCrossSectional) {
      // For cross-sectional: result columns should match input asset columns
      REQUIRE(result.num_cols() > 0);  // At least some columns produced

      // Validate each output column has the correct type
      for (auto const &output : outputs) {
        for (const auto& col_name : result.column_names()) {
          auto col = result[col_name];
          INFO("Cross-sectional Output: " << col_name << " (type from metadata: " << static_cast<int>(output.type) << ")");

          switch (output.type) {
          case IODataType::Any:
            // Any type can be any Arrow type including NULL
            break;
          case IODataType::Decimal:
          case IODataType::Number:
            REQUIRE(col.dtype()->id() == arrow::Type::DOUBLE);
            break;
          case IODataType::Integer:
            REQUIRE((col.dtype()->id() == arrow::Type::INT32 ||
                     col.dtype()->id() == arrow::Type::INT64));
            break;
          case IODataType::Timestamp:
            REQUIRE(col.dtype()->id() == arrow::Type::TIMESTAMP);
            break;
          case IODataType::Boolean:
            REQUIRE(col.dtype()->id() == arrow::Type::BOOL);
            break;
          case IODataType::String:
            REQUIRE(col.dtype()->id() == arrow::Type::STRING);
            break;
          default:
            break;
          }
        }
      }
    } else {
      // Non-cross-sectional: outputs should match 1:1 with result columns
      REQUIRE(outputs.size() == result.num_cols());

      for (auto const &output : outputs) {
        auto outputCol = transform->GetOutputId(output.id);
        INFO("Output: " << outputCol << "\nresult:\n" << result);

        REQUIRE(result.contains(outputCol));

        auto col = result[outputCol];
        switch (output.type) {
        case IODataType::Any:
          // Any type can be any Arrow type including NULL
          // No type checking needed for Any
          break;
        case IODataType::Decimal:
        case IODataType::Number:
          REQUIRE(col.dtype()->id() == arrow::Type::DOUBLE);
          break;
        case IODataType::Integer:
          // Strict integer type checking - should be INT32 or INT64 only
          REQUIRE((col.dtype()->id() == arrow::Type::INT32 ||
                   col.dtype()->id() == arrow::Type::INT64));
          break;
        case IODataType::Timestamp:
          // Timestamp type should be TIMESTAMP
          REQUIRE(col.dtype()->id() == arrow::Type::TIMESTAMP);
          break;
        case IODataType::Boolean:
          REQUIRE(col.dtype()->id() == arrow::Type::BOOL);
          break;
        case IODataType::String:
          REQUIRE(col.dtype()->id() == arrow::Type::STRING);
          break;
        default:
          break;
        }
      }
    }
  }
}