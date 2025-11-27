//
// Comprehensive tests for src/data/factory.cpp
// Tests public API functions for full coverage
//
#include "catch.hpp"
#include <epoch_script/data/factory.h>
#include "epoch_script/core/constants.h"
#include "epoch_script/strategy/data_options.h"
#include "epoch_script/strategy/date_period_config.h"
#include "epoch_script/strategy/strategy_config.h"
#include "epoch_script/strategy/introspection.h"
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_data_sdk/model/asset/asset_specification.hpp>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_script/transforms/core/transform_definition.h>
#include "transforms/runtime/transform_manager/transform_manager.h"

using namespace epoch_script::data;
using namespace epoch_script::data::factory;
using namespace epoch_script::transform;
using namespace epoch_core;
using namespace data_sdk::asset;
using epoch_script::MetaDataArgDefinitionMapping;

// ============================================================================
// Tests for MakeDataModuleOption
// NOTE: MakeDataModuleOption has been removed - only MakeDataModuleOptionFromStrategy
// is exposed now. These tests are disabled until they can be rewritten for the new API.
// ============================================================================

// TEST_CASE("MakeDataModuleOption sets date periods correctly", "[factory][options]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
//
//   epoch_script::strategy::DatePeriodConfig period{
//       .from = startDate,
//       .to = endDate
//   };
//
//   epoch_script::strategy::DataOption dataConfig{
//       .assets = {"AAPL-Stocks"},
//       .source = "",
//       .cache_dir = ""
//   };
//
//   auto result = MakeDataModuleOption(
//       CountryCurrency::USD,
//       period,
//       dataConfig,
//       DataCategory::DailyBars
//   );
//
//   REQUIRE(result.loader.startDate == startDate);
//   REQUIRE(result.loader.endDate == endDate);
// }

// TEST_CASE("MakeDataModuleOption sets primary category", "[factory][options]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
// 
//   epoch_script::strategy::DatePeriodConfig period{.from = startDate, .to = endDate};
//   epoch_script::strategy::DataOption dataConfig{.assets = {"AAPL-Stocks"}};
// 
//   auto result = MakeDataModuleOption(
//       CountryCurrency::USD,
//       period,
//       dataConfig,
//       DataCategory::MinuteBars
//   );
// 
//   REQUIRE(result.loader.categories.count(DataCategory::MinuteBars) == 1);
// }
// 
// TEST_CASE("MakeDataModuleOption includes auxiliary categories", "[factory][options]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
// 
//   epoch_script::strategy::DatePeriodConfig period{.from = startDate, .to = endDate};
//   epoch_script::strategy::DataOption dataConfig{.assets = {"AAPL-Stocks"}};
// 
//   std::vector<DataCategory> auxiliaryCategories = {
//       DataCategory::BalanceSheets,
//       DataCategory::News
//   };
// 
//   auto result = MakeDataModuleOption(
//       CountryCurrency::USD,
//       period,
//       dataConfig,
//       DataCategory::DailyBars,
//       auxiliaryCategories
//   );
// 
//   REQUIRE(result.loader.categories.count(DataCategory::DailyBars) == 1);
//   REQUIRE(result.loader.categories.count(DataCategory::BalanceSheets) == 1);
//   REQUIRE(result.loader.categories.count(DataCategory::News) == 1);
//   REQUIRE(result.loader.categories.size() == 3);
// }
// 
// TEST_CASE("MakeDataModuleOption with empty futures continuation", "[factory][options]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
// 
//   epoch_script::strategy::DatePeriodConfig period{.from = startDate, .to = endDate};
//   epoch_script::strategy::DataOption dataConfig{.assets = {"AAPL-Stocks"}};
// 
//   auto result = MakeDataModuleOption(
//       CountryCurrency::USD,
//       period,
//       dataConfig,
//       DataCategory::DailyBars
//   );
// 
//   // Should not have futures continuation for non-futures assets
//   REQUIRE_FALSE(result.futureContinuation.has_value());
// }
// 
// TEST_CASE("MakeDataModuleOption sets source path", "[factory][options]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
// 
//   epoch_script::strategy::DatePeriodConfig period{.from = startDate, .to = endDate};
// 
//   SECTION("Uses custom source path when provided") {
//     epoch_script::strategy::DataOption dataConfig{
//         .assets = {"AAPL-Stocks"},
//         .source = "/custom/path"
//     };
// 
//     auto result = MakeDataModuleOption(
//         CountryCurrency::USD,
//         period,
//         dataConfig,
//         DataCategory::DailyBars
//     );
// 
//     REQUIRE(result.loader.sourcePath.has_value());
//     REQUIRE(result.loader.sourcePath->string() == "/custom/path");
//   }
// 
//   SECTION("Uses default path when source is empty") {
//     epoch_script::strategy::DataOption dataConfig{
//         .assets = {"AAPL-Stocks"},
//         .source = ""
//     };
// 
//     auto result = MakeDataModuleOption(
//         CountryCurrency::USD,
//         period,
//         dataConfig,
//         DataCategory::DailyBars
//     );
// 
//     REQUIRE(result.loader.sourcePath.has_value());
//     REQUIRE(result.loader.sourcePath->string() == DEFAULT_DATABASE_PATH);
//   }
// }
// 
// TEST_CASE("MakeDataModuleOption sets cache directory", "[factory][options]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
// 
//   epoch_script::strategy::DatePeriodConfig period{.from = startDate, .to = endDate};
// 
//   SECTION("Sets cache dir when provided") {
//     epoch_script::strategy::DataOption dataConfig{
//         .assets = {"AAPL-Stocks"},
//         .cache_dir = "/cache/dir"
//     };
// 
//     auto result = MakeDataModuleOption(
//         CountryCurrency::USD,
//         period,
//         dataConfig,
//         DataCategory::DailyBars
//     );
// 
//     REQUIRE(result.loader.cacheDir.has_value());
//     REQUIRE(result.loader.cacheDir->string() == "/cache/dir");
//   }
// 
//   SECTION("No cache dir when empty") {
//     epoch_script::strategy::DataOption dataConfig{
//         .assets = {"AAPL-Stocks"},
//         .cache_dir = ""
//     };
// 
//     auto result = MakeDataModuleOption(
//         CountryCurrency::USD,
//         period,
//         dataConfig,
//         DataCategory::DailyBars
//     );
// 
//     REQUIRE_FALSE(result.loader.cacheDir.has_value());
//   }
// }

// ============================================================================
// Tests for DataModuleFactory Create methods
// ============================================================================
// NOTE: CreateResampler, CreateFutureContinuations, CreateTransforms, and
// CreateWebSocketManager are now private implementation details (free functions
// in factory namespace). They are tested indirectly through CreateDatabase().

// TEST_CASE("DataModuleFactory::CreateResampler with empty timeframes", "[factory][resampler]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
//
//   DataModuleOption option{
//       .loader = {
//           .startDate = startDate,
//           .endDate = endDate,
//           .categories = {DataCategory::DailyBars}
//       },
//       .barResampleTimeFrames = {}
//   };
//
//   factory::DataModuleFactory factory_obj(option);
//   auto resampler = factory_obj.CreateResampler();
//
//   REQUIRE_FALSE(resampler);  // Should return empty ptr
// }

// TEST_CASE("DataModuleFactory::CreateResampler with timeframes", "[factory][resampler]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
//
//   std::vector<epoch_script::TimeFrame> timeframes = {
//       epoch_script::TimeFrame{"5min"},
//       epoch_script::TimeFrame{"1h"}
//   };
//
//   DataModuleOption option{
//       .loader = {
//           .startDate = startDate,
//           .endDate = endDate,
//           .categories = {DataCategory::MinuteBars}
//       },
//       .barResampleTimeFrames = timeframes
//   };
//
//   factory::DataModuleFactory factory_obj(option);
//   auto resampler = factory_obj.CreateResampler();
//
//   REQUIRE(resampler);  // Should return valid resampler
// }
//
// TEST_CASE("DataModuleFactory::CreateFutureContinuations", "[factory][continuations]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
//
//   SECTION("Returns empty when no continuation config") {
//     DataModuleOption option{
//         .loader = {
//             .startDate = startDate,
//             .endDate = endDate,
//             .categories = {DataCategory::DailyBars}
//         },
//         .futureContinuation = std::nullopt
//     };
//
//     factory::DataModuleFactory factory_obj(option);
//     auto continuation = factory_obj.CreateFutureContinuations();
//
//     REQUIRE_FALSE(continuation);
//   }
//
//   SECTION("Creates continuation constructor when config provided") {
//     FuturesContinuationInput input{
//         .rollover = epoch_core::RolloverType::LastTradingDay,
//         .type = AdjustmentType::BackwardRatio,
//         .arg = 5
//     };
//
//     DataModuleOption option{
//         .loader = {
//             .startDate = startDate,
//             .endDate = endDate,
//             .categories = {DataCategory::DailyBars}
//         },
//         .futureContinuation = input
//     };
//
//     factory::DataModuleFactory factory_obj(option);
//     auto continuation = factory_obj.CreateFutureContinuations();
//
//     REQUIRE(continuation);
//   }
// }
//
// TEST_CASE("DataModuleFactory::CreateTransforms", "[factory][transforms]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
//
//   AssetHashSet strategyAssets;
//   strategyAssets.insert(MakeAsset(AssetSpecificationQuery{"AAPL-Stocks"}));
//   strategyAssets.insert(MakeAsset(AssetSpecificationQuery{"MSFT-Stocks"}));
//
//   DataModuleOption option{
//       .loader = {
//           .startDate = startDate,
//           .endDate = endDate,
//           .categories = {DataCategory::DailyBars},
//           .strategyAssets = strategyAssets
//       }
//   };
//
//   factory::DataModuleFactory factory_obj(option);
//   auto orchestrator = factory_obj.CreateTransforms();
//
//   REQUIRE(orchestrator);
// }
//
// TEST_CASE("DataModuleFactory::CreateWebSocketManager always returns empty", "[factory][websocket]") {
//   auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
//   auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();
//
//   DataModuleOption option{
//       .loader = {
//           .startDate = startDate,
//           .endDate = endDate,
//           .categories = {DataCategory::DailyBars}
//       },
//       .liveUpdates = true
//   };
//
//   factory::DataModuleFactory factory_obj(option);
//   auto wsManager = factory_obj.CreateWebSocketManager();
//
//   // Currently disabled, should return empty
//   REQUIRE(wsManager.empty());
// }

TEST_CASE("DataModuleFactory::CreateDatabase integrates all components", "[factory][database]") {
  auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
  auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();

  AssetHashSet assets;
  assets.insert(MakeAsset(AssetSpecificationQuery{"AAPL-Stocks"}));

  DataModuleOption option{
      .loader = {
          .startDate = startDate,
          .endDate = endDate,
          .requests = {},
          .dataloaderAssets = assets,
          .strategyAssets = assets
      },
      .transformManager = std::make_unique<epoch_script::runtime::TransformManager>()
  };
  option.loader.AddRequest(DataCategory::DailyBars);

  factory::DataModuleFactory factory_obj(std::move(option));
  auto database = factory_obj.CreateDatabase();

  REQUIRE(database);
}

// ============================================================================
// Tests for ProcessConfigurations
// ============================================================================

// Helper to create test transform configuration
TransformConfiguration MakeTestConfig(
    std::string const &transformType,
    epoch_core::TransformCategory category,
    epoch_script::TimeFrame const &timeframe) {

  epoch_script::TransformDefinitionData data{
      .type = transformType,
      .id = transformType + "_test",
      .options = {},
      .timeframe = timeframe,
      .inputs = {},
      .metaData = {
          .id = transformType,
          .category = category,
          .plotKind = epoch_core::TransformPlotKind::Null,
          .name = transformType,
          .options = {},
          .isCrossSectional = false,
          .desc = "Test transform",
          .inputs = {},
          .outputs = {},
          .atLeastOneInputRequired = false,
          .tags = {},
          .requiresTimeFrame = false,
          .requiredDataSources = {},
      }};

  return TransformConfiguration(epoch_script::TransformDefinition(std::move(data)));
}

TEST_CASE("ProcessConfigurations adds resampling timeframes", "[factory][process]") {
  auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
  auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();

  DataModuleOption option{
      .loader = {
          .startDate = startDate,
          .endDate = endDate,
          .requests = {}
      }
  };
  option.loader.AddRequest(DataCategory::MinuteBars);

  auto baseTimeframe = epoch_script::TimeFrame{"1min"};

  std::vector<std::unique_ptr<TransformConfiguration>> configs;
  configs.push_back(std::make_unique<TransformConfiguration>(
      MakeTestConfig("sma", epoch_core::TransformCategory::Trend, epoch_script::TimeFrame{"5min"})));
  configs.push_back(std::make_unique<TransformConfiguration>(
      MakeTestConfig("rsi", epoch_core::TransformCategory::Momentum, epoch_script::TimeFrame{"1h"})));

  ProcessConfigurations(configs, baseTimeframe, option);

  // Both 5min and 1h should be added (different from base 1min)
  REQUIRE(option.barResampleTimeFrames.size() == 2);
}

TEST_CASE("ProcessConfigurations does not add base timeframe to resampling", "[factory][process]") {
  auto startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date();
  auto endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date();

  DataModuleOption option{
      .loader = {
          .startDate = startDate,
          .endDate = endDate,
          .requests = {}
      }
  };
  option.loader.AddRequest(DataCategory::DailyBars);

  auto baseTimeframe = epoch_script::TimeFrame{"1d"};

  std::vector<std::unique_ptr<TransformConfiguration>> configs;
  configs.push_back(std::make_unique<TransformConfiguration>(
      MakeTestConfig("sma", epoch_core::TransformCategory::Trend, epoch_script::TimeFrame{"1d"})));

  ProcessConfigurations(configs, baseTimeframe, option);

  // Same timeframe as base should not be added
  REQUIRE(option.barResampleTimeFrames.empty());
}
