#pragma once
//
// Created by dewe on 1/29/23.
//

#include <epoch_data_sdk/dataloader/dataloader.hpp>
#include <epoch_data_sdk/dataloader/options.hpp>
#include <epoch_data_sdk/model/asset/asset.hpp>

#include <epoch_script/strategy/data_options.h>
#include <epoch_script/strategy/date_period_config.h>
#include <epoch_script/strategy/strategy_config.h>
#include <epoch_script/transforms/runtime/iorchestrator.h>
#include <epoch_script/data/database/updates/iwebsocket_manager.h>
#include <epoch_script/core/futures_continuation_input.h>
#include <epoch_script/data/database/database.h>

namespace epoch_script::data {

// Namespace aliases for epoch_data_sdk types
using IDataLoader = data_sdk::IDataLoader;
using IDataLoaderPtr = std::shared_ptr<IDataLoader>;
using DataloaderOption = data_sdk::DataLoaderOptions;

class WebSocketManagerSingleton {
public:
  static WebSocketManagerSingleton &instance() {
    static WebSocketManagerSingleton instance;
    return instance;
  }

  IWebSocketManagerPtr GetWebSocketManager(AssetClass assetClass);

private:
  WebSocketManagerSingleton();

  asset::AssetClassMap<IWebSocketManagerPtr> m_webSocketManager;
};

struct DataModuleOption {
  DataloaderOption loader;

  std::optional<FuturesContinuationInput> futureContinuation = {};

  std::vector<epoch_script::TimeFrame> barResampleTimeFrames = {};

  epoch_script::runtime::ITransformManagerPtr transformManager = nullptr;

  bool liveUpdates = false;
};

namespace factory {
class DataModuleFactory {
public:
  explicit DataModuleFactory(DataModuleOption option);

  std::unique_ptr<Database> CreateDatabase();

  [[nodiscard]] const DataModuleOption& GetOption() const;

  ~DataModuleFactory() = default;

protected:
  DataModuleOption m_option;
};

using DataModuleFactoryPtr = std::unique_ptr<DataModuleFactory>;
} // namespace factory

namespace factory {
// Extract auxiliary data categories from transform configurations
// Uses central GetDataCategoryForTransform() from data_category_mapper
std::vector<DataCategory>
ExtractAuxiliaryCategoriesFromTransforms(
    epoch_script::transform::TransformConfigurationPtrList const &configs);

// Extract cross-sectional data categories (FRED economic indicators) from transform configurations
std::vector<CrossSectionalDataCategory>
ExtractCrossSectionalCategoriesFromTransforms(
    epoch_script::transform::TransformConfigurationPtrList const &configs);

// Extract and validate index tickers from indices/common_indices transforms
// Throws if ticker's asset (e.g., "SPX-Indices") doesn't exist in AssetDatabase
std::set<std::string>
ExtractIndicesTickersFromTransforms(
    epoch_script::transform::TransformConfigurationPtrList const &configs);

void ProcessConfigurations(
    std::vector<std::unique_ptr<
        epoch_script::transform::TransformConfiguration>> const &configs,
    epoch_script::TimeFrame const &baseTimeframe,
    DataModuleOption &dataModuleOption);

// Strategy-aware factory - auto-detects primaryCategory from StrategyConfig
// by checking if any component requires intraday data
DataModuleOption
MakeDataModuleOptionFromStrategy(CountryCurrency baseCurrency,
                                 epoch_script::strategy::DatePeriodConfig const &period,
                                 epoch_script::strategy::StrategyConfig const &strategyConfig);

std::array<asset::AssetHashSet, 3>
MakeAssets(epoch_core::CountryCurrency baseCurrency,
           std::vector<std::string> const &assetIds, bool hasContinuation);
} // namespace factory
} // namespace epoch_script::data