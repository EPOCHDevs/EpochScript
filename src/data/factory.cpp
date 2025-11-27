//
// Created by dewe on 1/29/23.
//
#include "../../include/epoch_script/data/database/database.h"
#include "data/database/resample.h"
#include "../../include/epoch_script/data/database/updates/iwebsocket_manager.h"
#include "data/futures_continuation/continuations.h"
#include <epoch_script/data/factory.h>
#include <epoch_script/common/env_loader.h>
#include "data/database/database_impl.h"
#include "data/database/updates/alpaca_websocket_manager.h"

// Use epoch_data_sdk for all dataloader infrastructure
#include <epoch_data_sdk/dataloader/dataloader.hpp>
#include <epoch_data_sdk/dataloader/factory.hpp>
#include <epoch_data_sdk/dataloader/options.hpp>
#include <epoch_data_sdk/dataloader/fetch_kwargs.hpp>

#include "epoch_script/core/constants.h"
#include "epoch_script/strategy/introspection.h"
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_data_sdk/model/asset/asset_specification.hpp>
#include <epoch_data_sdk/model/asset/index_constituents.hpp>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_script/transforms/runtime/iorchestrator.h>
#include <epoch_script/transforms/runtime/transform_manager/itransform_manager.h>
#include "transforms/runtime/transform_manager/transform_manager.h"
#include "transforms/components/data_sources/data_category_mapper.h"
#include <cctype>
#include <cstring>
#include <spdlog/spdlog.h>
#include <unordered_set>
#include <variant>

namespace epoch_script::data {

// Namespace aliases for epoch_data_sdk
namespace asset = data_sdk::asset;

// Import asset builder functions
using asset::MakeAsset;
using asset::AssetSpecificationQuery;

IWebSocketManagerPtr
WebSocketManagerSingleton::GetWebSocketManager(AssetClass assetClass) {
  try {
    return m_webSocketManager.at(assetClass);
  } catch (std::out_of_range const &e) {
    throw std::runtime_error(std::format(
        "WebSocket manager for asset class {} not found",
        epoch_core::AssetClassWrapper::ToLongFormString(assetClass)));
  }
}

WebSocketManagerSingleton::WebSocketManagerSingleton() {
  m_webSocketManager = asset::AssetClassMap<IWebSocketManagerPtr>();
  // NOTE: AlpacaWebSocketManager disabled - not used for backtesting
  // Re-enable when live trading support is needed
  // for (auto assetClass :
  //      {epoch_core::AssetClass::Stocks, epoch_core::AssetClass::Crypto}) {
  //   m_webSocketManager[assetClass] = std::make_unique<AlpacaWebSocketManager>(
  //       AlpacaWebSocketManagerOptions{.assetClass = assetClass,
  //                                     .key = ENV("ALPACA_API_KEY").c_str(),
  //                                     .secret = ENV("ALPACA_API_SECRET").c_str()});
  // }
}

namespace factory {

// NOTE: CreateDataStreamer removed for EpochScript - not needed for backtesting
// DataStreamer was only needed for StratifyX's campaign runner

IDataLoaderPtr CreateDataloader(DataModuleOption const& option) {
  // Use epoch_data_sdk factory to create API+cache dataloader
  SPDLOG_INFO("Creating API+cache dataloader from epoch_data_sdk");
  return data_sdk::dataloader::CreateApiCacheDataLoader(
      option.loader, EPOCH_DB_S3);
}

IFuturesContinuationConstructor::Ptr CreateFutureContinuations(DataModuleOption const& option) {
  auto futuresContinuation = option.futureContinuation;
  if (!futuresContinuation) {
    return {};
  }

  return std::make_unique<FuturesContinuationConstructor>(
      std::make_unique<FuturesContinuation>(
          MakeRolloverMethod(futuresContinuation->rollover,
                             futuresContinuation->arg),
          MakeAdjustmentMethod(futuresContinuation->type)));
}

epoch_script::runtime::IDataFlowOrchestrator::Ptr CreateTransforms(DataModuleOption const& option) {
  std::set<std::string> assetIds;
  for (const auto& asset : option.loader.strategyAssets) {
    assetIds.insert(asset.GetID());
  }

  AssertFromStream(option.transformManager, "TransformManager is not initialized");

  auto managerCopy = std::make_unique<epoch_script::runtime::TransformManager>();
  for (const auto& config : *option.transformManager->GetTransforms()) {
    managerCopy->Insert(*config);
  }

  return epoch_script::runtime::CreateDataFlowRuntimeOrchestrator(
      assetIds,
      std::move(managerCopy));
}

IResamplerPtr CreateResampler(DataModuleOption const& option) {
    if (option.barResampleTimeFrames.empty()) {
        return {};
    }

    return std::make_unique<Resampler>(option.barResampleTimeFrames,
                                       option.loader.GetPrimaryCategory() ==
                                           DataCategory::MinuteBars);
}

asset::AssetClassMap<IWebSocketManagerPtr> CreateWebSocketManager() {
  // if (!m_option.liveUpdates) {
    return {};
  // }
  //
  // asset::AssetClassMap<IWebSocketManagerPtr> result;
  // for (auto [assetClass, assets] :
  //      asset::ExtractAssetsByAssetClass(m_option.loader.dataloaderAssets)) {
  //   result
  //       .insert_or_assign(assetClass, WebSocketManagerSingleton::instance()
  //                                         .GetWebSocketManager(assetClass))
  //       .first->second->Subscribe(assets);
  // }
  // return result;
}

    DataModuleFactory::DataModuleFactory(DataModuleOption option)
    : m_option(std::move(option)) {}

    const DataModuleOption& DataModuleFactory::GetOption() const { return m_option; }


std::unique_ptr<Database> DataModuleFactory::CreateDatabase() {
  return std::make_unique<Database>(
      std::make_unique<DatabaseImpl>(DatabaseImplOptions{
          .dataloader = CreateDataloader(m_option),
          .dataTransform = CreateTransforms(m_option),
          .futuresContinuationConstructor = CreateFutureContinuations(m_option),
          .resampler = CreateResampler(m_option),
          .websocketManager = CreateWebSocketManager()}));
}

std::array<asset::AssetHashSet, 3>
MakeAssets(epoch_core::CountryCurrency baseCurrency,
           std::vector<std::string> const &assetIds, bool hasContinuation) {
  asset::AssetHashSet dataloaderAssets, strategyAssets, continuationAssets;

  // Note: Asset IDs are now pre-resolved and validated by AssetIDContainer
  // Index expansion and FX/Crypto prefix handling is done before this function
  for (auto id : assetIds) {
    using namespace asset;

    // Create asset from the resolved ID
    const auto asset = MakeAsset(AssetSpecificationQuery{id});

    if (asset.IsFuturesContract()) {
      // we need to load the futures contract
      dataloaderAssets.insert(asset);

      if (hasContinuation) {
        auto continuationAsset = asset.MakeFuturesContinuation();
        continuationAssets.insert(continuationAsset);
        strategyAssets.insert(continuationAsset);
      }
      continue;
    }
    if (asset.IsFX() or asset.IsCrypto()) {
      auto [base, counter] = asset.GetCurrencyPair();
      if (base != baseCurrency && counter != baseCurrency) {
        for (auto const &currency : {base, counter}) {
          const epoch_script::Symbol symbol{
              std::format("^{}{}", currency.ToString(),
                          CountryCurrencyWrapper::ToString(baseCurrency))};
          const asset::Asset newAsset =
              MakeAsset(symbol.get(), asset.GetAssetClass(),
                        asset.GetExchange(), asset.GetCurrency());
          if (!dataloaderAssets.contains(newAsset)) {
            SPDLOG_INFO(
                "Added {} to list of Dataloader assets for FX conversion.",
                symbol.get());
            dataloaderAssets.insert(newAsset);
          }
        }
      }
    }
    dataloaderAssets.insert(asset);
    strategyAssets.insert(asset);
  }
  return {dataloaderAssets, strategyAssets, continuationAssets};
}

std::optional<FuturesContinuationInput> MakeContinuations(
    asset::AssetHashSet const &assets,
    std::optional<epoch_script::strategy::TemplatedGenericFunction<
        epoch_core::RolloverType>> const &config) {
    if (assets.empty() || !config ||
        config->type == epoch_core::RolloverType::Null) {
        return std::nullopt;
        }

    FuturesContinuationInput continuationOption{
        .rollover = config->type,
        .type = epoch_core::lookupDefault(config->args, "adjustment", epoch_script::MetaDataOptionDefinition{"BackwardRatio"})
                    .GetSelectOption<AdjustmentType>(),
        .arg = 0};

    AssertFromFormat(continuationOption.rollover !=
                         epoch_core::RolloverType::Null,
                     "epoch_core::RolloverType is Null");
    if (continuationOption.rollover == epoch_core::RolloverType::LiquidityBased) {
        continuationOption.arg = static_cast<int>(
            epoch_core::lookupDefault(config->args, "ratio", epoch_script::MetaDataOptionDefinition{0.3}).GetDecimal() * 100);
    } else {
        continuationOption.arg = static_cast<int>(
            epoch_core::lookupDefault(config->args, "offset", epoch_script::MetaDataOptionDefinition{0.0}).GetInteger());
    }

    return continuationOption;
}

// NOTE: Typed config functions (CreateFinancialsConfig, CreateMacroEconomicsConfig, CreateAlternativeDataConfig)
// have been removed as the new data_sdk no longer uses typed configurations.
// All categories now use std::monostate in AuxiliaryCategoryConfig.

// Helper to get AssetClass from ReferenceAgg transform type
epoch_core::AssetClass GetAssetClassForReferenceAggTransform(std::string const& transformType) {
  using namespace epoch_script::polygon;
  if (transformType == COMMON_REFERENCE_STOCKS || transformType == REFERENCE_STOCKS) {
    return epoch_core::AssetClass::Stocks;
  }
  if (transformType == COMMON_FX_PAIRS || transformType == FX_PAIRS) {
    return epoch_core::AssetClass::FX;
  }
  if (transformType == COMMON_CRYPTO_PAIRS || transformType == CRYPTO_PAIRS) {
    return epoch_core::AssetClass::Crypto;
  }
  if (transformType == COMMON_INDICES || transformType == INDICES) {
    return epoch_core::AssetClass::Indices;
  }
  throw std::runtime_error(std::format("Unknown ReferenceAgg transform type: {}", transformType));
}

std::vector<data_sdk::dataloader::DataRequest>
ExtractAuxiliaryCategoriesFromTransforms(
    epoch_script::transform::TransformConfigurationPtrList const &configs) {

  using namespace data_sdk::dataloader;

  // Use a map to deduplicate by category (latest kwargs wins if same category appears twice)
  // For ReferenceAgg, we need to track by ticker+asset_class since each has unique data
  std::map<DataCategory, DataRequest> requestMap;
  // ReferenceAgg requests keyed by "ticker:asset_class" to allow multiple
  std::map<std::string, DataRequest> referenceAggMap;

  for (auto const &config : configs) {
    auto const &metadata = config->GetTransformDefinition().GetMetadata();

    // Only process DataSource transforms
    if (metadata.category != epoch_core::TransformCategory::DataSource) {
      continue;
    }

    // Get the transform type/id and map to DataCategory using central mapper
    std::string transformType = config->GetTransformName();
    auto category = epoch_script::data_sources::GetDataCategoryForTransform(transformType);

    if (category.has_value()) {
      // Skip if it's a time-series category (those are primary, not auxiliary)
      if (!IsTimeSeriesCategory(*category)) {
        FetchKwargs kwargs = NoKwargs{};

        // Build kwargs based on category and config options
        if (*category == DataCategory::Dividends) {
          DividendsKwargs dk;
          auto typeStr = config->GetOptionValue("dividend_type").GetSelectOption();
          if (!typeStr.empty()) {
            dk.dividend_type = data_sdk::DividendTypeWrapper::FromString(typeStr);
          }
          kwargs = dk;
          requestMap[*category] = DataRequest{*category, kwargs};
        }
        else if (*category == DataCategory::BalanceSheets) {
          BalanceSheetsKwargs bk;
          auto period = config->GetOptionValue("period").GetSelectOption();
          bk.timeframe = data_sdk::BalanceSheetTimeframeWrapper::FromString(period);
          kwargs = bk;
          requestMap[*category] = DataRequest{*category, kwargs};
        }
        else if (*category == DataCategory::IncomeStatements ||
                 *category == DataCategory::CashFlowStatements) {
          FinancialsKwargs fk;
          auto period = config->GetOptionValue("period").GetSelectOption();
          fk.timeframe = data_sdk::FinancialsTimeframeWrapper::FromString(period);
          kwargs = fk;
          requestMap[*category] = DataRequest{*category, kwargs};
        }
        else if (*category == DataCategory::ReferenceAgg) {
          // ReferenceAgg transforms need ticker and asset_class
          ReferenceAggKwargs rk;

          // Get ticker from config options
          // common_* transforms use SelectOption, others use String
          SPDLOG_INFO("Processing ReferenceAgg transform: type={}", transformType);
          if (transformType == epoch_script::polygon::COMMON_INDICES ||
              transformType == epoch_script::polygon::COMMON_REFERENCE_STOCKS ||
              transformType == epoch_script::polygon::COMMON_FX_PAIRS ||
              transformType == epoch_script::polygon::COMMON_CRYPTO_PAIRS) {
            rk.ticker = config->GetOptionValue("ticker").GetSelectOption();
            SPDLOG_INFO("  -> SelectOption ticker: {}", rk.ticker);
          } else {
            rk.ticker = config->GetOptionValue("ticker").GetString();
            SPDLOG_INFO("  -> String ticker: {}", rk.ticker);
          }

          rk.asset_class = GetAssetClassForReferenceAggTransform(transformType);
          SPDLOG_INFO("  -> Asset class: {}", epoch_core::AssetClassWrapper::ToLongFormString(rk.asset_class));
          // is_eod will be set by dataloader from primary category

          // Validate: Check if asset exists in AssetDatabase
          // Use ToLongFormString for full asset class name (e.g., "Stocks" not "STK")
          // Asset ID format: "TICKER-AssetClass" for Stocks, "^TICKER-AssetClass" for FX/Crypto/Indices
          std::string assetClassName = epoch_core::AssetClassWrapper::ToLongFormString(rk.asset_class);
          std::string assetId;
          if (rk.asset_class == epoch_core::AssetClass::Stocks) {
            assetId = std::format("{}-{}", rk.ticker, assetClassName);
          } else {
            // FX, Crypto, Indices use ^ prefix
            assetId = std::format("^{}-{}", rk.ticker, assetClassName);
          }

          const auto& specs = asset::AssetSpecificationDatabase::GetInstance().GetAssetSpecifications();
          if (!specs.contains(assetId)) {
            throw std::runtime_error(std::format(
                "ReferenceAgg ticker '{}' validation failed: asset '{}' not found in AssetDatabase",
                rk.ticker, assetId));
          }

          // Key by ticker:asset_class to allow multiple reference aggs
          std::string key = assetId;  // Use validated assetId as key
          referenceAggMap[key] = DataRequest{*category, rk};
          SPDLOG_DEBUG("Extracted ReferenceAgg request: ticker={}, asset_class={} (validated)",
              rk.ticker, assetClassName);
        }
        else {
          // Other auxiliary categories with NoKwargs
          requestMap[*category] = DataRequest{*category, kwargs};
        }
      }
    }
  }

  // Convert maps to vector
  std::vector<DataRequest> result;
  result.reserve(requestMap.size() + referenceAggMap.size());
  for (auto& [_, request] : requestMap) {
    result.push_back(std::move(request));
  }
  for (auto& [_, request] : referenceAggMap) {
    result.push_back(std::move(request));
  }
  return result;
}

std::vector<CrossSectionalDataCategory>
ExtractCrossSectionalCategoriesFromTransforms(
    epoch_script::transform::TransformConfigurationPtrList const &configs) {

  // Use a set to deduplicate categories
  std::set<CrossSectionalDataCategory> categorySet;

  for (auto const &config : configs) {
    auto const &metadata = config->GetTransformDefinition().GetMetadata();

    // Only process DataSource transforms
    if (metadata.category != epoch_core::TransformCategory::DataSource) {
      continue;
    }

    // Get the transform type/id
    std::string transformType = config->GetTransformName();

    // Check if it's the economic_indicator transform
    if (transformType == epoch_script::fred::ECONOMIC_INDICATOR) {
      // Extract the selected category option (e.g., "CPI", "GDP")
      auto category_str = config->GetOptionValue("category").GetSelectOption();

      // Map string to CrossSectionalDataCategory enum
      auto csCategory = CrossSectionalDataCategoryWrapper::FromString(category_str);
      categorySet.insert(csCategory);
    }
  }

  // Convert set to vector
  return std::vector<CrossSectionalDataCategory>(categorySet.begin(), categorySet.end());
}

std::set<std::string>
ExtractIndicesTickersFromTransforms(
    epoch_script::transform::TransformConfigurationPtrList const &configs) {

  std::set<std::string> tickerSet;

  for (auto const &config : configs) {
    auto const &metadata = config->GetTransformDefinition().GetMetadata();

    // Only process DataSource transforms
    if (metadata.category != epoch_core::TransformCategory::DataSource) {
      continue;
    }

    std::string transformType = config->GetTransformName();

    // Check if it's indices or common_indices transform
    if (transformType == epoch_script::polygon::INDICES ||
        transformType == epoch_script::polygon::COMMON_INDICES) {

      // Extract ticker option
      // common_indices uses SelectOption, indices uses String
      std::string ticker;
      if (transformType == epoch_script::polygon::COMMON_INDICES) {
        ticker = config->GetOptionValue("ticker").GetSelectOption();
      } else {
        ticker = config->GetOptionValue("ticker").GetString();
      }

      // Validate: Check if asset "{ticker}-Indices" exists in AssetDatabase
      std::string assetId = std::format("^{}-Indices", ticker);  // e.g., "SPX-Indices"

      // Check if asset ID exists in the database
      const auto& specs = asset::AssetSpecificationDatabase::GetInstance().GetAssetSpecifications();
      if (!specs.contains(assetId)) {
        throw std::runtime_error(std::format(
            "Index ticker '{}' validation failed: asset '{}' not found in AssetDatabase",
            ticker, assetId));
      }

      // Asset exists, add ticker
      tickerSet.insert(ticker);
      SPDLOG_DEBUG("Index ticker '{}' validated (asset '{}' exists)", ticker, assetId);
    }
  }

  return tickerSet;
}

void ProcessConfigurations(
    std::vector<std::unique_ptr<
        epoch_script::transform::TransformConfiguration>> const
        &configurations,
    epoch_script::TimeFrame const &baseTimeframe,
    DataModuleOption &dataModuleOption) {

  for (auto const &definition : configurations) {
    auto timeframe = definition->GetTimeframe();
    if (timeframe != baseTimeframe) {
      dataModuleOption.barResampleTimeFrames.emplace_back(timeframe);
    }
  }

  // Extract all auxiliary categories including ReferenceAgg (Indices, Stocks, FX, Crypto)
  auto detectedRequests = ExtractAuxiliaryCategoriesFromTransforms(configurations);
  for (auto const& request : detectedRequests) {
    dataModuleOption.loader.AddRequest(request);
  }

  // Extract cross-sectional categories (FRED economic indicators)
  auto crossSectionalCategories = ExtractCrossSectionalCategoriesFromTransforms(configurations);
  for (auto const& cat : crossSectionalCategories) {
    dataModuleOption.loader.AddCrossSectionalCategory(cat);
  }

  // NOTE: ExtractIndicesTickersFromTransforms is no longer called here because
  // indices are now extracted by ExtractAuxiliaryCategoriesFromTransforms as ReferenceAgg.
  // Index validation (checking if asset exists in AssetDatabase) is not performed.
}

DataModuleOption
MakeDataModuleOption(CountryCurrency baseCurrency,
                     epoch_script::strategy::DatePeriodConfig const &period,
                     epoch_script::strategy::DataOption const &config,
                     DataCategory primaryCategory,
                     std::vector<DataCategory> const &auxiliaryCategories={}) {
  // Resolve and validate asset IDs using AssetIDContainer
  const auto resolvedAssetIds = config.assets.Resolve();

  const auto [dataloaderAssets, strategyAssets, continuationAssets] =
      MakeAssets(baseCurrency, resolvedAssetIds,
                 config.futures_continuation.has_value());

  DataModuleOption dataModuleConfig{
      .loader = {.startDate = period.from,
                 .endDate = period.to,
                 .requests = {},  // Will be filled below
                 .dataloaderAssets = dataloaderAssets,
                 .strategyAssets = strategyAssets,
                 .continuationAssets = continuationAssets,
                 .provider = data_sdk::DataProvider::Polygon,
                 .sourcePath = config.source.empty()
                     ? std::make_optional<std::filesystem::path>(DEFAULT_DATABASE_PATH)
                     : std::make_optional<std::filesystem::path>(config.source),
                 .cacheDir = config.cache_dir.empty()
                     ? std::nullopt
                     : std::make_optional<std::filesystem::path>(config.cache_dir)},
      .futureContinuation =
          MakeContinuations(continuationAssets, config.futures_continuation),
      .barResampleTimeFrames = {},
      .liveUpdates = false};

  // Add primary category and auxiliary categories using new API
  dataModuleConfig.loader.AddRequest(primaryCategory);
  for (auto const& cat : auxiliaryCategories) {
    dataModuleConfig.loader.AddRequest(cat);
  }

  return dataModuleConfig;
}

DataModuleOption MakeDataModuleOptionFromStrategy(
    CountryCurrency baseCurrency,
    epoch_script::strategy::DatePeriodConfig const &period,
    epoch_script::strategy::StrategyConfig const &strategyConfig
) {
    AssertFromStream(strategyConfig.trade_signal.source.has_value(),
                     "StrategyConfig must have trade_signal.source");

    auto primaryCategory = strategy::IsIntradayCampaign(strategyConfig)
                         ? DataCategory::MinuteBars
                         : DataCategory::DailyBars;

    auto dataModuleOption = MakeDataModuleOption(baseCurrency, period,
                                                 strategyConfig.data, primaryCategory);

    if (strategyConfig.trade_signal.source.has_value()) {
        const auto& source = strategyConfig.trade_signal.source.value();
        auto baseTimeframe = primaryCategory == DataCategory::MinuteBars
            ? TimeFrame(std::string(epoch_script::tf_str::k1Min))
            : TimeFrame(std::string(epoch_script::tf_str::k1D));

        dataModuleOption.transformManager = epoch_script::runtime::CreateTransformManager(source);

        for (const auto& config : *dataModuleOption.transformManager->GetTransforms()) {
            auto timeframe = config->GetTimeframe();
            if (timeframe != baseTimeframe) {
                dataModuleOption.barResampleTimeFrames.emplace_back(timeframe);
            }
        }

        // Extract all auxiliary categories including ReferenceAgg (Indices, Stocks, FX, Crypto)
        auto detectedRequests = ExtractAuxiliaryCategoriesFromTransforms(
            *dataModuleOption.transformManager->GetTransforms());
        for (auto const& request : detectedRequests) {
            dataModuleOption.loader.AddRequest(request);
        }

        // Extract cross-sectional categories (FRED economic indicators)
        auto crossSectionalCategories = ExtractCrossSectionalCategoriesFromTransforms(
            *dataModuleOption.transformManager->GetTransforms());
        for (auto const& cat : crossSectionalCategories) {
            dataModuleOption.loader.AddCrossSectionalCategory(cat);
        }

        // NOTE: Index validation removed - indices are now extracted as ReferenceAgg
    }

    return dataModuleOption;
}

} // namespace factory
} // namespace epoch_script::data
