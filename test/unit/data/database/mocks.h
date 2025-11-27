//
// Created by adesola on 4/21/25.
//

#pragma once
#include "../../../../include/epoch_script/data/database/database.h"
#include <trompeloeil.hpp>
#include <trompeloeil/mock.hpp>

#include "data/database/resample.h"
#include "../../../../include/epoch_script/data/database/updates/iwebsocket_manager.h"
#include "data/futures_continuation/icontinuations.h"
#include "epoch_script/transforms/core/itransform.h"
#include <epoch_script/transforms/runtime/iorchestrator.h>
#include <epoch_data_sdk/dataloader/dataloader.hpp>

using namespace epoch_script::data;
using namespace epoch_script;

// Trompeloeil mock for IDataLoader
class MockDataloader final : public data_sdk::IDataLoader {
  using OptionalSeries = std::optional<epoch_frame::Series>;
  using DataMap = data_sdk::asset::AssetHashMap<epoch_frame::DataFrame>;

public:
  MAKE_MOCK0(LoadData, void(), override);
  MAKE_CONST_MOCK0(GetStoredData, DataMap(), override);
  MAKE_CONST_MOCK0(GetDataCategory, data_sdk::DataCategory(), override);
  MAKE_CONST_MOCK0(GetAssets, data_sdk::asset::AssetHashSet(), override);
  MAKE_CONST_MOCK0(GetStrategyAssets, data_sdk::asset::AssetHashSet(), override);
  MAKE_CONST_MOCK0(GetBenchmark, OptionalSeries(), override);

  // LoadAssetBars and LoadAssetBarsAsync are not mocked - tests don't use them
  // Provide stub implementations since they're pure virtual in IDataLoader
  std::expected<epoch_frame::DataFrame, std::string>
  LoadAssetBars(const data_sdk::asset::Asset&,
                data_sdk::DataCategory,
                const data_sdk::dataloader::FetchKwargs& = data_sdk::dataloader::NoKwargs{}) const override {
    return std::unexpected("Not implemented in mock");
  }

  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadAssetBarsAsync(const data_sdk::asset::Asset&,
                     data_sdk::DataCategory,
                     data_sdk::dataloader::FetchKwargs = data_sdk::dataloader::NoKwargs{}) const override {
    co_return std::unexpected("Not implemented in mock");
  }

  // LoadEconomicIndicator and LoadEconomicIndicatorAsync stub implementations
  std::expected<epoch_frame::DataFrame, std::string>
  LoadEconomicIndicator(data_sdk::CrossSectionalDataCategory,
                        const epoch_frame::Date&,
                        const epoch_frame::Date&,
                        bool = true) const override {
    return std::unexpected("Not implemented in mock");
  }

  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadEconomicIndicatorAsync(data_sdk::CrossSectionalDataCategory,
                             const epoch_frame::Date&,
                             const epoch_frame::Date&,
                             bool = true) const override {
    co_return std::unexpected("Not implemented in mock");
  }

  // LoadIndexData and LoadIndexDataAsync stub implementations
  std::expected<epoch_frame::DataFrame, std::string>
  LoadIndexData(const std::string&,
                const epoch_frame::Date&,
                const epoch_frame::Date&,
                bool = true) const override {
    return std::unexpected("Not implemented in mock");
  }

  drogon::Task<std::expected<epoch_frame::DataFrame, std::string>>
  LoadIndexDataAsync(const std::string&,
                     const epoch_frame::Date&,
                     const epoch_frame::Date&,
                     bool = true) const override {
    co_return std::unexpected("Not implemented in mock");
  }
};

class MockTransformGraph final : public epoch_script::runtime::IDataFlowOrchestrator {
public:
  MAKE_MOCK1(ExecutePipeline,
             epoch_script::runtime::TimeFrameAssetDataFrameMap(epoch_script::runtime::TimeFrameAssetDataFrameMap), override);
  MAKE_CONST_MOCK0(GetGeneratedReports, epoch_script::runtime::AssetReportMap(), override);
  MAKE_CONST_MOCK0(GetGeneratedEventMarkers, epoch_script::runtime::AssetEventMarkerMap(), override);
};

class MockFuturesContinuation final : public IFuturesContinuationConstructor {
public:
  MAKE_CONST_MOCK1(Build, AssetDataFrameMap(AssetDataFrameMap const &),
                   override);
};

class MockResampler final : public IResampler {
public:
  using T = std::vector<
      std::tuple<epoch_script::TimeFrameNotation,
                 data_sdk::asset::Asset, epoch_frame::DataFrame>>;
  MAKE_CONST_MOCK1(Build, T(AssetDataFrameMap const &), override);
};

class MockWebSocketManager final : public IWebSocketManager {
public:
  MAKE_MOCK0(Connect, void(), override);
  MAKE_MOCK0(Disconnect, void(), override);
  MAKE_MOCK1(HandleNewMessage, void(const NewMessageObserver &), override);
  MAKE_MOCK1(Subscribe, void(const data_sdk::asset::AssetHashSet &),
             override);
};

using DatabaseIndexerConstRef = const DatabaseIndexer &;
using TransformedDataTypeConstRef = const TransformedDataType &;
using OptionalSeries = std::optional<epoch_frame::Series>;
using TearSheetMap = std::unordered_map<std::string, epoch_proto::TearSheet> ;

class MockDatabaseImpl final : public IDatabaseImpl {
public:
  MAKE_MOCK0(RunPipeline, void(), override);
  MAKE_CONST_MOCK0(GetBenchmark, OptionalSeries(), override);
  MAKE_CONST_MOCK0(GetGeneratedReports, TearSheetMap(), override);
  MAKE_CONST_MOCK0(GetGeneratedEventMarkers, epoch_script::runtime::AssetEventMarkerMap(), override);

  MAKE_MOCK0(RefreshPipeline, void(), override);
  MAKE_CONST_MOCK0(GetIndexer, DatabaseIndexerConstRef(), override);
  MAKE_CONST_MOCK0(GetTransformedData, TransformedDataTypeConstRef(), override);
  MAKE_CONST_MOCK0(GetTimestampIndex, (const TimestampIndex&()), override);
  MAKE_CONST_MOCK0(GetDataCategory, (DataCategory()), override);
  MAKE_CONST_MOCK0(GetAssets, data_sdk::asset::AssetHashSet(),
                   noexcept override);
  MAKE_CONST_MOCK0(GetBaseTimeframe, (std::string()), override);
  MAKE_CONST_MOCK2(
      GetFrontContract,
      std::optional<std::string>(const data_sdk::asset::Asset &,
                                 const epoch_frame::DateTime &),
      override);
  MAKE_CONST_MOCK2(GetCurrentData,
                   epoch_frame::DataFrame(TimeFrameNotation const &,
                                          asset::Asset const &),
                   override);
};

// Helper to create a specific mock for our tests
inline std::unique_ptr<Database> createMockDatabase() {
  auto mock_impl = std::make_unique<MockDatabaseImpl>();
  return std::make_unique<Database>(std::move(mock_impl));
}

// Mock observers for testing signal connections
struct MockDataHandlerObserver {
  std::shared_ptr<std::optional<epoch_frame::DateTime>> ref{new std::optional<epoch_frame::DateTime>{}};
  void operator()(std::string const &, asset::Asset const &,
                  epoch_frame::DataFrame const &,
                  epoch_frame::DateTime const & t) const {
    *ref = t;
  }
  bool isCalled() const {
    return ref->has_value();
  }
};

// Note: MockAssetObserver and MockAssetClassObserver are commented out
// because AssetPricingModel doesn't exist in EpochScript. These can be
// re-enabled if needed in the future.
/*
struct MockAssetObserver {
  std::shared_ptr<std::optional<epoch_frame::DateTime>> ref{new std::optional<epoch_frame::DateTime>{}};
  void operator()(data::AssetPricingModel const &,
                    epoch_frame::DateTime const & t) const {
    *ref = t;
  }
  bool isCalled() const {
    return ref->has_value();
  }
};

struct MockAssetClassObserver {
  std::shared_ptr<std::optional<epoch_frame::DateTime>> ref{new std::optional<epoch_frame::DateTime>{}};
  void operator()(asset::Asset const &, data::AssetPricingModel const &,
                    epoch_frame::DateTime const & t) const {
    *ref = t;
  }
  bool isCalled() const {
    return ref->has_value();
  }
};
*/