//
// Created by adesola on 7/31/24.
//

#pragma once
// Use epoch_data_sdk for dataloader
#include <epoch_data_sdk/dataloader/dataloader.hpp>

#include "data/futures_continuation/continuations.h"
#include "epoch_frame/datetime.h"
#include <epoch_script/data/database/idatabase_impl.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include "resample.h"
#include <epoch_script/transforms/runtime/iorchestrator.h>
#include <epoch_script/transforms/runtime/types.h>
#include "../../../include/epoch_script/data/database/updates/iwebsocket_manager.h"


namespace epoch_proto {
  class TearSheet;
}

namespace epoch_script::data {

// Type aliases for epoch_data_sdk
using IDataLoader = data_sdk::IDataLoader;
using IDataLoaderPtr = std::shared_ptr<IDataLoader>;
namespace asset = data_sdk::asset;

struct DatabaseImplOptions {
  IDataLoaderPtr dataloader;
  epoch_script::runtime::IDataFlowOrchestrator::Ptr dataTransform;
  IFuturesContinuationConstructor::Ptr futuresContinuationConstructor = nullptr;
  IResamplerPtr resampler = nullptr;
  asset::AssetClassMap<IWebSocketManagerPtr> websocketManager;
};

struct NYSEMarketSession {
  epoch_frame::Date date;
  std::optional<epoch_frame::Time> market_open;
  std::optional<epoch_frame::Time> market_close;
};

class DatabaseImpl final : public IDatabaseImpl {

  template <AssetClass>
  struct GenericMessageHandler  {
    explicit GenericMessageHandler(DatabaseImpl &self)
        : m_self(self) {}

    void operator()(const BarList &);

    DatabaseImpl &m_self;
    std::optional<NYSEMarketSession> m_currentNYSEMarketSession;
  };

public:
  explicit DatabaseImpl(DatabaseImplOptions options);

  DataCategory GetDataCategory() const final {
    return m_dataloader->GetDataCategory();
  }

  asset::AssetHashSet GetAssets() const noexcept final {
    return m_dataloader->GetAssets();
  }

  std::string GetBaseTimeframe() const final { return m_baseTimeframe; }

  const TransformedDataType &GetTransformedData() const override {
    return m_transformedData;
  }

  epoch_frame::DataFrame GetCurrentData(const std::string &timeframe,
                                        const asset::Asset &asset) const override {
    return lookup(lookup(m_transformedData, timeframe), asset);
  }

  std::optional<epoch_frame::Series> GetBenchmark() const override {
    return m_dataloader->GetBenchmark();
  }

  void RunPipeline(data_sdk::events::ScopedProgressEmitter& emitter) final;
  void RefreshPipeline(data_sdk::events::ScopedProgressEmitter& emitter) final;

  std::optional<std::string>
  GetFrontContract(const asset::Asset &asset,
                   epoch_frame::DateTime const &) const final;

  std::unordered_map<std::string, epoch_proto::TearSheet> GetGeneratedReports() const override;

  epoch_script::runtime::AssetEventMarkerMap GetGeneratedEventMarkers() const override;

  const DatabaseIndexer &GetIndexer() const final { return m_indexer; }

  const TimestampIndex &GetTimestampIndex() const final { return m_timestampIndex; }

  static std::string DebugPrintDataFrame(epoch_frame::DataFrame const &df);

  static DatabaseIndexerValue
  GetTimestampIndexMapping(epoch_frame::IndexPtr const &index);

private:
  DatabaseIndexer m_indexer;
  TimestampIndex m_timestampIndex;  // O(1) inverted index

  IDataLoaderPtr m_dataloader;

  epoch_script::runtime::IDataFlowOrchestrator::Ptr m_dataTransform;

  data::IFuturesContinuationConstructor::Ptr m_futuresContinuationConstructor;
  IResamplerPtr m_resampler;

  // asset::AssetClassMap<NewMessageObserverPtr> m_messageHandlers;
  asset::AssetClassMap<IWebSocketManagerPtr> m_websocketManager;

  std::string m_baseTimeframe;

  // contains bar data, indexed by base timeframe
  std::mutex m_loadedBarDataMutex;
  AssetDataFrameMap m_loadedBarData;

  // contains data, indexed by timeframe/symbol
  TransformedDataType m_transformedData;

  epoch_script::runtime::AssetReportMap m_reports;
  epoch_script::runtime::AssetEventMarkerMap m_eventMarkers;

  void LoadData(data_sdk::events::ScopedProgressEmitter& emitter);

  TransformedDataType TransformBarData(StringAssetDataFrameMap,
                                       data_sdk::events::ScopedProgressEmitter& emitter);

  StringAssetDataFrameMap ResampleBarData(data_sdk::events::ScopedProgressEmitter& emitter);

  void AppendFuturesContinuations();

  void UpdateData();

  void CompletePipeline(data_sdk::events::ScopedProgressEmitter& emitter);
};
} // namespace epoch_script::data