//
// Created by adesola on 7/31/24.
//

#pragma once
#include <epoch_script/data/aliases.h>
#include <epoch_frame/dataframe.h>
#include <memory>
#include "epoch_protos/tearsheet.pb.h"
#include <epoch_script/transforms/runtime/iorchestrator.h>
#include <epoch_data_sdk/events/all.h>

namespace epoch_script::data {

struct IDatabaseImpl {
  virtual ~IDatabaseImpl() = default;

  virtual void RunPipeline(data_sdk::events::ScopedProgressEmitter& emitter) = 0;

  virtual void RefreshPipeline(data_sdk::events::ScopedProgressEmitter& emitter) = 0;

  virtual const DatabaseIndexer &GetIndexer() const = 0;

  virtual const TimestampIndex &GetTimestampIndex() const = 0;

  virtual const TransformedDataType &GetTransformedData() const = 0;

  virtual epoch_frame::DataFrame GetCurrentData(TimeFrameNotation const &,
                                                asset::Asset const &) const = 0;

  virtual DataCategory GetDataCategory() const = 0;

  virtual asset::AssetHashSet GetAssets() const noexcept = 0;

  virtual std::string GetBaseTimeframe() const = 0;

  virtual std::optional<epoch_frame::Series> GetBenchmark() const = 0;

  virtual std::optional<std::string>
  GetFrontContract(const asset::Asset &asset,
                   epoch_frame::DateTime const &) const = 0;

  virtual std::unordered_map<std::string, epoch_proto::TearSheet> GetGeneratedReports() const = 0;

  virtual epoch_script::runtime::AssetEventMarkerMap GetGeneratedEventMarkers() const = 0;
};
using IDatabaseImplPtr = std::unique_ptr<IDatabaseImpl>;
} // namespace epoch_script::data