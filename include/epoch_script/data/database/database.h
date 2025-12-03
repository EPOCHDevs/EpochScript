//
// Created by dewe on 10/2/21.
//

#pragma once
#include <epoch_script/data/database/idatabase_impl.h>
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_data_sdk/events/all.h>
#include <string>
#include <epoch_frame/series.h>

namespace epoch_script::data {
class Database {

public:
  using DataHandler = std::function<void(
      TimeFrameNotation const &, asset::Asset const &,
      epoch_frame::DataFrame const &, epoch_frame::DateTime const &)>;

  using Ptr = std::shared_ptr<Database>;
  using ConstPtr = const std::shared_ptr<Database>;

  explicit Database(Database const &) = delete;
  explicit Database(Database &&) noexcept = delete;
  Database &operator=(Database const &) = delete;
  Database &operator=(Database &&) noexcept = delete;

  Database() = default;
  ~Database();

  Database(IDatabaseImplPtr);

  void RunPipeline(data_sdk::events::ScopedProgressEmitter& emitter);

  std::unordered_map<std::string, epoch_proto::TearSheet> GetGeneratedReports() const {
    return m_impl->GetGeneratedReports();
  }

  epoch_script::runtime::AssetEventMarkerMap GetGeneratedEventMarkers() const {
    return m_impl->GetGeneratedEventMarkers();
  }

  inline const TransformedDataType &GetTransformedData() const {
    return m_impl->GetTransformedData();
  }

  std::optional<epoch_frame::Series> GetBenchmark() const { return m_impl->GetBenchmark(); }

  void HandleData(const DataHandler &dataHandler,
                  const epoch_frame::DateTime &t) const;

  DataCategory GetDataCategory() const { return m_impl->GetDataCategory(); }

  asset::AssetHashSet GetAssets() const noexcept { return m_impl->GetAssets(); }

  inline std::string GetBaseTimeframe() const {
    return m_impl->GetBaseTimeframe();
  }

  std::optional<std::string>
  GetFrontContract(const asset::Asset &asset,
                   epoch_frame::DateTime const &t) const {
    return m_impl->GetFrontContract(asset, t);
  }

private:
  IDatabaseImplPtr m_impl;
};

} // namespace epoch_script::data
