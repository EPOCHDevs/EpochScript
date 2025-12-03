//
// Created by dewe on 10/2/21.
//
#include "epoch_script/data/database/database.h"

namespace epoch_script::data {

Database::Database(IDatabaseImplPtr impl) : m_impl(std::move(impl)) {}

void Database::RunPipeline(data_sdk::events::ScopedProgressEmitter& emitter) { m_impl->RunPipeline(emitter); }

void Database::HandleData(const DataHandler &dataHandler,
                          const epoch_frame::DateTime &t) const {
  // ✅ O(1) timestamp index lookup (replaces O(T×A) linear scan)
  const auto &timestamp_index = m_impl->GetTimestampIndex();
  const auto it = timestamp_index.find(t.m_nanoseconds.count());

  if (it == timestamp_index.end()) {
    return;  // No data for this timestamp
  }

  // Process all (timeframe, asset, range) entries for this timestamp
  for (const auto &entry : it->second) {
    const auto &df = GetTransformedData().at(entry.timeframe).at(entry.asset);
    auto [start, end] = entry.range;
    auto sub_df = df.iloc({start, end+1});
    dataHandler(entry.timeframe, entry.asset, sub_df, t);
  }
}

Database::~Database() = default;

} // namespace epoch_script::data
