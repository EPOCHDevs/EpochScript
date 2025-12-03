#include "resample.h"

#include "epoch_frame/common.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/market_calendar.h"
#include "index/datetime_index.h"
#include "methods/time_grouper.h"
#include <epoch_script/data/model/exchange_calendar.h>
#include <common/epoch_thread_pool.h>
#include <epoch_frame/series.h>
#include <epoch_script/transforms/core/bar_resampler.h>
#include <oneapi/tbb/parallel_for.h>
#include <epoch_data_sdk/events/event_ids.h>

namespace epoch_script::data {
epoch_frame::DataFrame Resampler::AdjustTimestamps(
    asset::Asset const &asset, epoch_frame::IndexPtr const &baseIndex,
    epoch_frame::DataFrame const &resampled, bool isIntradayTF) const {
  if (baseIndex->size() == 0 || resampled.num_rows() == 0) {
    return resampled;
  }

  if (!m_isIntraday || (m_isIntraday && isIntradayTF)) {
    return resampled;
  }

  auto resampledIndex = resampled.index();

  auto calendar =
      epoch_script::calendar::GetExchangeCalendarFromSpec(asset.GetSpec());
  using namespace epoch_frame;

  auto marketEnd =
      calendar->days_at_time(resampledIndex, MarketTimeType::MarketClose);

  AssertFromStream(marketEnd.size() == resampledIndex->size(),
                   marketEnd.size() << " != " << resampledIndex->size());

  // Create new DataFrame with adjusted timestamps
  return DataFrame{
      std::make_shared<DateTimeIndex>(marketEnd.contiguous_array().value()),
      resampled.table()};
}

std::vector<std::tuple<TimeFrameNotation, asset::Asset, epoch_frame::DataFrame>>
Resampler::Build(AssetDataFrameMap const &group,
                 data_sdk::events::ScopedProgressEmitter& emitter) const {
  using namespace data_sdk::events;

  emitter.EmitInfo("Resampling " + std::to_string(group.size()) + " assets to " +
                   std::to_string(m_timeFrames.size()) + " timeframes");

  // Group work by timeframe for better viewer organization
  std::vector<std::tuple<TimeFrameNotation, asset::Asset, epoch_frame::DataFrame>> allResults;

  for (auto const &tf : m_timeFrames) {
    // Create a child scope for each timeframe
    auto tfEmitter = emitter.ChildScope(ScopeType::Stage, "Timeframe:" + tf.ToString());
    tfEmitter.EmitStarted("timeframe", tf.ToString());

    std::vector<asset::Asset> assetsToResample;
    for (auto const &asset : group | std::views::keys) {
      if (asset.IsFuturesContract() && !asset.IsFuturesContinuation()) {
        continue;
      }
      assetsToResample.push_back(asset);
    }

    std::vector<std::optional<std::tuple<TimeFrameNotation, asset::Asset, epoch_frame::DataFrame>>>
        tfResults;
    tfResults.resize(assetsToResample.size());

    std::atomic<size_t> completed{0};
    const size_t total = assetsToResample.size();

    epoch_frame::EpochThreadPool::getInstance().execute([&] {
      oneapi::tbb::parallel_for(
          oneapi::tbb::blocked_range<size_t>(0, assetsToResample.size()),
          [&](auto const &range) {
            for (size_t i = range.begin(); i < range.end(); ++i) {
              auto const &asset = assetsToResample[i];
              auto df = group.at(asset);
              AssertFromStream(
                  epoch_frame::arrow_utils::get_tz(df.index()->dtype()) == "UTC",
                  "Resampler only supports UTC timezones");

              df = this->AdjustTimestamps(
                  asset, df.index(),
                  epoch_script::transform::resample_ohlcv(df, tf.GetOffset()),
                  tf.IsIntraDay());
              tfResults[i] = {tf.ToString(), asset, df};

              // Emit progress for this timeframe
              tfEmitter.EmitProgress(
                  completed.fetch_add(1) + 1, total,
                  asset.GetSymbolStr());
            }
          });
    });

    tfEmitter.EmitCompleted("timeframe", tf.ToString());

    // Collect results
    for (auto& optResult : tfResults) {
      if (optResult.has_value()) {
        allResults.push_back(std::move(optResult.value()));
      }
    }
  }

  return allResults;
}
} // namespace epoch_script::data