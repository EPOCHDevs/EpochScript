//
// Created by adesola on 7/31/24.
//

#include "database_impl.h"
#include "epoch_core/common_utils.h"
#include "epoch_core/macros.h"
#include "epoch_frame/aliases.h"
#include "epoch_frame/calendar_common.h"
#include "epoch_frame/common.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/calendar_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/factory/scalar_factory.h"
#include "epoch_frame/market_calendar.h"
#include "index/datetime_index.h"
#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_data_sdk/model/asset/asset_specification.hpp>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_script/core/bar_attribute.h>
#include <arrow/array/builder_base.h>
#include <arrow/type_fwd.h>
#include <epoch_frame/index.h>
#include <epoch_script/core/symbol.h>
#include <functional>
#include <memory>
#include <oneapi/tbb/blocked_range.h>
#include <oneapi/tbb/parallel_for.h>
#include <oneapi/tbb/global_control.h>
#include <vector>

namespace epoch_script::data {
  struct BarArrayBuilder {
    arrow::DoubleBuilder open{};
    arrow::DoubleBuilder high{};
    arrow::DoubleBuilder low{};
    arrow::DoubleBuilder close{};
    arrow::DoubleBuilder volume{};
    arrow::TimestampBuilder timestamp{
      arrow::timestamp(arrow::TimeUnit::NANO, "UTC"),
      arrow::default_memory_pool()};

    void reserve(size_t size) {
      auto openS = open.Reserve(size);
      auto highS = high.Reserve(size);
      auto lowS = low.Reserve(size);
      auto closeS = close.Reserve(size);
      auto volumeS = volume.Reserve(size);
      auto timestampS = timestamp.Reserve(size);
      AssertFromFormat(openS.ok() && highS.ok() && lowS.ok() && closeS.ok() &&
                           volumeS.ok() && timestampS.ok(),
                       "failed to reserve BarArrayBuilder.");
    }
  };

  template <AssetClass assetClass>
  void DatabaseImpl::GenericMessageHandler<assetClass>::operator()(
      const BarList &barList) {
    asset::AssetHashMap<BarArrayBuilder> columns;
    arrow::FieldVector fields;

    if constexpr (assetClass == AssetClass::Stocks) {
      auto today = epoch_frame::DateTime::now().date();
      const auto &nyse =
          epoch_frame::calendar::CalendarFactory::instance().get_calendar("NYSE");

      if (!m_currentNYSEMarketSession ||
          m_currentNYSEMarketSession->date != today) {
        auto nyse_market_open =
            nyse->get_time_on(epoch_core::MarketTimeType::MarketOpen, today);
        auto nyse_market_close =
            nyse->get_time_on(epoch_core::MarketTimeType::MarketClose, today);

        if (nyse_market_open && nyse_market_close) {
          m_currentNYSEMarketSession = NYSEMarketSession{
            today, nyse_market_open->time, nyse_market_close->time};
        } else {
          m_currentNYSEMarketSession = {today, std::nullopt, std::nullopt};
        }
          }
    }

    for (auto const &data : barList) {
      auto asset = asset::MakeAsset(data.s, assetClass);

      if constexpr (assetClass == AssetClass::Stocks) {
        auto now = epoch_frame::DateTime::now("America/New_York");
        if (!m_currentNYSEMarketSession->market_close ||
            !m_currentNYSEMarketSession->market_open ||
            now.time() <= *m_currentNYSEMarketSession->market_open ||
            now.time() > *m_currentNYSEMarketSession->market_close) {
          SPDLOG_INFO("Skipping NYSE data outside of market hours");
          return;
            }
      }
      auto iter = columns.find(asset);
      if (iter == columns.end()) {
        BarArrayBuilder builder;
        builder.reserve(barList.size());
        iter = columns.emplace(asset, std::move(builder)).first;
      }

      auto &[open, high, low, close, volume, timestamp] = iter->second;
      open.UnsafeAppend(data.o);
      high.UnsafeAppend(data.h);
      low.UnsafeAppend(data.l);
      close.UnsafeAppend(data.c);
      volume.UnsafeAppend(data.v);
      timestamp.UnsafeAppend(data.t_utc);
    }

    for (auto &[asset, builder] : columns) {
      auto index = std::make_shared<epoch_frame::DateTimeIndex>(
          epoch_frame::AssertResultIsOk(builder.timestamp.Finish()));

      auto table = arrow::Table::Make(
          arrow::schema(epoch_script::BarsConstants::instance().all_fields),
          {epoch_frame::AssertArrayResultIsOk(builder.open.Finish()),
           epoch_frame::AssertArrayResultIsOk(builder.high.Finish()),
           epoch_frame::AssertArrayResultIsOk(builder.low.Finish()),
           epoch_frame::AssertArrayResultIsOk(builder.close.Finish()),
           epoch_frame::AssertArrayResultIsOk(builder.volume.Finish())});

      epoch_frame::DataFrame df(index, table);
      {
        std::lock_guard<std::mutex> lock(m_self.m_loadedBarDataMutex);
        if (auto loaded_data = m_self.m_loadedBarData.find(asset);
            loaded_data == m_self.m_loadedBarData.end()) {
          m_self.m_loadedBarData.insert_or_assign(asset, df);
            } else {
              loaded_data->second =
                  epoch_frame::concat({.frames = {loaded_data->second, df}});
            }
      }
    }
  }

  DatabaseImpl::DatabaseImpl(DatabaseImplOptions options)
      : m_dataloader(std::move(options.dataloader)),
        m_dataTransform(std::move(options.dataTransform)),
        m_futuresContinuationConstructor(
            std::move(options.futuresContinuationConstructor)),
        m_resampler(std::move(options.resampler)),
        m_websocketManager(std::move(options.websocketManager)) {
    AssertFromFormat(m_dataloader != nullptr,
                     "Database Construction failed: dataloader == nullptr");

    auto category = m_dataloader->GetDataCategory();
    AssertFalseFromStream(category == DataCategory::Null,
                          "Invalid Data Category");
    m_baseTimeframe =
        ((category == DataCategory::DailyBars)
             ? epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY
             : epoch_script::EpochStratifyXConstants::instance()
                   .MINUTE_FREQUENCY)
            .ToString();
  }

  std::optional<std::string>
  DatabaseImpl::GetFrontContract(const asset::Asset &asset,
                                 epoch_frame::DateTime const &currentTime) const {
    try {
      const auto frontContracts =
          m_transformedData.at(m_baseTimeframe)
              .at(asset)
              .loc(
                  epoch_frame::Scalar(currentTime),
                  epoch_script::EpochStratifyXConstants::instance().CONTRACT());
      return frontContracts.value<std::string>();
    } catch (std::exception const &exp) {
      SPDLOG_WARN("Failed to get front contract for asset {}: {}. Returning nullopt.",
                  asset.GetSymbolStr(), exp.what());
    }
    return std::nullopt;
  }

  void DatabaseImpl::RunPipeline() {
    LoadData();
    CompletePipeline();
  }

  void DatabaseImpl::RefreshPipeline() {
    UpdateData();
    CompletePipeline();
  }

  void DatabaseImpl::AppendFuturesContinuations() {
    for (auto const &[k, v] :
         m_futuresContinuationConstructor->Build(m_loadedBarData)) {
      m_loadedBarData.insert_or_assign(k, v);
         }
  }

  void DatabaseImpl::UpdateData() {
    if (m_websocketManager.empty()) {
      SPDLOG_WARN("No websocket managers to update data");
      return;
    }

    for (auto const &[assetClass, websocketManager] : m_websocketManager) {
      switch (assetClass) {
        case AssetClass::Stocks: {
          websocketManager->HandleNewMessage(
              GenericMessageHandler<AssetClass::Stocks>(*this));
          break;
        }
        case AssetClass::Crypto: {
          websocketManager->HandleNewMessage(
              GenericMessageHandler<AssetClass::Crypto>(*this));
          break;
        }
        default:
          SPDLOG_WARN("No websocket manager for asset class {}",
                      AssetClassWrapper::ToLongFormString(assetClass));
      }
    }
  }

  StringAssetDataFrameMap DatabaseImpl::ResampleBarData() {
    StringAssetDataFrameMap result{{m_baseTimeframe, m_loadedBarData}};
    if (m_resampler) {
      SPDLOG_DEBUG("Starting Resampling stage.");
      auto resampledData = m_resampler->Build(m_loadedBarData);
      for (auto const &[timeframe, asset, dataframe] : resampledData) {
        result[timeframe].insert_or_assign(asset, dataframe);
      }
    }
    else {
      SPDLOG_INFO("Resampling stage skipped");
    }
    return result;
  }

  void DatabaseImpl::CompletePipeline() {
    if (m_futuresContinuationConstructor) {
      AppendFuturesContinuations();
    }

    m_transformedData = TransformBarData(ResampleBarData());

    SPDLOG_DEBUG("Transformed Data:");

    // Flatten the entire data structure first
    struct WorkItem {
      std::string timeframe;
      asset::Asset asset;
      epoch_frame::DataFrame dataframe;
    };

    std::vector<WorkItem> flattened_data;
    // Pre-allocate approximate size
    size_t total_items = 0;
    for (const auto &[_, asset_map] : m_transformedData) {
      total_items += asset_map.size();
    }
    flattened_data.reserve(total_items);

    // Flatten the nested structure
    for (const auto &[timeframe, asset_map] : m_transformedData) {
      for (const auto &[asset, dataframe] : asset_map) {
        flattened_data.emplace_back(timeframe, asset, dataframe);
      }
    }

    // Process all items in parallel with a single thread pool
    m_indexer.reserve(flattened_data.size());
    std::mutex mutex;
    oneapi::tbb::parallel_for(
        oneapi::tbb::blocked_range<size_t>(0, flattened_data.size()),
        [&](const oneapi::tbb::blocked_range<size_t> &range) {
          for (size_t i = range.begin(); i < range.end(); ++i) {
            const auto &[timeframe, asset, dataframe] = flattened_data[i];
            if (dataframe.empty()) {
              continue;
              ;
            }
            auto result = std::make_unique<DatabaseIndexerItem>(
                timeframe, asset, GetTimestampIndexMapping(dataframe.index()));
            {
              std::lock_guard lock(mutex);
              m_indexer.emplace_back(std::move(result));
            }
            SPDLOG_DEBUG("{}|{}|{}", timeframe, asset.ToString(),
                         DebugPrintDataFrame(dataframe));
          }
        });

    // Build inverted timestamp index for O(1) lookup
    SPDLOG_DEBUG("Building timestamp index for {} indexer items", m_indexer.size());
    for (const auto &item : m_indexer) {
      const auto &[timeframe, asset, indexer] = *item;
      for (const auto &[timestamp, range] : indexer) {
        m_timestampIndex[timestamp].push_back({timeframe, asset, range});
      }
    }
    SPDLOG_DEBUG("Timestamp index built with {} unique timestamps", m_timestampIndex.size());
  }

  void DatabaseImpl::LoadData() {
    try {
      SPDLOG_DEBUG("Starting Data loading stage.");
      SPDLOG_DEBUG("DatabaseImpl: About to call m_dataloader->LoadData()");
      m_dataloader->LoadData();
      SPDLOG_DEBUG("DatabaseImpl: m_dataloader->LoadData() completed successfully");
    } catch (const std::exception& e) {
      SPDLOG_ERROR("Data loading stage failed with exception: {}", e.what());
      throw;
    } catch (...) {
      SPDLOG_ERROR("Data loading stage failed with unknown exception");
      throw;
    }
    SPDLOG_DEBUG("DatabaseImpl: Getting stored data from dataloader");
    m_loadedBarData = m_dataloader->GetStoredData();
    SPDLOG_DEBUG("DatabaseImpl: Retrieved {} assets from dataloader", m_loadedBarData.size());
  }

  TransformedDataType
  DatabaseImpl::TransformBarData(StringAssetDataFrameMap dataFrameMap) {
    TransformedDataType result;
    if (m_dataTransform) {
      SPDLOG_DEBUG("Starting Data Transformation stage.");
      const auto start = std::chrono::high_resolution_clock::now();

      // Optional runtime limiter for TBB parallelism to mitigate
      // non-thread-safe report generation in upstream builders.
      std::unique_ptr<oneapi::tbb::global_control> tbbLimiter;
      if (const char* maxThreadsEnv = std::getenv("EPOCH_MAX_TBB_THREADS")) {
        int n = std::max(1, std::atoi(maxThreadsEnv));
        tbbLimiter = std::make_unique<oneapi::tbb::global_control>(
            oneapi::tbb::global_control::max_allowed_parallelism, n);
        SPDLOG_INFO("TBB max parallelism limited via EPOCH_MAX_TBB_THREADS={}.", n);
      } else if (std::getenv("EPOCH_DISABLE_PARALLEL_REPORTS")) {
        tbbLimiter = std::make_unique<oneapi::tbb::global_control>(
            oneapi::tbb::global_control::max_allowed_parallelism, 1);
        SPDLOG_INFO("TBB parallelism limited to 1 via EPOCH_DISABLE_PARALLEL_REPORTS.");
      }

      // Build asset ID -> Asset mapping for reverse lookup
      std::unordered_map<std::string, asset::Asset> assetIdToAsset;
      std::unordered_map<std::string, std::unordered_map<std::string, epoch_frame::DataFrame>> stringKeyedMap;

      for (const auto& [timeframe, assetMap] : dataFrameMap) {
        for (const auto& [asset, df] : assetMap) {
          assetIdToAsset.emplace(asset.GetID(), asset);
          stringKeyedMap[timeframe].emplace(asset.GetID(), df);
        }
      }

      // Execute pipeline with string-keyed map
      auto transformedStringMap = m_dataTransform->ExecutePipeline(stringKeyedMap);

      // Convert back to Asset-keyed map
      dataFrameMap.clear();
      for (const auto& [timeframe, stringAssetMap] : transformedStringMap) {
        for (const auto& [assetId, df] : stringAssetMap) {
          // Look up the Asset object from our mapping
          auto it = assetIdToAsset.find(assetId);
          if (it != assetIdToAsset.end()) {
            dataFrameMap[timeframe].emplace(it->second, df);
          } else {
            throw std::runtime_error("Script Runtime Orchestrator returned invalid asset_id: " + assetId);
          }
        }
      }

      const auto end = std::chrono::high_resolution_clock::now();
      SPDLOG_INFO(
          "Data Transformation stage completed in {} s",
          std::chrono::duration_cast<std::chrono::seconds>(end - start).count());

      m_reports = m_dataTransform->GetGeneratedReports();
      m_eventMarkers = m_dataTransform->GetGeneratedEventMarkers();
    } else {
      SPDLOG_INFO("Data Transformation stage skipped");
    }

    for (auto &[tf, item] : dataFrameMap) {
      for (auto &[asset, df] : item) {
        result[tf].insert_or_assign(asset, df);
      }
    }

    return result;
  }

  DatabaseIndexerValue
  DatabaseImpl::GetTimestampIndexMapping(epoch_frame::IndexPtr const &index) {
    DatabaseIndexerValue result;

    const auto &timeStampIndex = index->array().to_timestamp_view();
    for (auto iterator = timeStampIndex->begin();
         iterator != timeStampIndex->end(); ++iterator) {
      int64_t intIndex = iterator.index();
      auto timeStamp = **iterator;
      if (auto it = result.find(timeStamp); it == result.end()) {
        result[timeStamp] = {intIndex, intIndex};
      } else {
        it->second.second = intIndex;
      }
         }
    return result;
  }

  std::string
  DatabaseImpl::DebugPrintDataFrame(epoch_frame::DataFrame const &df) {
    uint64_t N = std::min(df.num_rows(), 5UL);
    auto headDF = df.head(N);
    auto tailDF = df.tail(N);

    std::stringstream ss;
    ss << "Data Merged all Symbols Successfully\n"
       << "Data Head Preview:\n"
       << headDF << "\n"
       << "\nData Tail Preview:\n"
       << tailDF << "\n"
       << "\nData Shape: [" << df.shape().at(0) << ", " << df.shape().at(1)
       << "]";
    return ss.str();
  }

std::unordered_map<std::string, epoch_proto::TearSheet> DatabaseImpl::GetGeneratedReports() const {
  // AssetReportMap already uses string keys (asset IDs)
  return m_reports;
}

epoch_script::runtime::AssetEventMarkerMap DatabaseImpl::GetGeneratedEventMarkers() const {
  return m_eventMarkers;
}

} // namespace epoch_script::data
