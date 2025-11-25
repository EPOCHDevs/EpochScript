//
// Created by adesola on 4/9/25.
//
#include <epoch_script/transforms/core/registration.h>
#include "../core/doc_deserialization_helper.h"
#include <epoch_script/transforms/core/registry.h>
#include "components/sql/sql_query_metadata.h"
#include "components/operators/validation_metadata.h"
#include "components/operators/static_cast_metadata.h"
#include "components/operators/stringify_metadata.h"
#include "components/operators/alias_metadata.h"
#include "components/operators/groupby_agg_metadata.h"
#include "components/data_sources/polygon_metadata.h"
#include "components/data_sources/polygon_indices_metadata.h"
#include "components/data_sources/fred_metadata.h"
#include "components/data_sources/sec_metadata.h"
#include "components/data_sources/reference_stocks_metadata.h"
#include "components/data_sources/news_metadata.h"
#include "components/data_sources/dividends_metadata.h"
#include "components/data_sources/splits_metadata.h"
#include "components/data_sources/ticker_events_metadata.h"
#include "components/data_sources/short_interest_metadata.h"
#include "components/data_sources/short_volume_metadata.h"
#include "components/indicators/forward_returns.h"
#include "components/indicators/intraday_returns.h"
#include "components/datetime/datetime_metadata.h"
#include "components/ml/sagemaker_sentiment_metadata.h"

namespace epoch_script::transforms {
void RegisterStrategyMetaData(const std::string &name,
                              const TransformsMetaDataCreator &metaData) {
  ITransformRegistry::GetInstance().Register(metaData(name));
}

void RegisterTransformMetadata(FileLoaderInterface const &loader) {
  std::vector<std::vector<TransformsMetaData>> metaDataList{
      LoadFromFile<TransformsMetaData>(loader, "transforms")};

  metaDataList.emplace_back(MakeDataSource());
  metaDataList.emplace_back(MakeComparativeMetaData());
  metaDataList.emplace_back(MakeTulipIndicators());
  metaDataList.emplace_back(MakeTulipCandles());
  metaDataList.emplace_back(MakeTradeSignalExecutor());
  metaDataList.emplace_back(MakeScalarMetaData());
  metaDataList.emplace_back(MakeLagMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakeForwardReturnsMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakeIntradayReturnsMetaData());
  metaDataList.emplace_back(MakeChartFormationMetaData());
  metaDataList.emplace_back(MakeCalendarEffectMetaData());
  metaDataList.emplace_back(MakeStringTransformMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakeValidationMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakeStaticCastMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakeStringifyMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakeAliasMetaData());
  // metaDataList.emplace_back(epoch_script::transform::MakeSQLQueryMetaData()); // DISABLED
  metaDataList.emplace_back(epoch_script::transform::MakeGroupByNumericAggMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakeGroupByBooleanAggMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakeGroupByAnyAggMetaData());
  metaDataList.emplace_back(epoch_script::transform::MakePolygonDataSources());
  metaDataList.emplace_back(epoch_script::transform::MakePolygonIndicesDataSources());
  metaDataList.emplace_back(epoch_script::transform::MakeFREDDataSource());
  // metaDataList.emplace_back(epoch_script::transform::MakeSECDataSources()); // DISABLED: SEC Form 13F and Insider Trading not exposed
  metaDataList.emplace_back(epoch_script::transform::MakeReferenceStocksDataSources());
  metaDataList.emplace_back(epoch_script::transform::MakeNewsDataSource());
  metaDataList.emplace_back(epoch_script::transform::MakeDividendsDataSource());
  metaDataList.emplace_back(epoch_script::transform::MakeSplitsDataSource());
  metaDataList.emplace_back(epoch_script::transform::MakeTickerEventsDataSource());
  metaDataList.emplace_back(epoch_script::transform::MakeShortInterestDataSource());
  metaDataList.emplace_back(epoch_script::transform::MakeShortVolumeDataSource());
  metaDataList.emplace_back(epoch_script::transform::MakeDatetimeTransforms());
  metaDataList.emplace_back(epoch_script::transform::MakeSageMakerSentimentTransforms());
  // Aggregation nodes are loaded from the transforms.yaml file

  for (auto &&indicator : std::views::join(metaDataList)) {
    if (!indicator.requiredDataSources.empty()) {
      indicator.requiresTimeFrame = true;
    }

    if (kIntradayOnlyIds.contains(indicator.id)) {
      indicator.intradayOnly = true;
    }

    if (indicator.category == epoch_core::TransformCategory::Executor) {
      indicator.allowNullInputs = true;
    }

    ITransformRegistry::GetInstance().Register(indicator);
  }
}
} // namespace epoch_script::transforms
