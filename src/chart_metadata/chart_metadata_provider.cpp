//
// Created by adesola on 5/1/25.
//

#include "chart_metadata_provider_impl.h"
#include "axis_manager.h"
#include "data_column_resolver.h"
#include "series_configuration_builder.h"
#include "epoch_script/data/common/constants.h"
#include "epoch_script/transforms/core/config_helper.h"
#include <glaze/glaze.hpp>

namespace epoch_script {
struct SessionHash {
  size_t operator()(const epoch_frame::SessionRange &range) const {
    return std::hash<size_t>{}(range.start.to_duration().count()) ^
           std::hash<size_t>{}(range.end.to_duration().count());
  }
};

struct SessionEqual {
  bool operator()(const epoch_frame::SessionRange &range1,
                  const epoch_frame::SessionRange &range2) const {
    return (range1.start.to_duration().count() ==
                range2.start.to_duration().count() &&
            range1.end.to_duration().count() ==
                range2.end.to_duration().count());
  }
};

// Comparison operator for SessionRange
bool operator==(const epoch_frame::SessionRange& lhs, const epoch_frame::SessionRange& rhs) {
    return lhs.start == rhs.start && lhs.end == rhs.end;
}

// Custom comparison operator for ChartPaneMetadata
bool ChartPaneMetadata::operator==(const ChartPaneMetadata &rhs) const {
    return yAxis == rhs.yAxis && 
           series == rhs.series && 
           std::equal(sessionRanges.begin(), sessionRanges.end(), 
                     rhs.sessionRanges.begin(), rhs.sessionRanges.end(),
                     [](const epoch_frame::SessionRange& a, const epoch_frame::SessionRange& b) {
                         return a.start == b.start && a.end == b.end;
                     });
}

std::ostream &operator<<(std::ostream &os, const ChartPaneMetadata &metadata) {
  return os << glz::prettify(metadata);
}

SeriesInfo ChartMetadataProvider::CreateSeries(
    epoch_script::transform::TransformConfiguration const &cfg, uint8_t chosenAxis,
    std::optional<std::string> const &linkedTo, std::string const &seriesId) {
  return chart_metadata::SeriesConfigurationBuilder::BuildSeries(
      cfg, chosenAxis, linkedTo, seriesId);
}

ChartMetadataProvider::ChartMetadataProvider(
    const std::unordered_set<std::string> &timeframes,
    const epoch_script::transform::TransformConfigurationList &transforms) {
  // Initialize axis manager
  chart_metadata::AxisManager axisManager;

  // Price-related inputs (column names for OHLCV data)
  const std::unordered_set<std::string> priceKeys{"o", "h", "l", "c"};
  const std::string volumeKey = "v";

  /* ---------------------------------------------------------------------
     1.  Build the base panes (price & volume) for every timeframe
  --------------------------------------------------------------------- */
  for (const std::string &tf : timeframes) {
    ChartPaneMetadata paneMeta;

    /* ----  candlestick  ---- */
    SeriesInfo candleSeries =
        chart_metadata::SeriesConfigurationBuilder::BuildCandlestickSeries(tf);
    paneMeta.series.push_back(candleSeries);
    axisManager.RegisterSeries(tf, candleSeries.id, candleSeries.yAxis);

    /* ----  volume  ---- */
    SeriesInfo volumeSeries =
        chart_metadata::SeriesConfigurationBuilder::BuildVolumeSeries(tf);
    paneMeta.series.push_back(volumeSeries);
    axisManager.RegisterSeries(tf, volumeSeries.id, volumeSeries.yAxis);

    // /* ----  position  ---- */
    // SeriesInfo posSeries =
    //     chart_metadata::SeriesConfigurationBuilder::BuildPositionQuantitySeries(tf);
    // paneMeta.series.push_back(posSeries);
    // axisManager.RegisterSeries(tf, posSeries.id, posSeries.yAxis);
    //
    // /* ---- protection ---- */
    // auto exitLevels =
    // chart_metadata::SeriesConfigurationBuilder::BuildExitLevelsSeries(tf);
    // paneMeta.series.push_back(exitLevels);
    // axisManager.RegisterSeries(tf, exitLevels.id, exitLevels.yAxis);

    // Initialize base axes in axis manager
    axisManager.InitializeBaseAxes(tf);
    auto axes = axisManager.GetAxes(tf);

    for (const auto &axis : axes) {
      paneMeta.yAxis.push_back(YAxis{axis.top, axis.height});
    }

    m_chartMetaData.emplace(tf, std::move(paneMeta));
  }

  /* ---------------------------------------------------------------------
     2.  Process every transform and add a new series / yAxis when needed
  --------------------------------------------------------------------- */
  std::unordered_map<std::string, int64_t> outputHandlesToSeriesId;
  std::unordered_map<std::string, std::unordered_set<epoch_frame::SessionRange,
                                                     SessionHash, SessionEqual>>
      rangeCache;

  for (const epoch_script::transform::TransformConfiguration &cfg : transforms) {
    const std::string tf = cfg.GetTimeframe().ToString();
    auto paneIt = m_chartMetaData.find(tf);
    if (paneIt == m_chartMetaData.end()) {
      SPDLOG_WARN("Timeframe {} not found in chart metadata", tf);
      continue; // time-frame not present
    }

    auto &paneMeta = paneIt->second;
    if (auto sessionRange = cfg.GetSessionRange()) {
      auto value = *sessionRange;
      if (!rangeCache.contains(tf)) {
        rangeCache[tf] = {value};
        paneMeta.sessionRanges.push_back(value);
      } else if (!rangeCache[tf].contains(value)) {
        rangeCache[tf].insert(value);
        paneMeta.sessionRanges.push_back(value);
      }
    }

    // Check if transform has a valid plot kind (not 30/Null)
    const auto &metadata = cfg.GetTransformDefinition().GetMetadata();
    if (metadata.plotKind == epoch_core::TransformPlotKind::Null) {
      SPDLOG_DEBUG("Skipping transform {} with no plot kind",
                   cfg.GetTransformName());
      continue;
    }

    /* ----  Resolve axis & linkage using new architecture ---- */
    auto [chosenAxis, linkedTo] = axisManager.AssignAxis(
        cfg, tf, priceKeys, volumeKey, outputHandlesToSeriesId);

    std::string seriesId = cfg.GetId();

    // Register the series with axis manager
    axisManager.RegisterSeries(tf, seriesId, chosenAxis);

    // Build the series using the new builder
    paneMeta.series.push_back(
        CreateSeries(cfg, chosenAxis, linkedTo, seriesId));

    // Update output handles mapping
    for (auto const &outputHandle : cfg.GetOutputs()) {
      outputHandlesToSeriesId[cfg.GetOutputId(outputHandle.id).GetColumnName()] =
          paneMeta.series.size() - 1;
    }

    // Update axes if changed
    paneMeta.yAxis.clear();
    auto axes = axisManager.GetAxes(tf);
    for (const auto &axis : axes) {
      paneMeta.yAxis.push_back(YAxis{axis.top, axis.height});
    }
  }
}

// Factory function implementation
ChartMetaDataProviderPtr CreateChartMetadataProvider(
    const std::unordered_set<std::string> &timeframes,
    const epoch_script::transform::TransformConfigurationList &transforms) {
  return std::make_unique<ChartMetadataProvider>(timeframes, transforms);
}

} // namespace epoch_script

