#pragma once

#include <cstdint>
#include <epoch_script/core/metadata_options.h>
#include <epoch_script/core/time_frame.h>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>
#include "epoch_script/transforms/core/transform_configuration.h"
#include "epoch_frame/datetime.h"

namespace epoch_script {

/**
 * @brief Information about a single series (indicator/chart) to be rendered
 */
struct SeriesInfo {
  std::string id;                    ///< Unique series identifier
  std::string type;                  ///< Chart type (line, candlestick, macd, etc.)
  std::string name;                  ///< Display name
  std::unordered_map<std::string, std::string> dataMapping; ///< Semantic name → column mapping
  std::unordered_map<std::string, std::string> templateDataMapping; ///< Template placeholder → column mapping (for flags)
  uint32_t zIndex{0};                ///< Rendering layer (higher = on top)
  uint32_t yAxis{0};                 ///< Which Y-axis this series uses
  std::optional<std::string> linkedTo; ///< Optional series ID to link to
  epoch_script::MetaDataArgDefinitionMapping configOptions; ///< Config for annotations/thresholds

  bool operator==(const SeriesInfo &rhs) const = default;
};

/**
 * @brief Y-axis configuration for chart panels
 */
struct YAxis {
  uint32_t top{};      ///< Top position (percentage)
  uint32_t height{100}; ///< Height (percentage)

  bool operator==(const YAxis &rhs) const = default;
};

/**
 * @brief Complete metadata for a single chart pane (one timeframe)
 */
struct ChartPaneMetadata {
  std::vector<YAxis> yAxis;          ///< Y-axes configuration
  std::vector<SeriesInfo> series;    ///< All series in this pane
  std::vector<epoch_frame::SessionRange> sessionRanges; ///< Intraday session ranges for plot bands

  friend std::ostream &operator<<(std::ostream &os, const ChartPaneMetadata &metadata);
  bool operator==(const ChartPaneMetadata &rhs) const;
};

/**
 * @brief Chart metadata for all timeframes
 * Maps timeframe string → chart pane metadata
 */
using TimeFrameChartMetadata = std::unordered_map<std::string, ChartPaneMetadata>;

/**
 * @brief Interface for chart metadata providers
 */
struct IChartMetadataProvider {
  virtual ~IChartMetadataProvider() = default;
  virtual TimeFrameChartMetadata GetMetaData() const = 0;
};

using ChartMetaDataProviderPtr = std::unique_ptr<IChartMetadataProvider>;

/**
 * @brief Factory function to create chart metadata provider
 * @param timeframes Set of timeframe strings to generate metadata for
 * @param transforms List of transform configurations to visualize
 * @return Smart pointer to chart metadata provider
 */
ChartMetaDataProviderPtr CreateChartMetadataProvider(
    const std::unordered_set<std::string> &timeframes,
    const epoch_script::transform::TransformConfigurationList &transforms);

} // namespace epoch_script
