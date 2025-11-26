#include "series_configuration_builder.h"
#include "epoch_script/chart_metadata/chart_metadata_provider.h"
#include "data_column_resolver.h"
#include "epoch_script/data/common/constants.h"
#include "plot_kinds/registry.h"
#include "plot_kinds/builders/flag_builder.h"  // For GetActualColumnName
#include <algorithm>
#include <functional>
#include <regex>
#include <spdlog/spdlog.h> // For SPDLOG_DEBUG

namespace epoch_script::chart_metadata {

namespace {
// Constants for chart types
constexpr const char *CANDLESTICK_CHART = "candlestick";
constexpr const char *VOLUME_CHART = "column";

} // anonymous namespace

std::string SeriesConfigurationBuilder::BuildDescriptiveName(
    const epoch_script::transform::TransformConfiguration &cfg) {
  std::stringstream ss;

  // Get the ID and convert to uppercase like TradingView
  const auto &metadata = cfg.GetTransformDefinition().GetMetadata();
  std::string baseName = metadata.id;

  // Convert to uppercase for display
  std::transform(baseName.begin(), baseName.end(), baseName.begin(), ::toupper);
  ss << baseName;

  // Use the actual configured options, not the default ones
  const auto &configuredOptions = cfg.GetOptions();
  for (const auto &[optionId, optionValue] : configuredOptions) {
    ss << " " << optionId << "=";
    std::visit(
        [&ss]<typename T>(const T &arg) {
          if constexpr (std::is_same_v<T, std::string>) {
            ss << arg;
          } else if constexpr (std::is_same_v<T, double>) {
            bool isInteger = std::floor(arg) == arg;
            if (isInteger) {
              ss << static_cast<int64_t>(arg);
            } else {
              ss << std::fixed << std::setprecision(2) << arg;
            }
          } else if constexpr (std::is_same_v<T, bool>) {
            ss << std::boolalpha << arg;
          } else if constexpr (std::is_same_v<T,
                                              epoch_script::MetaDataArgRef>) {
            ss << "$" << arg.refName;
          }
        },
        optionValue.GetVariant());
  }
  return ss.str();
}

SeriesInfo SeriesConfigurationBuilder::BuildSeries(
    const epoch_script::transform::TransformConfiguration &cfg, uint8_t chosenAxis,
    const std::optional<std::string> &linkedTo, const std::string &seriesId) {

  // Get plot kind from metadata
  const auto &metadata = cfg.GetTransformDefinition().GetMetadata();
  auto plotKind = metadata.plotKind; // Use enum directly

  // Get registry instance and build data mapping
  auto &registry = plot_kinds::PlotKindBuilderRegistry::Instance();
  auto dataMapping = registry.Build(plotKind, cfg);

  // Build descriptive name with parameters
  std::string displayName = BuildDescriptiveName(cfg);

  SeriesInfo series;
  series.id = seriesId;

  // Convert PlotKind enum to string using wrapper
  series.type = epoch_core::TransformPlotKindWrapper::ToString(plotKind);

  // Get metadata directly from builder via registry
  series.zIndex = registry.GetZIndex(plotKind);
  series.name = displayName;
  series.dataMapping = dataMapping;
  series.yAxis = chosenAxis;
  series.linkedTo = linkedTo;

  // For flags, populate templateDataMapping with all outputs for template substitution
  // For DataSource transforms, use actual column names from requiredDataSources
  if (plotKind == epoch_core::TransformPlotKind::flag) {
    for (const auto &output : cfg.GetOutputs()) {
      series.templateDataMapping[output.id] = plot_kinds::GetActualColumnName(cfg, output.id);
    }
  }

  // Build config options for annotations and thresholds
  series.configOptions = BuildConfigOptions(cfg);

  return series;
}

SeriesInfo SeriesConfigurationBuilder::BuildCandlestickSeries(
    const std::string &timeframe) {
  const auto &C = epoch_script::EpochStratifyXConstants::instance();

  SeriesInfo series;
  series.id = std::format("{}_candlestick", timeframe);
  series.type = CANDLESTICK_CHART;
  series.name = "";
  series.dataMapping = {{"index", INDEX_COLUMN},
                        {"open", C.OPEN()},
                        {"high", C.HIGH()},
                        {"low", C.LOW()},
                        {"close", C.CLOSE()}};
  series.zIndex = 0; // Candlestick always has z-index 0
  series.yAxis = 0;
  series.linkedTo = std::nullopt;

  // Initialize empty config options for candlestick series (no transform
  // config)
  series.configOptions = {};

  return series;
}

SeriesInfo
SeriesConfigurationBuilder::BuildVolumeSeries(const std::string &timeframe) {
  const auto &C = epoch_script::EpochStratifyXConstants::instance();

  SeriesInfo series;
  series.id = std::format("{}_volume", timeframe);
  series.type = VOLUME_CHART;
  series.name = "Volume";
  series.dataMapping = {{"index", INDEX_COLUMN}, {"value", C.VOLUME()}};
  series.zIndex = 0; // Volume always has z-index 0
  series.yAxis = 1;
  series.linkedTo = std::nullopt;

  // Initialize empty config options for volume series (no transform config)
  series.configOptions = {};

  return series;
}

// SeriesInfo SeriesConfigurationBuilder::BuildExitLevelsSeries(
//     const std::string &timeframe) {
//
//   SeriesInfo exitLevels;
//   exitLevels.id = std::format("{}_exit_levels", timeframe);
//   exitLevels.type = "exit_levels";
//   exitLevels.name = "Exit Levels";
//   exitLevels.dataMapping = {{"index", INDEX_COLUMN},
//                             {"stop_loss", "stop_loss"},
//                             {"take_profit", "take_profit"}};
//   exitLevels.zIndex = 100; // Stop Loss always has z-index 100
//   exitLevels.yAxis = 0;
//   exitLevels.linkedTo = std::format("{}_candlestick", timeframe);
//
//   return exitLevels;
// }
//
// SeriesInfo SeriesConfigurationBuilder::BuildPositionQuantitySeries(
//     const std::string &timeframe) {
//   SeriesInfo series;
//   series.id = std::format("{}_qty", timeframe);
//   series.type = "position";
//   series.name = "Position Size";
//   series.dataMapping = {{"index", INDEX_COLUMN}, {"value", "quantity"}};
//   series.zIndex = 0; // Equity always has z-index 0
//   series.yAxis = 2;
//   series.linkedTo = std::nullopt;
//
//   return series;
// }

bool SeriesConfigurationBuilder::IsIntradayTimeframe(
    const std::string &timeframe) {
  // Check for minute-based timeframes (e.g., 1Min, 5Min, 15Min, 30Min)
  if (std::regex_match(timeframe, std::regex(R"(\d+Min)"))) {
    return true;
  }

  // Check for hour-based timeframes (e.g., 1H, 2H, 4H)
  if (std::regex_match(timeframe, std::regex(R"(\d+H)"))) {
    return true;
  }

  // Check for second-based timeframes (e.g., 30S, 1S)
  if (std::regex_match(timeframe, std::regex(R"(\d+S)"))) {
    return true;
  }

  return false;
}

epoch_script::MetaDataArgDefinitionMapping
SeriesConfigurationBuilder::BuildConfigOptions(
    const epoch_script::transform::TransformConfiguration &cfg) {

  epoch_script::MetaDataArgDefinitionMapping configOptions;

  try {
    // Extract only the configured options from cfg.GetOptions()
    // Return them as-is with their original variant types
    configOptions = cfg.GetOptions();

    // Get PlotKind-specific defaults from the builder
    const auto &metadata = cfg.GetTransformDefinition().GetMetadata();
    auto& registry = plot_kinds::PlotKindBuilderRegistry::Instance();

    if (registry.IsRegistered(metadata.plotKind)) {
      const auto& builder = registry.GetBuilder(metadata.plotKind);

      // Get defaults from the PlotKind builder (each builder knows its own defaults)
      auto defaults = builder.GetDefaultConfigOptions(cfg);

      // Apply defaults only if not already configured
      for (const auto& [key, value] : defaults) {
        if (configOptions.find(key) == configOptions.end()) {
          configOptions[key] = value;
        }
      }
    }

  } catch (const std::exception& exp) {
    // If any error occurs, return empty options, but log the error with details
    SPDLOG_WARN("Failed to build config options for transform {}: {}. Returning empty options.",
                cfg.GetId(), exp.what());
  } catch (...) {
    // If any unknown error occurs, return empty options
    SPDLOG_WARN("Failed to build config options for transform {} (unknown error). Returning empty options.",
                cfg.GetId());
  }

  return configOptions;
}

} // namespace epoch_script::chart_metadata