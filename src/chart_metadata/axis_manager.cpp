#include "axis_manager.h"
#include "plot_kinds/registry.h"
#include "series_configuration_builder.h"
#include <algorithm>
#include <format>
#include <spdlog/spdlog.h>

namespace epoch_script::chart_metadata {

  AxisManager::AxisManager() {
    // Initialize with default axes for each timeframe as needed
  }

  std::pair<uint8_t, std::optional<std::string>> AxisManager::AssignAxis(
      const epoch_script::transform::TransformConfiguration &cfg, const std::string &timeframe,
      const std::unordered_set<std::string> &priceInputs,
      const std::string &volumeInput,
      const std::unordered_map<std::string, int64_t> &outputHandlesToSeriesId) {

    // Ensure base axes exist for this timeframe
    if (m_axes.find(timeframe) == m_axes.end()) {
      // Create price and volume axes
      m_axes[timeframe] = {
        {0, 0, 70}, // Price axis
        {1, 70, 30} // Volume axis
      };
    }

    uint8_t chosenAxis = 0;
    std::optional<std::string> linkedTo;

    // Check if this transform requires its own axis
    if (RequiresOwnAxis(cfg)) {
      // Use the shared function to build descriptive name
      std::string descriptiveName =
          SeriesConfigurationBuilder::BuildDescriptiveName(cfg);

      // For chained indicators, add input source information to make them unique
      auto inputs = cfg.GetInputs();
      for (const auto &[inputKey, connections] : inputs) {
        for (const auto &handle : connections) {
          // Skip literal values - only process node references
          if (!handle.IsNodeReference()) {
            continue;
          }
          const std::string& handle_ref = handle.GetNodeReference().GetColumnName();
          // If this input comes from another transform (not a base data source)
          if (outputHandlesToSeriesId.find(handle_ref) !=
              outputHandlesToSeriesId.end()) {
            descriptiveName += "_CHAINED_" + handle_ref;
            break; // Only need to mark as chained once
              }
        }
      }

      chosenAxis = FindOrCreateIndicatorAxis(timeframe, descriptiveName);
    } else {
      // Determine axis based on inputs - this is the key logic
      chosenAxis =
          DetermineAxisFromInputs(cfg, timeframe, priceInputs, volumeInput,
                                  outputHandlesToSeriesId, linkedTo);
    }

    return {chosenAxis, linkedTo};
  }

  uint8_t AxisManager::DetermineAxisFromInputs(
      const epoch_script::transform::TransformConfiguration &cfg, const std::string &timeframe,
      const std::unordered_set<std::string> &priceInputs,
      const std::string &volumeInput,
      const std::unordered_map<std::string, int64_t> &outputHandlesToSeriesId,
      std::optional<std::string> &linkedTo) {

    // Priority 1: Check if input is another transform's output (chained
    // transforms)
    auto inputs = cfg.GetInputs();
    for (const auto &connections : inputs | std::views::values) {
      for (const auto &handle : connections) {
        // Skip literal values - only process node references
        if (!handle.IsNodeReference()) {
          continue;
        }
        const std::string& handle_ref = handle.GetNodeReference().GetColumnName();
        auto it = outputHandlesToSeriesId.find(handle_ref);
        if (it != outputHandlesToSeriesId.end() &&
            static_cast<size_t>(it->second) < m_seriesOrder[timeframe].size()) {
          const auto &seriesRef = m_seriesOrder[timeframe][it->second];
          uint8_t parentAxis = m_seriesAxisMap[timeframe][seriesRef];
          linkedTo = seriesRef;
          return parentAxis; // Inherit axis from parent transform
            }
      }
    }

    // Priority 2: Check if directly using price inputs
    if (HasPriceInputs(cfg, priceInputs)) {
      if (!m_seriesOrder[timeframe].empty()) {
        linkedTo = m_seriesOrder[timeframe][0]; // Link to candlestick series
      }
      return 0; // Price axis
    }

    // Priority 3: Check if directly using volume input
    if (HasVolumeInput(cfg, volumeInput)) {
      if (m_seriesOrder[timeframe].size() > 1) {
        linkedTo = m_seriesOrder[timeframe][1]; // Link to volume series
      }
      return 1; // Volume axis
    }

    // Priority 4: Default to price axis for transforms with no clear input
    // dependency
    if (!m_seriesOrder[timeframe].empty()) {
      linkedTo = m_seriesOrder[timeframe][0]; // Link to candlestick series
    }
    return 0; // Default to price axis
  }

  std::vector<AxisManager::AxisInfo>
  AxisManager::GetAxes(const std::string &timeframe) const {
    auto it = m_axes.find(timeframe);
    if (it != m_axes.end()) {
      return it->second;
    }
    return {};
  }

  void AxisManager::RegisterSeries(const std::string &timeframe,
                                   const std::string &seriesId,
                                   uint8_t axisIndex) {
    m_seriesAxisMap[timeframe][seriesId] = axisIndex;
    m_seriesOrder[timeframe].push_back(seriesId);
  }

  std::string AxisManager::GetSeriesIdAtIndex(const std::string &timeframe,
                                              size_t index) const {
    auto it = m_seriesOrder.find(timeframe);
    if (it != m_seriesOrder.end() && index < it->second.size()) {
      return it->second[index];
    }
    return "";
  }

  uint8_t AxisManager::CreateIndicatorAxis(const std::string &timeframe) {
    auto &axes = m_axes[timeframe];
    uint8_t newIndex = static_cast<uint8_t>(axes.size());
    axes.push_back({newIndex, 0, 0}); // Temporary values
    RecalculateAxisHeights(timeframe);
    return newIndex;
  }

  uint8_t
  AxisManager::FindOrCreateIndicatorAxis(const std::string &timeframe,
                                         const std::string &descriptiveName) {
    // Check if we already have an axis for this descriptive name
    auto &indicatorAxes = m_indicatorTypeToAxis[timeframe];
    auto it = indicatorAxes.find(descriptiveName);

    if (it != indicatorAxes.end()) {
      // Found existing axis for this descriptive name
      return it->second;
    }

    // Need to create a new axis for this descriptive name
    uint8_t newAxis = CreateIndicatorAxis(timeframe);
    indicatorAxes[descriptiveName] = newAxis;

    SPDLOG_DEBUG("Created new axis {} for indicator name {} in timeframe {}",
                 newAxis, descriptiveName, timeframe);

    return newAxis;
  }

  void AxisManager::RecalculateAxisHeights(const std::string &timeframe) {
    auto &axes = m_axes[timeframe];
    if (axes.empty())
      return;

    const size_t totalPanes = axes.size();
    if (totalPanes <= 2) {
      // Keep default 70/30 split for price/volume
      return;
    }

    // Price gets double height, others get equal shares
    const double paneHeight = 100.0 / (totalPanes + 1);
    axes[0].height = static_cast<uint8_t>(paneHeight * 2);
    axes[0].top = 0;

    double currentTop = paneHeight * 2;
    for (size_t i = 1; i < axes.size(); ++i) {
      axes[i].top = static_cast<uint8_t>(currentTop);
      axes[i].height = static_cast<uint8_t>(paneHeight);
      currentTop += paneHeight;
    }
  }

  bool AxisManager::RequiresOwnAxis(
      const epoch_script::transform::TransformConfiguration &cfg) {
    const auto &metadata = cfg.GetTransformDefinition().GetMetadata();
    const auto plotKind = metadata.plotKind; // Use enum directly

    // Use builder registry for metadata
    auto &registry = plot_kinds::PlotKindBuilderRegistry::Instance();
    return registry.RequiresOwnAxis(plotKind);
  }

  bool AxisManager::HasPriceInputs(
      const epoch_script::transform::TransformConfiguration &cfg,
      const std::unordered_set<std::string> &priceInputs) {
    const auto &inputs = cfg.GetInputs() | std::views::values;
    return std::ranges::any_of(inputs, [&](const auto &inputList) {
      return std::ranges::any_of(inputList, [&](const auto& input_value) {
        // Skip literals - only check node references
        if (!input_value.IsNodeReference()) {
          return false;
        }
        std::string input = input_value.GetNodeReference().GetColumnName();
        auto sepPosition = input.find("#");
        input =
            (sepPosition == std::string::npos ? input
                                              : input.substr(sepPosition + 1));
        return priceInputs.contains(input);
      });
    });
  }

  bool AxisManager::HasVolumeInput(const epoch_script::transform::TransformConfiguration &cfg,
                                   const std::string &volumeInput) {
    const auto &inputs = cfg.GetInputs() | std::views::values;
    return std::ranges::any_of(inputs, [&](const auto &inputList) {
      auto view = inputList | std::views::filter([](const auto& input_value) {
                    // Only include node references
                    return input_value.IsNodeReference();
                  })
                  | std::views::transform([](const auto& input_value) {
                    return input_value.GetNodeReference().GetHandle();
                  });
      return std::ranges::find(view, volumeInput) != view.end();
      ;
    });
  }

  void AxisManager::InitializeBaseAxes(const std::string &timeframe) {
    // Ensure base axes exist for this timeframe
    if (m_axes.find(timeframe) == m_axes.end()) {
      // Create price and volume axes
      m_axes[timeframe] = {
        {0, 0, 85}, // Price axis
        {1, 85, 15} // Volume axis
      };
    }
  }
} // namespace epoch_script::chart_metadata