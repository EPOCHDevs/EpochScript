#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Sentiment PlotKind
 *
 * Visualizes sentiment analysis results with:
 * - sentiment: String label ("positive", "neutral", "negative")
 * - score: Confidence score (0.0 to 1.0)
 *
 * The visualization will color-code based on sentiment label
 * and use score as the value to plot.
 */
class SentimentBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"sentiment", cfg.GetOutputId("sentiment").GetColumnName()},
      {"score", cfg.GetOutputId("score").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // Must have both sentiment and score outputs
    if (!cfg.ContainsOutputId("sentiment")) {
      throw std::runtime_error(
        "Sentiment transform must have 'sentiment' output"
      );
    }

    if (!cfg.ContainsOutputId("score")) {
      throw std::runtime_error(
        "Sentiment transform must have 'score' output"
      );
    }
  }

  uint8_t GetZIndex() const override {
    return 5; // Same as panel indicators
  }

  bool RequiresOwnAxis() const override {
    return true; // Display in separate panel below price chart
  }
};

} // namespace epoch_script::chart_metadata::plot_kinds
