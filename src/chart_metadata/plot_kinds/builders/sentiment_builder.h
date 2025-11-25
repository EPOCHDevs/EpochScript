#pragma once

#include "../multi_line.h"

namespace epoch_script::chart_metadata::plot_kinds {

/**
 * @brief Builder for Sentiment PlotKind
 *
 * Visualizes sentiment analysis results with:
 * - positive: Boolean flag indicating positive sentiment
 * - neutral: Boolean flag indicating neutral sentiment
 * - negative: Boolean flag indicating negative sentiment
 * - confidence: Confidence score (0.0 to 1.0)
 *
 * The visualization will color-code based on sentiment flags
 * and use confidence as the value to plot.
 */
class SentimentBuilder : public IPlotKindBuilder {
public:
  std::unordered_map<std::string, std::string> Build(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    Validate(cfg);

    return {
      {"index", INDEX_COLUMN},
      {"positive", cfg.GetOutputId("positive").GetColumnName()},
      {"neutral", cfg.GetOutputId("neutral").GetColumnName()},
      {"negative", cfg.GetOutputId("negative").GetColumnName()},
      {"confidence", cfg.GetOutputId("confidence").GetColumnName()}
    };
  }

  void Validate(
    const epoch_script::transform::TransformConfiguration &cfg
  ) const override {
    // Must have positive, neutral, negative, and confidence outputs
    if (!cfg.ContainsOutputId("positive")) {
      throw std::runtime_error(
        "Sentiment transform must have 'positive' output"
      );
    }

    if (!cfg.ContainsOutputId("neutral")) {
      throw std::runtime_error(
        "Sentiment transform must have 'neutral' output"
      );
    }

    if (!cfg.ContainsOutputId("negative")) {
      throw std::runtime_error(
        "Sentiment transform must have 'negative' output"
      );
    }

    if (!cfg.ContainsOutputId("confidence")) {
      throw std::runtime_error(
        "Sentiment transform must have 'confidence' output"
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
