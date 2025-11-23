#pragma once

#include <epoch_script/transforms/core/metadata.h>

namespace epoch_script::transform {

// Factory function to create metadata for SageMaker FinBERT sentiment analysis
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeSageMakerSentimentTransforms() {
  std::vector<epoch_script::transforms::TransformsMetaData> metadataList;

  // SageMaker FinBERT Sentiment Analysis Transform
  metadataList.emplace_back(
      epoch_script::transforms::TransformsMetaData{
          .id = "finbert_sentiment",
          .category = epoch_core::TransformCategory::ML,
          .plotKind = epoch_core::TransformPlotKind::sentiment,  // Custom sentiment visualization with color bands
          .name = "FinBERT Sentiment Analysis",
          .options = {},
          .isCrossSectional = false,
          .desc = "Analyze financial sentiment of text using AWS SageMaker FinBERT model. "
                  "Returns boolean flags for positive, neutral, and negative sentiment with confidence scores[0-1]. ",
          .inputs =
              {
                  {epoch_core::IODataType::String, ARG, "Text to Analyze", false, false},
              },
          .outputs =
              {
                  {epoch_core::IODataType::Boolean, "positive", "Positive Sentiment Flag"},
                  {epoch_core::IODataType::Boolean, "neutral", "Neutral Sentiment Flag"},
                  {epoch_core::IODataType::Boolean, "negative", "Negative Sentiment Flag"},
                  {epoch_core::IODataType::Decimal, "confidence", "Confidence Score[0-1]"},
              },
          .atLeastOneInputRequired = true,
          .tags = {"ml", "nlp", "sentiment", "finbert", "aws", "sagemaker", "financial-text"},
          .requiresTimeFrame = false,
          .requiredDataSources = {},
          .allowNullInputs = false,
          .strategyTypes = {"sentiment-driven", "news-based", "event-driven"},
          .relatedTransforms = {"news", "stringify"},
          .assetRequirements = {},
          .usageContext =
              "Analyze sentiment of financial text from news, earnings transcripts, "
              "social media, or analyst reports. Use for sentiment-driven trading strategies, "
              "news impact analysis, or market mood tracking. "
              "Example: news = polygon_news(); "
              "sent = finbert_sentiment(news.description); "
              "positive_news = sent.positive; "
              "high_conf_positive = sent.positive and sent.confidence > 0.8",
          .limitations =
              "Empty or null text returns neutral=true with confidence 0.0. "
              "Network latency and AWS costs apply per inference request.",
      });

  return metadataList;
}

} // namespace epoch_script::transform
