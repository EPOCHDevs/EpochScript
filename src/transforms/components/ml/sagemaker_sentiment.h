//
// AWS SageMaker FinBERT Sentiment Analysis Transform
// Provides financial sentiment analysis using AWS SageMaker FinBERT endpoint
//

#pragma once
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/ml/sagemaker_client.h>
#include <vector>
#include <string>

// Forward declaration
struct SentimentItem;

namespace epoch_script::transform {

/**
 * @brief SageMaker FinBERT Sentiment Analysis Transform
 *
 * This transform calls an AWS SageMaker FinBERT endpoint to perform batch sentiment analysis
 * on financial text data (e.g., financial news, social media, earnings transcripts).
 *
 * Input: String column containing text to analyze
 * Outputs:
 *   - positive: Boolean column indicating positive sentiment
 *   - neutral: Boolean column indicating neutral sentiment
 *   - negative: Boolean column indicating negative sentiment
 *   - confidence: Float64 column with confidence scores (0.0 to 1.0)
 *
 * Requirements:
 * - AWS credentials in environment variables (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)
 * - AWS region set to us-west-2
 * - IAM permission: sagemaker:InvokeEndpoint
 * - FinBERT SageMaker endpoint deployed and active
 */
class SageMakerFinBERTTransform : public ITransform {
public:
  explicit SageMakerFinBERTTransform(const TransformConfiguration &config);

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;

private:
  static constexpr const char *ENDPOINT_NAME = "finbert-1763790064";
  static constexpr int TIMEOUT_MS = 30000;
  static constexpr size_t BATCH_SIZE = 100; // Process 100 texts per API call

  /**
   * @brief Call FinBERT SageMaker endpoint for batch text inputs
   * @param texts Vector of input texts to analyze
   * @return Vector of SentimentItem results
   */
  std::vector<SentimentItem>
  InvokeFinBERTBatch(const std::vector<std::string> &texts) const;

  /**
   * @brief Parse FinBERT batch response
   * @param response_body JSON response from FinBERT
   * @return Vector of SentimentItem results
   */
  std::vector<SentimentItem>
  ParseFinBERTBatchResponse(const std::string &response_body) const;
};

} // namespace epoch_script::transform
