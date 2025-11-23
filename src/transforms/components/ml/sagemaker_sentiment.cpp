//
// AWS SageMaker FinBERT Sentiment Analysis Transform Implementation
//

#include "sagemaker_sentiment.h"
#include <epoch_frame/series.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/array_factory.h>
#include <spdlog/spdlog.h>
#include <glaze/glaze.hpp>
#include <aws/sagemaker-runtime/SageMakerRuntimeClient.h>
#include <aws/sagemaker-runtime/model/InvokeEndpointRequest.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>

// Sentiment result structure (internal to implementation)
struct SentimentItem {
  bool positive;
  bool neutral;
  bool negative;
  double confidence;
};

// Raw sentiment item for JSON parsing (matches API response format)
struct RawSentimentItem {
  std::string label;
  double score;
};

namespace epoch_script::transform {

// Glaze request struct - Glaze will auto-reflect this
struct FinBERTRequest {
  std::vector<std::string> inputs;
};

SageMakerFinBERTTransform::SageMakerFinBERTTransform(
    const TransformConfiguration &config)
    : ITransform(config) {
  // Initialize singleton client on first use
  auto *client_mgr = SageMakerClientManager::Instance();
  AssertFromStream(client_mgr->GetClient() != nullptr,
                   "Failed to get SageMaker client");
}

std::vector<SentimentItem>
SageMakerFinBERTTransform::InvokeFinBERTBatch(
    const std::vector<std::string> &texts) const {

  auto *client = SageMakerClientManager::Instance()->GetClient();

  try {
    // Prepare request
    Aws::SageMakerRuntime::Model::InvokeEndpointRequest request;
    request.SetEndpointName(ENDPOINT_NAME);
    request.SetContentType("application/json");

    // Create JSON payload using Glaze
    FinBERTRequest req_data{.inputs = texts};
    auto payload_result = glz::write_json(req_data);
    if (!payload_result) {
      spdlog::error("Failed to serialize FinBERT request");
      return std::vector<SentimentItem>(texts.size(),
                                       SentimentItem{false, true, false, 0.0});
    }
    std::string payload_str = payload_result.value();

    // Set request body - create iostream from string
    auto body_stream = std::make_shared<Aws::StringStream>(payload_str);
    request.SetBody(body_stream);

    // Make inference call
    auto outcome = client->InvokeEndpoint(request);

    if (!outcome.IsSuccess()) {
      spdlog::error("FinBERT batch inference failed: {}",
                    outcome.GetError().GetMessage());
      // Return neutral sentiment for all on error
      return std::vector<SentimentItem>(
          texts.size(), SentimentItem{false, true, false, 0.0});
    }

    // Parse response
    auto &result = outcome.GetResult();
    auto &response_stream = result.GetBody();
    std::string response_body{
        std::istreambuf_iterator<char>(response_stream),
        std::istreambuf_iterator<char>()};

    return ParseFinBERTBatchResponse(response_body);

  } catch (const std::exception &e) {
    spdlog::error("Exception during FinBERT batch inference: {}", e.what());
    // Return neutral sentiment for all on error
    return std::vector<SentimentItem>(
        texts.size(), SentimentItem{false, true, false, 0.0});
  }
}

std::vector<SentimentItem>
SageMakerFinBERTTransform::ParseFinBERTBatchResponse(
    const std::string &response_body) const {

  try {
    // Parse JSON response using Glaze
    // FinBERT batch returns: [{'label': 'positive', 'score': 0.948}, ...]
    // Flat array of results, one per input

    std::vector<RawSentimentItem> raw_results;
    auto parse_error = glz::read_json(raw_results, response_body);

    if (parse_error) {
      spdlog::error("Failed to parse FinBERT batch response: {}",
                    glz::format_error(parse_error, response_body));
      return {};
    }

    // Convert to boolean flag format
    std::vector<SentimentItem> batch_results;
    batch_results.reserve(raw_results.size());

    for (const auto &raw : raw_results) {
      std::string label = raw.label;
      std::transform(label.begin(), label.end(), label.begin(), ::tolower);

      // Convert string label to boolean flags
      SentimentItem item{
        .positive = (label == "positive"),
        .neutral = (label == "neutral"),
        .negative = (label == "negative"),
        .confidence = raw.score
      };

      // Validate that exactly one flag is set
      if (!item.positive && !item.neutral && !item.negative) {
        spdlog::warn("Unexpected FinBERT label: {}, defaulting to neutral", label);
        item.neutral = true;
        item.confidence = 0.0;
      }

      batch_results.push_back(item);
    }

    return batch_results;

  } catch (const std::exception &e) {
    spdlog::error("Exception parsing FinBERT batch response: {}", e.what());
    spdlog::debug("Response body: {}", response_body);
    return {};
  }
}

epoch_frame::DataFrame SageMakerFinBERTTransform::TransformData(
    epoch_frame::DataFrame const &bars) const {

  using namespace epoch_frame;

  // Get input text column
  Series input = bars[GetInputId()];
  const size_t total_size = input.size();

  // Reserve output vectors for 4 columns
  std::vector<bool> all_positive;
  std::vector<bool> all_neutral;
  std::vector<bool> all_negative;
  std::vector<double> all_confidence;
  all_positive.reserve(total_size);
  all_neutral.reserve(total_size);
  all_negative.reserve(total_size);
  all_confidence.reserve(total_size);

  // Process in batches
  for (size_t batch_start = 0; batch_start < total_size; batch_start += BATCH_SIZE) {
    size_t batch_end = std::min(batch_start + BATCH_SIZE, total_size);
    size_t batch_size = batch_end - batch_start;

    // Collect batch texts
    std::vector<std::string> batch_texts;
    batch_texts.reserve(batch_size);

    for (size_t i = batch_start; i < batch_end; ++i) {
      batch_texts.push_back(input.iloc(i).repr());
    }

    // Invoke FinBERT for this batch
    auto batch_results = InvokeFinBERTBatch(batch_texts);

    // Handle empty results (error case)
    if (batch_results.size() != batch_size) {
      spdlog::error("Batch result size mismatch: expected {}, got {}",
                    batch_size, batch_results.size());
      // Fill with neutral for this batch
      for (size_t i = 0; i < batch_size; ++i) {
        all_positive.push_back(false);
        all_neutral.push_back(true);
        all_negative.push_back(false);
        all_confidence.push_back(0.0);
      }
      continue;
    }

    // Append results
    for (const auto &result : batch_results) {
      all_positive.push_back(result.positive);
      all_neutral.push_back(result.neutral);
      all_negative.push_back(result.negative);
      all_confidence.push_back(result.confidence);
    }

#ifndef NDEBUG
    // Log progress for large datasets
    spdlog::debug("Processed {}/{} FinBERT analyses", batch_end, total_size);
#endif
  }

  // Create output series using factory
  auto positive_array = factory::array::make_array(all_positive);
  auto neutral_array = factory::array::make_array(all_neutral);
  auto negative_array = factory::array::make_array(all_negative);
  auto confidence_array = factory::array::make_array(all_confidence);

  // Return DataFrame with four columns: positive, neutral, negative, confidence
  return make_dataframe(
      bars.index(),
      {positive_array, neutral_array, negative_array, confidence_array},
      {GetOutputId("positive"), GetOutputId("neutral"), GetOutputId("negative"), GetOutputId("confidence")});
}

} // namespace epoch_script::transform
