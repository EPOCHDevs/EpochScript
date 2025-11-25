//
// Unit tests for SageMaker FinBERT Sentiment Analysis Transform
//

#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

// Helper function to create test dataframe with financial text
DataFrame createFinancialTextDataFrame() {
  auto index = epoch_frame::factory::index::make_datetime_index(
      {epoch_frame::DateTime{2024y, std::chrono::January, 1d},
       epoch_frame::DateTime{2024y, std::chrono::January, 2d},
       epoch_frame::DateTime{2024y, std::chrono::January, 3d},
       epoch_frame::DateTime{2024y, std::chrono::January, 4d},
       epoch_frame::DateTime{2024y, std::chrono::January, 5d}});

  return make_dataframe<std::string>(
      index,
      {{"The company reported record profits this quarter with 25% growth",
        "Stock prices are falling due to market uncertainty and recession fears",
        "The quarterly earnings met analyst expectations",
        "Major layoffs announced as company struggles with declining revenue",
        "New product launch expected to boost sales significantly"}},
      {"text"});
}

TEST_CASE("FinBERT Sentiment Transform - Configuration", "[ml][finbert][config]") {
  const auto tf = EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  SECTION("Basic configuration") {
    auto config = finbert_sentiment_cfg("test_finbert", strategy::InputValue(transform::ConstantValue(std::string("text"))), tf);

    REQUIRE(config.GetTransformName() == "finbert_sentiment");
    REQUIRE(config.GetId() == "test_finbert");

    // Verify transform can be instantiated
    auto transformBase = MAKE_TRANSFORM(config);
    REQUIRE(transformBase != nullptr);

    // Verify it's the correct transform type
    auto transform = dynamic_cast<ITransform *>(transformBase.get());
    REQUIRE(transform != nullptr);
  }

  SECTION("Metadata validation") {
    auto &registry = transforms::ITransformRegistry::GetInstance();
    REQUIRE(registry.IsValid("finbert_sentiment"));

    auto metadataOpt = registry.GetMetaData("finbert_sentiment");
    REQUIRE(metadataOpt.has_value());

    auto metadata = metadataOpt.value().get();
    REQUIRE(metadata.id == "finbert_sentiment");
    REQUIRE(metadata.name == "FinBERT Sentiment Analysis");
    REQUIRE(metadata.category == TransformCategory::ML);
    REQUIRE(metadata.plotKind == TransformPlotKind::sentiment);

    // Verify outputs
    REQUIRE(metadata.outputs.size() == 4);
    REQUIRE(metadata.outputs[0].id == "positive");
    REQUIRE(metadata.outputs[0].type == IODataType::Boolean);
    REQUIRE(metadata.outputs[1].id == "neutral");
    REQUIRE(metadata.outputs[1].type == IODataType::Boolean);
    REQUIRE(metadata.outputs[2].id == "negative");
    REQUIRE(metadata.outputs[2].type == IODataType::Boolean);
    REQUIRE(metadata.outputs[3].id == "confidence");
    REQUIRE(metadata.outputs[3].type == IODataType::Decimal);
  }
}

// Integration test (requires AWS credentials and active SageMaker endpoint)
// Mark with [.integration] tag to skip by default in CI/CD
TEST_CASE("FinBERT Sentiment Transform - Integration Test",
          "[.integration][ml][finbert][aws]") {

  // This test requires:
  // 1. AWS credentials in environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
  // 2. AWS_REGION=us-west-2
  // 3. Active SageMaker endpoint named "finbert"

  auto input = createFinancialTextDataFrame();
  auto index = input.index();
  const auto tf = EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  SECTION("End-to-end sentiment analysis") {
    auto config = finbert_sentiment_cfg("test_integration", strategy::InputValue(transform::ConstantValue(std::string("text"))), tf);
    auto transformBase = MAKE_TRANSFORM(config);
    auto transform = dynamic_cast<ITransform *>(transformBase.get());

    // Execute transform
    DataFrame output = transform->TransformData(input);

    // Verify output structure
    REQUIRE(output.num_cols() == 4);
    REQUIRE(output.contains(config.GetOutputId("positive").GetColumnName()));
    REQUIRE(output.contains(config.GetOutputId("neutral").GetColumnName()));
    REQUIRE(output.contains(config.GetOutputId("negative").GetColumnName()));
    REQUIRE(output.contains(config.GetOutputId("confidence").GetColumnName()));
    REQUIRE(output.size() == 5);

    // Get output columns
    auto positive_col = output[config.GetOutputId("positive").GetColumnName()];
    auto neutral_col = output[config.GetOutputId("neutral").GetColumnName()];
    auto negative_col = output[config.GetOutputId("negative").GetColumnName()];
    auto confidence_col = output[config.GetOutputId("confidence").GetColumnName()];

    // Verify first result (positive sentiment expected)
    REQUIRE(positive_col.iloc(0).as_bool() == true);
    REQUIRE(neutral_col.iloc(0).as_bool() == false);
    REQUIRE(negative_col.iloc(0).as_bool() == false);
    REQUIRE(confidence_col.iloc(0).as_double() > 0.9);

    // Verify second result (negative sentiment expected)
    REQUIRE(positive_col.iloc(1).as_bool() == false);
    REQUIRE(neutral_col.iloc(1).as_bool() == false);
    REQUIRE(negative_col.iloc(1).as_bool() == true);
    REQUIRE(confidence_col.iloc(1).as_double() > 0.9);

    // Verify all results have valid structure
    for (size_t i = 0; i < output.size(); ++i) {
      // Confidence score should be in valid range
      double confidence = confidence_col.iloc(i).as_double();
      REQUIRE(confidence >= 0.0);
      REQUIRE(confidence <= 1.0);

      // Exactly one sentiment flag should be true
      bool pos = positive_col.iloc(i).as_bool();
      bool neu = neutral_col.iloc(i).as_bool();
      bool neg = negative_col.iloc(i).as_bool();
      int count = (pos ? 1 : 0) + (neu ? 1 : 0) + (neg ? 1 : 0);
      REQUIRE(count == 1);
    }
  }
}
