//
// Created by assistant on 08/24/25.
//
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <arrow/compute/api_vector.h>
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/serialization.h>
#include <index/datetime_index.h>
#include <vector>

#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/core/constants.h>

using namespace epoch_core;
using namespace epoch_frame;
using namespace epoch_script;
using namespace epoch_script::transform;

namespace {

DataFrame read_hmm_input(const std::string &file) {
  auto path = std::filesystem::path(HMM_TEST_DATA_DIR) / file;
  auto df_res = epoch_frame::read_csv_file(path, epoch_frame::CSVReadOptions{});
  REQUIRE(df_res.ok());
  auto df = df_res.ValueOrDie().set_index("index");
  // Rename columns to use "src#" prefix for node#column format
  std::unordered_map<std::string, std::string> rename_map;
  for (const auto& col : df.column_names()) {
    rename_map[col] = "src#" + col;
  }
  return df.rename(rename_map);
}

} // namespace

TEST_CASE("HMMTransform detects correlated features (2 states)", "[hmm]") {
  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  // Load test input with correlated features - should throw
  auto df2 = read_hmm_input("hmm_input_2.csv");

  for (auto states : {2}) {
    DYNAMIC_SECTION("states=" << states) {
      // Build config for hmm transform with n_states option
      std::string type = "hmm_2";

      auto cfg = run_op(type, std::string("hmm_test_") + std::to_string(states),
          {{epoch_script::ARG, {input_ref("src", "x"), input_ref("src", "y"), input_ref("src", "z")}}},
          {{"max_iterations", MetaDataOptionDefinition{1000.0}},
           {"tolerance", MetaDataOptionDefinition{1e-5}},
           {"compute_zscore", MetaDataOptionDefinition{true}},
           {"min_training_samples", MetaDataOptionDefinition{100.0}},
           {"lookback_window", MetaDataOptionDefinition{0.0}}},
          tf);

      auto tbase = MAKE_TRANSFORM(cfg);
      auto t = dynamic_cast<ITransform *>(tbase.get());
      REQUIRE(t != nullptr);

      // Test with correlated data - should throw exception with correlation message
      REQUIRE_THROWS_WITH(t->TransformData(df2),
          Catch::Matchers::ContainsSubstring("correlated"));

      INFO("HMM correctly detected correlated features and threw exception");
    }
  }
}

TEST_CASE("HMMTransform with lookback window", "[hmm]") {
  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  // 150 samples
  // Build a simple constant series with UTC index
  auto base = read_hmm_input("hmm_input_2.csv");

  // Only keep first 150 rows to guarantee enough length
  if (base.num_rows() > 150) {
    base = base.head(150);
  }
  // Build single-column input "src#x"
  auto df = base["src#x"].to_frame();

  // hmm_2 with lookback_window=100
  // NEW BEHAVIOR: Train on first 100 rows, predict on remaining 50 rows
  auto cfg = run_op("hmm_2", "hmm_lb",
      {{epoch_script::ARG, {input_ref("src", "x")}}},
      {{"lookback_window", MetaDataOptionDefinition{100.0}},
       {"min_training_samples", MetaDataOptionDefinition{100.0}},
       {"max_iterations", MetaDataOptionDefinition{1000.0}},
       {"tolerance", MetaDataOptionDefinition{1e-5}},
       {"compute_zscore", MetaDataOptionDefinition{true}}},
      tf);
  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  // With 150 total rows and lookback_window=100:
  // - Train on rows 0-99 (100 rows)
  // - Predict on rows 100-149 (50 rows)
  // Output should be 50 rows (prediction window only)
  REQUIRE(out.num_rows() == 50);
}

TEST_CASE("HMMTransform insufficient samples throws", "[hmm]") {
  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  // Fewer than default min_training_samples (100)
  // Use first 20 rows from input for consistent index/build
  auto base = read_hmm_input("hmm_input_2.csv");
  auto df = base["src#x"].iloc({0, 50}).to_frame();

  auto cfg = run_op("hmm_2", "hmm_small",
      {{epoch_script::ARG, {input_ref("src", "x")}}},
      {{"max_iterations", MetaDataOptionDefinition{1000.0}},
       {"tolerance", MetaDataOptionDefinition{1e-5}},
       {"compute_zscore", MetaDataOptionDefinition{true}},
       {"min_training_samples", MetaDataOptionDefinition{100.0}},
       {"lookback_window", MetaDataOptionDefinition{0.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  REQUIRE_THROWS(t->TransformData(df));
}

TEST_CASE("HMMTransform with uncorrelated features", "[hmm]") {
  const auto tf =
      epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  // Load test data with uncorrelated features:
  // - x: exponential distribution (volatility-like)
  // - y: linear trend with noise (trend-like)
  // - z: random walk (momentum-like)
  // These features are generated independently with low correlations
  auto df = read_hmm_input("hmm_input_uncorrelated.csv");

  for (auto states : {2, 3}) {
    DYNAMIC_SECTION("Uncorrelated features with states=" << states) {
      std::string type = "hmm_" + std::to_string(states);

      auto cfg = run_op(type, std::string("hmm_uncorr_") + std::to_string(states),
          {{epoch_script::ARG, {input_ref("src", "x"), input_ref("src", "y"), input_ref("src", "z")}}},
          {{"max_iterations", MetaDataOptionDefinition{1000.0}},
           {"tolerance", MetaDataOptionDefinition{1e-5}},
           {"compute_zscore", MetaDataOptionDefinition{true}},
           {"min_training_samples", MetaDataOptionDefinition{100.0}},
           {"lookback_window", MetaDataOptionDefinition{0.0}}},
          tf);

      auto tbase = MAKE_TRANSFORM(cfg);
      auto t = dynamic_cast<ITransform *>(tbase.get());
      REQUIRE(t != nullptr);

      auto out = t->TransformData(df);

      // Output should have same number of rows as input
      REQUIRE(out.num_rows() == df.num_rows());

      // Expected columns: state + individual state probabilities
      size_t expected_cols = 1 + states;
      REQUIRE(out.num_cols() == expected_cols);

      // Verify state column is valid
      auto state_col = out[cfg.GetOutputId("state").GetColumnName()]
                           .contiguous_array()
                           .to_vector<int64_t>();
      REQUIRE(state_col.size() == df.num_rows());
      for (auto s : state_col) {
        REQUIRE(s >= 0);
        REQUIRE(s < states);
      }

      // Verify probability columns exist and are valid
      for (int i = 0; i < states; ++i) {
        auto prob_col_name = cfg.GetOutputId("state_" + std::to_string(i) + "_prob").GetColumnName();
        REQUIRE(out.contains(prob_col_name));

        constexpr double epsilon = 1e-9;
        auto prob_vec = out[prob_col_name].contiguous_array().to_vector<double>();
        for (auto p : prob_vec) {
          REQUIRE(p >= -epsilon);
          REQUIRE(p <= 1.0 + epsilon);
        }
      }

      INFO("HMM transform with uncorrelated features (volatility, trend, momentum) "
           << "completed successfully with " << states << " states");
    }
  }
}
