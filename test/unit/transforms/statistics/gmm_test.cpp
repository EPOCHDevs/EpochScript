//
// GMM Transform Tests
//
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/serialization.h>

#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/core/constants.h>

using namespace epoch_core;
using namespace epoch_frame;
using namespace epoch_script;
using namespace epoch_script::transform;

namespace {

DataFrame read_gmm_input(const std::string &file) {
  auto path = std::filesystem::path(GMM_TEST_DATA_DIR) / file;
  auto df_res = epoch_frame::read_csv_file(path, epoch_frame::CSVReadOptions{});
  REQUIRE(df_res.ok());
  auto df = df_res.ValueOrDie().set_index("index");
  std::unordered_map<std::string, std::string> rename_map;
  for (const auto& col : df.column_names()) {
    rename_map[col] = "src#" + col;
  }
  return df.rename(rename_map);
}

} // namespace

TEST_CASE("GMM2 transform basic functionality", "[gmm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_gmm_input("gmm_input_2.csv");

  auto cfg = run_op("gmm_2", "gmm_test",
      {{epoch_script::ARG, {input_ref("src", "feature_0"), input_ref("src", "feature_1")}}},
      {{"max_iterations", MetaDataOptionDefinition{300.0}},
       {"tolerance", MetaDataOptionDefinition{1e-10}},
              {"min_training_samples", MetaDataOptionDefinition{100.0}},
       {"lookback_window", MetaDataOptionDefinition{0.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // Verify output shape
  REQUIRE(out.num_rows() == df.num_rows());
  REQUIRE(out.num_cols() == 4); // component + 2 probs + log_likelihood

  // Verify component values are 0 or 1
  auto component_col = out[cfg.GetOutputId("component").GetColumnName()]
                           .contiguous_array()
                           .to_vector<int64_t>();
  for (auto c : component_col) {
    REQUIRE(c >= 0);
    REQUIRE(c < 2);
  }

  // Verify probabilities sum to ~1
  auto prob0 = out[cfg.GetOutputId("component_0_prob").GetColumnName()]
                   .contiguous_array()
                   .to_vector<double>();
  auto prob1 = out[cfg.GetOutputId("component_1_prob").GetColumnName()]
                   .contiguous_array()
                   .to_vector<double>();

  for (size_t i = 0; i < prob0.size(); ++i) {
    REQUIRE(std::abs(prob0[i] + prob1[i] - 1.0) < 1e-6);
  }
}

TEST_CASE("GMM3 transform output validation", "[gmm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_gmm_input("gmm_input_3.csv");

  auto cfg = run_op("gmm_3", "gmm3_test",
      {{epoch_script::ARG, {input_ref("src", "feature_0"), input_ref("src", "feature_1")}}},
      {{"min_training_samples", MetaDataOptionDefinition{100.0}},
       {"lookback_window", MetaDataOptionDefinition{0.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // 3 components: component + 3 probs + log_likelihood = 5 cols
  REQUIRE(out.num_cols() == 5);

  // Component values 0-2
  auto component_col = out[cfg.GetOutputId("component").GetColumnName()]
                           .contiguous_array()
                           .to_vector<int64_t>();
  for (auto c : component_col) {
    REQUIRE(c >= 0);
    REQUIRE(c < 3);
  }

  // Verify all 3 probabilities sum to ~1
  auto prob0 = out[cfg.GetOutputId("component_0_prob").GetColumnName()]
                   .contiguous_array()
                   .to_vector<double>();
  auto prob1 = out[cfg.GetOutputId("component_1_prob").GetColumnName()]
                   .contiguous_array()
                   .to_vector<double>();
  auto prob2 = out[cfg.GetOutputId("component_2_prob").GetColumnName()]
                   .contiguous_array()
                   .to_vector<double>();

  for (size_t i = 0; i < prob0.size(); ++i) {
    double sum = prob0[i] + prob1[i] + prob2[i];
    REQUIRE(std::abs(sum - 1.0) < 1e-6);
  }
}

TEST_CASE("GMM4 transform output validation", "[gmm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_gmm_input("gmm_input_4.csv");

  auto cfg = run_op("gmm_4", "gmm4_test",
      {{epoch_script::ARG, {input_ref("src", "feature_0"), input_ref("src", "feature_1")}}},
      {{"min_training_samples", MetaDataOptionDefinition{100.0}},
       {"lookback_window", MetaDataOptionDefinition{0.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // 4 components: component + 4 probs + log_likelihood = 6 cols
  REQUIRE(out.num_cols() == 6);

  // Component values 0-3
  auto component_col = out[cfg.GetOutputId("component").GetColumnName()]
                           .contiguous_array()
                           .to_vector<int64_t>();
  for (auto c : component_col) {
    REQUIRE(c >= 0);
    REQUIRE(c < 4);
  }
}

TEST_CASE("GMM insufficient samples throws", "[gmm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_gmm_input("gmm_input_2.csv");
  df = df.head(50); // Only 50 samples

  auto cfg = run_op("gmm_2", "gmm_small",
      {{epoch_script::ARG, {input_ref("src", "feature_0"), input_ref("src", "feature_1")}}},
      {{"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  REQUIRE_THROWS(t->TransformData(df));
}

TEST_CASE("GMM with lookback window", "[gmm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_gmm_input("gmm_input_2.csv");
  // Ensure we have enough data: 200 train + 100 predict = 300 total
  REQUIRE(df.num_rows() >= 300);

  auto cfg = run_op("gmm_2", "gmm_lb",
      {{epoch_script::ARG, {input_ref("src", "feature_0"), input_ref("src", "feature_1")}}},
      {{"lookback_window", MetaDataOptionDefinition{200.0}},
       {"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // With lookback_window=200, output is only prediction rows
  REQUIRE(out.num_rows() == df.num_rows() - 200);
}

TEST_CASE("GMM log_likelihood output", "[gmm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_gmm_input("gmm_input_2.csv");

  auto cfg = run_op("gmm_2", "gmm_ll",
      {{epoch_script::ARG, {input_ref("src", "feature_0"), input_ref("src", "feature_1")}}},
      {{"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // Verify log_likelihood column exists and contains finite values
  auto ll_col = out[cfg.GetOutputId("log_likelihood").GetColumnName()]
                    .contiguous_array()
                    .to_vector<double>();

  REQUIRE(ll_col.size() == df.num_rows());
  for (auto ll : ll_col) {
    REQUIRE(std::isfinite(ll));
    // Log-likelihood should be negative (log of probability)
    REQUIRE(ll < 0);
  }
}
