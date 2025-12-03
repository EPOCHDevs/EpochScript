//
// ML Preprocessing Transform Tests
//
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
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

DataFrame read_ml_input(const std::string &file) {
  auto path = std::filesystem::path(ML_TEST_DATA_DIR) / file;
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

TEST_CASE("ml_zscore basic functionality", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("ml_zscore_2", "zscore_test",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{0.7}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // Should have 2 output columns (one per input)
  REQUIRE(out.num_cols() == 2);
  REQUIRE(out.num_rows() == df.num_rows());

  // Verify outputs are finite
  auto col0 = out[cfg.GetOutputId("scaled_0").GetColumnName()]
                  .contiguous_array()
                  .to_vector<double>();
  auto col1 = out[cfg.GetOutputId("scaled_1").GetColumnName()]
                  .contiguous_array()
                  .to_vector<double>();

  for (auto v : col0) {
    REQUIRE(std::isfinite(v));
  }
  for (auto v : col1) {
    REQUIRE(std::isfinite(v));
  }
}

TEST_CASE("ml_zscore training set has zero mean unit variance", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");
  const double split_ratio = 0.7;
  const size_t train_size = static_cast<size_t>(std::ceil(df.num_rows() * split_ratio));

  auto cfg = run_op("ml_zscore_2", "zscore_stats",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{split_ratio}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  auto out = t->TransformData(df);

  auto scaled = out[cfg.GetOutputId("scaled_0").GetColumnName()]
                    .contiguous_array()
                    .to_vector<double>();

  // Compute mean and std of training portion
  double train_sum = 0.0;
  for (size_t i = 0; i < train_size; ++i) {
    train_sum += scaled[i];
  }
  double train_mean = train_sum / train_size;

  double train_var_sum = 0.0;
  for (size_t i = 0; i < train_size; ++i) {
    train_var_sum += (scaled[i] - train_mean) * (scaled[i] - train_mean);
  }
  double train_std = std::sqrt(train_var_sum / train_size);

  // Training portion should have mean ~0 and std ~1
  REQUIRE_THAT(train_mean, Catch::Matchers::WithinAbs(0.0, 0.01));
  REQUIRE_THAT(train_std, Catch::Matchers::WithinAbs(1.0, 0.05));
}

TEST_CASE("ml_minmax basic functionality", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("ml_minmax_2", "minmax_test",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{0.7}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  REQUIRE(out.num_cols() == 2);
  REQUIRE(out.num_rows() == df.num_rows());

  // Verify outputs are finite
  auto col0 = out[cfg.GetOutputId("scaled_0").GetColumnName()]
                  .contiguous_array()
                  .to_vector<double>();

  for (auto v : col0) {
    REQUIRE(std::isfinite(v));
  }
}

TEST_CASE("ml_minmax training set in [0,1] range", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");
  const double split_ratio = 0.7;
  const size_t train_size = static_cast<size_t>(std::ceil(df.num_rows() * split_ratio));

  auto cfg = run_op("ml_minmax_2", "minmax_range",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{split_ratio}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  auto out = t->TransformData(df);

  auto scaled = out[cfg.GetOutputId("scaled_0").GetColumnName()]
                    .contiguous_array()
                    .to_vector<double>();

  // Training portion should be in [0, 1]
  double train_min = scaled[0];
  double train_max = scaled[0];
  for (size_t i = 0; i < train_size; ++i) {
    train_min = std::min(train_min, scaled[i]);
    train_max = std::max(train_max, scaled[i]);
  }

  REQUIRE_THAT(train_min, Catch::Matchers::WithinAbs(0.0, 0.01));
  REQUIRE_THAT(train_max, Catch::Matchers::WithinAbs(1.0, 0.01));
}

TEST_CASE("ml_robust basic functionality", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("ml_robust_2", "robust_test",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{0.7}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  REQUIRE(out.num_cols() == 2);
  REQUIRE(out.num_rows() == df.num_rows());

  // Verify outputs are finite
  auto col0 = out[cfg.GetOutputId("scaled_0").GetColumnName()]
                  .contiguous_array()
                  .to_vector<double>();

  for (auto v : col0) {
    REQUIRE(std::isfinite(v));
  }
}

TEST_CASE("ml_robust training set centered on median", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");
  const double split_ratio = 0.7;
  const size_t train_size = static_cast<size_t>(std::ceil(df.num_rows() * split_ratio));

  auto cfg = run_op("ml_robust_2", "robust_median",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{split_ratio}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  auto out = t->TransformData(df);

  auto scaled = out[cfg.GetOutputId("scaled_0").GetColumnName()]
                    .contiguous_array()
                    .to_vector<double>();

  // Compute median of training portion
  std::vector<double> train_values(scaled.begin(), scaled.begin() + train_size);
  std::sort(train_values.begin(), train_values.end());
  double train_median = train_values[train_size / 2];

  // Median should be approximately 0
  REQUIRE_THAT(train_median, Catch::Matchers::WithinAbs(0.0, 0.1));
}

TEST_CASE("ml_zscore with different split ratios", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  for (double ratio : {0.5, 0.7, 0.9}) {
    DYNAMIC_SECTION("split_ratio=" << ratio) {
      auto cfg = run_op("ml_zscore_2", "zscore_ratio",
          {{"feature_0", {input_ref("src", "signal_1")}},
           {"feature_1", {input_ref("src", "signal_2")}}},
          {{"split_ratio", MetaDataOptionDefinition{ratio}}},
          tf);

      auto tbase = MAKE_TRANSFORM(cfg);
      auto t = dynamic_cast<ITransform *>(tbase.get());

      auto out = t->TransformData(df);
      REQUIRE(out.num_rows() == df.num_rows());
    }
  }
}

TEST_CASE("ml_zscore invalid split_ratio throws", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  // split_ratio = 0 should throw during transform construction
  auto cfg0 = run_op("ml_zscore_2", "zscore_invalid",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{0.0}}},
      tf);
  REQUIRE_THROWS(MAKE_TRANSFORM(cfg0));

  // split_ratio > 1 should throw during transform construction
  auto cfg1 = run_op("ml_zscore_2", "zscore_invalid2",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{1.5}}},
      tf);
  REQUIRE_THROWS(MAKE_TRANSFORM(cfg1));
}

TEST_CASE("ml_preprocess with two features", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("ml_zscore_2", "zscore_two",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}}},
      {{"split_ratio", MetaDataOptionDefinition{0.7}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  auto out = t->TransformData(df);
  REQUIRE(out.num_cols() == 2);
}

TEST_CASE("ml_preprocess with many features", "[ml_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("ml_zscore_3", "zscore_multi",
      {{"feature_0", {input_ref("src", "signal_1")}},
       {"feature_1", {input_ref("src", "signal_2")}},
       {"feature_2", {input_ref("src", "noise")}}},
      {{"split_ratio", MetaDataOptionDefinition{0.7}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  auto out = t->TransformData(df);
  REQUIRE(out.num_cols() == 3);
}
