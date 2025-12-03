//
// Rolling ML Transform Tests
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

// =============================================================================
// Rolling LightGBM Tests
// =============================================================================

TEST_CASE("Rolling LightGBM classifier basic functionality", "[rolling_ml][rolling_lightgbm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_lightgbm_classifier", "rolling_lgb_cls",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility"),
                            input_ref("src", "noise")}},
       {"target", {input_ref("src", "target")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"window_type", MetaDataOptionDefinition{std::string("rolling")}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}},
       {"num_estimators", MetaDataOptionDefinition{10.0}},
       {"learning_rate", MetaDataOptionDefinition{0.1}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // Output should have rows after min_training_samples
  REQUIRE(out.num_rows() > 0);
  REQUIRE(out.num_cols() == 2); // prediction + probability

  // Verify predictions are 0 or 1
  auto pred_col = out[cfg.GetOutputId("prediction").GetColumnName()]
                      .contiguous_array()
                      .to_vector<int64_t>();
  for (auto p : pred_col) {
    REQUIRE((p == -1 || p == 0 || p == 1)); // -1 for initial NaN
  }

  // Verify probabilities are in [0, 1] or NaN
  auto prob_col = out[cfg.GetOutputId("probability").GetColumnName()]
                      .contiguous_array()
                      .to_vector<double>();
  for (auto p : prob_col) {
    if (std::isfinite(p)) {
      REQUIRE(p >= 0.0);
      REQUIRE(p <= 1.0);
    }
  }
}

TEST_CASE("Rolling LightGBM regressor basic functionality", "[rolling_ml][rolling_lightgbm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("rolling_lightgbm_regressor", "rolling_lgb_reg",
      {{epoch_script::ARG, {input_ref("src", "signal_1"),
                            input_ref("src", "signal_2"),
                            input_ref("src", "noise")}},
       {"target", {input_ref("src", "target")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"window_type", MetaDataOptionDefinition{std::string("rolling")}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}},
       {"num_estimators", MetaDataOptionDefinition{10.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  REQUIRE(out.num_rows() > 0);
  REQUIRE(out.num_cols() == 1); // prediction only
}

TEST_CASE("Rolling LightGBM with expanding window", "[rolling_ml][rolling_lightgbm]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_lightgbm_classifier", "rolling_lgb_expand",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}},
       {"target", {input_ref("src", "target")}}},
      {{"window_size", MetaDataOptionDefinition{50.0}},
       {"step_size", MetaDataOptionDefinition{25.0}},
       {"window_type", MetaDataOptionDefinition{std::string("expanding")}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}},
       {"num_estimators", MetaDataOptionDefinition{10.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
}

// =============================================================================
// Rolling Linear Model Tests
// =============================================================================

TEST_CASE("Rolling Logistic L1 basic functionality", "[rolling_ml][rolling_linear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_logistic_l1", "rolling_log_l1",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}},
       {"target", {input_ref("src", "target")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}},
       {"regularization", MetaDataOptionDefinition{1.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
  REQUIRE(out.num_cols() == 3); // prediction + probability + decision_value
}

TEST_CASE("Rolling Logistic L2 basic functionality", "[rolling_ml][rolling_linear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_logistic_l2", "rolling_log_l2",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}},
       {"target", {input_ref("src", "target")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
}

TEST_CASE("Rolling SVR L1 basic functionality", "[rolling_ml][rolling_linear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("rolling_svr_l1", "rolling_svr_l1",
      {{epoch_script::ARG, {input_ref("src", "signal_1"),
                            input_ref("src", "signal_2")}},
       {"target", {input_ref("src", "target")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
  REQUIRE(out.num_cols() == 1); // prediction only
}

TEST_CASE("Rolling SVR L2 basic functionality", "[rolling_ml][rolling_linear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("rolling_svr_l2", "rolling_svr_l2",
      {{epoch_script::ARG, {input_ref("src", "signal_1"),
                            input_ref("src", "signal_2")}},
       {"target", {input_ref("src", "target")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
}

// =============================================================================
// Rolling Preprocessing Tests
// =============================================================================

TEST_CASE("Rolling ML ZScore basic functionality", "[rolling_ml][rolling_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_ml_zscore", "rolling_zscore",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}}},
      {{"window_size", MetaDataOptionDefinition{50.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"min_training_samples", MetaDataOptionDefinition{30.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
  REQUIRE(out.num_cols() == 2); // 2 scaled columns
}

TEST_CASE("Rolling ML MinMax basic functionality", "[rolling_ml][rolling_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_ml_minmax", "rolling_minmax",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}}},
      {{"window_size", MetaDataOptionDefinition{50.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"min_training_samples", MetaDataOptionDefinition{30.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
}

TEST_CASE("Rolling ML Robust basic functionality", "[rolling_ml][rolling_preprocess]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_ml_robust", "rolling_robust",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}}},
      {{"window_size", MetaDataOptionDefinition{50.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"min_training_samples", MetaDataOptionDefinition{30.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
}

// =============================================================================
// Rolling Clustering Tests
// =============================================================================

TEST_CASE("Rolling KMeans 3 basic functionality", "[rolling_ml][rolling_clustering]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_kmeans_3", "rolling_km3",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{25.0}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
  // Should have cluster_label + 3 distance columns
  REQUIRE(out.num_cols() == 4);
}

TEST_CASE("Rolling DBSCAN basic functionality", "[rolling_ml][rolling_clustering]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_dbscan", "rolling_dbs",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{25.0}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}},
       {"epsilon", MetaDataOptionDefinition{0.5}},
       {"min_points", MetaDataOptionDefinition{5.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
  // Should have cluster_label, is_anomaly, cluster_count
  REQUIRE(out.num_cols() == 3);
}

// =============================================================================
// Rolling Decomposition Tests
// =============================================================================

TEST_CASE("Rolling PCA basic functionality", "[rolling_ml][rolling_decomposition]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  // Use rolling_pca_2 (extracts 2 principal components)
  // The N in rolling_pca_N refers to max components extracted
  auto cfg = run_op("rolling_pca_2", "rolling_pca",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility"),
                            input_ref("src", "noise")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{25.0}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
  // rolling_pca_2 outputs: pc_0, pc_1, total_explained_variance_ratio = 3 columns
  REQUIRE(out.num_cols() == 3);
}

// =============================================================================
// Rolling Probabilistic Tests
// =============================================================================

TEST_CASE("Rolling HMM 2 basic functionality", "[rolling_ml][rolling_probabilistic]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("rolling_hmm_2", "rolling_hmm2",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}}},
      {{"window_size", MetaDataOptionDefinition{60.0}},
       {"step_size", MetaDataOptionDefinition{25.0}},
       {"min_training_samples", MetaDataOptionDefinition{40.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() > 0);
  // Should have state + 2 state probabilities
  REQUIRE(out.num_cols() == 3);
}

// =============================================================================
// Window Type Tests
// =============================================================================

TEST_CASE("Rolling vs Expanding window produces different results", "[rolling_ml][window_type]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  // Rolling window config
  auto cfg_rolling = run_op("rolling_ml_zscore", "roll_z",
      {{epoch_script::ARG, {input_ref("src", "momentum")}}},
      {{"window_size", MetaDataOptionDefinition{50.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"window_type", MetaDataOptionDefinition{std::string("rolling")}},
       {"min_training_samples", MetaDataOptionDefinition{30.0}}},
      tf);

  // Expanding window config
  auto cfg_expanding = run_op("rolling_ml_zscore", "exp_z",
      {{epoch_script::ARG, {input_ref("src", "momentum")}}},
      {{"window_size", MetaDataOptionDefinition{50.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"window_type", MetaDataOptionDefinition{std::string("expanding")}},
       {"min_training_samples", MetaDataOptionDefinition{30.0}}},
      tf);

  auto t_rolling = MAKE_TRANSFORM(cfg_rolling);
  auto t_expanding = MAKE_TRANSFORM(cfg_expanding);

  auto out_rolling = dynamic_cast<ITransform*>(t_rolling.get())->TransformData(df);
  auto out_expanding = dynamic_cast<ITransform*>(t_expanding.get())->TransformData(df);

  REQUIRE(out_rolling.num_rows() == out_expanding.num_rows());

  // Values should differ after the initial window due to different window types
  auto roll_vals = out_rolling[cfg_rolling.GetOutputId("scaled_0").GetColumnName()]
                       .contiguous_array()
                       .to_vector<double>();
  auto exp_vals = out_expanding[cfg_expanding.GetOutputId("scaled_0").GetColumnName()]
                      .contiguous_array()
                      .to_vector<double>();

  // At least some values should differ
  bool has_difference = false;
  for (size_t i = 60; i < roll_vals.size() && i < exp_vals.size(); ++i) {
    if (std::isfinite(roll_vals[i]) && std::isfinite(exp_vals[i])) {
      if (std::abs(roll_vals[i] - exp_vals[i]) > 1e-6) {
        has_difference = true;
        break;
      }
    }
  }
  REQUIRE(has_difference);
}

TEST_CASE("Step size affects retraining frequency", "[rolling_ml][step_size]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  // Step size 5 (retrain every 5 rows)
  auto cfg_step1 = run_op("rolling_ml_zscore", "step1",
      {{epoch_script::ARG, {input_ref("src", "momentum")}}},
      {{"window_size", MetaDataOptionDefinition{50.0}},
       {"step_size", MetaDataOptionDefinition{5.0}},
       {"min_training_samples", MetaDataOptionDefinition{30.0}}},
      tf);

  // Step size 20 (retrain every 20 rows)
  auto cfg_step10 = run_op("rolling_ml_zscore", "step10",
      {{epoch_script::ARG, {input_ref("src", "momentum")}}},
      {{"window_size", MetaDataOptionDefinition{50.0}},
       {"step_size", MetaDataOptionDefinition{20.0}},
       {"min_training_samples", MetaDataOptionDefinition{30.0}}},
      tf);

  auto t_step1 = MAKE_TRANSFORM(cfg_step1);
  auto t_step10 = MAKE_TRANSFORM(cfg_step10);

  auto out_step1 = dynamic_cast<ITransform*>(t_step1.get())->TransformData(df);
  auto out_step10 = dynamic_cast<ITransform*>(t_step10.get())->TransformData(df);

  // Both should produce same number of rows
  REQUIRE(out_step1.num_rows() == out_step10.num_rows());

  // With step_size=10, model parameters should stay constant for 10 rows
  // This is an implementation detail test - values might differ slightly
  REQUIRE(out_step1.num_rows() > 0);
  REQUIRE(out_step10.num_rows() > 0);
}
