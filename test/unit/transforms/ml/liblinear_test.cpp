//
// LIBLINEAR Transform Tests
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

TEST_CASE("Logistic L2 classifier basic functionality", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("logistic_l2", "log_l2_test",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility"),
                            input_ref("src", "noise")}},
       {"target", {input_ref("src", "target")}}},
      {{"C", MetaDataOptionDefinition{1.0}},
              {"min_training_samples", MetaDataOptionDefinition{100.0}},
       {"lookback_window", MetaDataOptionDefinition{0.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // prediction + probability + decision_value = 3 outputs
  REQUIRE(out.num_cols() == 3);

  // Verify predictions are 0 or 1
  auto pred_col = out[cfg.GetOutputId("prediction").GetColumnName()]
                      .contiguous_array()
                      .to_vector<int64_t>();
  for (auto p : pred_col) {
    REQUIRE((p == 0 || p == 1));
  }

  // Verify probabilities are in [0, 1]
  auto prob_col = out[cfg.GetOutputId("probability").GetColumnName()]
                      .contiguous_array()
                      .to_vector<double>();
  for (auto p : prob_col) {
    REQUIRE(p >= 0.0);
    REQUIRE(p <= 1.0);
  }

  // Verify decision values are finite
  auto dv_col = out[cfg.GetOutputId("decision_value").GetColumnName()]
                    .contiguous_array()
                    .to_vector<double>();
  for (auto dv : dv_col) {
    REQUIRE(std::isfinite(dv));
  }
}

TEST_CASE("Logistic L1 classifier basic functionality", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  auto cfg = run_op("logistic_l1", "log_l1_test",
      {{epoch_script::ARG, {input_ref("src", "momentum"),
                            input_ref("src", "volatility")}},
       {"target", {input_ref("src", "target")}}},
      {{"C", MetaDataOptionDefinition{1.0}},
       {"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() == df.num_rows());
  REQUIRE(out.num_cols() == 3); // prediction + probability + decision_value

  // Verify predictions are valid
  auto pred_col = out[cfg.GetOutputId("prediction").GetColumnName()]
                      .contiguous_array()
                      .to_vector<int64_t>();
  for (auto p : pred_col) {
    REQUIRE((p == 0 || p == 1));
  }
}

TEST_CASE("Logistic L2 with different C values", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");

  for (double C : {0.1, 1.0, 10.0}) {
    DYNAMIC_SECTION("C=" << C) {
      auto cfg = run_op("logistic_l2", "log_l2_C",
          {{epoch_script::ARG, {input_ref("src", "momentum"),
                                input_ref("src", "volatility")}},
           {"target", {input_ref("src", "target")}}},
          {{"C", MetaDataOptionDefinition{C}},
           {"min_training_samples", MetaDataOptionDefinition{100.0}}},
          tf);

      auto tbase = MAKE_TRANSFORM(cfg);
      auto t = dynamic_cast<ITransform *>(tbase.get());
      REQUIRE(t != nullptr);

      auto out = t->TransformData(df);
      REQUIRE(out.num_rows() == df.num_rows());
    }
  }
}

TEST_CASE("SVR L2 regressor basic functionality", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("svr_l2", "svr_l2_test",
      {{epoch_script::ARG, {input_ref("src", "signal_1"),
                            input_ref("src", "signal_2"),
                            input_ref("src", "noise")}},
       {"target", {input_ref("src", "target")}}},
      {{"C", MetaDataOptionDefinition{1.0}},
       {"eps", MetaDataOptionDefinition{0.1}},
              {"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);

  // SVR only outputs prediction
  REQUIRE(out.num_cols() == 1);

  // Verify predictions are finite
  auto pred_col = out[cfg.GetOutputId("prediction").GetColumnName()]
                      .contiguous_array()
                      .to_vector<double>();
  for (auto p : pred_col) {
    REQUIRE(std::isfinite(p));
  }
}

TEST_CASE("SVR L1 regressor basic functionality", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");

  auto cfg = run_op("svr_l1", "svr_l1_test",
      {{epoch_script::ARG, {input_ref("src", "signal_1"),
                            input_ref("src", "signal_2")}},
       {"target", {input_ref("src", "target")}}},
      {{"C", MetaDataOptionDefinition{1.0}},
       {"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());
  REQUIRE(t != nullptr);

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() == df.num_rows());
  REQUIRE(out.num_cols() == 1);

  // Verify predictions are finite
  auto pred_col = out[cfg.GetOutputId("prediction").GetColumnName()]
                      .contiguous_array()
                      .to_vector<double>();
  for (auto p : pred_col) {
    REQUIRE(std::isfinite(p));
  }
}

TEST_CASE("LIBLINEAR logistic with lookback window", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");
  REQUIRE(df.num_rows() >= 400);

  auto cfg = run_op("logistic_l2", "log_lb",
      {{epoch_script::ARG, {input_ref("src", "momentum")}},
       {"target", {input_ref("src", "target")}}},
      {{"lookback_window", MetaDataOptionDefinition{300.0}},
       {"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() == df.num_rows() - 300);

  // Verify predictions are still valid
  auto pred_col = out[cfg.GetOutputId("prediction").GetColumnName()]
                      .contiguous_array()
                      .to_vector<int64_t>();
  for (auto p : pred_col) {
    REQUIRE((p == 0 || p == 1));
  }
}

TEST_CASE("LIBLINEAR SVR with lookback window", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");
  REQUIRE(df.num_rows() >= 400);

  auto cfg = run_op("svr_l2", "svr_lb",
      {{epoch_script::ARG, {input_ref("src", "signal_1")}},
       {"target", {input_ref("src", "target")}}},
      {{"lookback_window", MetaDataOptionDefinition{300.0}},
       {"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  auto out = t->TransformData(df);
  REQUIRE(out.num_rows() == df.num_rows() - 300);
}

TEST_CASE("LIBLINEAR logistic insufficient samples throws", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("classification_input.csv");
  df = df.head(50);

  auto cfg = run_op("logistic_l2", "log_small",
      {{epoch_script::ARG, {input_ref("src", "momentum")}},
       {"target", {input_ref("src", "target")}}},
      {{"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  REQUIRE_THROWS(t->TransformData(df));
}

TEST_CASE("LIBLINEAR SVR insufficient samples throws", "[liblinear]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto df = read_ml_input("regression_input.csv");
  df = df.head(50);

  auto cfg = run_op("svr_l2", "svr_small",
      {{epoch_script::ARG, {input_ref("src", "signal_1")}},
       {"target", {input_ref("src", "target")}}},
      {{"min_training_samples", MetaDataOptionDefinition{100.0}}},
      tf);

  auto tbase = MAKE_TRANSFORM(cfg);
  auto t = dynamic_cast<ITransform *>(tbase.get());

  REQUIRE_THROWS(t->TransformData(df));
}
