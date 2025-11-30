#include <catch.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>

#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/scalar.h"

#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/core/constants.h>
#include <epoch_script/transforms/core/config_helper.h>

#include "transforms/components/hosseinmoein/statistics/frac_diff.h"
#include <catch2/catch_test_macros.hpp>

using namespace epoch_frame;
using namespace epoch_script::transform;
using namespace Catch;


namespace {

// Helper to load CSV test data
struct TestData {
  std::vector<double> input;
  std::vector<double> expected;
  double d;
  double threshold;
};

TestData loadTestData(const std::string& filepath) {
  TestData data;
  std::ifstream file(filepath);
  REQUIRE(file.is_open());

  std::string line;
  std::getline(file, line); // Skip header

  while (std::getline(file, line)) {
    std::stringstream ss(line);
    std::string val;

    std::getline(ss, val, ',');
    data.input.push_back(std::stod(val));

    std::getline(ss, val, ',');
    if (val.empty() || val == "nan" || val == "NaN") {
      data.expected.push_back(std::numeric_limits<double>::quiet_NaN());
    } else {
      data.expected.push_back(std::stod(val));
    }

    std::getline(ss, val, ',');
    data.d = std::stod(val);

    std::getline(ss, val, ',');
    data.threshold = std::stod(val);
  }

  return data;
}

// Helper to compare values with NaN handling
bool approxEqual(double a, double b, double tol = 1e-6) {
  if (std::isnan(a) && std::isnan(b)) return true;
  if (std::isnan(a) || std::isnan(b)) return false;
  return std::abs(a - b) < tol * std::max(1.0, std::abs(b));
}

} // namespace

TEST_CASE("FracDiff weight calculation", "[hosseinmoein][frac_diff]") {
  // Test weight calculation for d=0.5
  auto weights = frac_diff_detail::compute_ffd_weights(0.5, 1e-5);

  // First weight should be 1.0
  REQUIRE(weights[0] == Approx(1.0));

  // Second weight: -1 * (0.5 - 1 + 1) / 1 = -0.5
  REQUIRE(weights[1] == Approx(-0.5));

  // Third weight: -(-0.5) * (0.5 - 2 + 1) / 2 = 0.5 * (-0.5) / 2 = -0.125
  REQUIRE(weights[2] == Approx(-0.125));

  // Fourth weight: -(-0.125) * (0.5 - 3 + 1) / 3 = 0.125 * (-1.5) / 3 = -0.0625
  REQUIRE(weights[3] == Approx(-0.0625));
}

TEST_CASE("FracDiff d=1.0 should give two weights", "[hosseinmoein][frac_diff]") {
  // d=1 should only have weights [1, -1] since next weight is 0
  auto weights = frac_diff_detail::compute_ffd_weights(1.0, 1e-5);

  REQUIRE(weights.size() == 2);
  REQUIRE(weights[0] == Approx(1.0));
  REQUIRE(weights[1] == Approx(-1.0));
}

TEST_CASE("FracDiff basic transformation", "[hosseinmoein][frac_diff]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto cfg = run_op("frac_diff", "frac_diff_id",
      {{"SLOT", {input_ref("input")}}},
      {{"d", epoch_script::MetaDataOptionDefinition{0.5}},
       {"threshold", epoch_script::MetaDataOptionDefinition{1e-5}}},
      tf);

  // Simple test: linear series - needs enough data for window
  auto weights = frac_diff_detail::compute_ffd_weights(0.5, 1e-5);
  const size_t window = weights.size();
  const size_t N = window + 50;  // Enough data beyond window

  std::vector<int64_t> ticks(N);
  std::iota(ticks.begin(), ticks.end(), 0);
  auto idx_arr = factory::array::make_contiguous_array(ticks);
  auto index = factory::index::make_index(idx_arr, MonotonicDirection::Increasing, "i");

  std::vector<double> input(N);
  for (size_t i = 0; i < N; ++i) {
    input[i] = static_cast<double>(i + 1);
  }
  auto df = make_dataframe<double>(index, {input}, {"#input"});

  FracDiff fracdiff{cfg};
  auto out = fracdiff.TransformData(df);

  REQUIRE(out.contains(cfg.GetOutputId("result").GetColumnName()));

  const auto result_series = out[cfg.GetOutputId("result").GetColumnName()];

  // First (window-1) values should be null
  for (size_t i = 0; i < window - 1; ++i) {
    REQUIRE(result_series.iloc(static_cast<int64_t>(i)).is_null());
  }

  // After window fills, values should be valid (not null)
  for (size_t i = window - 1; i < N; ++i) {
    REQUIRE_FALSE(result_series.iloc(static_cast<int64_t>(i)).is_null());
  }
}

TEST_CASE("FracDiff d=1.0 approximates first difference", "[hosseinmoein][frac_diff]") {
  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;

  auto cfg = run_op("frac_diff", "frac_diff_id",
      {{"SLOT", {input_ref("input")}}},
      {{"d", epoch_script::MetaDataOptionDefinition{1.0}},
       {"threshold", epoch_script::MetaDataOptionDefinition{1e-5}}},
      tf);

  // d=1 has weights [1, -1], window=2
  auto weights = frac_diff_detail::compute_ffd_weights(1.0, 1e-5);
  REQUIRE(weights.size() == 2);  // window is 2 for d=1

  const size_t N = 50;
  std::vector<int64_t> ticks(N);
  std::iota(ticks.begin(), ticks.end(), 0);
  auto idx_arr = factory::array::make_contiguous_array(ticks);
  auto index = factory::index::make_index(idx_arr, MonotonicDirection::Increasing, "i");

  // Linear series: first diff should be constant (1.0)
  std::vector<double> input(N);
  for (size_t i = 0; i < N; ++i) {
    input[i] = static_cast<double>(i + 1);
  }
  auto df = make_dataframe<double>(index, {input}, {"#input"});

  FracDiff fracdiff{cfg};
  auto out = fracdiff.TransformData(df);

  const auto result_series = out[cfg.GetOutputId("result").GetColumnName()];

  // d=1 with weights [1, -1] gives: 1*X[t] + (-1)*X[t-1] = X[t] - X[t-1] = 1.0 for linear series
  // First value is null (not enough data), rest should be 1.0
  REQUIRE(result_series.iloc(0).is_null());
  for (size_t i = 1; i < N; ++i) {
    auto scalar = result_series.iloc(static_cast<int64_t>(i));
    REQUIRE_FALSE(scalar.is_null());
    REQUIRE(scalar.as_double() == Approx(1.0).epsilon(1e-10));
  }
}

TEST_CASE("FracDiff against Python reference - linear d=0.5", "[hosseinmoein][frac_diff][python]") {
  std::string data_path = std::string(FRAC_DIFF_TEST_DATA_DIR) + "/linear_d05.csv";

  if (!std::filesystem::exists(data_path)) {
    WARN("Test data not found: " << data_path);
    return;
  }

  auto testData = loadTestData(data_path);

  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  auto cfg = run_op("frac_diff", "frac_diff_id",
      {{"SLOT", {input_ref("input")}}},
      {{"d", epoch_script::MetaDataOptionDefinition{testData.d}},
       {"threshold", epoch_script::MetaDataOptionDefinition{testData.threshold}}},
      tf);

  const size_t N = testData.input.size();
  std::vector<int64_t> ticks(N);
  std::iota(ticks.begin(), ticks.end(), 0);
  auto idx_arr = factory::array::make_contiguous_array(ticks);
  auto index = factory::index::make_index(idx_arr, MonotonicDirection::Increasing, "i");
  auto df = make_dataframe<double>(index, {testData.input}, {"#input"});

  FracDiff fracdiff{cfg};
  auto out = fracdiff.TransformData(df);

  const auto result_series = out[cfg.GetOutputId("result").GetColumnName()];

  // Strict comparison - values must match exactly
  for (size_t i = 0; i < N; ++i) {
    auto scalar = result_series.iloc(static_cast<int64_t>(i));
    bool expected_null = std::isnan(testData.expected[i]);

    if (expected_null) {
      REQUIRE(scalar.is_null());
    } else {
      REQUIRE_FALSE(scalar.is_null());
      double actual = scalar.as_double();
      REQUIRE(approxEqual(actual, testData.expected[i], 1e-6));
    }
  }
}

TEST_CASE("FracDiff against Python reference - random walk d=0.5", "[hosseinmoein][frac_diff][python]") {
  std::string data_path = std::string(FRAC_DIFF_TEST_DATA_DIR) + "/random_walk_d05.csv";

  if (!std::filesystem::exists(data_path)) {
    WARN("Test data not found: " << data_path);
    return;
  }

  auto testData = loadTestData(data_path);

  const auto tf = epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
  auto cfg = run_op("frac_diff", "frac_diff_id",
      {{"SLOT", {input_ref("input")}}},
      {{"d", epoch_script::MetaDataOptionDefinition{testData.d}},
       {"threshold", epoch_script::MetaDataOptionDefinition{testData.threshold}}},
      tf);

  const size_t N = testData.input.size();
  std::vector<int64_t> ticks(N);
  std::iota(ticks.begin(), ticks.end(), 0);
  auto idx_arr = factory::array::make_contiguous_array(ticks);
  auto index = factory::index::make_index(idx_arr, MonotonicDirection::Increasing, "i");
  auto df = make_dataframe<double>(index, {testData.input}, {"#input"});

  FracDiff fracdiff{cfg};
  auto out = fracdiff.TransformData(df);

  const auto result_series = out[cfg.GetOutputId("result").GetColumnName()];

  // Strict comparison - values must match exactly
  for (size_t i = 0; i < N; ++i) {
    auto scalar = result_series.iloc(static_cast<int64_t>(i));
    bool expected_null = std::isnan(testData.expected[i]);

    if (expected_null) {
      REQUIRE(scalar.is_null());
    } else {
      REQUIRE_FALSE(scalar.is_null());
      double actual = scalar.as_double();
      REQUIRE(approxEqual(actual, testData.expected[i], 1e-6));
    }
  }
}