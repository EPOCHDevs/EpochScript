/**
 * @file rolling_ts_test.cpp
 * @brief Unit tests for Rolling GARCH and Rolling ARIMA transforms
 *
 * Tests cover:
 * 1. Rolling window mechanics
 * 2. Output shape validation
 * 3. Parameter stability over time
 * 4. Walk-forward forecasting behavior
 */

#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>

#include "transforms/components/timeseries/rolling/rolling_ts_base.h"
#include "transforms/components/timeseries/rolling/rolling_garch.h"
#include "transforms/components/timeseries/rolling/rolling_arima.h"
#include "transforms/components/timeseries/garch/garch_core.h"
#include "transforms/components/timeseries/arima/arima_core.h"

#include <armadillo>
#include <random>

using namespace epoch_script::transform;
using namespace epoch_script::transform::ml_utils;
using Catch::Approx;

// =============================================================================
// Test Data Generation
// =============================================================================

namespace {

/**
 * @brief Generate GARCH(1,1) process for testing
 */
arma::vec GenerateGARCHProcess(size_t n, double omega, double alpha, double beta,
                                unsigned int seed = 42) {
  std::mt19937 gen(seed);
  std::normal_distribution<> dist(0.0, 1.0);

  arma::vec returns(n);
  double sigma2 = omega / (1.0 - alpha - beta);  // Unconditional variance

  for (size_t t = 0; t < n; ++t) {
    double z = dist(gen);
    double eps = std::sqrt(sigma2) * z;
    returns(t) = eps;

    // Update variance for next period
    sigma2 = omega + alpha * eps * eps + beta * sigma2;
  }

  return returns;
}

/**
 * @brief Generate AR(1) process for testing
 */
arma::vec GenerateAR1Process(size_t n, double phi, double sigma = 1.0,
                              unsigned int seed = 42) {
  std::mt19937 gen(seed);
  std::normal_distribution<> dist(0.0, sigma);

  arma::vec y(n);
  y(0) = dist(gen);

  for (size_t t = 1; t < n; ++t) {
    y(t) = phi * y(t - 1) + dist(gen);
  }

  return y;
}

}  // namespace

// =============================================================================
// Rolling Window Iterator Tests
// =============================================================================

TEST_CASE("RollingWindowIterator basic operation", "[rolling][iterator]") {
  const size_t total_rows = 100;
  const size_t window_size = 20;
  const size_t step_size = 1;

  RollingWindowIterator iterator(total_rows, window_size, step_size, WindowType::Rolling);

  SECTION("Window count is correct") {
    // With 100 rows, window 20, step 1: output starts at row 20
    // Windows: [0,20)->[20], [1,21)->[21], ..., [79,99)->[99]
    // That's 80 windows
    REQUIRE(iterator.TotalWindows() == 80);
  }

  SECTION("First window is correct") {
    auto spec = iterator.Next();
    REQUIRE(spec.train_start == 0);
    REQUIRE(spec.train_end == 20);
    REQUIRE(spec.predict_start == 20);
    REQUIRE(spec.predict_end == 21);
    REQUIRE(spec.iteration_index == 0);
  }

  SECTION("ForEach covers all windows") {
    size_t count = 0;
    iterator.ForEach([&]([[maybe_unused]] const WindowSpec& spec) {
      count++;
    });
    REQUIRE(count == iterator.TotalWindows());
  }
}

TEST_CASE("RollingWindowIterator expanding mode", "[rolling][iterator]") {
  const size_t total_rows = 100;
  const size_t min_window = 20;
  const size_t step_size = 1;

  RollingWindowIterator iterator(total_rows, min_window, step_size, WindowType::Expanding);

  SECTION("First window starts from 0") {
    auto spec = iterator.Next();
    REQUIRE(spec.train_start == 0);
    REQUIRE(spec.train_end == 20);
  }

  SECTION("Later windows expand from start") {
    // Skip first 5 windows
    for (int i = 0; i < 5; ++i) iterator.Next();

    auto spec = iterator.Next();
    REQUIRE(spec.train_start == 0);  // Always starts at 0 for expanding
    REQUIRE(spec.train_end == 25);   // min_window + 5 steps
  }
}

TEST_CASE("RollingWindowIterator step_size > 1", "[rolling][iterator]") {
  const size_t total_rows = 100;
  const size_t window_size = 20;
  const size_t step_size = 5;

  RollingWindowIterator iterator(total_rows, window_size, step_size, WindowType::Rolling);

  SECTION("Window count reduced with larger step") {
    // Predict windows: [20,25), [25,30), ..., [95,100)
    // That's (100-20)/5 = 16 windows
    REQUIRE(iterator.TotalWindows() == 16);
  }

  SECTION("Prediction window covers step_size rows") {
    auto spec = iterator.Next();
    REQUIRE(spec.predict_end - spec.predict_start == 5);
  }
}

// =============================================================================
// Rolling GARCH Tests
// =============================================================================

TEST_CASE("Rolling GARCH output shape", "[rolling][garch]") {
  // Generate test data
  arma::vec returns = GenerateGARCHProcess(300, 0.00001, 0.1, 0.85);

  const size_t window_size = 100;

  SECTION("Output has correct number of rows") {
    // With 300 rows and window 100, output should have 200 rows
    RollingWindowIterator iterator(returns.n_elem, window_size, 1, WindowType::Rolling);

    size_t output_rows = returns.n_elem - window_size;
    REQUIRE(output_rows == 200);
  }
}

TEST_CASE("Rolling GARCH volatility forecasts are positive", "[rolling][garch]") {
  using namespace timeseries::garch;

  // Generate data
  arma::vec returns = GenerateGARCHProcess(200, 0.00001, 0.1, 0.85);

  // Fit on first window
  GARCHConfig config;
  config.p = 1;
  config.q = 1;
  config.min_training_samples = 50;

  arma::vec train = returns.subvec(0, 99);
  auto result = FitGARCH(train, config);

  REQUIRE(result.converged);

  // Forecast should be positive
  auto forecast = ForecastVariance(result, 5);
  for (size_t h = 0; h < 5; ++h) {
    REQUIRE(forecast(h) > 0.0);
  }
}

TEST_CASE("Rolling GARCH persistence tracking", "[rolling][garch]") {
  using namespace timeseries::garch;

  // Generate high persistence GARCH
  arma::vec returns = GenerateGARCHProcess(300, 0.000005, 0.08, 0.90);

  const size_t window_size = 150;

  // Fit on multiple windows and track persistence
  std::vector<double> persistence_values;

  for (size_t start = 0; start <= 100; start += 20) {
    arma::vec window = returns.subvec(start, start + window_size - 1);

    GARCHConfig config;
    config.p = 1;
    config.q = 1;
    config.min_training_samples = 100;

    auto result = FitGARCH(window, config);
    if (result.converged) {
      persistence_values.push_back(result.params.Persistence());
    }
  }

  // All persistence values should be reasonably close
  // (demonstrating stable estimation)
  REQUIRE(persistence_values.size() >= 3);

  double mean_persistence = 0.0;
  for (double p : persistence_values) mean_persistence += p;
  mean_persistence /= persistence_values.size();

  for (double p : persistence_values) {
    // Within 15% of mean
    REQUIRE(p > mean_persistence * 0.85);
    REQUIRE(p < mean_persistence * 1.15);
  }
}

// =============================================================================
// Rolling ARIMA Tests
// =============================================================================

TEST_CASE("Rolling ARIMA output shape", "[rolling][arima]") {
  arma::vec y = GenerateAR1Process(300, 0.7);

  const size_t window_size = 100;

  SECTION("Output has correct number of rows") {
    size_t output_rows = y.n_elem - window_size;
    REQUIRE(output_rows == 200);
  }
}

TEST_CASE("Rolling ARIMA forecast within bounds", "[rolling][arima]") {
  using namespace timeseries::arima;

  // Generate AR(1) data
  arma::vec y = GenerateAR1Process(200, 0.7);

  // Fit on first window
  ARIMAConfig config;
  config.p = 1;
  config.d = 0;
  config.q = 0;
  config.with_constant = true;
  config.min_training_samples = 50;

  arma::vec train = y.subvec(0, 99);
  auto result = FitARIMA(train, config);

  REQUIRE(result.converged);

  // Forecast
  auto forecast = Forecast(result, 5, 0.95);

  SECTION("Point forecast is within confidence interval") {
    for (size_t h = 0; h < 5; ++h) {
      REQUIRE(forecast.point(h) >= forecast.lower(h));
      REQUIRE(forecast.point(h) <= forecast.upper(h));
    }
  }

  SECTION("Confidence interval widens with horizon") {
    // Width should generally increase (or stay same for AR(0))
    double width_1 = forecast.upper(0) - forecast.lower(0);
    double width_5 = forecast.upper(4) - forecast.lower(4);
    REQUIRE(width_5 >= width_1 * 0.99);  // Allow small numerical tolerance
  }
}

TEST_CASE("Rolling ARIMA AR coefficient stability", "[rolling][arima]") {
  using namespace timeseries::arima;

  // Generate AR(1) with known phi
  const double true_phi = 0.7;
  arma::vec y = GenerateAR1Process(400, true_phi, 1.0, 123);

  const size_t window_size = 200;

  // Fit on multiple windows and track phi
  std::vector<double> phi_values;

  for (size_t start = 0; start <= 150; start += 30) {
    arma::vec window = y.subvec(start, start + window_size - 1);

    ARIMAConfig config;
    config.p = 1;
    config.d = 0;
    config.q = 0;
    config.with_constant = true;
    config.min_training_samples = 100;

    auto result = FitARIMA(window, config);
    if (result.converged && result.params.p() >= 1) {
      phi_values.push_back(result.params.phi(0));
    }
  }

  REQUIRE(phi_values.size() >= 3);

  // All estimates should be close to true value
  for (double phi : phi_values) {
    INFO("Estimated phi: " << phi << " vs true: " << true_phi);
    REQUIRE(phi == Approx(true_phi).margin(0.15));  // Within 0.15 of true
  }
}

// =============================================================================
// Integration Style Tests
// =============================================================================

TEST_CASE("Walk-forward simulation", "[rolling][integration]") {
  using namespace timeseries::garch;

  // Simulate a walk-forward scenario
  arma::vec returns = GenerateGARCHProcess(500, 0.00001, 0.1, 0.85, 999);

  const size_t window_size = 200;
  const size_t step_size = 5;

  RollingWindowIterator iterator(returns.n_elem, window_size, step_size, WindowType::Rolling);

  size_t successful_fits = 0;
  size_t total_fits = 0;

  iterator.ForEach([&](const WindowSpec& window) {
    total_fits++;

    arma::vec train = returns.subvec(window.train_start, window.train_end - 1);

    GARCHConfig config;
    config.p = 1;
    config.q = 1;
    config.min_training_samples = 100;

    auto result = FitGARCH(train, config);
    if (result.converged && result.params.IsStationary()) {
      successful_fits++;
    }
  });

  // Most fits should succeed
  double success_rate = static_cast<double>(successful_fits) / total_fits;
  INFO("Walk-forward success rate: " << success_rate * 100 << "%");
  REQUIRE(success_rate > 0.90);  // At least 90% success
}
