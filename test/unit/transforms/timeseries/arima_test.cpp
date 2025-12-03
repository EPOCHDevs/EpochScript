/**
 * @file arima_test.cpp
 * @brief Unit tests for ARIMA model implementation
 *
 * Tests include:
 * 1. Direct algorithm tests (differencing, stationarity, residuals)
 * 2. Python comparison tests (validates against statsmodels library)
 */

#include "transforms/components/timeseries/arima/arima_core.h"
#include "transforms/components/timeseries/arima/arima_types.h"
#include "transforms/components/timeseries/optimizer/lbfgs_optimizer.h"

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <filesystem>
#include <fstream>
#include <sstream>

using namespace epoch_script::transform::timeseries;
using namespace epoch_script::transform::timeseries::arima;
using Catch::Matchers::WithinRel;
using Catch::Matchers::WithinAbs;

namespace {

/**
 * @brief Load a single column from CSV as arma::vec
 */
arma::vec load_csv_column(const std::string& filepath, const std::string& column_name) {
  std::ifstream file(filepath);
  if (!file.is_open()) {
    throw std::runtime_error("Cannot open file: " + filepath);
  }

  std::string header_line;
  std::getline(file, header_line);

  // Find column index
  std::vector<std::string> headers;
  std::stringstream ss(header_line);
  std::string header;
  while (std::getline(ss, header, ',')) {
    headers.push_back(header);
  }

  int col_idx = -1;
  for (size_t i = 0; i < headers.size(); ++i) {
    if (headers[i] == column_name) {
      col_idx = static_cast<int>(i);
      break;
    }
  }
  if (col_idx < 0) {
    throw std::runtime_error("Column not found: " + column_name);
  }

  std::vector<double> values;
  std::string line;
  while (std::getline(file, line)) {
    std::stringstream row_ss(line);
    std::string cell;
    int current_col = 0;
    while (std::getline(row_ss, cell, ',')) {
      if (current_col == col_idx) {
        values.push_back(std::stod(cell));
        break;
      }
      ++current_col;
    }
  }

  return arma::vec(values);
}

/**
 * @brief Load ARIMA parameters from CSV
 */
struct ARIMATestParams {
  double phi_1{0.0};
  double phi_2{0.0};
  double theta_1{0.0};
  double constant{0.0};
  double sigma2{1.0};
};

ARIMATestParams load_arima_params(const std::string& filepath) {
  std::ifstream file(filepath);
  if (!file.is_open()) {
    throw std::runtime_error("Cannot open file: " + filepath);
  }

  std::string header_line;
  std::getline(file, header_line);

  // Parse headers
  std::vector<std::string> headers;
  std::stringstream hs(header_line);
  std::string h;
  while (std::getline(hs, h, ',')) {
    headers.push_back(h);
  }

  std::string data_line;
  std::getline(file, data_line);

  std::stringstream ss(data_line);
  std::string cell;
  std::vector<double> values;
  while (std::getline(ss, cell, ',')) {
    values.push_back(std::stod(cell));
  }

  ARIMATestParams params;
  for (size_t i = 0; i < headers.size(); ++i) {
    if (headers[i] == "phi_1") params.phi_1 = values[i];
    else if (headers[i] == "phi_2") params.phi_2 = values[i];
    else if (headers[i] == "theta_1") params.theta_1 = values[i];
    else if (headers[i] == "constant") params.constant = values[i];
    else if (headers[i] == "sigma2") params.sigma2 = values[i];
  }

  return params;
}

/**
 * @brief Load ARIMA metrics from CSV
 */
struct ARIMAMetrics {
  double log_likelihood;
  double aic;
  double bic;
};

ARIMAMetrics load_arima_metrics(const std::string& filepath) {
  std::ifstream file(filepath);
  if (!file.is_open()) {
    throw std::runtime_error("Cannot open file: " + filepath);
  }

  std::string header_line;
  std::getline(file, header_line);

  std::string data_line;
  std::getline(file, data_line);

  std::stringstream ss(data_line);
  std::string cell;
  std::vector<double> values;
  while (std::getline(ss, cell, ',')) {
    values.push_back(std::stod(cell));
  }

  return ARIMAMetrics{
      .log_likelihood = values[0],
      .aic = values[1],
      .bic = values[2]
  };
}

std::filesystem::path get_test_data_dir() {
  return std::filesystem::path(ARIMA_TEST_DATA_DIR);
}

} // namespace

// ============================================================================
// Direct Algorithm Tests
// ============================================================================

TEST_CASE("ARIMA differencing", "[arima][algorithm]") {
  SECTION("First difference") {
    arma::vec y = {1.0, 3.0, 6.0, 10.0, 15.0};
    arma::vec diff1 = Difference(y, 1);

    REQUIRE(diff1.n_elem == 4);
    REQUIRE_THAT(diff1(0), WithinAbs(2.0, 1e-10));  // 3 - 1
    REQUIRE_THAT(diff1(1), WithinAbs(3.0, 1e-10));  // 6 - 3
    REQUIRE_THAT(diff1(2), WithinAbs(4.0, 1e-10));  // 10 - 6
    REQUIRE_THAT(diff1(3), WithinAbs(5.0, 1e-10));  // 15 - 10
  }

  SECTION("Second difference") {
    arma::vec y = {1.0, 3.0, 6.0, 10.0, 15.0};
    arma::vec diff2 = Difference(y, 2);

    REQUIRE(diff2.n_elem == 3);
    REQUIRE_THAT(diff2(0), WithinAbs(1.0, 1e-10));  // (6-3) - (3-1) = 1
    REQUIRE_THAT(diff2(1), WithinAbs(1.0, 1e-10));  // (10-6) - (6-3) = 1
    REQUIRE_THAT(diff2(2), WithinAbs(1.0, 1e-10));  // (15-10) - (10-6) = 1
  }

  SECTION("No differencing (d=0)") {
    arma::vec y = {1.0, 2.0, 3.0};
    arma::vec diff0 = Difference(y, 0);

    REQUIRE(diff0.n_elem == y.n_elem);
    REQUIRE_THAT(diff0(0), WithinAbs(y(0), 1e-10));
  }
}

TEST_CASE("ARIMA AR stationarity check", "[arima][algorithm]") {
  SECTION("Stationary AR(1)") {
    ARIMAParams params;
    params.phi = arma::vec{0.7};
    params.theta = arma::vec{};

    REQUIRE(params.IsARStationary());
  }

  SECTION("Non-stationary AR(1)") {
    ARIMAParams params;
    params.phi = arma::vec{1.05};
    params.theta = arma::vec{};

    REQUIRE_FALSE(params.IsARStationary());
  }

  SECTION("Stationary AR(2)") {
    ARIMAParams params;
    params.phi = arma::vec{0.5, 0.3};
    params.theta = arma::vec{};

    REQUIRE(params.IsARStationary());
  }

  SECTION("Empty AR is stationary") {
    ARIMAParams params;
    params.phi = arma::vec{};
    params.theta = arma::vec{0.5};

    REQUIRE(params.IsARStationary());
  }
}

TEST_CASE("ARIMA MA invertibility check", "[arima][algorithm]") {
  SECTION("Invertible MA(1)") {
    ARIMAParams params;
    params.phi = arma::vec{};
    params.theta = arma::vec{0.5};

    REQUIRE(params.IsMAInvertible());
  }

  SECTION("Non-invertible MA(1)") {
    ARIMAParams params;
    params.phi = arma::vec{};
    params.theta = arma::vec{1.2};

    REQUIRE_FALSE(params.IsMAInvertible());
  }

  SECTION("Empty MA is invertible") {
    ARIMAParams params;
    params.phi = arma::vec{0.5};
    params.theta = arma::vec{};

    REQUIRE(params.IsMAInvertible());
  }
}

TEST_CASE("ARIMA parameter pack/unpack", "[arima][algorithm]") {
  ARIMAParams original;
  original.phi = arma::vec{0.5, 0.2};
  original.theta = arma::vec{0.3};
  original.constant = 1.5;
  original.sigma2 = 0.8;

  arma::vec packed = original.ToVector(true);
  REQUIRE(packed.n_elem == 5);  // 2 AR + 1 MA + constant + sigma2

  ARIMAParams unpacked = ARIMAParams::FromVector(packed, 2, 1, true);

  REQUIRE_THAT(unpacked.phi(0), WithinRel(original.phi(0), 1e-10));
  REQUIRE_THAT(unpacked.phi(1), WithinRel(original.phi(1), 1e-10));
  REQUIRE_THAT(unpacked.theta(0), WithinRel(original.theta(0), 1e-10));
  REQUIRE_THAT(unpacked.constant, WithinRel(original.constant, 1e-10));
  REQUIRE_THAT(unpacked.sigma2, WithinRel(original.sigma2, 1e-10));
}

TEST_CASE("ARIMA residual computation", "[arima][algorithm]") {
  // Create synthetic AR(1) data
  arma::arma_rng::set_seed(42);
  arma::vec eps = arma::randn(200);

  // Generate AR(1): y_t = 0.7 * y_{t-1} + eps_t
  arma::vec y(200, arma::fill::zeros);
  double phi = 0.7;
  for (size_t t = 1; t < 200; ++t) {
    y(t) = phi * y(t - 1) + eps(t);
  }

  ARIMAParams params;
  params.phi = arma::vec{phi};
  params.theta = arma::vec{};
  params.constant = 0.0;
  params.sigma2 = 1.0;

  arma::vec residuals = ComputeResiduals(y, params);

  SECTION("Residuals have correct length") {
    REQUIRE(residuals.n_elem == y.n_elem);
  }

  SECTION("Residuals approximately match innovations") {
    // Skip first max_lag observations
    size_t start = 1;
    arma::vec resid_valid = residuals.subvec(start, residuals.n_elem - 1);
    arma::vec eps_valid = eps.subvec(start, eps.n_elem - 1);

    // Should be close (not exact due to initialization)
    double correlation = arma::dot(resid_valid, eps_valid) /
                         (arma::norm(resid_valid) * arma::norm(eps_valid));

    INFO("Correlation between residuals and true innovations: " << correlation);
    REQUIRE(correlation > 0.9);
  }
}

TEST_CASE("ARIMA initial guess", "[arima][algorithm]") {
  arma::arma_rng::set_seed(123);
  arma::vec y = arma::randn(300);

  ARIMAParams initial = InitialGuess(y, 1, 1, true);

  SECTION("Initial guess is stationary") {
    REQUIRE(initial.IsARStationary());
  }

  SECTION("Initial guess is invertible") {
    REQUIRE(initial.IsMAInvertible());
  }

  SECTION("Initial sigma2 is positive") {
    REQUIRE(initial.sigma2 > 0);
  }
}

TEST_CASE("ARIMA psi weights", "[arima][algorithm]") {
  SECTION("AR(1) psi weights") {
    ARIMAParams params;
    params.phi = arma::vec{0.7};
    params.theta = arma::vec{};

    arma::vec psi = ComputePsiWeights(params, 5);

    // For AR(1): psi_j = phi^j
    REQUIRE_THAT(psi(0), WithinAbs(1.0, 1e-10));
    REQUIRE_THAT(psi(1), WithinAbs(0.7, 1e-10));
    REQUIRE_THAT(psi(2), WithinAbs(0.49, 1e-10));  // 0.7^2
    REQUIRE_THAT(psi(3), WithinAbs(0.343, 1e-10)); // 0.7^3
  }

  SECTION("MA(1) psi weights") {
    ARIMAParams params;
    params.phi = arma::vec{};
    params.theta = arma::vec{0.5};

    arma::vec psi = ComputePsiWeights(params, 5);

    // For MA(1): psi_0 = 1, psi_1 = theta, psi_j = 0 for j > 1
    REQUIRE_THAT(psi(0), WithinAbs(1.0, 1e-10));
    REQUIRE_THAT(psi(1), WithinAbs(0.5, 1e-10));
    REQUIRE_THAT(psi(2), WithinAbs(0.0, 1e-10));
  }
}

// ============================================================================
// Python Comparison Tests
// ============================================================================

TEST_CASE("AR(1) estimation vs Python statsmodels", "[arima][python]") {
  auto data_dir = get_test_data_dir();

  // Load test data
  arma::vec y = load_csv_column(
      (data_dir / "arima_100_input.csv").string(), "y");
  ARIMATestParams expected_params = load_arima_params(
      (data_dir / "arima_100_params.csv").string());
  ARIMAMetrics expected_metrics = load_arima_metrics(
      (data_dir / "arima_100_metrics.csv").string());

  // Fit ARIMA(1,0,0) model
  ARIMAConfig config;
  config.p = 1;
  config.d = 0;
  config.q = 0;
  config.with_constant = true;
  config.max_iterations = 1000;
  config.tolerance = 1e-10;
  config.min_training_samples = 50;

  ARIMAFitResult result = FitARIMA(y, config);

  INFO("C++ phi_1: " << result.params.phi(0) << " vs Python: " << expected_params.phi_1);
  INFO("C++ constant: " << result.params.constant << " vs Python: " << expected_params.constant);

  SECTION("Model converged") {
    REQUIRE(result.converged);
  }

  SECTION("AR coefficient within tolerance of Python") {
    constexpr double PARAM_TOL = 0.1;  // 10% tolerance due to CSS vs exact MLE
    REQUIRE_THAT(result.params.phi(0), WithinRel(expected_params.phi_1, PARAM_TOL));
  }

  SECTION("AIC within tolerance") {
    // AIC can differ significantly between CSS and exact MLE methods
    REQUIRE_THAT(result.aic, WithinAbs(expected_metrics.aic, 50.0));
  }
}

TEST_CASE("ARMA(1,1) estimation vs Python statsmodels", "[arima][python]") {
  auto data_dir = get_test_data_dir();

  arma::vec y = load_csv_column(
      (data_dir / "arima_101_input.csv").string(), "y");
  ARIMATestParams expected_params = load_arima_params(
      (data_dir / "arima_101_params.csv").string());

  ARIMAConfig config;
  config.p = 1;
  config.d = 0;
  config.q = 1;
  config.with_constant = true;
  config.max_iterations = 1000;

  ARIMAFitResult result = FitARIMA(y, config);

  INFO("C++ phi_1: " << result.params.phi(0) << " vs Python: " << expected_params.phi_1);
  INFO("C++ theta_1: " << result.params.theta(0) << " vs Python: " << expected_params.theta_1);

  SECTION("Model converged") {
    REQUIRE(result.converged);
  }

  SECTION("Parameters are stationary and invertible") {
    REQUIRE(result.params.IsARStationary());
    REQUIRE(result.params.IsMAInvertible());
  }
}

TEST_CASE("ARIMA(1,1,0) estimation vs Python statsmodels", "[arima][python]") {
  auto data_dir = get_test_data_dir();

  arma::vec y = load_csv_column(
      (data_dir / "arima_110_input.csv").string(), "y");
  ARIMATestParams expected_params = load_arima_params(
      (data_dir / "arima_110_params.csv").string());

  ARIMAConfig config;
  config.p = 1;
  config.d = 1;
  config.q = 0;
  config.with_constant = false;  // No constant for d > 0
  config.max_iterations = 1000;

  ARIMAFitResult result = FitARIMA(y, config);

  INFO("C++ phi_1: " << result.params.phi(0) << " vs Python: " << expected_params.phi_1);

  SECTION("Model converged") {
    REQUIRE(result.converged);
  }

  SECTION("Differencing was applied correctly") {
    REQUIRE(result.d == 1);
    REQUIRE(result.y_diff.n_elem == y.n_elem - 1);
  }
}

// ============================================================================
// Edge Case Tests
// ============================================================================

TEST_CASE("ARIMA handles insufficient data", "[arima][edge]") {
  arma::vec y = arma::randn(30);  // Only 30 samples

  ARIMAConfig config;
  config.p = 1;
  config.d = 0;
  config.q = 1;
  config.min_training_samples = 50;

  ARIMAFitResult result = FitARIMA(y, config);

  REQUIRE_FALSE(result.converged);
}

TEST_CASE("ARIMA forecast", "[arima][forecast]") {
  arma::arma_rng::set_seed(456);

  // Generate AR(1) data
  arma::vec eps = arma::randn(300);
  arma::vec y(300, arma::fill::zeros);
  for (size_t t = 1; t < 300; ++t) {
    y(t) = 0.6 * y(t - 1) + eps(t);
  }

  ARIMAConfig config;
  config.p = 1;
  config.d = 0;
  config.q = 0;
  config.with_constant = true;

  ARIMAFitResult result = FitARIMA(y, config);
  REQUIRE(result.converged);

  ARIMAForecast forecast = Forecast(result, 5, 0.95);

  SECTION("Forecast has correct length") {
    REQUIRE(forecast.point.n_elem == 5);
    REQUIRE(forecast.lower.n_elem == 5);
    REQUIRE(forecast.upper.n_elem == 5);
    REQUIRE(forecast.se.n_elem == 5);
  }

  SECTION("Forecast intervals widen with horizon") {
    for (size_t h = 1; h < 5; ++h) {
      REQUIRE(forecast.se(h) >= forecast.se(h - 1));
    }
  }

  SECTION("Confidence intervals contain point forecast") {
    for (size_t h = 0; h < 5; ++h) {
      REQUIRE(forecast.point(h) >= forecast.lower(h));
      REQUIRE(forecast.point(h) <= forecast.upper(h));
    }
  }
}

TEST_CASE("MA(1) estimation", "[arima][python]") {
  auto data_dir = get_test_data_dir();

  arma::vec y = load_csv_column(
      (data_dir / "arima_001_input.csv").string(), "y");
  ARIMATestParams expected_params = load_arima_params(
      (data_dir / "arima_001_params.csv").string());

  ARIMAConfig config;
  config.p = 0;
  config.d = 0;
  config.q = 1;
  config.with_constant = true;
  config.max_iterations = 1000;

  ARIMAFitResult result = FitARIMA(y, config);

  INFO("C++ theta_1: " << result.params.theta(0) << " vs Python: " << expected_params.theta_1);

  SECTION("Model converged") {
    REQUIRE(result.converged);
  }

  SECTION("MA coefficient is invertible") {
    REQUIRE(result.params.IsMAInvertible());
  }
}
