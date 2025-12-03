#pragma once
/**
 * @file arima_types.h
 * @brief ARIMA model types, parameter structures, and configurations
 *
 * ARIMA(p,d,q) model:
 *   (1 - phi_1*L - ... - phi_p*L^p)(1-L)^d * y_t = c + (1 + theta_1*L + ... + theta_q*L^q) * eps_t
 *
 * where L is the lag operator, eps_t ~ N(0, sigma^2)
 */

#include <armadillo>
#include <string>
#include <cmath>

namespace epoch_script::transform::timeseries::arima {

/**
 * @brief ARIMA(p,d,q) model parameters
 */
struct ARIMAParams {
  arma::vec phi;           ///< AR coefficients (p parameters)
  arma::vec theta;         ///< MA coefficients (q parameters)
  double constant{0.0};    ///< Constant/intercept term
  double sigma2{1.0};      ///< Innovation variance

  [[nodiscard]] size_t p() const { return phi.n_elem; }   ///< AR order
  [[nodiscard]] size_t q() const { return theta.n_elem; } ///< MA order

  /**
   * @brief Check AR stationarity: roots of AR polynomial outside unit circle
   *
   * For AR(1): |phi_1| < 1
   * For higher orders: check eigenvalues of companion matrix
   */
  [[nodiscard]] bool IsARStationary() const {
    if (phi.is_empty()) return true;

    // For AR(1), simple check
    if (phi.n_elem == 1) {
      return std::abs(phi(0)) < 1.0;
    }

    // For higher order: build companion matrix and check eigenvalues
    size_t n = phi.n_elem;
    arma::mat companion(n, n, arma::fill::zeros);

    // First row is AR coefficients
    companion.row(0) = phi.t();

    // Sub-diagonal is identity
    for (size_t i = 1; i < n; ++i) {
      companion(i, i - 1) = 1.0;
    }

    arma::cx_vec eigval = arma::eig_gen(companion);
    return arma::all(arma::abs(eigval) < 1.0);
  }

  /**
   * @brief Check MA invertibility: roots of MA polynomial outside unit circle
   */
  [[nodiscard]] bool IsMAInvertible() const {
    if (theta.is_empty()) return true;

    // For MA(1), simple check
    if (theta.n_elem == 1) {
      return std::abs(theta(0)) < 1.0;
    }

    // For higher order: build companion matrix
    size_t n = theta.n_elem;
    arma::mat companion(n, n, arma::fill::zeros);
    companion.row(0) = theta.t();
    for (size_t i = 1; i < n; ++i) {
      companion(i, i - 1) = 1.0;
    }

    arma::cx_vec eigval = arma::eig_gen(companion);
    return arma::all(arma::abs(eigval) < 1.0);
  }

  /**
   * @brief Pack parameters into optimization vector
   * @return Vector: [phi_1, ..., phi_p, theta_1, ..., theta_q, constant, sigma2]
   */
  [[nodiscard]] arma::vec ToVector(bool include_constant = true) const {
    size_t n = p() + q() + 1;  // +1 for sigma2
    if (include_constant) n += 1;

    arma::vec params(n);
    size_t idx = 0;

    // AR coefficients
    for (size_t i = 0; i < p(); ++i) {
      params(idx++) = phi(i);
    }

    // MA coefficients
    for (size_t i = 0; i < q(); ++i) {
      params(idx++) = theta(i);
    }

    // Constant (if included)
    if (include_constant) {
      params(idx++) = constant;
    }

    // Sigma^2
    params(idx) = sigma2;

    return params;
  }

  /**
   * @brief Unpack from optimization vector
   */
  static ARIMAParams FromVector(const arma::vec& params, size_t p, size_t q,
                                 bool include_constant = true) {
    ARIMAParams ap;
    size_t idx = 0;

    // AR coefficients
    if (p > 0) {
      ap.phi = params.subvec(idx, idx + p - 1);
      idx += p;
    }

    // MA coefficients
    if (q > 0) {
      ap.theta = params.subvec(idx, idx + q - 1);
      idx += q;
    }

    // Constant
    if (include_constant) {
      ap.constant = params(idx++);
    }

    // Sigma^2
    ap.sigma2 = params(idx);

    return ap;
  }
};

/**
 * @brief Configuration for ARIMA estimation
 */
struct ARIMAConfig {
  size_t p{1};                      ///< AR order
  size_t d{0};                      ///< Differencing order
  size_t q{1};                      ///< MA order
  bool with_constant{true};         ///< Include constant term
  size_t max_iterations{500};
  double tolerance{1e-8};
  size_t forecast_horizon{1};
  size_t min_training_samples{50};
  double confidence_level{0.95};    ///< For forecast intervals
};

/**
 * @brief Result of ARIMA model fitting
 */
struct ARIMAFitResult {
  ARIMAParams params;
  arma::vec fitted;                 ///< Fitted values (on original scale)
  arma::vec residuals;              ///< Residuals (eps_t)
  double log_likelihood;
  double aic;
  double bic;
  bool converged{false};
  std::string message;

  // Store differenced data for forecasting
  arma::vec y_diff;                 ///< Differenced series
  arma::vec y_original;             ///< Original series (for integration)
  size_t d{0};                      ///< Differencing order used
};

/**
 * @brief Forecast result with prediction intervals
 */
struct ARIMAForecast {
  arma::vec point;                  ///< Point forecasts
  arma::vec lower;                  ///< Lower confidence bound
  arma::vec upper;                  ///< Upper confidence bound
  arma::vec se;                     ///< Standard errors
};

}  // namespace epoch_script::transform::timeseries::arima
