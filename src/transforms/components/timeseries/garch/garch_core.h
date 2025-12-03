#pragma once
/**
 * @file garch_core.h
 * @brief Core GARCH(p,q) algorithms: variance recursion, likelihood, and estimation
 *
 * Implements the standard GARCH model:
 *   sigma^2_t = omega + sum_{i=1}^p alpha_i * eps^2_{t-i} + sum_{j=1}^q beta_j * sigma^2_{t-j}
 *
 * where eps_t = r_t - mu (demeaned returns)
 */

#include "garch_types.h"
#include "../optimizer/lbfgs_optimizer.h"
#include <cmath>
#include <algorithm>

namespace epoch_script::transform::timeseries::garch {

// Minimum variance floor to prevent numerical issues
constexpr double VARIANCE_FLOOR = 1e-12;

/**
 * @brief Compute conditional variance series using GARCH recursion
 *
 * Initializes variance with sample variance (backcast) for observations
 * before enough history is available.
 *
 * @param returns Demeaned return series (eps_t = r_t - mean(r))
 * @param params GARCH parameters
 * @return Conditional variance series (sigma^2_t)
 */
inline arma::vec ComputeConditionalVariance(
    const arma::vec& returns,
    const GARCHParams& params) {

  const size_t T = returns.n_elem;
  const size_t p = params.p();
  const size_t q = params.q();
  const size_t max_lag = std::max(p, q);

  // Initialize with sample variance (backcast)
  double sample_var = arma::var(returns);
  if (sample_var < VARIANCE_FLOOR) {
    sample_var = VARIANCE_FLOOR;
  }

  arma::vec sigma2(T, arma::fill::value(sample_var));
  arma::vec eps2 = arma::square(returns);

  // Main GARCH recursion
  for (size_t t = max_lag; t < T; ++t) {
    double var_t = params.omega;

    // ARCH terms: alpha_i * eps^2_{t-i}
    for (size_t i = 0; i < p; ++i) {
      var_t += params.alpha(i) * eps2(t - 1 - i);
    }

    // GARCH terms: beta_j * sigma^2_{t-j}
    for (size_t j = 0; j < q; ++j) {
      var_t += params.beta(j) * sigma2(t - 1 - j);
    }

    // Ensure positivity
    sigma2(t) = std::max(var_t, VARIANCE_FLOOR);
  }

  return sigma2;
}

/**
 * @brief Gaussian log-likelihood for GARCH model
 *
 * L = -0.5 * sum[ log(2*pi) + log(sigma^2_t) + eps^2_t / sigma^2_t ]
 *
 * @param returns Demeaned returns
 * @param sigma2 Conditional variance series
 * @return Log-likelihood value
 */
inline double GaussianLogLikelihood(
    const arma::vec& returns,
    const arma::vec& sigma2) {

  const size_t T = returns.n_elem;
  arma::vec eps2 = arma::square(returns);

  // Ensure positive variance
  arma::vec safe_sigma2 = arma::clamp(sigma2, VARIANCE_FLOOR, arma::datum::inf);

  double ll = 0.0;
  constexpr double LOG_2PI = 1.8378770664093453;  // log(2*pi)

  for (size_t t = 0; t < T; ++t) {
    ll -= 0.5 * (LOG_2PI + std::log(safe_sigma2(t)) + eps2(t) / safe_sigma2(t));
  }

  return std::isfinite(ll) ? ll : -1e20;
}

/**
 * @brief Student's t log-likelihood for GARCH (handles fat tails)
 *
 * @param returns Demeaned returns
 * @param sigma2 Conditional variance series
 * @param nu Degrees of freedom (must be > 2)
 * @return Log-likelihood value
 */
inline double StudentTLogLikelihood(
    const arma::vec& returns,
    const arma::vec& sigma2,
    double nu) {

  if (nu <= 2.0) return -1e20;

  const size_t T = returns.n_elem;
  arma::vec safe_sigma2 = arma::clamp(sigma2, VARIANCE_FLOOR, arma::datum::inf);

  // Compute standardized residuals squared
  arma::vec z2 = arma::square(returns) / safe_sigma2;

  // Log-likelihood terms
  double const_term = std::lgamma((nu + 1.0) / 2.0) - std::lgamma(nu / 2.0)
                    - 0.5 * std::log((nu - 2.0) * arma::datum::pi);

  double ll = 0.0;
  for (size_t t = 0; t < T; ++t) {
    ll += const_term - 0.5 * std::log(safe_sigma2(t))
        - ((nu + 1.0) / 2.0) * std::log(1.0 + z2(t) / (nu - 2.0));
  }

  return std::isfinite(ll) ? ll : -1e20;
}

/**
 * @brief Generate initial parameter guess using moment matching
 *
 * Uses sample variance and typical persistence values.
 *
 * @param returns Return series
 * @param p ARCH order
 * @param q GARCH order
 * @return Initial parameter guess
 */
inline GARCHParams InitialGuess(const arma::vec& returns, size_t p, size_t q) {
  GARCHParams params;

  // Typical starting values from literature
  double alpha_sum = 0.05 * p;
  double beta_sum = 0.90;

  params.alpha = arma::vec(p, arma::fill::value(0.05));
  params.beta = arma::vec(q, arma::fill::value(0.90 / static_cast<double>(q)));

  // omega = sample_var * (1 - persistence)
  double sample_var = arma::var(returns);
  double persistence = alpha_sum + beta_sum;
  params.omega = std::max(sample_var * (1.0 - persistence), 1e-8);

  return params;
}

/**
 * @brief Get box constraints for GARCH parameters
 *
 * omega > 0, alpha >= 0, beta >= 0, sum(alpha + beta) < 1
 */
inline BoxConstraints GetGARCHConstraints(size_t p, size_t q) {
  size_t n_params = 1 + p + q;
  BoxConstraints bc;
  bc.lower = arma::vec(n_params, arma::fill::value(1e-8));
  bc.upper = arma::vec(n_params);

  // Omega upper bound
  bc.upper(0) = 1.0;

  // Alpha and beta upper bounds (each < 1, but sum < 1)
  for (size_t i = 1; i < n_params; ++i) {
    bc.upper(i) = 0.999;
  }

  return bc;
}

/**
 * @brief Fit GARCH(p,q) model using Maximum Likelihood Estimation
 *
 * @param returns Return series (will be demeaned internally)
 * @param config Model configuration
 * @return Fit result with parameters, variance series, and diagnostics
 */
inline GARCHFitResult FitGARCH(
    const arma::vec& returns,
    const GARCHConfig& config) {

  GARCHFitResult result;
  result.converged = false;

  // Validate inputs
  if (returns.n_elem < config.min_training_samples) {
    result.message = "Insufficient data for GARCH estimation";
    return result;
  }

  // Demean returns
  double mean_return = arma::mean(returns);
  arma::vec eps = returns - mean_return;

  // Initial guess
  GARCHParams init_params = InitialGuess(eps, config.p, config.q);
  arma::vec x0 = init_params.ToVector();

  // Box constraints
  BoxConstraints constraints = GetGARCHConstraints(config.p, config.q);

  // Objective function (negative log-likelihood to minimize)
  auto objective = [&eps, &config](const arma::vec& theta) -> double {
    GARCHParams p = GARCHParams::FromVector(theta, config.p, config.q);

    // Check stationarity
    if (!p.IsStationary()) {
      return 1e20;  // Large penalty
    }

    arma::vec sigma2 = ComputeConditionalVariance(eps, p);

    double ll;
    switch (config.distribution) {
      case DistributionType::StudentT:
        ll = StudentTLogLikelihood(eps, sigma2, config.df);
        break;
      case DistributionType::Normal:
      default:
        ll = GaussianLogLikelihood(eps, sigma2);
        break;
    }

    return -ll;  // Minimize negative log-likelihood
  };

  // Configure optimizer
  OptimizerConfig opt_config;
  opt_config.max_iterations = config.max_iterations;
  opt_config.tolerance = config.tolerance;
  opt_config.num_restarts = 3;

  // Optimize
  OptimResult opt_result = LBFGSOptimizer::Minimize(
      objective, x0, opt_config, constraints);

  // Extract results
  result.params = GARCHParams::FromVector(opt_result.params, config.p, config.q);
  result.converged = opt_result.converged && result.params.IsStationary();

  // Compute final variance series and residuals
  result.conditional_variance = ComputeConditionalVariance(eps, result.params);
  result.standardized_residuals = eps / arma::sqrt(result.conditional_variance);

  // Compute log-likelihood and information criteria
  switch (config.distribution) {
    case DistributionType::StudentT:
      result.log_likelihood = StudentTLogLikelihood(
          eps, result.conditional_variance, config.df);
      break;
    default:
      result.log_likelihood = GaussianLogLikelihood(
          eps, result.conditional_variance);
      break;
  }

  size_t n_params = 1 + config.p + config.q;
  size_t T = returns.n_elem;

  result.aic = -2.0 * result.log_likelihood + 2.0 * static_cast<double>(n_params);
  result.bic = -2.0 * result.log_likelihood +
               static_cast<double>(n_params) * std::log(static_cast<double>(T));

  result.message = opt_result.message;

  return result;
}

/**
 * @brief Forecast GARCH volatility h steps ahead
 *
 * Uses the analytical forecast formula for GARCH.
 *
 * @param fit Fitted GARCH model
 * @param h Forecast horizon
 * @return Vector of h variance forecasts
 */
inline arma::vec ForecastVariance(
    const GARCHFitResult& fit,
    size_t h) {

  const GARCHParams& p = fit.params;
  arma::vec forecasts(h);

  // Last observed values
  double last_eps2 = std::pow(fit.standardized_residuals.back() *
                               std::sqrt(fit.conditional_variance.back()), 2);
  double last_sigma2 = fit.conditional_variance.back();

  double unconditional = p.UnconditionalVariance();
  double persistence = p.Persistence();

  // Multi-step forecast
  for (size_t i = 0; i < h; ++i) {
    if (i == 0) {
      // One-step forecast
      double forecast = p.omega;
      for (size_t j = 0; j < p.p(); ++j) {
        forecast += p.alpha(j) * last_eps2;
      }
      for (size_t j = 0; j < p.q(); ++j) {
        forecast += p.beta(j) * last_sigma2;
      }
      forecasts(i) = forecast;
    } else {
      // Multi-step: converges to unconditional variance
      forecasts(i) = unconditional + std::pow(persistence, i) *
                     (forecasts(0) - unconditional);
    }
  }

  return forecasts;
}

}  // namespace epoch_script::transform::timeseries::garch
