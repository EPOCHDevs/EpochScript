#pragma once
/**
 * @file arima_core.h
 * @brief Core ARIMA(p,d,q) algorithms: differencing, CSS likelihood, and estimation
 *
 * Implements ARIMA model fitting using Conditional Sum of Squares (CSS) method:
 *   y_t = c + phi_1*y_{t-1} + ... + phi_p*y_{t-p} + eps_t + theta_1*eps_{t-1} + ... + theta_q*eps_{t-q}
 *
 * The CSS method conditions on initial values and is computationally efficient.
 * For small samples, exact MLE via Kalman filter would be more accurate.
 */

#include "arima_types.h"
#include "../optimizer/lbfgs_optimizer.h"
#include <cmath>
#include <algorithm>

namespace epoch_script::transform::timeseries::arima {

/**
 * @brief Apply differencing to a time series
 *
 * @param y Original series
 * @param d Differencing order (1 = first difference, 2 = second difference)
 * @return Differenced series (length = n - d)
 */
inline arma::vec Difference(const arma::vec& y, size_t d) {
  if (d == 0) return y;

  arma::vec result = y;
  for (size_t i = 0; i < d; ++i) {
    arma::vec diff(result.n_elem - 1);
    for (size_t t = 0; t < diff.n_elem; ++t) {
      diff(t) = result(t + 1) - result(t);
    }
    result = diff;
  }
  return result;
}

/**
 * @brief Integrate (reverse differencing) forecasts back to original scale
 *
 * @param forecasts Forecasts on differenced scale
 * @param y_original Original (undifferenced) series
 * @param d Differencing order
 * @return Forecasts on original scale
 */
inline arma::vec Integrate(const arma::vec& forecasts, const arma::vec& y_original, size_t d) {
  if (d == 0) return forecasts;

  arma::vec result = forecasts;

  // For d=1: y_{T+h} = y_T + sum_{i=1}^h forecast_i (cumsum)
  // For d=2: need two levels of integration

  for (size_t i = 0; i < d; ++i) {
    // Get last value from appropriate differencing level
    arma::vec y_diff = Difference(y_original, d - 1 - i);
    double last_val = y_diff(y_diff.n_elem - 1);

    // Cumulative sum from last observed value
    arma::vec integrated(result.n_elem);
    integrated(0) = last_val + result(0);
    for (size_t t = 1; t < result.n_elem; ++t) {
      integrated(t) = integrated(t - 1) + result(t);
    }
    result = integrated;
  }

  return result;
}

/**
 * @brief Compute ARMA residuals given parameters (CSS approach)
 *
 * Recursively computes: eps_t = y_t - c - sum(phi*y_{t-i}) - sum(theta*eps_{t-i})
 *
 * @param y Differenced series
 * @param params ARIMA parameters
 * @return Residual series
 */
inline arma::vec ComputeResiduals(const arma::vec& y, const ARIMAParams& params) {
  const size_t T = y.n_elem;
  const size_t p = params.p();
  const size_t q = params.q();
  const size_t max_lag = std::max(p, q);

  arma::vec eps(T, arma::fill::zeros);

  // Initialize first max_lag residuals to zero (CSS conditioning)
  for (size_t t = max_lag; t < T; ++t) {
    double y_hat = params.constant;

    // AR component
    for (size_t i = 0; i < p; ++i) {
      y_hat += params.phi(i) * y(t - 1 - i);
    }

    // MA component
    for (size_t j = 0; j < q; ++j) {
      y_hat += params.theta(j) * eps(t - 1 - j);
    }

    eps(t) = y(t) - y_hat;
  }

  return eps;
}

/**
 * @brief Compute fitted values from residuals
 */
inline arma::vec ComputeFitted(const arma::vec& y, const arma::vec& residuals) {
  return y - residuals;
}

/**
 * @brief Conditional Sum of Squares (CSS) log-likelihood
 *
 * L = -T/2 * log(2*pi) - T/2 * log(sigma^2) - 1/(2*sigma^2) * sum(eps^2)
 *
 * For optimization, we use concentrated likelihood where sigma^2 = sum(eps^2)/T
 *
 * @param y Differenced series
 * @param params ARIMA parameters
 * @return Negative log-likelihood (for minimization)
 */
inline double CSSLogLikelihood(const arma::vec& y, const ARIMAParams& params) {
  arma::vec eps = ComputeResiduals(y, params);

  // Skip initial max_lag observations
  size_t max_lag = std::max(params.p(), params.q());
  if (max_lag >= y.n_elem) return 1e20;

  arma::vec eps_valid = eps.subvec(max_lag, eps.n_elem - 1);
  size_t T = eps_valid.n_elem;

  if (T < 2) return 1e20;

  double ss = arma::dot(eps_valid, eps_valid);
  double sigma2 = ss / static_cast<double>(T);

  if (sigma2 <= 0 || !std::isfinite(sigma2)) return 1e20;

  // Log-likelihood (concentrated form)
  constexpr double LOG_2PI = 1.8378770664093453;
  double ll = -0.5 * static_cast<double>(T) * (LOG_2PI + std::log(sigma2) + 1.0);

  return std::isfinite(ll) ? -ll : 1e20;  // Return negative for minimization
}

/**
 * @brief Generate initial parameter guess
 *
 * Uses method of moments / OLS for AR, zeros for MA
 */
inline ARIMAParams InitialGuess(const arma::vec& y, size_t p, size_t q,
                                 bool with_constant) {
  ARIMAParams params;

  // Initialize AR coefficients
  if (p > 0) {
    params.phi = arma::vec(p, arma::fill::zeros);

    // Use Yule-Walker for AR(1) initial estimate
    if (p >= 1 && y.n_elem > 2) {
      double acf1 = 0.0;
      double mean_y = arma::mean(y);
      arma::vec y_centered = y - mean_y;
      double var = arma::dot(y_centered, y_centered);

      if (var > 0) {
        for (size_t t = 1; t < y.n_elem; ++t) {
          acf1 += y_centered(t) * y_centered(t - 1);
        }
        acf1 /= var;

        // Bound to stationary region
        params.phi(0) = std::clamp(acf1, -0.95, 0.95);
      }
    }
  }

  // Initialize MA coefficients to small values
  if (q > 0) {
    params.theta = arma::vec(q, arma::fill::value(0.1));
  }

  // Constant
  if (with_constant) {
    params.constant = arma::mean(y) * (1.0 - arma::sum(params.phi));
  }

  // Initial sigma^2
  params.sigma2 = arma::var(y);
  if (params.sigma2 <= 0) params.sigma2 = 1.0;

  return params;
}

/**
 * @brief Get box constraints for ARIMA parameters
 *
 * AR and MA coefficients bounded to ensure approximate stationarity/invertibility
 */
inline BoxConstraints GetARIMAConstraints(size_t p, size_t q, bool with_constant) {
  size_t n_params = p + q + 1;  // +1 for sigma2
  if (with_constant) n_params += 1;

  BoxConstraints bc;
  bc.lower = arma::vec(n_params);
  bc.upper = arma::vec(n_params);

  size_t idx = 0;

  // AR coefficients: [-0.999, 0.999]
  for (size_t i = 0; i < p; ++i) {
    bc.lower(idx) = -0.999;
    bc.upper(idx) = 0.999;
    ++idx;
  }

  // MA coefficients: [-0.999, 0.999]
  for (size_t i = 0; i < q; ++i) {
    bc.lower(idx) = -0.999;
    bc.upper(idx) = 0.999;
    ++idx;
  }

  // Constant: unbounded (use large bounds)
  if (with_constant) {
    bc.lower(idx) = -1e6;
    bc.upper(idx) = 1e6;
    ++idx;
  }

  // Sigma^2: positive
  bc.lower(idx) = 1e-10;
  bc.upper(idx) = 1e10;

  return bc;
}

/**
 * @brief Fit ARIMA(p,d,q) model using CSS method
 *
 * @param y Original time series
 * @param config Model configuration
 * @return Fit result with parameters, fitted values, and diagnostics
 */
inline ARIMAFitResult FitARIMA(const arma::vec& y, const ARIMAConfig& config) {
  ARIMAFitResult result;
  result.converged = false;
  result.d = config.d;
  result.y_original = y;

  // Validate inputs
  if (y.n_elem < config.min_training_samples) {
    result.message = "Insufficient data for ARIMA estimation";
    return result;
  }

  // Apply differencing
  arma::vec y_diff = Difference(y, config.d);
  result.y_diff = y_diff;

  if (y_diff.n_elem < config.p + config.q + 5) {
    result.message = "Insufficient data after differencing";
    return result;
  }

  // Initial guess
  ARIMAParams init_params = InitialGuess(y_diff, config.p, config.q, config.with_constant);
  arma::vec x0 = init_params.ToVector(config.with_constant);

  // Box constraints
  BoxConstraints constraints = GetARIMAConstraints(config.p, config.q, config.with_constant);

  // Objective function (negative log-likelihood)
  auto objective = [&y_diff, &config](const arma::vec& theta) -> double {
    ARIMAParams p = ARIMAParams::FromVector(theta, config.p, config.q, config.with_constant);

    // Check stationarity/invertibility
    if (!p.IsARStationary() || !p.IsMAInvertible()) {
      return 1e20;
    }

    return CSSLogLikelihood(y_diff, p);
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
  result.params = ARIMAParams::FromVector(opt_result.params, config.p, config.q,
                                           config.with_constant);
  result.converged = opt_result.converged;

  // Compute final residuals and fitted values
  result.residuals = ComputeResiduals(y_diff, result.params);

  // Estimate sigma^2 from residuals
  size_t max_lag = std::max(config.p, config.q);
  if (max_lag < result.residuals.n_elem) {
    arma::vec eps_valid = result.residuals.subvec(max_lag, result.residuals.n_elem - 1);
    result.params.sigma2 = arma::dot(eps_valid, eps_valid) / static_cast<double>(eps_valid.n_elem);
  }

  // Compute fitted values on original scale
  arma::vec fitted_diff = ComputeFitted(y_diff, result.residuals);

  // Pad fitted values to match original length (NaN for differenced observations)
  result.fitted = arma::vec(y.n_elem, arma::fill::value(std::numeric_limits<double>::quiet_NaN()));
  for (size_t t = config.d; t < y.n_elem; ++t) {
    // Integration: fitted value on original scale
    if (config.d == 0) {
      result.fitted(t) = fitted_diff(t);
    } else if (config.d == 1) {
      result.fitted(t) = y(t - 1) + fitted_diff(t - config.d);
    } else if (config.d == 2 && t >= 2) {
      // For d=2, integrate twice
      result.fitted(t) = 2 * y(t - 1) - y(t - 2) + fitted_diff(t - config.d);
    }
  }

  // Compute log-likelihood and information criteria
  size_t T = y_diff.n_elem - max_lag;
  size_t k = config.p + config.q + (config.with_constant ? 1 : 0) + 1;  // +1 for sigma2

  constexpr double LOG_2PI = 1.8378770664093453;
  result.log_likelihood = -0.5 * static_cast<double>(T) *
      (LOG_2PI + std::log(result.params.sigma2) + 1.0);

  result.aic = -2.0 * result.log_likelihood + 2.0 * static_cast<double>(k);
  result.bic = -2.0 * result.log_likelihood +
               static_cast<double>(k) * std::log(static_cast<double>(T));

  result.message = opt_result.message;

  return result;
}

/**
 * @brief Compute psi-weights for forecast variance calculation
 *
 * The psi-weights come from the MA(infinity) representation of the ARMA process
 */
inline arma::vec ComputePsiWeights(const ARIMAParams& params, size_t h) {
  arma::vec psi(h, arma::fill::zeros);
  psi(0) = 1.0;

  size_t p = params.p();
  size_t q = params.q();

  for (size_t j = 1; j < h; ++j) {
    // psi_j = theta_j + sum_{i=1}^{min(j,p)} phi_i * psi_{j-i}
    double theta_j = (j <= q) ? params.theta(j - 1) : 0.0;
    double ar_sum = 0.0;

    for (size_t i = 1; i <= std::min(j, p); ++i) {
      ar_sum += params.phi(i - 1) * psi(j - i);
    }

    psi(j) = theta_j + ar_sum;
  }

  return psi;
}

/**
 * @brief Forecast ARIMA model h steps ahead
 *
 * @param fit Fitted ARIMA model
 * @param h Forecast horizon
 * @param confidence Confidence level for prediction intervals (default 0.95)
 * @return Forecast with point predictions and intervals
 */
inline ARIMAForecast Forecast(const ARIMAFitResult& fit, size_t h, double confidence = 0.95) {
  ARIMAForecast fc;
  fc.point = arma::vec(h, arma::fill::zeros);
  fc.se = arma::vec(h, arma::fill::zeros);

  const ARIMAParams& params = fit.params;
  const arma::vec& y = fit.y_diff;
  const arma::vec& eps = fit.residuals;

  size_t T = y.n_elem;
  size_t p = params.p();
  size_t q = params.q();

  // Extended series for forecast recursion
  arma::vec y_ext(T + h, arma::fill::zeros);
  arma::vec eps_ext(T + h, arma::fill::zeros);

  y_ext.subvec(0, T - 1) = y;
  eps_ext.subvec(0, T - 1) = eps;

  // Point forecasts on differenced scale
  for (size_t i = 0; i < h; ++i) {
    size_t t = T + i;
    double forecast = params.constant;

    // AR component
    for (size_t j = 0; j < p; ++j) {
      if (t > j) {
        forecast += params.phi(j) * y_ext(t - 1 - j);
      }
    }

    // MA component (only for known residuals, future eps = 0)
    for (size_t j = 0; j < q; ++j) {
      if (t > j && t - 1 - j < T) {
        forecast += params.theta(j) * eps_ext(t - 1 - j);
      }
    }

    y_ext(t) = forecast;
    fc.point(i) = forecast;
  }

  // Compute forecast standard errors using psi-weights
  arma::vec psi = ComputePsiWeights(params, h);
  double sigma = std::sqrt(params.sigma2);

  for (size_t i = 0; i < h; ++i) {
    double var = 0.0;
    for (size_t j = 0; j <= i; ++j) {
      var += psi(j) * psi(j);
    }
    fc.se(i) = sigma * std::sqrt(var);
  }

  // Integrate forecasts back to original scale
  if (fit.d > 0) {
    fc.point = Integrate(fc.point, fit.y_original, fit.d);

    // Standard errors also need to be adjusted for integration
    // For d=1: Var(Y_{T+h}) = Var(sum of h forecasts) = h * sigma^2 for RW
    // This is approximate - exact formula depends on the specific model
    for (size_t i = 0; i < h; ++i) {
      fc.se(i) *= std::sqrt(static_cast<double>(i + 1));
    }
  }

  // Prediction intervals
  // z_alpha/2 for 95% confidence = 1.96
  double z = 1.96;  // Could compute from confidence level
  if (std::abs(confidence - 0.99) < 0.01) z = 2.576;
  else if (std::abs(confidence - 0.90) < 0.01) z = 1.645;

  fc.lower = fc.point - z * fc.se;
  fc.upper = fc.point + z * fc.se;

  return fc;
}

}  // namespace epoch_script::transform::timeseries::arima
