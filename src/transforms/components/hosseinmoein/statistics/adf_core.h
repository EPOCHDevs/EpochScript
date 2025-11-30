//
// Core ADF (Augmented Dickey-Fuller) Implementation
// Matches statsmodels.tsa.stattools.adfuller
//

#pragma once

#include <armadillo>
#include <cmath>
#include <vector>

namespace epoch_script::transform::adf {

/**
 * ADF test result structure
 */
struct ADFResult {
  double adf_stat;     // t-statistic for gamma (unit root coefficient)
  double pvalue;       // Approximate p-value
  int used_lag;        // Number of lags used
  int nobs;            // Number of observations used in regression
  double gamma;        // Coefficient on y_{t-1}
  double se_gamma;     // Standard error of gamma
};

/**
 * Compute ADF test statistic using OLS regression
 *
 * The ADF regression is:
 *   Δy_t = α + γ*y_{t-1} + Σ(β_i*Δy_{t-i}) + ε_t
 *
 * for i = 1, ..., maxlag
 *
 * Test statistic = γ / SE(γ)
 *
 * @param y Input time series
 * @param maxlag Maximum lag for differenced terms (default: 1)
 * @param regression "c" (constant), "ct" (constant+trend), "nc" (none)
 * @return ADFResult with test statistic and related values
 */
inline ADFResult compute_adf(const std::vector<double>& y, int maxlag = 1,
                             const std::string& regression = "c") {
  const size_t n = y.size();
  if (n < static_cast<size_t>(maxlag + 3)) {
    return {std::nan(""), std::nan(""), maxlag, 0, std::nan(""), std::nan("")};
  }

  // Compute first differences: dy[t] = y[t+1] - y[t]
  std::vector<double> dy(n - 1);
  for (size_t i = 0; i < n - 1; ++i) {
    dy[i] = y[i + 1] - y[i];
  }

  // Build regression matrices
  // Dependent variable: Δy_t for t = maxlag+1, ..., n-1
  // Regressors:
  //   - constant (if regression contains 'c')
  //   - trend (if regression contains 't')
  //   - y_{t-1} (lagged level)
  //   - Δy_{t-1}, Δy_{t-2}, ..., Δy_{t-maxlag} (lagged differences)

  const size_t nobs = n - maxlag - 1;  // Number of observations in regression
  if (nobs < 5) {
    return {std::nan(""), std::nan(""), maxlag, 0, std::nan(""), std::nan("")};
  }

  // Count number of regressors
  int n_regressors = 1;  // y_{t-1} always included
  n_regressors += maxlag;  // lagged differences
  bool has_const = (regression.find('c') != std::string::npos);
  bool has_trend = (regression.find('t') != std::string::npos && regression != "nc");
  if (has_const) n_regressors++;
  if (has_trend) n_regressors++;

  // Build design matrix X and response vector Y
  arma::vec Y(nobs);
  arma::mat X(nobs, n_regressors);

  for (size_t i = 0; i < nobs; ++i) {
    size_t t = i + maxlag;  // Index into dy (starting from maxlag)

    // Response: Δy_t
    Y(i) = dy[t];

    int col = 0;

    // Constant term
    if (has_const) {
      X(i, col++) = 1.0;
    }

    // Trend term
    if (has_trend) {
      X(i, col++) = static_cast<double>(i + 1);
    }

    // Lagged level: y_{t-1} (using original series, so y[t])
    X(i, col++) = y[t];

    // Lagged differences: Δy_{t-1}, ..., Δy_{t-maxlag}
    for (int lag = 1; lag <= maxlag; ++lag) {
      X(i, col++) = dy[t - lag];
    }
  }

  // OLS: β = (X'X)^{-1} X'Y
  arma::mat XtX = X.t() * X;
  arma::mat XtX_inv;
  if (!arma::inv(XtX_inv, XtX)) {
    // Matrix is singular, try pseudo-inverse
    XtX_inv = arma::pinv(XtX);
  }

  arma::vec beta = XtX_inv * (X.t() * Y);

  // Residuals
  arma::vec residuals = Y - X * beta;

  // Residual variance (unbiased estimate)
  double s2 = arma::dot(residuals, residuals) / (nobs - n_regressors);

  // Variance-covariance matrix of coefficients
  arma::mat var_beta = s2 * XtX_inv;

  // Extract gamma (coefficient on y_{t-1}) and its standard error
  // gamma is at position: has_const + has_trend
  int gamma_idx = (has_const ? 1 : 0) + (has_trend ? 1 : 0);
  double gamma = beta(gamma_idx);
  double se_gamma = std::sqrt(var_beta(gamma_idx, gamma_idx));

  // ADF statistic = gamma / SE(gamma)
  double adf_stat = gamma / se_gamma;

  ADFResult result;
  result.adf_stat = adf_stat;
  result.used_lag = maxlag;
  result.nobs = static_cast<int>(nobs);
  result.gamma = gamma;
  result.se_gamma = se_gamma;
  result.pvalue = std::nan("");  // Will be computed separately using MacKinnon tables

  return result;
}

/**
 * Compute ADF from Armadillo vector
 */
inline ADFResult compute_adf(const arma::vec& y, int maxlag = 1,
                             const std::string& regression = "c") {
  std::vector<double> y_vec(y.begin(), y.end());
  return compute_adf(y_vec, maxlag, regression);
}

}  // namespace epoch_script::transform::adf
