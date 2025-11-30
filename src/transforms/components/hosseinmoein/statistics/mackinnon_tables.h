//
// Created by Claude on 2025-11-28.
// MacKinnon (2010) Critical Values for Unit Root and Cointegration Tests
//

#pragma once

#include <array>
#include <cmath>
#include <stdexcept>
#include <string>

namespace epoch_script::transform::mackinnon {

// MacKinnon (2010) regression coefficients for ADF critical values
// tau = tau_inf + tau_1/T + tau_2/T^2
// Indexed by: [deterministic_type][significance_level]
// Deterministic types: 0=nc (no constant), 1=c (constant), 2=ct (constant+trend)
// Significance levels: 0=1%, 1=5%, 2=10%

struct ADFCriticalValueCoeffs {
  double tau_inf;
  double tau_1;
  double tau_2;
};

// ADF critical value coefficients from MacKinnon (2010) Table 1
// For standard unit root tests (not cointegration)
inline constexpr std::array<std::array<ADFCriticalValueCoeffs, 3>, 3>
    ADF_COEFFICIENTS = {{
        // No constant (nc)
        {{{-2.5658, -1.960, -10.04},   // 1%
          {-1.9393, -0.398, 0.0},      // 5%
          {-1.6156, -0.181, 0.0}}},    // 10%
        // Constant (c)
        {{{-3.4336, -5.999, -29.25},   // 1%
          {-2.8621, -2.738, -8.36},    // 5%
          {-2.5671, -1.438, -4.48}}},  // 10%
        // Constant + trend (ct)
        {{{-3.9638, -8.353, -47.44},   // 1%
          {-3.4126, -4.039, -17.83},   // 5%
          {-3.1279, -2.418, -7.58}}}   // 10%
    }};

// Engle-Granger cointegration critical values (MacKinnon 2010, Table 2)
// More stringent than standard ADF because residuals are estimated
// Indexed by: [n_variables - 2][significance_level]
// n_variables: 2-7 (index 0-5)
// Significance: 0=1%, 1=5%, 2=10%
inline constexpr std::array<std::array<ADFCriticalValueCoeffs, 3>, 6>
    COINTEGRATION_COEFFICIENTS = {{
        // N=2 variables
        {{{-3.9001, -10.534, -30.03},  // 1%
          {-3.3377, -5.967, -8.98},    // 5%
          {-3.0462, -4.069, -5.73}}},  // 10%
        // N=3 variables
        {{{-4.2981, -13.790, -46.37},  // 1%
          {-3.7429, -8.352, -13.41},   // 5%
          {-3.4518, -6.241, -2.79}}},  // 10%
        // N=4 variables
        {{{-4.6493, -17.188, -59.20},  // 1%
          {-4.1193, -10.745, -21.57},  // 5%
          {-3.8344, -8.317, -13.13}}}, // 10%
        // N=5 variables
        {{{-4.9695, -20.222, -77.332}, // 1%
          {-4.4294, -13.461, -22.75},  // 5%
          {-4.1474, -10.741, -19.57}}},// 10%
        // N=6 variables
        {{{-5.2528, -23.636, -83.93},  // 1%
          {-4.7154, -15.809, -34.85},  // 5%
          {-4.4345, -12.845, -24.48}}},// 10%
        // N=7 variables
        {{{-5.5127, -26.538, -101.82}, // 1%
          {-4.9767, -18.023, -38.23},  // 5%
          {-4.6999, -14.942, -29.38}}} // 10%
    }};

// P-value computation using MacKinnon (2010) response surface
// These are approximation coefficients for the normal CDF transformation
struct PValueCoeffs {
  std::array<double, 4> small_p;  // For small p (left tail)
  std::array<double, 4> large_p;  // For large p (right tail)
};

// Convert deterministic string to index
inline int GetDeterministicIndex(const std::string &det) {
  if (det == "nc" || det == "n" || det == "none")
    return 0;
  if (det == "c" || det == "constant")
    return 1;
  if (det == "ct" || det == "trend" || det == "constant_trend")
    return 2;
  throw std::invalid_argument("Invalid deterministic type: " + det +
                              ". Use 'nc', 'c', or 'ct'");
}

// Convert significance level to index
inline int GetSignificanceIndex(double sig) {
  if (std::abs(sig - 0.01) < 1e-6)
    return 0;
  if (std::abs(sig - 0.05) < 1e-6)
    return 1;
  if (std::abs(sig - 0.10) < 1e-6)
    return 2;
  throw std::invalid_argument(
      "Significance must be 0.01, 0.05, or 0.10");
}

/**
 * Standard ADF critical values (for unit root tests, not cointegration)
 */
struct ADFCriticalValues {
  /**
   * Get ADF critical value for given sample size, deterministic type, and
   * significance
   * @param T Sample size
   * @param deterministic "nc" (no constant), "c" (constant), "ct"
   * (constant+trend)
   * @param significance 0.01, 0.05, or 0.10
   * @return Critical value (reject null if test stat < critical value)
   */
  static double get_critical_value(size_t T, const std::string &deterministic,
                                   double significance) {
    int det_idx = GetDeterministicIndex(deterministic);
    int sig_idx = GetSignificanceIndex(significance);

    const auto &coef = ADF_COEFFICIENTS[det_idx][sig_idx];
    double T_inv = 1.0 / static_cast<double>(T);

    return coef.tau_inf + coef.tau_1 * T_inv + coef.tau_2 * T_inv * T_inv;
  }

  /**
   * Get all three critical values (1%, 5%, 10%)
   */
  static std::array<double, 3>
  get_all_critical_values(size_t T, const std::string &deterministic) {
    return {get_critical_value(T, deterministic, 0.01),
            get_critical_value(T, deterministic, 0.05),
            get_critical_value(T, deterministic, 0.10)};
  }

  /**
   * Approximate p-value using MacKinnon regression surface
   * Uses linear interpolation between critical values
   * @param tau ADF test statistic
   * @param T Sample size
   * @param deterministic "nc", "c", or "ct"
   * @return Approximate p-value
   */
  static double get_pvalue(double tau, size_t T,
                           const std::string &deterministic) {
    // Get critical values at standard significance levels
    auto cvs = get_all_critical_values(T, deterministic);
    double cv_1pct = cvs[0];
    double cv_5pct = cvs[1];
    double cv_10pct = cvs[2];

    // Simple interpolation approach
    // More negative tau = smaller p-value (stronger rejection)
    if (tau <= cv_1pct) {
      // p < 0.01: extrapolate
      double slope = (0.05 - 0.01) / (cv_5pct - cv_1pct);
      double p = 0.01 + slope * (tau - cv_1pct);
      return std::max(0.0001, p);
    } else if (tau <= cv_5pct) {
      // 0.01 < p < 0.05: interpolate
      double t = (tau - cv_1pct) / (cv_5pct - cv_1pct);
      return 0.01 + t * (0.05 - 0.01);
    } else if (tau <= cv_10pct) {
      // 0.05 < p < 0.10: interpolate
      double t = (tau - cv_5pct) / (cv_10pct - cv_5pct);
      return 0.05 + t * (0.10 - 0.05);
    } else {
      // p > 0.10: extrapolate (less significant)
      double slope = (0.10 - 0.05) / (cv_10pct - cv_5pct);
      double p = 0.10 + slope * (tau - cv_10pct);
      return std::min(0.9999, p);
    }
  }
};

/**
 * Engle-Granger cointegration critical values
 * More stringent than standard ADF because residuals are estimated
 */
struct CointegrationCriticalValues {
  /**
   * Get cointegration critical value
   * @param T Sample size
   * @param n_variables Number of variables in cointegrating regression (2-7)
   * @param significance 0.01, 0.05, or 0.10
   * @return Critical value
   */
  static double get_critical_value(size_t T, size_t n_variables,
                                   double significance) {
    if (n_variables < 2 || n_variables > 7) {
      throw std::invalid_argument(
          "n_variables must be between 2 and 7 for cointegration test");
    }

    int n_idx = static_cast<int>(n_variables) - 2;
    int sig_idx = GetSignificanceIndex(significance);

    const auto &coef = COINTEGRATION_COEFFICIENTS[n_idx][sig_idx];
    double T_inv = 1.0 / static_cast<double>(T);

    return coef.tau_inf + coef.tau_1 * T_inv + coef.tau_2 * T_inv * T_inv;
  }

  /**
   * Get all three critical values (1%, 5%, 10%)
   */
  static std::array<double, 3> get_all_critical_values(size_t T,
                                                       size_t n_variables) {
    return {get_critical_value(T, n_variables, 0.01),
            get_critical_value(T, n_variables, 0.05),
            get_critical_value(T, n_variables, 0.10)};
  }

  /**
   * Approximate p-value for cointegration test
   * @param tau ADF test statistic on cointegrating residuals
   * @param T Sample size
   * @param n_variables Number of variables (2-7)
   * @return Approximate p-value
   */
  static double get_pvalue(double tau, size_t T, size_t n_variables) {
    auto cvs = get_all_critical_values(T, n_variables);
    double cv_1pct = cvs[0];
    double cv_5pct = cvs[1];
    double cv_10pct = cvs[2];

    // Same interpolation logic as ADF
    if (tau <= cv_1pct) {
      double slope = (0.05 - 0.01) / (cv_5pct - cv_1pct);
      double p = 0.01 + slope * (tau - cv_1pct);
      return std::max(0.0001, p);
    } else if (tau <= cv_5pct) {
      double t = (tau - cv_1pct) / (cv_5pct - cv_1pct);
      return 0.01 + t * (0.05 - 0.01);
    } else if (tau <= cv_10pct) {
      double t = (tau - cv_5pct) / (cv_10pct - cv_5pct);
      return 0.05 + t * (0.10 - 0.05);
    } else {
      double slope = (0.10 - 0.05) / (cv_10pct - cv_5pct);
      double p = 0.10 + slope * (tau - cv_10pct);
      return std::min(0.9999, p);
    }
  }
};

} // namespace epoch_script::transform::mackinnon