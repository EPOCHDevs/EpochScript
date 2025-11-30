//
// Created by Claude on 2025-11-28.
// Johansen (1995) Critical Values for Cointegration Rank Tests
// Reference: Osterwald-Lenum (1992), Johansen (1995)
//

#pragma once

#include <array>
#include <stdexcept>
#include <string>

namespace epoch_script::transform::johansen {

/**
 * Johansen test deterministic specification cases:
 *
 * Case 0 (-1): No intercept or trend in VAR or cointegrating equation
 * Case 1 (0):  Restricted constant - intercept in cointegrating equation only
 * Case 2 (1):  Unrestricted constant - intercept in VAR (most common)
 * Case 3 (2):  Restricted trend - linear trend in cointegrating equation
 * Case 4 (3):  Unrestricted trend - linear trend in VAR
 *
 * det_order mapping (statsmodels convention):
 *   -1 = Case 0 (no deterministic terms)
 *    0 = Case 1 (restricted constant)
 *    1 = Case 2 (unrestricted constant) - DEFAULT
 */

// Critical values indexed by [k-r][significance]
// k = number of variables, r = rank being tested
// k-r ranges from 1 to 12
// significance: 0=90%, 1=95%, 2=99%

// Trace test critical values - Case 2 (unrestricted constant)
// H0: rank <= r vs H1: rank > r
inline constexpr std::array<std::array<double, 3>, 12> TRACE_CV_CASE2 = {{
    {7.52, 9.24, 12.97},      // k-r = 1
    {17.85, 19.96, 24.60},    // k-r = 2
    {32.00, 34.91, 41.07},    // k-r = 3
    {49.65, 53.12, 60.16},    // k-r = 4
    {71.86, 76.07, 84.45},    // k-r = 5
    {97.18, 102.14, 111.01},  // k-r = 6
    {126.58, 131.70, 143.09}, // k-r = 7
    {159.48, 165.58, 177.20}, // k-r = 8
    {196.37, 202.92, 215.74}, // k-r = 9
    {236.54, 244.15, 257.68}, // k-r = 10
    {282.45, 291.40, 307.64}, // k-r = 11
    {330.81, 341.02, 359.41}, // k-r = 12
}};

// Max-eigenvalue test critical values - Case 2 (unrestricted constant)
// H0: rank = r vs H1: rank = r+1
inline constexpr std::array<std::array<double, 3>, 12> MAX_EIGEN_CV_CASE2 = {{
    {7.52, 9.24, 12.97},    // k-r = 1
    {13.75, 15.67, 20.20},  // k-r = 2
    {19.77, 22.00, 26.81},  // k-r = 3
    {25.56, 28.14, 33.24},  // k-r = 4
    {31.66, 34.40, 39.79},  // k-r = 5
    {37.45, 40.30, 46.82},  // k-r = 6
    {43.25, 46.45, 52.31},  // k-r = 7
    {48.91, 52.00, 57.95},  // k-r = 8
    {54.35, 57.42, 63.71},  // k-r = 9
    {60.25, 63.57, 70.05},  // k-r = 10
    {66.02, 69.74, 76.28},  // k-r = 11
    {72.07, 76.07, 82.51},  // k-r = 12
}};

// Trace test critical values - Case 1 (restricted constant)
inline constexpr std::array<std::array<double, 3>, 12> TRACE_CV_CASE1 = {{
    {2.69, 3.76, 6.65},       // k-r = 1
    {13.33, 15.41, 20.04},    // k-r = 2
    {26.79, 29.68, 35.65},    // k-r = 3
    {43.95, 47.21, 54.46},    // k-r = 4
    {64.84, 68.52, 77.74},    // k-r = 5
    {89.48, 94.15, 104.96},   // k-r = 6
    {118.50, 124.24, 136.06}, // k-r = 7
    {151.38, 157.87, 170.80}, // k-r = 8
    {188.21, 195.53, 209.95}, // k-r = 9
    {228.95, 237.19, 253.25}, // k-r = 10
    {273.00, 283.00, 300.00}, // k-r = 11 (approximate)
    {322.00, 333.00, 352.00}, // k-r = 12 (approximate)
}};

// Max-eigenvalue test critical values - Case 1 (restricted constant)
inline constexpr std::array<std::array<double, 3>, 12> MAX_EIGEN_CV_CASE1 = {{
    {2.69, 3.76, 6.65},     // k-r = 1
    {12.07, 14.07, 18.63},  // k-r = 2
    {18.60, 20.97, 25.52},  // k-r = 3
    {24.73, 27.07, 32.24},  // k-r = 4
    {30.90, 33.46, 38.77},  // k-r = 5
    {36.76, 39.37, 45.10},  // k-r = 6
    {42.32, 44.91, 51.38},  // k-r = 7
    {48.33, 51.07, 57.69},  // k-r = 8
    {53.98, 56.74, 63.37},  // k-r = 9
    {59.62, 62.57, 69.09},  // k-r = 10
    {65.38, 68.83, 75.95},  // k-r = 11
    {71.80, 75.32, 83.00},  // k-r = 12
}};

// Trace test critical values - Case 0 (no deterministic)
inline constexpr std::array<std::array<double, 3>, 12> TRACE_CV_CASE0 = {{
    {2.69, 3.76, 6.65},       // k-r = 1
    {12.07, 14.07, 18.63},    // k-r = 2
    {24.60, 27.58, 33.73},    // k-r = 3
    {40.49, 44.50, 51.54},    // k-r = 4
    {60.05, 64.84, 73.73},    // k-r = 5
    {83.20, 89.37, 99.45},    // k-r = 6
    {110.42, 117.45, 128.45}, // k-r = 7
    {141.01, 149.58, 162.30}, // k-r = 8
    {176.67, 186.54, 200.14}, // k-r = 9
    {215.17, 226.34, 241.55}, // k-r = 10
    {257.00, 270.00, 287.00}, // k-r = 11 (approximate)
    {303.00, 318.00, 337.00}, // k-r = 12 (approximate)
}};

// Max-eigenvalue test critical values - Case 0 (no deterministic)
inline constexpr std::array<std::array<double, 3>, 12> MAX_EIGEN_CV_CASE0 = {{
    {2.69, 3.76, 6.65},     // k-r = 1
    {11.03, 12.98, 17.37},  // k-r = 2
    {17.18, 19.31, 23.65},  // k-r = 3
    {23.46, 25.73, 30.34},  // k-r = 4
    {29.37, 31.79, 36.90},  // k-r = 5
    {35.07, 37.69, 43.05},  // k-r = 6
    {40.78, 43.61, 49.51},  // k-r = 7
    {46.82, 49.95, 55.75},  // k-r = 8
    {52.50, 55.67, 61.24},  // k-r = 9
    {58.24, 61.29, 67.48},  // k-r = 10
    {64.24, 67.88, 74.36},  // k-r = 11
    {70.60, 74.50, 81.50},  // k-r = 12
}};

/**
 * Convert det_order to internal case index
 * statsmodels convention: -1, 0, 1
 * Internal: 0, 1, 2
 */
inline int GetCaseIndex(int det_order) {
  if (det_order == -1)
    return 0; // Case 0: no deterministic
  if (det_order == 0)
    return 1; // Case 1: restricted constant
  if (det_order == 1)
    return 2; // Case 2: unrestricted constant
  throw std::invalid_argument(
      "det_order must be -1, 0, or 1. Use 1 for most common case.");
}

/**
 * Convert significance level to index
 * 0.10 -> 0 (90%)
 * 0.05 -> 1 (95%)
 * 0.01 -> 2 (99%)
 */
inline int GetSignificanceIndex(double sig) {
  if (std::abs(sig - 0.10) < 1e-6)
    return 0;
  if (std::abs(sig - 0.05) < 1e-6)
    return 1;
  if (std::abs(sig - 0.01) < 1e-6)
    return 2;
  throw std::invalid_argument(
      "Significance must be 0.01, 0.05, or 0.10 for Johansen test");
}

/**
 * Johansen critical value lookup
 */
struct JohansenCriticalValues {
  /**
   * Get trace test critical value
   * @param k Number of variables in the system
   * @param r Rank being tested (H0: rank <= r)
   * @param det_order Deterministic order: -1, 0, or 1
   * @param significance 0.01, 0.05, or 0.10
   * @return Critical value (reject H0 if trace stat > critical value)
   */
  static double get_trace_cv(int k, int r, int det_order, double significance) {
    int k_minus_r = k - r;
    if (k_minus_r < 1 || k_minus_r > 12) {
      throw std::invalid_argument("k - r must be between 1 and 12");
    }

    int case_idx = GetCaseIndex(det_order);
    int sig_idx = GetSignificanceIndex(significance);
    int k_idx = k_minus_r - 1;

    switch (case_idx) {
    case 0:
      return TRACE_CV_CASE0[k_idx][sig_idx];
    case 1:
      return TRACE_CV_CASE1[k_idx][sig_idx];
    case 2:
    default:
      return TRACE_CV_CASE2[k_idx][sig_idx];
    }
  }

  /**
   * Get max-eigenvalue test critical value
   * @param k Number of variables in the system
   * @param r Rank being tested (H0: rank = r)
   * @param det_order Deterministic order: -1, 0, or 1
   * @param significance 0.01, 0.05, or 0.10
   * @return Critical value (reject H0 if max-eigen stat > critical value)
   */
  static double get_max_eigen_cv(int k, int r, int det_order,
                                 double significance) {
    int k_minus_r = k - r;
    if (k_minus_r < 1 || k_minus_r > 12) {
      throw std::invalid_argument("k - r must be between 1 and 12");
    }

    int case_idx = GetCaseIndex(det_order);
    int sig_idx = GetSignificanceIndex(significance);
    int k_idx = k_minus_r - 1;

    switch (case_idx) {
    case 0:
      return MAX_EIGEN_CV_CASE0[k_idx][sig_idx];
    case 1:
      return MAX_EIGEN_CV_CASE1[k_idx][sig_idx];
    case 2:
    default:
      return MAX_EIGEN_CV_CASE2[k_idx][sig_idx];
    }
  }

  /**
   * Get all trace critical values for a given k and det_order
   * Returns array of [cv_90, cv_95, cv_99] for each rank r = 0 to k-1
   */
  static std::vector<std::array<double, 3>>
  get_all_trace_cvs(int k, int det_order) {
    std::vector<std::array<double, 3>> result;
    for (int r = 0; r < k; ++r) {
      result.push_back({get_trace_cv(k, r, det_order, 0.10),
                        get_trace_cv(k, r, det_order, 0.05),
                        get_trace_cv(k, r, det_order, 0.01)});
    }
    return result;
  }

  /**
   * Get all max-eigenvalue critical values for a given k and det_order
   */
  static std::vector<std::array<double, 3>>
  get_all_max_eigen_cvs(int k, int det_order) {
    std::vector<std::array<double, 3>> result;
    for (int r = 0; r < k; ++r) {
      result.push_back({get_max_eigen_cv(k, r, det_order, 0.10),
                        get_max_eigen_cv(k, r, det_order, 0.05),
                        get_max_eigen_cv(k, r, det_order, 0.01)});
    }
    return result;
  }

  /**
   * Estimate cointegration rank using trace test
   * @param trace_stats Vector of trace statistics for r=0,1,...,k-1
   * @param k Number of variables
   * @param det_order Deterministic order
   * @param significance Test significance level
   * @return Estimated rank
   */
  static int estimate_rank_trace(const std::vector<double> &trace_stats, int k,
                                 int det_order, double significance) {
    for (int r = 0; r < k; ++r) {
      double cv = get_trace_cv(k, r, det_order, significance);
      if (trace_stats[r] <= cv) {
        return r; // Cannot reject H0: rank <= r
      }
    }
    return k; // Reject all, full rank
  }

  /**
   * Estimate cointegration rank using max-eigenvalue test
   */
  static int estimate_rank_max_eigen(const std::vector<double> &max_stats,
                                     int k, int det_order,
                                     double significance) {
    for (int r = 0; r < k; ++r) {
      double cv = get_max_eigen_cv(k, r, det_order, significance);
      if (max_stats[r] <= cv) {
        return r; // Cannot reject H0: rank = r
      }
    }
    return k;
  }
};

} // namespace epoch_script::transform::johansen