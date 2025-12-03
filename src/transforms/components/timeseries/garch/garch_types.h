#pragma once
/**
 * @file garch_types.h
 * @brief GARCH model types, parameter structures, and configurations
 */

#include <armadillo>
#include <string>

namespace epoch_script::transform::timeseries::garch {

/**
 * @brief GARCH model variants
 */
enum class GARCHType {
  GARCH,      ///< Standard GARCH(p,q)
  EGARCH,     ///< Exponential GARCH (asymmetric, log-variance)
  TARCH,      ///< Threshold ARCH / GJR-GARCH
  FIGARCH     ///< Fractionally Integrated GARCH (long memory)
};

/**
 * @brief Error distribution types for GARCH models
 */
enum class DistributionType {
  Normal,     ///< Gaussian errors
  StudentT,   ///< Student's t errors (fat tails)
  SkewT,      ///< Skewed Student's t
  GED         ///< Generalized Error Distribution
};

/**
 * @brief Parse distribution type from string (for Select options)
 */
inline DistributionType ParseDistributionType(const std::string& str) {
  if (str == "studentt" || str == "StudentT" || str == "t") {
    return DistributionType::StudentT;
  }
  if (str == "skewt" || str == "SkewT") {
    return DistributionType::SkewT;
  }
  if (str == "ged" || str == "GED") {
    return DistributionType::GED;
  }
  return DistributionType::Normal;  // Default
}

/**
 * @brief Convert distribution type to string
 */
inline std::string DistributionTypeToString(DistributionType type) {
  switch (type) {
    case DistributionType::Normal: return "normal";
    case DistributionType::StudentT: return "studentt";
    case DistributionType::SkewT: return "skewt";
    case DistributionType::GED: return "ged";
  }
  return "normal";
}

/**
 * @brief GARCH(p,q) model parameters
 *
 * Variance equation: sigma^2_t = omega + sum_{i=1}^p alpha_i * eps^2_{t-i}
 *                                      + sum_{j=1}^q beta_j * sigma^2_{t-j}
 */
struct GARCHParams {
  double omega{1e-6};      ///< Constant term (must be > 0)
  arma::vec alpha;         ///< ARCH coefficients (shock impact)
  arma::vec beta;          ///< GARCH coefficients (persistence)

  [[nodiscard]] size_t p() const { return alpha.n_elem; }  ///< ARCH order
  [[nodiscard]] size_t q() const { return beta.n_elem; }   ///< GARCH order

  /**
   * @brief Check covariance stationarity: sum(alpha) + sum(beta) < 1
   */
  [[nodiscard]] bool IsStationary() const {
    if (omega <= 0) return false;
    double persistence = arma::sum(alpha) + arma::sum(beta);
    return persistence < 1.0 && arma::all(alpha >= 0) && arma::all(beta >= 0);
  }

  /**
   * @brief Get persistence (sum of alpha + beta)
   */
  [[nodiscard]] double Persistence() const {
    return arma::sum(alpha) + arma::sum(beta);
  }

  /**
   * @brief Unconditional variance: omega / (1 - persistence)
   */
  [[nodiscard]] double UnconditionalVariance() const {
    double pers = Persistence();
    if (pers >= 1.0) return std::numeric_limits<double>::infinity();
    return omega / (1.0 - pers);
  }

  /**
   * @brief Pack parameters into optimization vector
   * @return Vector: [omega, alpha_1, ..., alpha_p, beta_1, ..., beta_q]
   */
  [[nodiscard]] arma::vec ToVector() const {
    arma::vec params(1 + p() + q());
    params(0) = omega;
    if (p() > 0) params.subvec(1, p()) = alpha;
    if (q() > 0) params.subvec(1 + p(), 1 + p() + q() - 1) = beta;
    return params;
  }

  /**
   * @brief Unpack from optimization vector
   */
  static GARCHParams FromVector(const arma::vec& params, size_t p, size_t q) {
    GARCHParams gp;
    gp.omega = params(0);
    if (p > 0) gp.alpha = params.subvec(1, p);
    if (q > 0) gp.beta = params.subvec(1 + p, 1 + p + q - 1);
    return gp;
  }
};

/**
 * @brief Configuration for GARCH estimation
 */
struct GARCHConfig {
  size_t p{1};                      ///< ARCH order
  size_t q{1};                      ///< GARCH order
  DistributionType distribution{DistributionType::Normal};
  double df{8.0};                   ///< Degrees of freedom (for StudentT)
  size_t max_iterations{500};
  double tolerance{1e-8};
  size_t forecast_horizon{1};
  size_t min_training_samples{100};
};

/**
 * @brief Result of GARCH model fitting
 */
struct GARCHFitResult {
  GARCHParams params;
  arma::vec conditional_variance;    ///< sigma^2_t series
  arma::vec standardized_residuals;  ///< eps_t / sigma_t
  double log_likelihood;
  double aic;
  double bic;
  bool converged{false};
  std::string message;

  /**
   * @brief Compute volatility from variance
   */
  [[nodiscard]] arma::vec ConditionalVolatility() const {
    return arma::sqrt(conditional_variance);
  }
};

}  // namespace epoch_script::transform::timeseries::garch
