#pragma once
/**
 * @file optimizer_base.h
 * @brief Base types and utilities for time series model optimization
 *
 * Provides common structures for optimization results, box constraints,
 * and a wrapper around ensmallen's L-BFGS optimizer for GARCH/ARIMA fitting.
 */

#include <armadillo>
#include <functional>
#include <optional>
#include <string>

namespace epoch_script::transform::timeseries {

/**
 * @brief Result of an optimization run
 */
struct OptimResult {
  arma::vec params;           ///< Optimized parameters
  double objective_value;     ///< Final objective function value
  size_t iterations;          ///< Number of iterations used
  bool converged;             ///< Whether optimization converged
  std::string message;        ///< Status message (optional)
};

/**
 * @brief Box constraints for bounded optimization
 *
 * Parameters are constrained to: lower(i) <= x(i) <= upper(i)
 * Uses barrier/penalty method to enforce constraints in unconstrained optimizer.
 */
struct BoxConstraints {
  arma::vec lower;  ///< Lower bounds for each parameter
  arma::vec upper;  ///< Upper bounds for each parameter

  /**
   * @brief Check if parameters are within bounds
   */
  [[nodiscard]] bool IsFeasible(const arma::vec& x) const {
    return arma::all(x >= lower) && arma::all(x <= upper);
  }

  /**
   * @brief Project parameters onto feasible region
   */
  [[nodiscard]] arma::vec Project(const arma::vec& x) const {
    arma::vec projected = x;
    for (size_t i = 0; i < x.n_elem; ++i) {
      if (projected(i) < lower(i)) projected(i) = lower(i);
      if (projected(i) > upper(i)) projected(i) = upper(i);
    }
    return projected;
  }

  /**
   * @brief Create uniform bounds for n parameters
   */
  static BoxConstraints Uniform(size_t n, double lo, double hi) {
    BoxConstraints bc;
    bc.lower = arma::vec(n, arma::fill::value(lo));
    bc.upper = arma::vec(n, arma::fill::value(hi));
    return bc;
  }
};

/**
 * @brief Configuration for the optimizer
 */
struct OptimizerConfig {
  size_t max_iterations = 1000;
  double tolerance = 1e-8;
  size_t memory_size = 10;      ///< L-BFGS memory (number of past gradients)
  double min_gradient_norm = 1e-6;
  size_t num_restarts = 3;      ///< Number of restarts with different initial points
  double constraint_penalty = 1e8;  ///< Penalty weight for constraint violations
};

// Function type aliases
using ObjectiveFunc = std::function<double(const arma::vec&)>;
using GradientFunc = std::function<arma::vec(const arma::vec&)>;

}  // namespace epoch_script::transform::timeseries
