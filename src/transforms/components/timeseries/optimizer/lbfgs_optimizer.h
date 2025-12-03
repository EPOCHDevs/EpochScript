#pragma once
/**
 * @file lbfgs_optimizer.h
 * @brief L-BFGS optimizer wrapper using ensmallen
 *
 * Provides a convenient interface for minimizing objective functions
 * with optional box constraints (via penalty method).
 */

#include "optimizer_base.h"
#include <ensmallen.hpp>
#include <cmath>
#include <random>

namespace epoch_script::transform::timeseries {

/**
 * @brief Functor wrapper for ensmallen optimization
 *
 * Wraps user-provided objective and gradient functions into the
 * format expected by ensmallen optimizers. Applies penalty for
 * constraint violations when box constraints are provided.
 */
class ObjectiveFunctor {
public:
  ObjectiveFunctor(ObjectiveFunc objective,
                   std::optional<GradientFunc> gradient,
                   std::optional<BoxConstraints> constraints,
                   double penalty_weight = 1e8)
    : m_objective(std::move(objective))
    , m_gradient(std::move(gradient))
    , m_constraints(std::move(constraints))
    , m_penalty_weight(penalty_weight) {}

  /**
   * @brief Evaluate objective function (required by ensmallen)
   */
  double Evaluate(const arma::mat& x) const {
    arma::vec params = arma::vectorise(x);
    double obj = m_objective(params);

    // Add penalty for constraint violations
    if (m_constraints.has_value()) {
      obj += ComputePenalty(params);
    }

    // Handle NaN/Inf
    if (!std::isfinite(obj)) {
      return 1e20;
    }

    return obj;
  }

  /**
   * @brief Compute gradient (required by ensmallen)
   */
  void Gradient(const arma::mat& x, arma::mat& g) const {
    arma::vec params = arma::vectorise(x);

    arma::vec grad;
    if (m_gradient.has_value()) {
      grad = m_gradient.value()(params);
    } else {
      grad = NumericalGradient(params);
    }

    // Add penalty gradient
    if (m_constraints.has_value()) {
      grad += PenaltyGradient(params);
    }

    // Handle NaN in gradient
    for (size_t i = 0; i < grad.n_elem; ++i) {
      if (!std::isfinite(grad(i))) {
        grad(i) = 0.0;
      }
    }

    g = arma::reshape(grad, x.n_rows, x.n_cols);
  }

private:
  ObjectiveFunc m_objective;
  std::optional<GradientFunc> m_gradient;
  std::optional<BoxConstraints> m_constraints;
  double m_penalty_weight;

  /**
   * @brief Compute quadratic penalty for constraint violations
   */
  double ComputePenalty(const arma::vec& x) const {
    const auto& bc = m_constraints.value();
    double penalty = 0.0;

    for (size_t i = 0; i < x.n_elem; ++i) {
      if (x(i) < bc.lower(i)) {
        penalty += m_penalty_weight * std::pow(bc.lower(i) - x(i), 2);
      }
      if (x(i) > bc.upper(i)) {
        penalty += m_penalty_weight * std::pow(x(i) - bc.upper(i), 2);
      }
    }

    return penalty;
  }

  /**
   * @brief Gradient of penalty function
   */
  arma::vec PenaltyGradient(const arma::vec& x) const {
    const auto& bc = m_constraints.value();
    arma::vec grad(x.n_elem, arma::fill::zeros);

    for (size_t i = 0; i < x.n_elem; ++i) {
      if (x(i) < bc.lower(i)) {
        grad(i) = -2.0 * m_penalty_weight * (bc.lower(i) - x(i));
      }
      if (x(i) > bc.upper(i)) {
        grad(i) = 2.0 * m_penalty_weight * (x(i) - bc.upper(i));
      }
    }

    return grad;
  }

  /**
   * @brief Numerical gradient via central differences
   */
  arma::vec NumericalGradient(const arma::vec& x, double eps = 1e-7) const {
    arma::vec grad(x.n_elem);

    for (size_t i = 0; i < x.n_elem; ++i) {
      arma::vec x_plus = x;
      arma::vec x_minus = x;
      x_plus(i) += eps;
      x_minus(i) -= eps;

      double f_plus = m_objective(x_plus);
      double f_minus = m_objective(x_minus);

      // Handle NaN
      if (!std::isfinite(f_plus) || !std::isfinite(f_minus)) {
        grad(i) = 0.0;
      } else {
        grad(i) = (f_plus - f_minus) / (2.0 * eps);
      }
    }

    return grad;
  }
};

/**
 * @brief L-BFGS optimizer for time series models
 */
class LBFGSOptimizer {
public:
  /**
   * @brief Minimize objective function using L-BFGS
   *
   * @param objective Function to minimize
   * @param x0 Initial parameter guess
   * @param config Optimizer configuration
   * @param constraints Optional box constraints
   * @param gradient Optional analytical gradient (uses numerical if not provided)
   * @return Optimization result
   */
  static OptimResult Minimize(
      ObjectiveFunc objective,
      const arma::vec& x0,
      const OptimizerConfig& config = {},
      std::optional<BoxConstraints> constraints = std::nullopt,
      std::optional<GradientFunc> gradient = std::nullopt) {

    OptimResult best_result;
    best_result.objective_value = std::numeric_limits<double>::infinity();
    best_result.converged = false;

    // Try multiple restarts
    for (size_t restart = 0; restart < config.num_restarts; ++restart) {
      arma::vec x = x0;

      // Perturb initial point for restarts > 0
      if (restart > 0) {
        x = PerturbInitialPoint(x, constraints, restart);
      }

      // Create functor
      ObjectiveFunctor functor(objective, gradient, constraints,
                               config.constraint_penalty);

      // Configure L-BFGS
      ens::L_BFGS optimizer(
          config.memory_size,      // numBasis
          config.max_iterations,   // maxIterations
          1e-4,                    // armijoConstant
          0.9,                     // wolfe
          config.min_gradient_norm, // minGradientNorm
          config.tolerance         // factr
      );

      // Convert to matrix for ensmallen
      arma::mat x_mat = x;

      // Optimize
      double final_value;
      try {
        final_value = optimizer.Optimize(functor, x_mat);
      } catch (const std::exception& e) {
        // If optimization fails, try next restart
        continue;
      }

      // Convert back to vector
      arma::vec x_final = arma::vectorise(x_mat);

      // Project to feasible region if needed
      if (constraints.has_value()) {
        x_final = constraints->Project(x_final);
        // Re-evaluate objective at projected point
        final_value = objective(x_final);
      }

      // Check if this is the best result
      if (std::isfinite(final_value) && final_value < best_result.objective_value) {
        best_result.params = x_final;
        best_result.objective_value = final_value;
        best_result.iterations = config.max_iterations;  // L-BFGS doesn't expose this
        best_result.converged = true;
        best_result.message = "Converged on restart " + std::to_string(restart);
      }
    }

    if (!best_result.converged) {
      best_result.message = "Failed to converge after " +
                            std::to_string(config.num_restarts) + " restarts";
      // Return initial point as fallback
      if (best_result.params.is_empty()) {
        best_result.params = x0;
        best_result.objective_value = objective(x0);
      }
    }

    return best_result;
  }

private:
  /**
   * @brief Perturb initial point for restart
   */
  static arma::vec PerturbInitialPoint(
      const arma::vec& x0,
      const std::optional<BoxConstraints>& constraints,
      size_t restart_idx) {

    std::mt19937 rng(42 + restart_idx);
    std::normal_distribution<double> dist(0.0, 0.1);

    arma::vec x = x0;
    for (size_t i = 0; i < x.n_elem; ++i) {
      x(i) += x(i) * dist(rng);
    }

    // Project to feasible region
    if (constraints.has_value()) {
      x = constraints->Project(x);
    }

    return x;
  }
};

}  // namespace epoch_script::transform::timeseries
