# Task 10: Mean-Variance Optimization (OSQP)

## Objective
Implement classical Markowitz mean-variance optimization using OSQP quadratic programming.

## Prerequisites
- Task 01 (OSQP Setup)
- Task 03 (Covariance)
- Task 09 (Risk Measures)

## Optimization Problem

**Min Variance:**
```
minimize    (1/2) w'Σw
subject to  Σw_i = 1
            w_min ≤ w ≤ w_max
            μ'w ≥ target_return (optional)
```

**Max Sharpe:**
```
maximize    (μ'w - rf) / sqrt(w'Σw)
```
Solved via Cornuejols-Tutuncu transformation.

## File to Create

### `src/transforms/components/portfolio/optimizers/mean_variance.h`

```cpp
class MeanVarianceOptimizer : public PortfolioOptimizerBase<MeanVarianceOptimizer> {
public:
    arma::vec Optimize(const arma::mat& returns, const arma::mat& cov,
                       const arma::mat& corr, const arma::mat& dist) const {
        // Compute expected returns
        arma::vec mu = arma::mean(returns, 0).t();

        switch (m_objective) {
            case MVOObjective::MinVariance:
                return SolveMinVariance(cov);
            case MVOObjective::MaxSharpe:
                return SolveMaxSharpe(mu, cov, m_risk_free);
            case MVOObjective::TargetReturn:
                return SolveTargetReturn(mu, cov, m_target_return);
        }
    }

private:
    MVOObjective m_objective{MVOObjective::MinVariance};
    double m_risk_free{0.0};
    double m_target_return{0.0};

    arma::vec SolveMinVariance(const arma::mat& cov) const {
        // Use OsqpEigen::Solver
        // P = cov, q = 0
        // A = [ones; I; -I]
        // l = [1; w_min; -w_max], u = [1; w_max; -w_min]
    }
};
```

## OSQP Setup Pattern

```cpp
#include <OsqpEigen/OsqpEigen.h>

Eigen::SparseMatrix<double> H = ConvertToEigenSparse(cov);
Eigen::VectorXd q = Eigen::VectorXd::Zero(N);

OsqpEigen::Solver solver;
solver.settings()->setVerbosity(false);
solver.data()->setNumberOfVariables(N);
solver.data()->setNumberOfConstraints(1 + 2*N);
solver.data()->setHessianMatrix(H);
solver.data()->setGradient(q);
solver.data()->setLinearConstraintsMatrix(A);
solver.data()->setLowerBound(lb);
solver.data()->setUpperBound(ub);

solver.initSolver();
solver.solve();

Eigen::VectorXd solution = solver.getSolution();
```

## Metadata Options
- `objective`: min_variance, max_sharpe, target_return
- `risk_free_rate`: 0.0 (default)
- `target_return`: required if objective=target_return
- `covariance_method`, `min_weight`, `max_weight`

## Cross-Validation
Compare against PyPortfolioOpt `EfficientFrontier`:
- `ef.min_volatility()` for MinVariance
- `ef.max_sharpe()` for MaxSharpe
- Tolerance: rtol=0.02, atol=0.01

## Deliverables
- [ ] `mean_variance.h` with OSQP integration
- [ ] Eigen/Armadillo conversion utilities
- [ ] Metadata and registration
- [ ] Unit tests with PyPortfolioOpt baseline
