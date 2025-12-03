# Task 12: Maximum Diversification Optimizer

## Objective
Implement Maximum Diversification Ratio portfolio.

## Prerequisites
- Task 01 (OSQP Setup)
- Task 03 (Covariance)

## Maximum Diversification Definition

**Diversification Ratio:**
```
DR = (σ'w) / sqrt(w'Σw)
    = (weighted avg volatility) / (portfolio volatility)
```

**Optimization:**
```
maximize    σ'w / sqrt(w'Σw)
subject to  Σw_i = 1
            w ≥ 0
```

## Equivalent QP Formulation

Transform to minimize portfolio variance for unit weighted volatility:
```
minimize    w'Σw
subject to  σ'w = 1  (or Σw_i = 1 with normalization)
            w ≥ 0
```

Then normalize: w_final = w / sum(w)

## File to Create

### `src/transforms/components/portfolio/optimizers/max_diversification.h`

```cpp
class MaxDiversificationOptimizer : public PortfolioOptimizerBase<MaxDiversificationOptimizer> {
public:
    arma::vec Optimize(const arma::mat& returns, const arma::mat& cov,
                       const arma::mat& corr, const arma::mat& dist) const {
        // Get volatilities
        arma::vec sigma = arma::sqrt(cov.diag());

        // Solve QP: min w'Σw s.t. σ'w = 1, w >= 0
        // Using OSQP...

        // Normalize to sum to 1
        return w / arma::accu(w);
    }
};
```

## Metadata Options
- `covariance_method`: sample, ledoit_wolf, etc.
- `min_weight`, `max_weight`, `long_only`

## Cross-Validation
Compare against Riskfolio-Lib or manual Python implementation:
- Tolerance: rtol=0.03, atol=0.015

## Deliverables
- [ ] `max_diversification.h` implementation
- [ ] Metadata and registration
- [ ] Unit tests
