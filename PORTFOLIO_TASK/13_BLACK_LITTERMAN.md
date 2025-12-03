# Task 13: Black-Litterman Model

## Objective
Implement the Black-Litterman Bayesian portfolio allocation model.

## Prerequisites
- Task 10 (Mean-Variance Optimization)
- Task 03 (Covariance)

## Black-Litterman Framework

**Equilibrium Returns (CAPM implied):**
```
π = δ * Σ * w_mkt
where δ = risk aversion, w_mkt = market cap weights
```

**Posterior Returns (combining views):**
```
μ_BL = [(τΣ)^-1 + P'Ω^-1 P]^-1 * [(τΣ)^-1 π + P'Ω^-1 Q]
```

where:
- P = K x N view matrix (which assets affected)
- Q = K x 1 view returns
- Ω = K x K view uncertainty (confidence)
- τ = scalar (typically 0.025-0.05)

**Final Optimization:**
Use posterior μ_BL in mean-variance optimization.

## File to Create

### `src/transforms/components/portfolio/optimizers/black_litterman.h`

```cpp
class BlackLittermanOptimizer : public PortfolioOptimizerBase<BlackLittermanOptimizer> {
public:
    arma::vec Optimize(const arma::mat& returns, const arma::mat& cov,
                       const arma::mat& corr, const arma::mat& dist) const {
        // 1. Compute equilibrium returns (reverse optimization)
        arma::vec pi = ComputeEquilibriumReturns(cov, m_market_weights, m_delta);

        // 2. If no views, use equilibrium
        if (m_views.empty()) {
            return SolveMVO(pi, cov);
        }

        // 3. Construct view matrices P, Q, Omega
        auto [P, Q, Omega] = ConstructViewMatrices();

        // 4. Compute posterior returns
        arma::vec mu_bl = ComputePosteriorReturns(pi, cov, P, Q, Omega, m_tau);

        // 5. Optimize using posterior
        return SolveMVO(mu_bl, cov);
    }

private:
    double m_delta{2.5};           // Risk aversion
    double m_tau{0.05};            // Uncertainty in equilibrium
    arma::vec m_market_weights;    // Market cap weights (or equal)
    std::vector<View> m_views;     // User-specified views
};

struct View {
    std::string asset;
    double expected_return;
    double confidence;  // 0-1
};
```

## View Specification
Views passed as options:
```yaml
views:
  - asset: AAPL
    return: 0.10  # 10% expected
    confidence: 0.8
  - asset: GOOGL
    return: 0.05
    confidence: 0.5
```

## Metadata Options
- `tau`: 0.05 (default)
- `delta`: 2.5 (default risk aversion)
- `views`: list of view specifications
- `covariance_method`, `min_weight`, `max_weight`

## Cross-Validation
Compare against PyPortfolioOpt `BlackLittermanModel`:
- Tolerance: rtol=0.05, atol=0.02

## Deliverables
- [ ] `black_litterman.h` implementation
- [ ] View matrix construction
- [ ] Posterior return computation
- [ ] Integration with MVO
- [ ] Metadata and registration
- [ ] Unit tests with PyPortfolioOpt baseline
