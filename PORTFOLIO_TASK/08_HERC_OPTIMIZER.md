# Task 08: HERC (Hierarchical Equal Risk Contribution) Optimizer

## Objective
Implement HERC which extends HRP with risk-based allocation within clusters.

## Prerequisites
- Task 07 (HRP)
- Task 09 (Risk Measures)

## Algorithm
1. Same clustering/quasi-diag as HRP
2. **Inter-cluster weights**: Inversely proportional to cluster risk
3. **Intra-cluster weights**: Risk parity within each cluster
4. Final weights = intra * inter

## Difference from HRP
- HRP: Simple inverse-variance at each bisection level
- HERC: Uses chosen risk measure (Variance, CVaR, MAD) for allocation

## File to Create

### `src/transforms/components/portfolio/optimizers/herc.h`

```cpp
class HERCOptimizer : public PortfolioOptimizerBase<HERCOptimizer> {
public:
    arma::vec Optimize(const arma::mat& returns, const arma::mat& cov,
                       const arma::mat& corr, const arma::mat& dist) const {
        // 1. Cluster and quasi-diagonalize (same as HRP)
        // 2. Identify optimal number of clusters (k)
        // 3. Inter-cluster: allocate by risk measure
        // 4. Intra-cluster: equal risk within cluster
        // 5. Combine
    }
private:
    RiskMeasure m_risk_measure{RiskMeasure::Variance};
    int m_max_clusters{10};
};
```

## Metadata Options
- All HRP options plus:
- `risk_measure`: variance (default), cvar, mad

## Cross-Validation
Compare against Riskfolio-Lib `HCPortfolio.optimization(model="HERC")`:
- Tolerance: rtol=0.05, atol=0.02

## Deliverables
- [ ] `herc.h` implementation
- [ ] Metadata in `portfolio_metadata.h`
- [ ] Register in `register.h`
- [ ] Unit tests with Riskfolio-Lib baseline
