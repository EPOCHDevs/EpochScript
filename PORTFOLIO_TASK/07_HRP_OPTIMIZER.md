# Task 07: HRP (Hierarchical Risk Parity) Optimizer

## Objective
Implement Lopez de Prado's Hierarchical Risk Parity algorithm.

## Prerequisites
- Task 03 (Covariance)
- Task 06 (Clustering Utils)

## Algorithm (4 Steps)
1. Compute correlation/distance matrix from returns
2. Hierarchical clustering on distance matrix
3. Quasi-diagonalization (reorder by cluster proximity)
4. Recursive bisection (allocate inversely to variance)

## File to Create

### `src/transforms/components/portfolio/optimizers/hrp.h`

```cpp
class HRPOptimizer : public PortfolioOptimizerBase<HRPOptimizer> {
public:
    arma::vec Optimize(const arma::mat& returns, const arma::mat& cov,
                       const arma::mat& corr, const arma::mat& dist) const {
        // 1. Hierarchical clustering
        HierarchicalClustering clustering(m_linkage);
        auto dendrogram = clustering.Cluster(dist);

        // 2. Quasi-diagonalization
        auto sorted_idx = QuasiDiagonalize(dendrogram, cov.n_cols);

        // 3. Recursive bisection
        return RecursiveBisection(cov, sorted_idx);
    }
private:
    LinkageType m_linkage{LinkageType::Ward};
};
```

## Metadata Options
- `linkage`: ward (default), single, complete, average
- `covariance_method`: sample, ledoit_wolf, ewma, gerber, rmt
- `min_weight`, `max_weight`, `long_only`

## Cross-Validation
Compare against PyPortfolioOpt `HRPOpt.optimize()`:
- Tolerance: rtol=0.05, atol=0.02 (clustering can vary)

## Deliverables
- [ ] `hrp.h` implementation
- [ ] Metadata in `portfolio_metadata.h`
- [ ] Register in `register.h`
- [ ] Unit tests with PyPortfolioOpt baseline
