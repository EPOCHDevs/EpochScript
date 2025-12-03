# Task 06: Clustering Utilities for HRP/HERC

## Objective
Implement hierarchical clustering utilities using mlpack's AgglomerativeClustering.

## Prerequisites
- Task 02 (Foundation)

## Files to Create

### 1. `src/transforms/components/portfolio/utils/hierarchical_clustering.h`
- Wrap `mlpack::AgglomerativeClustering`
- Support linkage types: Ward, Single, Complete, Average
- Input: N x N distance matrix
- Output: Dendrogram as (N-1) x 3 matrix [left, right, distance]

### 2. `src/transforms/components/portfolio/utils/quasi_diagonalization.h`
- Traverse dendrogram to reorder assets
- Output: Vector of asset indices in quasi-diagonal order
- Algorithm: Preorder traversal of binary tree

### 3. `src/transforms/components/portfolio/utils/recursive_bisection.h`
- Split asset set recursively at cluster boundaries
- Allocate weights inversely proportional to cluster variance
- Input: Covariance matrix, sorted indices
- Output: Weight vector

## Key Implementation Details

```cpp
enum class LinkageType { Ward, Single, Complete, Average };

// Clustering wrapper
class HierarchicalClustering {
    arma::mat Cluster(const arma::mat& dist) const;  // Returns dendrogram
};

// Quasi-diagonalization
std::vector<size_t> QuasiDiagonalize(const arma::mat& dendrogram, size_t n_assets);

// Recursive bisection
arma::vec RecursiveBisection(const arma::mat& cov, const std::vector<size_t>& sorted_idx);
```

## Reference
- PyPortfolioOpt: `pypfopt/hierarchical_portfolio.py` lines 80-150
- Uses scipy.cluster.hierarchy.linkage internally

## Deliverables
- [ ] `hierarchical_clustering.h` with mlpack wrapper
- [ ] `quasi_diagonalization.h` with tree traversal
- [ ] `recursive_bisection.h` with weight allocation
- [ ] Unit tests for each utility
