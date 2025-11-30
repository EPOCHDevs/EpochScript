# HClust Integration Plan

## Overview

Hierarchical clustering (hclust) for cross-sectional analysis at rebalance frequency. Primary use case: Hierarchical Risk Parity (HRP) position sizing.

## Architecture

```
EpochScript (Precompute & Store)          StratifyX (Consume & Execute)
─────────────────────────────────         ─────────────────────────────

At each rebalance timestamp:              At rebalance time:
1. Compute correlation matrix             1. Fetch precomputed hclust results
2. Compute distance matrix                2. Run HRP algorithm → weights
3. Run hclust → tree structure            3. Execute rebalance
4. Store results
   → Reports: heatmap, dendrogram
   → Data: for StratifyX HRP
```

## Storage Structure

```cpp
struct HClustResult {
    // Timestamp
    int64_t rebalance_ts;

    // Asset list (ordering matters)
    std::vector<std::string> assets;  // ["AAPL", "MSFT", "GOOG", ...]

    // Core hclust output (for HRP)
    std::vector<int> merge;           // 2*(n-1) elements - which clusters merged
    std::vector<double> height;       // n-1 elements - distance at each merge
    std::vector<int> order;           // n elements - leaf ordering (quasi-diagonalization)

    // For reports
    std::vector<double> correlation_matrix;  // n*n
    std::vector<int> cluster_labels;         // n elements (at chosen k)
};
```

## Consumer Requirements

| Consumer | Fields Needed |
|----------|---------------|
| **StratifyX HRP** | `assets`, `merge`, `height`, `order` |
| **Dendrogram chart** | `assets`, `merge`, `height` |
| **Heatmap chart** | `assets`, `correlation_matrix`, `order` (for sorting) |
| **Cluster breakdown** | `assets`, `cluster_labels` |

## Report Outputs

| Report | Visualization | Finance Use |
|--------|---------------|-------------|
| Dendrogram | Tree chart | Asset similarity structure, diversification insight |
| Correlation Heatmap | 2D heatmap (ordered by tree) | Correlation regime, sector relationships |
| Cluster Labels | Colored groups | Asset groupings at chosen granularity |

## Conceptual Pipeline (Future)

```yaml
rebalance_freq: "1w"

cross_sectional:
  - correlation_matrix:
      input: returns
      window: 252
  - distance_matrix:
      input: correlation_matrix
      method: "1 - abs(corr)"
  - hclust:
      input: distance_matrix
      linkage: "ward"
      outputs:
        - dendrogram → report
        - cluster_labels → report
        - tree_structure → stratifyx
```

## HRP Algorithm (StratifyX Side)

1. **Cluster assets** - using precomputed hclust tree
2. **Quasi-diagonalize** - reorder correlation matrix by tree order
3. **Recursive bisection** - split capital top-down through tree
4. **Result** - diversified weights respecting correlation structure

Reference: Marcos López de Prado, "Building Diversified Portfolios that Outperform Out-of-Sample"

## hclust-cpp Usage

```cpp
#include "fastcluster.h"

// Input: condensed distance matrix (upper triangle, n*(n-1)/2 elements)
std::vector<double> distmat = compute_distances(correlation_matrix, n);

// Output buffers
std::vector<int> merge(2 * (n - 1));
std::vector<double> height(n - 1);

// Run clustering (ward linkage recommended for HRP)
hclust_fast(n, distmat.data(), HCLUST_METHOD_WARD, merge.data(), height.data());

// Get leaf ordering for quasi-diagonalization
std::vector<int> order(n);
hclust_reorder(n, merge.data(), height.data(), order.data());

// Optional: cut tree at k clusters
std::vector<int> labels(n);
cutree_k(n, merge.data(), k, labels.data());
```

## Linkage Methods Available

| Method | Constant | Use Case |
|--------|----------|----------|
| Single | `HCLUST_METHOD_SINGLE` | Chaining, sensitive to outliers |
| Complete | `HCLUST_METHOD_COMPLETE` | Compact clusters |
| Average | `HCLUST_METHOD_AVERAGE` | Balanced |
| Ward | `HCLUST_METHOD_WARD` | **Recommended for HRP** - minimizes variance |

## Status

- [x] Library linked via CPM (`ML::hclust`)
- [ ] Cross-sectional transform implementation
- [ ] Storage format finalization
- [ ] Report integration (dendrogram, heatmap)
- [ ] StratifyX HRP consumer