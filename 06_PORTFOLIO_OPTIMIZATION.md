# Portfolio Optimization Guide

## Objective

Build a **C++ portfolio optimization engine** that:
1. Matches Riskfolio-Lib's functionality
2. Uses Eigen + Spectra + OSQP (same backend as Riskfolio)
3. Runs at EpochScript time (pre-computation)
4. Outputs weights to `weight_sink()` for runtime execution

Cross-reference tests against PyPortfolioOpt and Riskfolio-Lib for correctness.

---

## Library Stack

| Library | Purpose | Classes Used |
|---------|---------|--------------|
| **Eigen** | Matrix math | `MatrixXd`, `VectorXd`, `SparseMatrix` |
| **Spectra** | Fast eigenvalues | `SymEigsSolver`, `SymEigsShiftSolver` |
| **OSQP** | Quadratic programming | `OsqpSolver`, `OsqpSettings` |
| **mlpack** | Clustering | `AgglomerativeClustering` |
| **Armadillo** | Matrix ops (mlpack dep) | `arma::mat`, `arma::vec` |

---

## Spectra Classes

### What Spectra Provides

```cpp
#include <Spectra/SymEigsSolver.h>
#include <Spectra/SymEigsShiftSolver.h>
#include <Spectra/SymGEigsSolver.h>
```

| Class | Use Case | Portfolio Application |
|-------|----------|----------------------|
| `SymEigsSolver` | Top-k eigenvalues of symmetric matrix | Covariance denoising, PCA, factor models |
| `SymEigsShiftSolver` | Eigenvalues near a target | Finding smallest eigenvalues |
| `SymGEigsSolver` | Generalized eigenvalue problem | Constrained optimization |
| `DavidsonSymEigsSolver` | Large sparse matrices | Huge asset universes (1000+) |

### Usage: Covariance Denoising

```cpp
#include <Spectra/SymEigsSolver.h>
#include <Spectra/MatOp/DenseSymMatProd.h>

Eigen::MatrixXd denoise_covariance(
    const Eigen::MatrixXd& cov,
    int n_factors
) {
    Spectra::DenseSymMatProd<double> op(cov);
    Spectra::SymEigsSolver<decltype(op)> solver(
        op,
        n_factors,           // Number of eigenvalues to compute
        2 * n_factors + 1    // Convergence parameter
    );

    solver.init();
    solver.compute(Spectra::SortRule::LargestAlge);

    if (solver.info() != Spectra::CompInfo::Successful) {
        throw std::runtime_error("Eigenvalue computation failed");
    }

    Eigen::VectorXd eigenvalues = solver.eigenvalues();
    Eigen::MatrixXd eigenvectors = solver.eigenvectors();

    // Reconstruct with top-k factors only
    return eigenvectors * eigenvalues.asDiagonal() * eigenvectors.transpose();
}
```

### Usage: Random Matrix Theory Denoising

```cpp
// Marchenko-Pastur filtering
Eigen::MatrixXd rmt_denoise(
    const Eigen::MatrixXd& corr,
    int n_observations,
    int n_assets
) {
    double q = static_cast<double>(n_observations) / n_assets;
    double lambda_max = std::pow(1.0 + std::sqrt(1.0 / q), 2);

    // Get all eigenvalues
    Spectra::DenseSymMatProd<double> op(corr);
    Spectra::SymEigsSolver<decltype(op)> solver(op, n_assets - 1, n_assets);
    solver.init();
    solver.compute();

    Eigen::VectorXd eigenvalues = solver.eigenvalues();
    Eigen::MatrixXd eigenvectors = solver.eigenvectors();

    // Filter: keep eigenvalues above MP threshold
    for (int i = 0; i < eigenvalues.size(); ++i) {
        if (eigenvalues(i) < lambda_max) {
            eigenvalues(i) = eigenvalues.mean();  // Replace with average
        }
    }

    return eigenvectors * eigenvalues.asDiagonal() * eigenvectors.transpose();
}
```

---

## OSQP Classes

### What OSQP Provides

```cpp
#include <osqp/osqp.h>
// Or with Eigen wrapper:
#include <OsqpEigen/OsqpEigen.h>
```

| Class | Use Case | Portfolio Application |
|-------|----------|----------------------|
| `OsqpSolver` | Quadratic programming | Mean-variance, risk parity, constraints |
| `OsqpSettings` | Solver configuration | Tolerance, iterations, warm start |

### Usage: Mean-Variance Optimization

```cpp
#include <OsqpEigen/OsqpEigen.h>

Eigen::VectorXd mean_variance_optimize(
    const Eigen::MatrixXd& cov,        // Covariance matrix
    const Eigen::VectorXd& mu,          // Expected returns
    double target_return,
    double max_weight = 0.20
) {
    int n = cov.rows();

    // QP: minimize (1/2)w'Σw
    // subject to: μ'w >= target_return
    //             Σw_i = 1
    //             0 <= w_i <= max_weight

    // Hessian (P matrix)
    Eigen::SparseMatrix<double> P = cov.sparseView();

    // Linear term (q vector) - zero for min variance
    Eigen::VectorXd q = Eigen::VectorXd::Zero(n);

    // Constraints: [mu'; ones'; I; -I] * w
    // Bounds: [target; 1; zeros; -max_weights]

    Eigen::SparseMatrix<double> A(2 + 2*n, n);
    std::vector<Eigen::Triplet<double>> triplets;

    // Return constraint: μ'w >= target
    for (int i = 0; i < n; ++i) {
        triplets.emplace_back(0, i, mu(i));
    }

    // Sum constraint: Σw = 1
    for (int i = 0; i < n; ++i) {
        triplets.emplace_back(1, i, 1.0);
    }

    // Upper bounds: w <= max_weight
    for (int i = 0; i < n; ++i) {
        triplets.emplace_back(2 + i, i, 1.0);
    }

    // Lower bounds: w >= 0
    for (int i = 0; i < n; ++i) {
        triplets.emplace_back(2 + n + i, i, -1.0);
    }

    A.setFromTriplets(triplets.begin(), triplets.end());

    // Bounds
    Eigen::VectorXd lower(2 + 2*n);
    Eigen::VectorXd upper(2 + 2*n);

    lower << target_return, 1.0, Eigen::VectorXd::Zero(n), -Eigen::VectorXd::Constant(n, max_weight);
    upper << INFINITY, 1.0, Eigen::VectorXd::Constant(n, max_weight), Eigen::VectorXd::Zero(n);

    // Solve
    OsqpEigen::Solver solver;
    solver.settings()->setVerbosity(false);
    solver.settings()->setWarmStart(true);
    solver.data()->setNumberOfVariables(n);
    solver.data()->setNumberOfConstraints(2 + 2*n);
    solver.data()->setHessianMatrix(P);
    solver.data()->setGradient(q);
    solver.data()->setLinearConstraintsMatrix(A);
    solver.data()->setLowerBound(lower);
    solver.data()->setUpperBound(upper);

    solver.initSolver();
    solver.solve();

    return solver.getSolution();
}
```

### Usage: Risk Parity

```cpp
Eigen::VectorXd risk_parity_optimize(
    const Eigen::MatrixXd& cov,
    int max_iterations = 1000,
    double tol = 1e-8
) {
    int n = cov.rows();
    Eigen::VectorXd w = Eigen::VectorXd::Constant(n, 1.0 / n);  // Start equal

    // Newton iteration for risk parity
    for (int iter = 0; iter < max_iterations; ++iter) {
        Eigen::VectorXd sigma_w = cov * w;
        double port_vol = std::sqrt(w.dot(sigma_w));

        // Risk contribution: RC_i = w_i * (Σw)_i / σ_p
        Eigen::VectorXd rc = (w.array() * sigma_w.array()) / port_vol;

        // Target: equal risk contribution
        double target_rc = port_vol / n;
        Eigen::VectorXd rc_diff = rc.array() - target_rc;

        if (rc_diff.norm() < tol) break;

        // Update weights (simplified gradient step)
        Eigen::VectorXd grad = 2.0 * (sigma_w.array() * rc_diff.array()) / port_vol;
        w = w - 0.5 * grad;

        // Project to simplex (sum = 1, w >= 0)
        w = w.cwiseMax(1e-8);
        w = w / w.sum();
    }

    return w;
}
```

---

## mlpack Classes

### What mlpack Provides

```cpp
#include <mlpack/methods/neighbor_search/neighbor_search.hpp>
#include <mlpack/core.hpp>
```

| Class | Use Case | Portfolio Application |
|-------|----------|----------------------|
| `AgglomerativeClustering` | Hierarchical clustering | HRP, HERC |
| `KMeans` | K-means clustering | Alternative clustering |
| `GaussianDistribution` | Covariance estimation | Shrinkage estimators |

### Usage: Hierarchical Clustering for HRP

```cpp
#include <mlpack/core.hpp>
#include <mlpack/methods/approx_kfn/approx_kfn.hpp>

arma::Mat<size_t> hierarchical_cluster(
    const Eigen::MatrixXd& distance_matrix,
    const std::string& linkage = "ward"
) {
    // Convert Eigen to Armadillo
    arma::mat dist(distance_matrix.data(),
                   distance_matrix.rows(),
                   distance_matrix.cols());

    // Perform agglomerative clustering
    arma::Mat<size_t> dendrogram;

    mlpack::hc::AgglomerativeClustering ac;
    ac.Cluster(dist, dendrogram);

    return dendrogram;
}
```

---

## Complete HRP Implementation

```cpp
#include <Eigen/Dense>
#include <mlpack/core.hpp>
#include <Spectra/SymEigsSolver.h>

class HRPOptimizer {
public:
    Eigen::VectorXd optimize(const Eigen::MatrixXd& returns) {
        int n_assets = returns.cols();

        // 1. Compute correlation matrix
        Eigen::MatrixXd corr = compute_correlation(returns);

        // 2. Distance matrix: d = sqrt((1 - corr) / 2)
        Eigen::MatrixXd dist = ((1.0 - corr.array()) / 2.0).sqrt();

        // 3. Hierarchical clustering
        auto dendrogram = hierarchical_cluster(dist);

        // 4. Quasi-diagonalization
        std::vector<int> sorted_idx = quasi_diagonalize(dendrogram, n_assets);

        // 5. Compute covariance
        Eigen::MatrixXd cov = compute_covariance(returns);

        // 6. Recursive bisection
        Eigen::VectorXd weights = recursive_bisection(cov, sorted_idx);

        return weights;
    }

private:
    std::vector<int> quasi_diagonalize(
        const arma::Mat<size_t>& dendrogram,
        int n_assets
    ) {
        // Traverse dendrogram, reorder assets by cluster proximity
        std::vector<int> order;
        order.reserve(n_assets);

        // Start from root, recursively add leaves in order
        std::function<void(int)> traverse = [&](int node) {
            if (node < n_assets) {
                order.push_back(node);
            } else {
                int idx = node - n_assets;
                traverse(dendrogram(0, idx));  // Left child
                traverse(dendrogram(1, idx));  // Right child
            }
        };

        traverse(2 * n_assets - 2);  // Root node
        return order;
    }

    Eigen::VectorXd recursive_bisection(
        const Eigen::MatrixXd& cov,
        const std::vector<int>& sorted_idx
    ) {
        int n = sorted_idx.size();
        Eigen::VectorXd weights = Eigen::VectorXd::Ones(n);

        std::function<void(int, int, double)> bisect = [&](int start, int end, double weight) {
            if (end - start <= 1) {
                weights(sorted_idx[start]) = weight;
                return;
            }

            int mid = (start + end) / 2;

            // Compute cluster variances
            double var_left = cluster_variance(cov, sorted_idx, start, mid);
            double var_right = cluster_variance(cov, sorted_idx, mid, end);

            // Allocate inversely proportional to variance
            double alpha = var_right / (var_left + var_right);

            bisect(start, mid, weight * alpha);
            bisect(mid, end, weight * (1 - alpha));
        };

        bisect(0, n, 1.0);
        return weights;
    }

    double cluster_variance(
        const Eigen::MatrixXd& cov,
        const std::vector<int>& idx,
        int start, int end
    ) {
        int n = end - start;
        Eigen::VectorXd w = Eigen::VectorXd::Constant(n, 1.0 / n);

        double var = 0.0;
        for (int i = start; i < end; ++i) {
            for (int j = start; j < end; ++j) {
                var += w(i - start) * w(j - start) * cov(idx[i], idx[j]);
            }
        }
        return var;
    }

    Eigen::MatrixXd compute_correlation(const Eigen::MatrixXd& returns) {
        Eigen::MatrixXd centered = returns.rowwise() - returns.colwise().mean();
        Eigen::MatrixXd cov = (centered.transpose() * centered) / (returns.rows() - 1);

        Eigen::VectorXd std_dev = cov.diagonal().array().sqrt();
        return cov.array() / (std_dev * std_dev.transpose()).array();
    }

    Eigen::MatrixXd compute_covariance(const Eigen::MatrixXd& returns) {
        Eigen::MatrixXd centered = returns.rowwise() - returns.colwise().mean();
        return (centered.transpose() * centered) / (returns.rows() - 1);
    }
};
```

---

## EpochScript Interface

### Optimizer Category

```python
# Weight allocation optimizers - all return weights for weight_sink()

# Naive Allocators
def equal_weight(active: bool) -> float:
    """1/N allocation across active assets"""
    pass

def inv_vol_weight(active: bool, volatility: float) -> float:
    """Inverse volatility weighting"""
    pass

# Clustering-Based Optimizers
def hrp_weight(
    active: bool,
    returns: DataFrame,
    linkage: str = "ward",           # "single", "complete", "average", "ward"
    covariance: str = "sample",      # "sample", "ledoit_wolf", "oracle"
    denoise: bool = False            # Apply RMT denoising
) -> float:
    """Hierarchical Risk Parity"""
    pass

def herc_weight(
    active: bool,
    returns: DataFrame,
    risk_measure: str = "variance",  # "variance", "cvar", "mad"
    linkage: str = "ward"
) -> float:
    """Hierarchical Equal Risk Contribution"""
    pass

# Mean-Risk Optimizers
def mean_variance_weight(
    active: bool,
    returns: DataFrame,
    objective: str = "min_variance",  # "min_variance", "max_sharpe", "max_return"
    target_return: float = None,
    target_risk: float = None,
    max_weight: float = 1.0,
    min_weight: float = 0.0
) -> float:
    """Classical mean-variance optimization"""
    pass

def risk_parity_weight(
    active: bool,
    returns: DataFrame,
    risk_measure: str = "variance",  # "variance", "cvar", "mad"
    risk_budget: list[float] = None  # Custom risk budgets (default: equal)
) -> float:
    """Risk parity / risk budgeting"""
    pass

def max_diversification_weight(
    active: bool,
    returns: DataFrame
) -> float:
    """Maximum diversification ratio"""
    pass

# Black-Litterman
def black_litterman_weight(
    active: bool,
    returns: DataFrame,
    views: dict,                      # {"AAPL": 0.10, "GOOGL": 0.05}
    view_confidences: dict = None,
    tau: float = 0.05
) -> float:
    """Black-Litterman allocation"""
    pass
```

### Usage Examples

```python
# Example 1: HRP on momentum stocks
momentum = roc(close, 20)
top_20 = cs_rank(momentum) <= 20

weights = hrp_weight(
    active=top_20,
    returns=pct_change(close, 1),
    linkage="ward",
    denoise=True
)
weight_sink(weights)


# Example 2: Risk parity with CVaR
active = close > sma(close, 200)

weights = risk_parity_weight(
    active=active,
    returns=pct_change(close, 1),
    risk_measure="cvar"
)
weight_sink(weights)


# Example 3: Mean-variance with constraints
weights = mean_variance_weight(
    active=True,  # All assets
    returns=pct_change(close, 1),
    objective="max_sharpe",
    max_weight=0.20,  # 20% max per position
    min_weight=0.02   # 2% min per position
)
weight_sink(weights)


# Example 4: Equal weight (simplest)
top_10 = cs_rank(score) <= 10
weights = equal_weight(top_10)
weight_sink(weights)
```

---

## Covariance Estimation Options

```python
def covariance_estimator(
    returns: DataFrame,
    method: str = "sample",      # Estimation method
    denoise: bool = False,       # RMT denoising
    detone: bool = False         # Remove market factor
) -> CovarianceMatrix:
    """
    Methods:
    - "sample": Standard sample covariance
    - "ledoit_wolf": Shrinkage estimator (Ledoit-Wolf)
    - "oracle": Oracle approximating shrinkage
    - "ewma": Exponentially weighted (lambda=0.94)
    - "gerber": Gerber statistic (robust to outliers)
    """
    pass
```

---

## Testing Strategy

### Cross-Reference with Python

```python
# tests/test_hrp.py
import numpy as np
from pypfopt import HRPOpt
import riskfolio as rp

def test_hrp_matches_reference():
    returns = load_test_returns()

    # PyPortfolioOpt reference
    hrp = HRPOpt(returns)
    py_weights = hrp.optimize()

    # C++ implementation via pybind
    cpp_weights = epoch_stratifyx.hrp_weight(returns)

    np.testing.assert_allclose(cpp_weights, py_weights, rtol=1e-5)


def test_risk_parity_matches_riskfolio():
    returns = load_test_returns()

    # Riskfolio reference
    port = rp.Portfolio(returns=returns)
    rf_weights = port.rp_optimization(model='Classic', rm='MV')

    # C++ implementation
    cpp_weights = epoch_stratifyx.risk_parity_weight(returns)

    np.testing.assert_allclose(cpp_weights, rf_weights, rtol=1e-4)
```

---

## Summary

### Libraries & Classes

| Library | Classes | Purpose |
|---------|---------|---------|
| **Eigen** | `MatrixXd`, `VectorXd`, `SparseMatrix` | All matrix operations |
| **Spectra** | `SymEigsSolver` | Covariance denoising, PCA |
| **OSQP** | `OsqpSolver` | Mean-variance, risk parity constraints |
| **mlpack** | `AgglomerativeClustering` | HRP, HERC clustering |

### EpochScript Optimizer Functions

| Function | Algorithm | Needs OSQP? | Needs Spectra? |
|----------|-----------|-------------|----------------|
| `equal_weight()` | 1/N | ❌ | ❌ |
| `inv_vol_weight()` | 1/σ | ❌ | ❌ |
| `hrp_weight()` | HRP | ❌ | Optional (denoise) |
| `herc_weight()` | HERC | ❌ | Optional |
| `risk_parity_weight()` | Risk Parity | Optional | ❌ |
| `mean_variance_weight()` | MVO | ✅ | Optional |
| `max_diversification_weight()` | Max Div | ✅ | ❌ |
| `black_litterman_weight()` | B-L | ✅ | ❌ |

### Testing References

| Library | Use For |
|---------|---------|
| PyPortfolioOpt | HRP, mean-variance, simple cases |
| Riskfolio-Lib | Risk parity, HERC, advanced risk measures |
