# Portfolio Optimization Category - Task Overview

## Project Summary

Create a new **Portfolio** transform category for EpochScript implementing portfolio optimization algorithms cross-validated against PyPortfolioOpt and Riskfolio-Lib.

## Task Files

| Task | Description | Dependencies | Est. Complexity |
|------|-------------|--------------|-----------------|
| `01_SETUP_OSQP.md` | Install osqp-eigen dependency | None | Low |
| `02_FOUNDATION.md` | Category enum, directory structure, registration | 01 | Medium |
| `03_COVARIANCE_BASIC.md` | Sample covariance, interface, factory | 02 | Low |
| `04_NAIVE_ALLOCATORS.md` | Equal Weight, Inverse Volatility | 02, 03 | Low |
| `05_COVARIANCE_ADVANCED.md` | Ledoit-Wolf, EWMA, Gerber, RMT | 03 | High |
| `06_CLUSTERING_UTILS.md` | Hierarchical clustering, quasi-diag, recursive bisection | 02 | Medium |
| `07_HRP_OPTIMIZER.md` | Hierarchical Risk Parity | 06, 03 | Medium |
| `08_HERC_OPTIMIZER.md` | Hierarchical Equal Risk Contribution | 07, 09 | Medium |
| `09_RISK_MEASURES.md` | Variance, MAD, CVaR, EVaR, CDaR, etc. | 02 | High |
| `10_MEAN_VARIANCE.md` | Mean-Variance Optimization with OSQP | 01, 03, 09 | High |
| `11_RISK_PARITY.md` | Risk Parity / Risk Budgeting | 01, 03, 09 | High |
| `12_MAX_DIVERSIFICATION.md` | Maximum Diversification Ratio | 01, 03 | Medium |
| `13_BLACK_LITTERMAN.md` | Black-Litterman Model | 10, 03 | High |
| `14_ROLLING_OPTIMIZERS.md` | Rolling HRP, MVO, Risk Parity | 07, 10, 11 | Medium |
| `15_PYTHON_BASELINES.md` | Generate test data from PyPortfolioOpt/Riskfolio | None | Medium |
| `16_UNIT_TESTS.md` | All unit tests | 04-14, 15 | High |

## Dependency Graph

```
01_SETUP_OSQP
    │
    ├─────────────────────┬────────────────────┐
    │                     │                    │
02_FOUNDATION         (parallel)           (parallel)
    │
    ├──────────────┬──────────────┐
    │              │              │
03_COV_BASIC    06_CLUSTER    09_RISK
    │              │              │
    ├──────────────┼──────────────┤
    │              │              │
04_NAIVE        07_HRP        10_MVO ←── 01
    │              │              │
05_COV_ADV      08_HERC       11_RP ←── 01
                               │
                            12_MAX_DIV
                               │
                            13_BL
                               │
                            14_ROLLING
                               │
                            16_TESTS ←── 15_BASELINES
```

## Reference Libraries

| Library | Location | Key Files |
|---------|----------|-----------|
| PyPortfolioOpt | `/home/adesola/EpochLab/EpochScript/PyPortfolioOpt/` | `pypfopt/hierarchical_portfolio.py`, `pypfopt/efficient_frontier/` |
| Riskfolio-Lib | `/home/adesola/EpochLab/EpochScript/Riskfolio-Lib/` | `riskfolio/src/Portfolio.py`, `riskfolio/src/HCPortfolio.py` |
| Empyrical | `/home/adesola/EpochLab/EpochScript/include/empyrical/` | Already has CVaR, Sharpe, Sortino, MaxDrawdown |

## Existing Patterns to Follow

| Pattern | Reference File |
|---------|---------------|
| Transform impl | `src/transforms/components/statistics/kmeans.h` |
| Registration | `src/transforms/components/cross_sectional/register.h` |
| Metadata | `src/transforms/components/ml/lightgbm_metadata.h` |
| Rolling window | `src/transforms/components/ml/rolling_ml_base.h` |
| Armadillo bridge | `src/transforms/components/statistics/dataframe_armadillo_utils.h` |
| Test pattern | `test/unit/transforms/ml/rolling_ml_test.cpp` |


| Task | File                      | Size    | Description                              |
  |------|---------------------------|---------|------------------------------------------|
| 00   | 00_OVERVIEW.md            | 3.7 KB  | Project overview, dependency graph       |
| 01   | 01_SETUP_OSQP.md          | 2.7 KB  | OSQP-Eigen installation                  |
| 02   | 02_FOUNDATION.md          | 6.8 KB  | Category enum, directories, registration |
| 03   | 03_COVARIANCE_BASIC.md    | 12.6 KB | Covariance interface + sample covariance |
| 04   | 04_NAIVE_ALLOCATORS.md    | 22.8 KB | Equal Weight + Inverse Volatility        |
| 05   | 05_COVARIANCE_ADVANCED.md | 18.7 KB | Ledoit-Wolf, EWMA, Gerber, RMT           |
| 06   | 06_CLUSTERING_UTILS.md    | 1.7 KB  | Hierarchical clustering utilities        |
| 07   | 07_HRP_OPTIMIZER.md       | 1.6 KB  | Hierarchical Risk Parity                 |
| 08   | 08_HERC_OPTIMIZER.md      | 1.6 KB  | Hierarchical Equal Risk Contribution     |
| 09   | 09_RISK_MEASURES.md       | 2.1 KB  | 10+ risk measures                        |
| 10   | 10_MEAN_VARIANCE.md       | 2.8 KB  | Mean-Variance with OSQP                  |
| 11   | 11_RISK_PARITY.md         | 2.0 KB  | Risk Parity optimization                 |
| 12   | 12_MAX_DIVERSIFICATION.md | 1.6 KB  | Max Diversification Ratio                |
| 13   | 13_BLACK_LITTERMAN.md     | 2.7 KB  | Black-Litterman model                    |
| 14   | 14_ROLLING_OPTIMIZERS.md  | 3.3 KB  | Rolling HRP, MVO, Risk Parity            |
| 15   | 15_PYTHON_BASELINES.md    | 4.0 KB  | Test data generation script              |
| 16   | 16_UNIT_TESTS.md          | 4.4 KB  | Comprehensive unit tests                 |

Recommended Execution Order

1. Parallel Group 1: Tasks 01, 02, 15 (OSQP setup, Foundation, Python baselines)
2. Sequential: Task 03 (Covariance Basic) → Task 04 (Naive Allocators)
3. Parallel Group 2: Tasks 05, 06, 09 (Advanced Cov, Clustering, Risk Measures)
4. Sequential Clustering: Task 07 (HRP) → Task 08 (HERC)
5. Sequential QP: Task 10 (MVO) → Task 11 (RP) → Task 12 (MaxDiv) → Task 13 (BL)
6. Final: Task 14 (Rolling) → Task 16 (Tests)
