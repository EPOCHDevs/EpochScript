# Task 15: Python Baseline Generation

## Objective
Create Python scripts to generate test data and expected outputs using PyPortfolioOpt and Riskfolio-Lib.

## Prerequisites
- None (can run in parallel with C++ implementation)

## Scripts to Create

### `test/unit/transforms/portfolio/portfolio_python_gen/generate_portfolio_expected.py`

```python
#!/usr/bin/env python3
"""Generate portfolio optimization test baselines."""

import numpy as np
import pandas as pd
from pypfopt import HRPOpt, EfficientFrontier, risk_models, expected_returns
from pypfopt.risk_models import CovarianceShrinkage
import riskfolio as rp

SEED = 42
N_SAMPLES = 500
OUT_DIR = "portfolio_test_data"

def make_returns(n_assets, n_samples, seed=SEED):
    """Generate factor + idiosyncratic returns."""
    rng = np.random.default_rng(seed)
    loadings = rng.normal(0, 0.3, (n_assets, 2))
    factors = rng.normal(0.0001, 0.01, (n_samples, 2))
    idio = rng.normal(0, 0.02, (n_samples, n_assets))
    returns = factors @ loadings.T + idio

    cols = [f"asset_{i}" for i in range(n_assets)]
    idx = pd.date_range("2020-01-01", periods=n_samples, freq="D", tz="UTC")
    return pd.DataFrame(returns, index=idx, columns=cols)

def generate_all():
    # Returns datasets
    datasets = {
        "returns_5": make_returns(5, N_SAMPLES),
        "returns_10": make_returns(10, N_SAMPLES),
        "returns_50": make_returns(50, N_SAMPLES),
    }
    for name, df in datasets.items():
        df.to_csv(f"{OUT_DIR}/{name}.csv")

    # Use 10-asset for baselines
    returns = datasets["returns_10"]

    # HRP baselines
    for linkage in ["single", "ward"]:
        hrp = HRPOpt(returns)
        w = hrp.optimize(linkage_method=linkage)
        pd.Series(w).to_csv(f"{OUT_DIR}/hrp_{linkage}_expected.csv")

    # MVO baselines
    mu = expected_returns.mean_historical_return(returns)
    cov = risk_models.sample_cov(returns)

    ef = EfficientFrontier(mu, cov)
    w_minvol = ef.min_volatility()
    pd.Series(w_minvol).to_csv(f"{OUT_DIR}/mvo_minvol_expected.csv")

    ef = EfficientFrontier(mu, cov)
    w_sharpe = ef.max_sharpe()
    pd.Series(w_sharpe).to_csv(f"{OUT_DIR}/mvo_sharpe_expected.csv")

    # Risk Parity (Riskfolio)
    port = rp.Portfolio(returns=returns)
    port.assets_stats(method_mu="hist", method_cov="hist")
    w_rp = port.rp_optimization(model="Classic", rm="MV")
    w_rp.to_csv(f"{OUT_DIR}/risk_parity_expected.csv")

    # HERC (Riskfolio)
    hc = rp.HCPortfolio(returns=returns)
    hc.assets_stats(method_mu="hist", method_cov="hist")
    w_herc = hc.optimization(model="HERC", rm="MV", linkage="ward")
    w_herc.to_csv(f"{OUT_DIR}/herc_expected.csv")

    # Covariance matrices
    cov_sample = returns.cov()
    cov_lw = CovarianceShrinkage(returns).ledoit_wolf()
    cov_sample.to_csv(f"{OUT_DIR}/cov_sample_expected.csv")
    cov_lw.to_csv(f"{OUT_DIR}/cov_ledoit_wolf_expected.csv")

if __name__ == "__main__":
    import os
    os.makedirs(OUT_DIR, exist_ok=True)
    generate_all()
```

## Test Data Files to Generate

| File | Description |
|------|-------------|
| `returns_5.csv` | 5 assets, 500 days |
| `returns_10.csv` | 10 assets, 500 days |
| `returns_50.csv` | 50 assets, 500 days |
| `returns_high_corr.csv` | Highly correlated assets |
| `hrp_ward_expected.csv` | HRP weights (ward linkage) |
| `hrp_single_expected.csv` | HRP weights (single linkage) |
| `mvo_minvol_expected.csv` | Min variance weights |
| `mvo_sharpe_expected.csv` | Max Sharpe weights |
| `risk_parity_expected.csv` | Risk parity weights |
| `herc_expected.csv` | HERC weights |
| `cov_sample_expected.csv` | Sample covariance |
| `cov_ledoit_wolf_expected.csv` | Ledoit-Wolf covariance |

## Running

```bash
cd /home/adesola/EpochLab/EpochScript/test/unit/transforms/portfolio/portfolio_python_gen
pip install pypfopt riskfolio-lib
python generate_portfolio_expected.py
```

## Deliverables
- [ ] `generate_portfolio_expected.py` script
- [ ] All CSV test data files generated
- [ ] Documentation of file formats
