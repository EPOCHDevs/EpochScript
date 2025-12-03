#!/usr/bin/env python3
"""
Generate portfolio optimization test baselines using PyPortfolioOpt and Riskfolio-Lib.

This script generates:
1. Synthetic return datasets (5, 10, 50 assets)
2. Expected weights from various optimizers (HRP, MVO, Risk Parity, HERC)
3. Expected covariance matrices (Sample, Ledoit-Wolf)

Usage:
    cd /home/adesola/EpochLab/EpochScript/test/unit/transforms/portfolio/portfolio_python_gen
    pip install pypfopt riskfolio-lib
    python generate_portfolio_expected.py
"""

import os
import numpy as np
import pandas as pd

# PyPortfolioOpt imports
from pypfopt import HRPOpt, EfficientFrontier, risk_models, expected_returns
from pypfopt.risk_models import CovarianceShrinkage

# Riskfolio-Lib imports
import riskfolio as rp

# Constants
SEED = 42
N_SAMPLES = 500
OUT_DIR = "portfolio_test_data"


def make_returns(n_assets: int, n_samples: int, seed: int = SEED) -> pd.DataFrame:
    """
    Generate synthetic returns using a factor model.

    Returns = Factor Loadings @ Factor Returns + Idiosyncratic Returns

    This creates realistic correlation structure across assets.

    Args:
        n_assets: Number of assets
        n_samples: Number of time periods
        seed: Random seed for reproducibility

    Returns:
        DataFrame with dates as index and asset_0, asset_1, ... as columns
    """
    rng = np.random.default_rng(seed)

    # 2 common factors (e.g., market + size)
    n_factors = 2
    loadings = rng.normal(0, 0.3, (n_assets, n_factors))

    # Factor returns (small mean, ~1% daily vol)
    factors = rng.normal(0.0001, 0.01, (n_samples, n_factors))

    # Idiosyncratic returns (~2% daily vol)
    idio = rng.normal(0, 0.02, (n_samples, n_assets))

    # Combine: common factors + idiosyncratic
    returns = factors @ loadings.T + idio

    # Create DataFrame with proper index and columns
    cols = [f"asset_{i}" for i in range(n_assets)]
    idx = pd.date_range("2020-01-01", periods=n_samples, freq="D", tz="UTC")

    return pd.DataFrame(returns, index=idx, columns=cols)


def make_high_corr_returns(n_assets: int, n_samples: int, seed: int = SEED) -> pd.DataFrame:
    """
    Generate highly correlated returns (stress test for covariance estimation).

    Uses a single dominant factor with high loadings.
    """
    rng = np.random.default_rng(seed)

    # Single dominant factor
    factor = rng.normal(0.0001, 0.015, (n_samples, 1))

    # High loadings (0.8-1.2)
    loadings = rng.uniform(0.8, 1.2, (n_assets, 1))

    # Small idiosyncratic noise
    idio = rng.normal(0, 0.005, (n_samples, n_assets))

    returns = factor @ loadings.T + idio

    cols = [f"asset_{i}" for i in range(n_assets)]
    idx = pd.date_range("2020-01-01", periods=n_samples, freq="D", tz="UTC")

    return pd.DataFrame(returns, index=idx, columns=cols)


def generate_hrp_baselines(returns: pd.DataFrame) -> dict:
    """Generate HRP baselines for different linkage methods."""
    baselines = {}

    for linkage in ["single", "ward", "complete", "average"]:
        hrp = HRPOpt(returns)
        weights = hrp.optimize(linkage_method=linkage)
        baselines[f"hrp_{linkage}"] = pd.Series(weights)

    return baselines


def generate_mvo_baselines(returns: pd.DataFrame) -> dict:
    """Generate Mean-Variance Optimization baselines."""
    baselines = {}

    # Expected returns and covariance
    mu = expected_returns.mean_historical_return(returns)
    cov = risk_models.sample_cov(returns)

    # Min volatility
    ef = EfficientFrontier(mu, cov)
    w_minvol = ef.min_volatility()
    baselines["mvo_minvol"] = pd.Series(w_minvol)

    # Max Sharpe (risk-free rate = 0)
    ef = EfficientFrontier(mu, cov)
    w_sharpe = ef.max_sharpe(risk_free_rate=0)
    baselines["mvo_sharpe"] = pd.Series(w_sharpe)

    return baselines


def generate_risk_parity_baselines(returns: pd.DataFrame) -> dict:
    """Generate Risk Parity baselines using Riskfolio-Lib."""
    baselines = {}

    # Create portfolio object
    port = rp.Portfolio(returns=returns)
    port.assets_stats(method_mu="hist", method_cov="hist")

    # Risk Parity with different risk measures
    for rm in ["MV", "MAD", "CVaR"]:
        try:
            w_rp = port.rp_optimization(model="Classic", rm=rm, hist=True)
            if w_rp is not None:
                baselines[f"risk_parity_{rm.lower()}"] = w_rp.squeeze()
        except Exception as e:
            print(f"Warning: Risk Parity {rm} failed: {e}")

    return baselines


def generate_herc_baselines(returns: pd.DataFrame) -> dict:
    """Generate HERC (Hierarchical Equal Risk Contribution) baselines."""
    baselines = {}

    hc = rp.HCPortfolio(returns=returns)
    hc.assets_stats(method_mu="hist", method_cov="hist")

    for linkage in ["ward", "single"]:
        try:
            w_herc = hc.optimization(model="HERC", rm="MV", linkage=linkage)
            if w_herc is not None:
                baselines[f"herc_{linkage}"] = w_herc.squeeze()
        except Exception as e:
            print(f"Warning: HERC {linkage} failed: {e}")

    return baselines


def generate_covariance_baselines(returns: pd.DataFrame) -> dict:
    """Generate covariance matrix baselines."""
    baselines = {}

    # Sample covariance
    baselines["cov_sample"] = returns.cov()

    # Ledoit-Wolf shrinkage
    cs = CovarianceShrinkage(returns)
    baselines["cov_ledoit_wolf"] = cs.ledoit_wolf()

    # Oracle Approximating Shrinkage (OAS)
    try:
        baselines["cov_oracle"] = cs.oracle_approximating()
    except Exception as e:
        print(f"Warning: Oracle shrinkage failed: {e}")

    return baselines


def generate_equal_weight_baseline(n_assets: int) -> pd.Series:
    """Generate equal weight baseline (trivial 1/N)."""
    weights = {f"asset_{i}": 1.0 / n_assets for i in range(n_assets)}
    return pd.Series(weights)


def generate_inverse_vol_baseline(returns: pd.DataFrame) -> pd.Series:
    """Generate inverse volatility baseline."""
    vols = returns.std()
    inv_vols = 1.0 / vols
    weights = inv_vols / inv_vols.sum()
    return weights


def save_weights(weights: pd.Series, filepath: str):
    """Save weights to CSV with asset names."""
    df = pd.DataFrame({"asset": weights.index, "weight": weights.values})
    df.to_csv(filepath, index=False)
    print(f"Saved: {filepath}")


def save_covariance(cov: pd.DataFrame, filepath: str):
    """Save covariance matrix to CSV."""
    cov.to_csv(filepath)
    print(f"Saved: {filepath}")


def save_returns(returns: pd.DataFrame, filepath: str):
    """Save returns to CSV."""
    # Reset index to include timestamp column
    returns_out = returns.copy()
    returns_out.index.name = "timestamp"
    returns_out.to_csv(filepath)
    print(f"Saved: {filepath}")


def generate_all():
    """Generate all test baselines."""
    os.makedirs(OUT_DIR, exist_ok=True)

    # =========================================================================
    # 1. Generate Return Datasets
    # =========================================================================
    print("\n=== Generating Return Datasets ===")

    datasets = {
        "returns_5": make_returns(5, N_SAMPLES),
        "returns_10": make_returns(10, N_SAMPLES),
        "returns_50": make_returns(50, N_SAMPLES),
        "returns_high_corr": make_high_corr_returns(10, N_SAMPLES),
    }

    for name, df in datasets.items():
        save_returns(df, f"{OUT_DIR}/{name}.csv")

    # Use 10-asset dataset for main baselines
    returns = datasets["returns_10"]

    # =========================================================================
    # 2. Generate Naive Allocator Baselines
    # =========================================================================
    print("\n=== Generating Naive Allocator Baselines ===")

    save_weights(
        generate_equal_weight_baseline(10),
        f"{OUT_DIR}/equal_weight_expected.csv"
    )
    save_weights(
        generate_inverse_vol_baseline(returns),
        f"{OUT_DIR}/inv_vol_expected.csv"
    )

    # =========================================================================
    # 3. Generate HRP Baselines (PyPortfolioOpt)
    # =========================================================================
    print("\n=== Generating HRP Baselines ===")

    hrp_baselines = generate_hrp_baselines(returns)
    for name, weights in hrp_baselines.items():
        save_weights(weights, f"{OUT_DIR}/{name}_expected.csv")

    # =========================================================================
    # 4. Generate MVO Baselines (PyPortfolioOpt)
    # =========================================================================
    print("\n=== Generating MVO Baselines ===")

    mvo_baselines = generate_mvo_baselines(returns)
    for name, weights in mvo_baselines.items():
        save_weights(weights, f"{OUT_DIR}/{name}_expected.csv")

    # =========================================================================
    # 5. Generate Risk Parity Baselines (Riskfolio-Lib)
    # =========================================================================
    print("\n=== Generating Risk Parity Baselines ===")

    rp_baselines = generate_risk_parity_baselines(returns)
    for name, weights in rp_baselines.items():
        save_weights(weights, f"{OUT_DIR}/{name}_expected.csv")

    # =========================================================================
    # 6. Generate HERC Baselines (Riskfolio-Lib)
    # =========================================================================
    print("\n=== Generating HERC Baselines ===")

    herc_baselines = generate_herc_baselines(returns)
    for name, weights in herc_baselines.items():
        save_weights(weights, f"{OUT_DIR}/{name}_expected.csv")

    # =========================================================================
    # 7. Generate Covariance Baselines
    # =========================================================================
    print("\n=== Generating Covariance Baselines ===")

    cov_baselines = generate_covariance_baselines(returns)
    for name, cov in cov_baselines.items():
        save_covariance(cov, f"{OUT_DIR}/{name}_expected.csv")

    # =========================================================================
    # Summary
    # =========================================================================
    print("\n=== Generation Complete ===")
    files = os.listdir(OUT_DIR)
    print(f"Generated {len(files)} files in {OUT_DIR}/")
    for f in sorted(files):
        size = os.path.getsize(f"{OUT_DIR}/{f}")
        print(f"  {f}: {size:,} bytes")


if __name__ == "__main__":
    generate_all()
