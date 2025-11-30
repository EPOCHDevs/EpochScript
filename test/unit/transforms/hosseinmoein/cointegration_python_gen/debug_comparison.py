#!/usr/bin/env python3
"""
Debug comparison script for cointegration transforms.

Compares C++ output against Python reference implementations step-by-step
to identify algorithmic differences.

Usage:
    python debug_comparison.py [--transform half_life|adf|engle_granger|johansen]
"""

import argparse
import os
import numpy as np
import pandas as pd
from pathlib import Path

try:
    from statsmodels.tsa.stattools import adfuller, coint
    from statsmodels.tsa.vector_ar.vecm import coint_johansen
    from statsmodels.regression.linear_model import OLS
    from statsmodels.tools import add_constant
except ImportError as e:
    raise SystemExit("Please install statsmodels: pip install statsmodels") from e


SCRIPT_DIR = Path(__file__).parent
DATA_DIR = SCRIPT_DIR / "cointegration_test_data"


def load_cointegrated_pair():
    """Load the cointegrated pair input data."""
    df = pd.read_csv(DATA_DIR / "cointegrated_pair.csv")
    return df


def compute_spread(df):
    """Compute the spread from true parameters."""
    true_beta = df["true_beta"].iloc[0]
    true_alpha = df["true_alpha"].iloc[0]
    x = df["x"].values
    y = df["y"].values
    spread = y - true_alpha - true_beta * x
    return spread, x, y


def debug_half_life_ar1(window: int = 60):
    """Debug half-life AR(1) calculation differences."""
    print("\n" + "=" * 60)
    print("DEBUG: HalfLifeAR1 Transform")
    print("=" * 60)

    df = load_cointegrated_pair()
    spread, _, _ = compute_spread(df)

    # Load expected if available
    expected_path = DATA_DIR / "half_life_expected.csv"
    if expected_path.exists():
        df_expected = pd.read_csv(expected_path)
    else:
        df_expected = None
        print("WARNING: No expected CSV found")

    print(f"\nWindow size: {window}")
    print(f"Total samples: {len(spread)}")

    # Show a few sample calculations
    for test_idx in [window - 1, window + 10, window + 50, len(spread) - 1]:
        if test_idx >= window - 1:
            window_data = spread[test_idx - window + 1:test_idx + 1]

            print(f"\n--- Index {test_idx} ---")

            # Python calculation (statsmodels-style)
            y_t = window_data[1:]
            y_lag = window_data[:-1]

            # Method 1: np.cov (uses N-1 denominator for sample covariance)
            cov_matrix = np.cov(y_t, y_lag)
            cov_py = cov_matrix[0, 1]
            var_lag_py = np.var(y_lag, ddof=1)
            phi_py = cov_py / var_lag_py if var_lag_py > 1e-10 else 0.0

            # Method 2: Manual with N denominator (population variance - likely C++ bug)
            n = len(y_lag)
            mean_y = np.mean(y_t)
            mean_y_lag = np.mean(y_lag)
            cov_pop = np.mean(y_t * y_lag) - mean_y * mean_y_lag
            var_lag_pop = np.mean(y_lag**2) - mean_y_lag**2
            phi_pop = cov_pop / var_lag_pop if var_lag_pop > 1e-10 else 0.0

            # Method 3: Manual with N-1 denominator (sample variance - correct)
            sum_cross = np.sum(y_t * y_lag)
            sum_y_lag_sq = np.sum(y_lag**2)
            cov_sample = (sum_cross / (n-1)) - (np.sum(y_t) / (n-1)) * (np.sum(y_lag) / (n-1)) * (n-1) / n
            # Actually, the correct sample cov formula:
            cov_sample_correct = np.sum((y_t - mean_y) * (y_lag - mean_y_lag)) / (n - 1)
            var_lag_sample = np.sum((y_lag - mean_y_lag)**2) / (n - 1)
            phi_sample = cov_sample_correct / var_lag_sample if var_lag_sample > 1e-10 else 0.0

            print(f"  Window data range: [{window_data[0]:.4f} ... {window_data[-1]:.4f}]")
            print(f"  n (pairs): {n}")
            print(f"")
            print(f"  [Python np.cov/np.var(ddof=1)]:")
            print(f"    cov(y_t, y_lag) = {cov_py:.6f}")
            print(f"    var(y_lag)      = {var_lag_py:.6f}")
            print(f"    phi             = {phi_py:.6f}")

            print(f"")
            print(f"  [Population variance (N denominator) - likely C++ current]:")
            print(f"    cov(y_t, y_lag) = {cov_pop:.6f}")
            print(f"    var(y_lag)      = {var_lag_pop:.6f}")
            print(f"    phi             = {phi_pop:.6f}")

            print(f"")
            print(f"  [Sample variance (N-1 denominator) - should match np.cov]:")
            print(f"    cov(y_t, y_lag) = {cov_sample_correct:.6f}")
            print(f"    var(y_lag)      = {var_lag_sample:.6f}")
            print(f"    phi             = {phi_sample:.6f}")

            print(f"")
            print(f"  Difference (pop vs sample phi): {abs(phi_pop - phi_sample):.6f}")

            # Half-life
            if 0 < phi_py < 1:
                hl_py = -np.log(2) / np.log(phi_py)
                print(f"  Half-life (Python): {hl_py:.2f}")
            if 0 < phi_pop < 1:
                hl_pop = -np.log(2) / np.log(phi_pop)
                print(f"  Half-life (Pop var): {hl_pop:.2f}")
            if 0 < phi_sample < 1:
                hl_sample = -np.log(2) / np.log(phi_sample)
                print(f"  Half-life (Sample var): {hl_sample:.2f}")

            if df_expected is not None:
                expected_ar1 = df_expected["ar1_coef"].iloc[test_idx]
                expected_hl = df_expected["half_life"].iloc[test_idx]
                print(f"")
                print(f"  Expected (CSV):")
                print(f"    ar1_coef   = {expected_ar1}")
                print(f"    half_life  = {expected_hl}")


def debug_rolling_adf(window: int = 60):
    """Debug rolling ADF calculation differences."""
    print("\n" + "=" * 60)
    print("DEBUG: RollingADF Transform")
    print("=" * 60)

    df = load_cointegrated_pair()
    spread, _, _ = compute_spread(df)

    expected_path = DATA_DIR / "adf_expected.csv"
    if expected_path.exists():
        df_expected = pd.read_csv(expected_path)
    else:
        df_expected = None
        print("WARNING: No expected CSV found")

    print(f"\nWindow size: {window}")

    for test_idx in [window - 1, window + 50, len(spread) - 1]:
        if test_idx >= window - 1:
            window_data = spread[test_idx - window + 1:test_idx + 1]

            print(f"\n--- Index {test_idx} ---")

            try:
                result = adfuller(window_data, maxlag=1, regression="c")
                adf_stat = result[0]
                p_value = result[1]
                crit_1pct = result[4]["1%"]
                crit_5pct = result[4]["5%"]
                crit_10pct = result[4]["10%"]

                print(f"  statsmodels.adfuller():")
                print(f"    ADF stat    = {adf_stat:.6f}")
                print(f"    p-value     = {p_value:.6f}")
                print(f"    Critical values:")
                print(f"      1%:  {crit_1pct:.6f}")
                print(f"      5%:  {crit_5pct:.6f}")
                print(f"      10%: {crit_10pct:.6f}")
                print(f"    Is stationary (p<0.05): {p_value < 0.05}")

            except Exception as e:
                print(f"  Error: {e}")

            if df_expected is not None:
                print(f"")
                print(f"  Expected (CSV):")
                print(f"    adf_stat = {df_expected['adf_stat'].iloc[test_idx]}")
                print(f"    p_value  = {df_expected['p_value'].iloc[test_idx]}")


def debug_engle_granger(window: int = 60):
    """Debug Engle-Granger calculation differences."""
    print("\n" + "=" * 60)
    print("DEBUG: EngleGranger Transform")
    print("=" * 60)

    df = load_cointegrated_pair()
    x = df["x"].values
    y = df["y"].values

    expected_path = DATA_DIR / "engle_granger_expected.csv"
    if expected_path.exists():
        df_expected = pd.read_csv(expected_path)
    else:
        df_expected = None
        print("WARNING: No expected CSV found")

    print(f"\nWindow size: {window}")
    print(f"True beta: {df['true_beta'].iloc[0]}")
    print(f"True alpha: {df['true_alpha'].iloc[0]}")

    for test_idx in [window - 1, window + 50, len(y) - 1]:
        if test_idx >= window - 1:
            y_win = y[test_idx - window + 1:test_idx + 1]
            x_win = x[test_idx - window + 1:test_idx + 1]

            print(f"\n--- Index {test_idx} ---")

            try:
                # OLS regression
                X = add_constant(x_win)
                model = OLS(y_win, X).fit()
                alpha = model.params[0]
                beta = model.params[1]

                print(f"  OLS Regression:")
                print(f"    intercept (alpha) = {alpha:.6f}")
                print(f"    hedge_ratio (beta) = {beta:.6f}")

                # Cointegration test
                coint_result = coint(y_win, x_win, trend="c", maxlag=1)

                print(f"")
                print(f"  statsmodels.coint():")
                print(f"    coint_t (ADF stat) = {coint_result[0]:.6f}")
                print(f"    p-value            = {coint_result[1]:.6f}")
                print(f"    Critical values:")
                print(f"      1%:  {coint_result[2][0]:.6f}")
                print(f"      5%:  {coint_result[2][1]:.6f}")
                print(f"      10%: {coint_result[2][2]:.6f}")
                print(f"    Is cointegrated (p<0.05): {coint_result[1] < 0.05}")

            except Exception as e:
                print(f"  Error: {e}")

            if df_expected is not None:
                print(f"")
                print(f"  Expected (CSV):")
                print(f"    hedge_ratio = {df_expected['hedge_ratio'].iloc[test_idx]}")
                print(f"    adf_stat    = {df_expected['adf_stat'].iloc[test_idx]}")
                print(f"    p_value     = {df_expected['p_value'].iloc[test_idx]}")


def debug_johansen(window: int = 60):
    """Debug Johansen calculation differences."""
    print("\n" + "=" * 60)
    print("DEBUG: Johansen Transform")
    print("=" * 60)

    df = load_cointegrated_pair()
    x = df["x"].values
    y = df["y"].values
    data = np.column_stack([y, x])

    expected_path = DATA_DIR / "johansen_expected.csv"
    if expected_path.exists():
        df_expected = pd.read_csv(expected_path)
    else:
        df_expected = None
        print("WARNING: No expected CSV found")

    print(f"\nWindow size: {window}")

    for test_idx in [window - 1, window + 50, len(y) - 1]:
        if test_idx >= window - 1:
            data_win = data[test_idx - window + 1:test_idx + 1, :]

            print(f"\n--- Index {test_idx} ---")

            try:
                result = coint_johansen(data_win, det_order=0, k_ar_diff=1)

                print(f"  coint_johansen():")
                print(f"    Eigenvalues: {result.eig}")
                print(f"    Trace stats (lr1): {result.lr1}")
                print(f"    Max eig stats (lr2): {result.lr2}")
                print(f"    Trace critical values (5%):")
                print(f"      r=0: {result.cvt[0, 1]:.6f}")
                print(f"      r=1: {result.cvt[1, 1]:.6f}")

                # Determine rank
                rank = 0
                if result.lr1[0] > result.cvt[0, 1]:
                    rank = 1
                    if result.lr1[1] > result.cvt[1, 1]:
                        rank = 2
                print(f"    Estimated rank: {rank}")

                print(f"    Eigenvectors (beta):")
                print(f"      {result.evec}")

            except Exception as e:
                print(f"  Error: {e}")

            if df_expected is not None:
                print(f"")
                print(f"  Expected (CSV):")
                print(f"    rank        = {df_expected['rank'].iloc[test_idx]}")
                print(f"    trace_stat_0 = {df_expected['trace_stat_0'].iloc[test_idx]}")
                print(f"    eigval_0    = {df_expected['eigval_0'].iloc[test_idx]}")


def main():
    parser = argparse.ArgumentParser(description="Debug cointegration transform differences")
    parser.add_argument(
        "--transform",
        choices=["half_life", "adf", "engle_granger", "johansen", "all"],
        default="all",
        help="Which transform to debug"
    )
    parser.add_argument(
        "--window",
        type=int,
        default=60,
        help="Window size for rolling calculations"
    )
    args = parser.parse_args()

    if args.transform in ["half_life", "all"]:
        debug_half_life_ar1(args.window)

    if args.transform in ["adf", "all"]:
        debug_rolling_adf(args.window)

    if args.transform in ["engle_granger", "all"]:
        debug_engle_granger(args.window)

    if args.transform in ["johansen", "all"]:
        debug_johansen(args.window)


if __name__ == "__main__":
    main()
