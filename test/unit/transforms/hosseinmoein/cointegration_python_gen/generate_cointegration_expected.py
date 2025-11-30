#!/usr/bin/env python3
"""
Generate cointegration test fixtures using statsmodels.

Outputs into cointegration_test_data/:
- cointegrated_pair.csv          - Input: two cointegrated price series
- half_life_expected.csv         - Expected half-life AR(1) results
- adf_expected.csv               - Expected ADF test results
- engle_granger_expected.csv     - Expected Engle-Granger results
- johansen_expected.csv          - Expected Johansen results (2-var)

Reference implementations:
- statsmodels.tsa.stattools.adfuller
- statsmodels.tsa.stattools.coint (Engle-Granger)
- statsmodels.tsa.vector_ar.vecm.coint_johansen
"""

import os
import numpy as np
import pandas as pd

try:
    from statsmodels.tsa.stattools import adfuller, coint
    from statsmodels.tsa.vector_ar.vecm import coint_johansen
    from statsmodels.regression.linear_model import OLS
    from statsmodels.tools import add_constant
except ImportError as e:
    raise SystemExit("Please install statsmodels: pip install statsmodels") from e


def generate_cointegrated_pair(n: int = 500, seed: int = 42) -> pd.DataFrame:
    """
    Generate two cointegrated price series.

    y = beta * x + alpha + epsilon (stationary residuals)

    Where x follows a random walk and epsilon is stationary noise.
    """
    rng = np.random.default_rng(seed)

    # Generate random walk for x
    x_returns = rng.normal(0.0, 1.0, n)
    x = 100 + np.cumsum(x_returns)

    # True cointegration parameters
    true_beta = 1.5
    true_alpha = 10.0

    # Generate stationary noise (mean-reverting)
    noise = np.zeros(n)
    phi = 0.7  # AR(1) coefficient for mean-reverting noise
    for i in range(1, n):
        noise[i] = phi * noise[i-1] + rng.normal(0.0, 2.0)

    # Cointegrated y series
    y = true_alpha + true_beta * x + noise

    # UTC timestamp index
    dt_index = pd.date_range("2020-01-01", periods=n, freq="D", tz="UTC")
    index_col = dt_index.strftime("%Y-%m-%dT%H:%M:%SZ")

    return pd.DataFrame({
        "index": index_col,
        "x": x,
        "y": y,
        "true_beta": true_beta,
        "true_alpha": true_alpha,
        "true_phi": phi,
    })


def compute_half_life_ar1(series: np.ndarray, window: int = 60) -> pd.DataFrame:
    """
    Compute rolling half-life from AR(1) coefficient.

    half_life = -log(2) / log(phi)
    where phi is the AR(1) coefficient from y_t = phi * y_{t-1} + epsilon
    """
    n = len(series)
    half_lives = np.full(n, np.nan)
    ar1_coefs = np.full(n, np.nan)
    is_mean_reverting = np.zeros(n, dtype=int)

    for i in range(window - 1, n):
        window_data = series[i - window + 1:i + 1]

        # AR(1) regression: y_t on y_{t-1}
        y_t = window_data[1:]
        y_lag = window_data[:-1]

        # Simple OLS: phi = Cov(y_t, y_{t-1}) / Var(y_{t-1})
        cov = np.cov(y_t, y_lag)[0, 1]
        var_lag = np.var(y_lag, ddof=1)

        if var_lag > 1e-10:
            phi = cov / var_lag
        else:
            phi = 0.0

        ar1_coefs[i] = phi

        # Check if mean-reverting (0 < phi < 1)
        if 0 < phi < 1:
            is_mean_reverting[i] = 1
            half_lives[i] = -np.log(2) / np.log(phi)
            # Cap at reasonable values
            half_lives[i] = min(half_lives[i], window * 10)
        else:
            half_lives[i] = np.nan

    return pd.DataFrame({
        "half_life": half_lives,
        "ar1_coef": ar1_coefs,
        "is_mean_reverting": is_mean_reverting,
    })


def compute_rolling_adf(series: np.ndarray, window: int = 60,
                        maxlag: int = 1, regression: str = "c") -> pd.DataFrame:
    """
    Compute rolling ADF test statistics.
    """
    n = len(series)
    adf_stats = np.full(n, np.nan)
    p_values = np.full(n, np.nan)
    critical_1pct = np.full(n, np.nan)
    critical_5pct = np.full(n, np.nan)
    critical_10pct = np.full(n, np.nan)
    is_stationary = np.zeros(n, dtype=int)

    for i in range(window - 1, n):
        window_data = series[i - window + 1:i + 1]

        try:
            result = adfuller(window_data, maxlag=maxlag, regression=regression)
            adf_stats[i] = result[0]
            p_values[i] = result[1]
            critical_1pct[i] = result[4]["1%"]
            critical_5pct[i] = result[4]["5%"]
            critical_10pct[i] = result[4]["10%"]
            is_stationary[i] = 1 if result[1] < 0.05 else 0
        except Exception:
            pass

    return pd.DataFrame({
        "adf_stat": adf_stats,
        "p_value": p_values,
        "critical_1pct": critical_1pct,
        "critical_5pct": critical_5pct,
        "critical_10pct": critical_10pct,
        "is_stationary": is_stationary,
    })


def compute_engle_granger(y: np.ndarray, x: np.ndarray,
                          window: int = 60) -> pd.DataFrame:
    """
    Compute rolling Engle-Granger cointegration test.

    Step 1: OLS regression y = alpha + beta * x
    Step 2: ADF test on residuals
    """
    n = len(y)
    hedge_ratios = np.full(n, np.nan)
    intercepts = np.full(n, np.nan)
    adf_stats = np.full(n, np.nan)
    p_values = np.full(n, np.nan)
    critical_1pct = np.full(n, np.nan)
    critical_5pct = np.full(n, np.nan)
    critical_10pct = np.full(n, np.nan)
    is_cointegrated = np.zeros(n, dtype=int)
    spreads = np.full(n, np.nan)

    for i in range(window - 1, n):
        y_win = y[i - window + 1:i + 1]
        x_win = x[i - window + 1:i + 1]

        try:
            # Step 1: OLS regression
            X = add_constant(x_win)
            model = OLS(y_win, X).fit()
            alpha = model.params[0]
            beta = model.params[1]

            intercepts[i] = alpha
            hedge_ratios[i] = beta

            # Spread at current point
            spreads[i] = y[i] - alpha - beta * x[i]

            # Step 2: Engle-Granger cointegration test
            # Use statsmodels coint which returns (coint_t, pvalue, crit_values)
            coint_result = coint(y_win, x_win, trend="c", maxlag=1)
            adf_stats[i] = coint_result[0]
            p_values[i] = coint_result[1]
            critical_1pct[i] = coint_result[2][0]
            critical_5pct[i] = coint_result[2][1]
            critical_10pct[i] = coint_result[2][2]
            is_cointegrated[i] = 1 if coint_result[1] < 0.05 else 0

        except Exception:
            pass

    return pd.DataFrame({
        "hedge_ratio": hedge_ratios,
        "intercept": intercepts,
        "spread": spreads,
        "adf_stat": adf_stats,
        "p_value": p_values,
        "critical_1pct": critical_1pct,
        "critical_5pct": critical_5pct,
        "critical_10pct": critical_10pct,
        "is_cointegrated": is_cointegrated,
    })


def compute_johansen(data: np.ndarray, window: int = 60,
                     det_order: int = 0, k_ar_diff: int = 1) -> pd.DataFrame:
    """
    Compute rolling Johansen cointegration test for 2 variables.

    det_order: -1 (no deterministic), 0 (constant), 1 (trend)
    k_ar_diff: number of lagged differences in VECM
    """
    n = data.shape[0]
    n_vars = data.shape[1]

    # For 2 variables: rank can be 0, 1, or 2
    ranks = np.full(n, np.nan)
    trace_stat_0 = np.full(n, np.nan)
    trace_stat_1 = np.full(n, np.nan)
    max_stat_0 = np.full(n, np.nan)
    max_stat_1 = np.full(n, np.nan)
    eigval_0 = np.full(n, np.nan)
    eigval_1 = np.full(n, np.nan)

    for i in range(window - 1, n):
        data_win = data[i - window + 1:i + 1, :]

        try:
            result = coint_johansen(data_win, det_order=det_order, k_ar_diff=k_ar_diff)

            # Eigenvalues (sorted descending)
            eigval_0[i] = result.eig[0]
            eigval_1[i] = result.eig[1]

            # Trace statistics
            trace_stat_0[i] = result.lr1[0]  # r=0 vs r>=1
            trace_stat_1[i] = result.lr1[1]  # r<=1 vs r=2

            # Max eigenvalue statistics
            max_stat_0[i] = result.lr2[0]
            max_stat_1[i] = result.lr2[1]

            # Determine rank by comparing trace stats to critical values (5%)
            # cvt columns: 0=10%, 1=5%, 2=1%
            rank = 0
            if result.lr1[0] > result.cvt[0, 1]:  # reject r=0
                rank = 1
                if result.lr1[1] > result.cvt[1, 1]:  # reject r<=1
                    rank = 2
            ranks[i] = rank

        except Exception:
            pass

    return pd.DataFrame({
        "rank": ranks,
        "trace_stat_0": trace_stat_0,
        "trace_stat_1": trace_stat_1,
        "max_stat_0": max_stat_0,
        "max_stat_1": max_stat_1,
        "eigval_0": eigval_0,
        "eigval_1": eigval_1,
    })


def main():
    script_dir = os.path.abspath(os.path.dirname(__file__))
    out_dir = os.path.join(script_dir, "cointegration_test_data")
    os.makedirs(out_dir, exist_ok=True)

    # Generate cointegrated pair
    print("Generating cointegrated pair...")
    df_pair = generate_cointegrated_pair(n=500, seed=42)
    pair_path = os.path.join(out_dir, "cointegrated_pair.csv")
    df_pair.to_csv(pair_path, index=False)
    print(f"Wrote {pair_path}")

    x = df_pair["x"].values
    y = df_pair["y"].values

    # Compute spread for half-life and ADF tests
    # Use true parameters for cleaner test
    true_beta = df_pair["true_beta"].iloc[0]
    true_alpha = df_pair["true_alpha"].iloc[0]
    spread = y - true_alpha - true_beta * x

    window = 60

    # Half-life AR(1)
    print("Computing half-life AR(1)...")
    df_halflife = compute_half_life_ar1(spread, window=window)
    df_halflife["index"] = df_pair["index"]
    halflife_path = os.path.join(out_dir, "half_life_expected.csv")
    df_halflife.to_csv(halflife_path, index=False)
    print(f"Wrote {halflife_path}")

    # Rolling ADF
    print("Computing rolling ADF...")
    df_adf = compute_rolling_adf(spread, window=window, maxlag=1, regression="c")
    df_adf["index"] = df_pair["index"]
    adf_path = os.path.join(out_dir, "adf_expected.csv")
    df_adf.to_csv(adf_path, index=False)
    print(f"Wrote {adf_path}")

    # Engle-Granger
    print("Computing Engle-Granger...")
    df_eg = compute_engle_granger(y, x, window=window)
    df_eg["index"] = df_pair["index"]
    eg_path = os.path.join(out_dir, "engle_granger_expected.csv")
    df_eg.to_csv(eg_path, index=False)
    print(f"Wrote {eg_path}")

    # Johansen (2 variables)
    print("Computing Johansen...")
    data_2var = np.column_stack([y, x])
    df_joh = compute_johansen(data_2var, window=window, det_order=0, k_ar_diff=1)
    df_joh["index"] = df_pair["index"]
    joh_path = os.path.join(out_dir, "johansen_expected.csv")
    df_joh.to_csv(joh_path, index=False)
    print(f"Wrote {joh_path}")

    print("\nDone! All cointegration test fixtures generated.")


if __name__ == "__main__":
    main()
