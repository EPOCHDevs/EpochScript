#!/usr/bin/env python3
"""
Generate ARIMA test data using Python's statsmodels library.

This script generates reference test data for validating the C++ ARIMA implementation.
Output is saved as CSV files that can be loaded by C++ unit tests.

Requirements:
    pip install statsmodels pandas numpy

Usage:
    python generate_arima_test_data.py

Output files:
    arima_test_data/arima_*_input.csv     - Input series data
    arima_test_data/arima_*_params.csv    - Fitted parameters
    arima_test_data/arima_*_fitted.csv    - Fitted values and residuals
    arima_test_data/arima_*_metrics.csv   - AIC, BIC, log-likelihood
    arima_test_data/arima_*_forecast.csv  - Forecasts with intervals
"""

import numpy as np
import pandas as pd
from statsmodels.tsa.arima.model import ARIMA
import os
from pathlib import Path

# Ensure output directory exists
OUTPUT_DIR = Path(__file__).parent / "arima_test_data"
OUTPUT_DIR.mkdir(exist_ok=True)


def generate_ar1_series(n: int = 500, phi: float = 0.7, sigma: float = 1.0, seed: int = 42) -> np.ndarray:
    """Generate AR(1) process."""
    np.random.seed(seed)
    y = np.zeros(n)
    eps = np.random.randn(n) * sigma

    for t in range(1, n):
        y[t] = phi * y[t-1] + eps[t]

    return y


def generate_arma_series(n: int = 500, phi: float = 0.5, theta: float = 0.3,
                          sigma: float = 1.0, seed: int = 42) -> np.ndarray:
    """Generate ARMA(1,1) process."""
    np.random.seed(seed)
    y = np.zeros(n)
    eps = np.random.randn(n) * sigma

    for t in range(1, n):
        y[t] = phi * y[t-1] + eps[t] + theta * eps[t-1]

    return y


def generate_random_walk(n: int = 500, drift: float = 0.0, sigma: float = 1.0, seed: int = 42) -> np.ndarray:
    """Generate random walk (needs d=1 differencing)."""
    np.random.seed(seed)
    eps = np.random.randn(n) * sigma
    y = np.cumsum(eps) + drift * np.arange(n)
    return y


def fit_arima_model(y: np.ndarray, order: tuple, with_constant: bool = True) -> dict:
    """
    Fit ARIMA model using statsmodels.

    Returns dict with:
        - params: fitted parameters
        - fitted: fitted values
        - residuals: residuals
        - log_likelihood, aic, bic
        - forecast: h-step forecasts with intervals
    """
    trend = 'c' if with_constant else 'n'
    model = ARIMA(y, order=order, trend=trend)
    result = model.fit()

    # Extract parameters - handle both Series and ndarray
    params = {}
    p, d, q = order

    # Get parameter names and values
    param_names = result.param_names if hasattr(result, 'param_names') else []
    param_values = result.params

    # Convert to dict if needed
    if hasattr(param_values, 'to_dict'):
        param_dict = param_values.to_dict()
    elif hasattr(param_values, 'index'):
        param_dict = dict(zip(param_values.index, param_values.values))
    else:
        param_dict = dict(zip(param_names, param_values))

    # AR coefficients
    for i in range(1, p + 1):
        key = f'ar.L{i}'
        if key in param_dict:
            params[f'phi_{i}'] = param_dict[key]
        else:
            params[f'phi_{i}'] = 0.0

    # MA coefficients
    for i in range(1, q + 1):
        key = f'ma.L{i}'
        if key in param_dict:
            params[f'theta_{i}'] = param_dict[key]
        else:
            params[f'theta_{i}'] = 0.0

    # Constant
    if with_constant and 'const' in param_dict:
        params['constant'] = param_dict['const']
    else:
        params['constant'] = 0.0

    # Sigma^2 (innovation variance)
    if 'sigma2' in param_dict:
        params['sigma2'] = param_dict['sigma2']
    else:
        params['sigma2'] = result.scale if hasattr(result, 'scale') else np.var(result.resid)

    # Forecasts
    forecast_obj = result.get_forecast(steps=5)
    forecast_mean = forecast_obj.predicted_mean
    forecast_ci = forecast_obj.conf_int(alpha=0.05)

    # Handle Series/ndarray for forecasts
    if hasattr(forecast_mean, 'values'):
        forecast_mean = forecast_mean.values

    # Handle confidence interval - can be DataFrame or ndarray
    if hasattr(forecast_ci, 'iloc'):
        forecast_lower = forecast_ci.iloc[:, 0].values
        forecast_upper = forecast_ci.iloc[:, 1].values
    elif hasattr(forecast_ci, 'values'):
        forecast_lower = forecast_ci.values[:, 0]
        forecast_upper = forecast_ci.values[:, 1]
    else:
        # Already ndarray
        forecast_lower = forecast_ci[:, 0]
        forecast_upper = forecast_ci[:, 1]

    # Handle fitted values and residuals
    fitted = result.fittedvalues
    if hasattr(fitted, 'values'):
        fitted = fitted.values
    resid = result.resid
    if hasattr(resid, 'values'):
        resid = resid.values

    return {
        'params': params,
        'fitted': fitted,
        'residuals': resid,
        'log_likelihood': result.llf,
        'aic': result.aic,
        'bic': result.bic,
        'forecast_mean': forecast_mean,
        'forecast_lower': forecast_lower,
        'forecast_upper': forecast_upper,
    }


def save_test_case(name: str, y: np.ndarray, result: dict, order: tuple):
    """Save test case data to CSV files."""
    prefix = OUTPUT_DIR / name
    p, d, q = order

    # Save input series
    pd.DataFrame({'y': y}).to_csv(f"{prefix}_input.csv", index=False)

    # Save parameters
    params_df = pd.DataFrame([result['params']])
    params_df.to_csv(f"{prefix}_params.csv", index=False)

    # Save fitted values and residuals
    fitted_df = pd.DataFrame({
        'fitted': result['fitted'],
        'residuals': result['residuals']
    })
    fitted_df.to_csv(f"{prefix}_fitted.csv", index=False)

    # Save metrics
    metrics_df = pd.DataFrame([{
        'log_likelihood': result['log_likelihood'],
        'aic': result['aic'],
        'bic': result['bic'],
    }])
    metrics_df.to_csv(f"{prefix}_metrics.csv", index=False)

    # Save forecasts
    forecast_df = pd.DataFrame({
        'forecast_mean': result['forecast_mean'],
        'forecast_lower': result['forecast_lower'],
        'forecast_upper': result['forecast_upper'],
    })
    forecast_df.to_csv(f"{prefix}_forecast.csv", index=False)

    print(f"Saved test case: {name}")
    print(f"  Order: ARIMA{order}")
    print(f"  Parameters: {result['params']}")
    print(f"  AIC: {result['aic']:.4f}, BIC: {result['bic']:.4f}")
    print()


def main():
    print("Generating ARIMA test data for C++ validation...\n")

    # Test case 1: AR(1) - ARIMA(1,0,0)
    print("=" * 60)
    print("Test Case 1: AR(1) - ARIMA(1,0,0)")
    print("=" * 60)
    y_ar1 = generate_ar1_series(n=500, phi=0.7, seed=42)
    result_ar1 = fit_arima_model(y_ar1, order=(1, 0, 0), with_constant=True)
    save_test_case("arima_100", y_ar1, result_ar1, (1, 0, 0))

    # Test case 2: MA(1) - ARIMA(0,0,1)
    print("=" * 60)
    print("Test Case 2: MA(1) - ARIMA(0,0,1)")
    print("=" * 60)
    np.random.seed(123)
    eps = np.random.randn(500)
    y_ma1 = eps + 0.5 * np.roll(eps, 1)
    y_ma1[0] = eps[0]
    result_ma1 = fit_arima_model(y_ma1, order=(0, 0, 1), with_constant=True)
    save_test_case("arima_001", y_ma1, result_ma1, (0, 0, 1))

    # Test case 3: ARMA(1,1) - ARIMA(1,0,1)
    print("=" * 60)
    print("Test Case 3: ARMA(1,1) - ARIMA(1,0,1)")
    print("=" * 60)
    y_arma = generate_arma_series(n=500, phi=0.5, theta=0.3, seed=456)
    result_arma = fit_arima_model(y_arma, order=(1, 0, 1), with_constant=True)
    save_test_case("arima_101", y_arma, result_arma, (1, 0, 1))

    # Test case 4: Random walk with d=1 - ARIMA(1,1,0) (no constant for d>0)
    print("=" * 60)
    print("Test Case 4: Random walk - ARIMA(1,1,0)")
    print("=" * 60)
    y_rw = generate_random_walk(n=500, drift=0.1, seed=789)
    result_rw = fit_arima_model(y_rw, order=(1, 1, 0), with_constant=False)
    save_test_case("arima_110", y_rw, result_rw, (1, 1, 0))

    # Test case 5: ARIMA(2,1,1) - more complex (no constant for d>0)
    print("=" * 60)
    print("Test Case 5: ARIMA(2,1,1)")
    print("=" * 60)
    y_complex = generate_random_walk(n=500, drift=0.05, seed=101)
    result_complex = fit_arima_model(y_complex, order=(2, 1, 1), with_constant=False)
    save_test_case("arima_211", y_complex, result_complex, (2, 1, 1))

    # Test case 6: No constant - ARIMA(1,0,0) without intercept
    print("=" * 60)
    print("Test Case 6: AR(1) no constant - ARIMA(1,0,0)")
    print("=" * 60)
    y_ar1_nc = generate_ar1_series(n=500, phi=0.6, seed=202)
    result_ar1_nc = fit_arima_model(y_ar1_nc, order=(1, 0, 0), with_constant=False)
    save_test_case("arima_100_no_const", y_ar1_nc, result_ar1_nc, (1, 0, 0))

    print("=" * 60)
    print("Test data generation complete!")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
