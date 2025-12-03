#!/usr/bin/env python3
"""
Generate GARCH test data using Python's arch library.

This script generates reference test data for validating the C++ GARCH implementation.
Output is saved as CSV files that can be loaded by C++ unit tests.

Requirements:
    pip install arch pandas numpy

Usage:
    python generate_garch_test_data.py

Output files:
    garch_test_data/garch_11_input.csv     - Input returns data
    garch_test_data/garch_11_params.csv    - Fitted parameters
    garch_test_data/garch_11_variance.csv  - Conditional variance series
    garch_test_data/garch_11_metrics.csv   - AIC, BIC, log-likelihood
"""

import numpy as np
import pandas as pd
from arch import arch_model
import os
from pathlib import Path

# Ensure output directory exists
OUTPUT_DIR = Path(__file__).parent / "garch_test_data"
OUTPUT_DIR.mkdir(exist_ok=True)


def generate_simulated_returns(n: int = 1000, seed: int = 42) -> np.ndarray:
    """Generate simulated returns with GARCH-like properties."""
    np.random.seed(seed)

    # Simulate GARCH(1,1) process
    omega = 0.00001
    alpha = 0.10
    beta = 0.85

    sigma2 = np.zeros(n)
    eps = np.zeros(n)
    returns = np.zeros(n)

    # Initial variance
    sigma2[0] = omega / (1 - alpha - beta)

    for t in range(1, n):
        sigma2[t] = omega + alpha * eps[t-1]**2 + beta * sigma2[t-1]
        eps[t] = np.sqrt(sigma2[t]) * np.random.randn()
        returns[t] = eps[t]

    return returns


def fit_garch_model(returns: np.ndarray, p: int = 1, q: int = 1,
                    dist: str = 'normal') -> dict:
    """
    Fit GARCH(p,q) model using arch library.

    Returns dict with:
        - params: fitted parameters
        - conditional_volatility: fitted volatility series
        - standardized_residuals: epsilon / sigma
        - log_likelihood, aic, bic
        - forecast_1step: 1-step ahead volatility forecast
    """
    # Create and fit model (rescale=False to match our C++ implementation)
    model = arch_model(returns, mean='Zero', vol='GARCH', p=p, q=q, dist=dist,
                       rescale=False)
    result = model.fit(disp='off')

    # Extract parameters
    params = {
        'omega': result.params['omega'],
    }

    # Alpha coefficients
    for i in range(1, p + 1):
        params[f'alpha_{i}'] = result.params[f'alpha[{i}]']

    # Beta coefficients
    for j in range(1, q + 1):
        params[f'beta_{j}'] = result.params[f'beta[{j}]']

    # Get fitted values (handle both Series and ndarray)
    cond_vol = result.conditional_volatility
    if hasattr(cond_vol, 'values'):
        cond_vol = cond_vol.values
    std_resid = result.std_resid
    if hasattr(std_resid, 'values'):
        std_resid = std_resid.values

    # Forecast
    forecast = result.forecast(horizon=1)
    forecast_var = forecast.variance
    if hasattr(forecast_var, 'values'):
        forecast_variance = forecast_var.values[-1, 0]
    else:
        forecast_variance = forecast_var[-1, 0]
    forecast_vol = np.sqrt(forecast_variance)

    return {
        'params': params,
        'conditional_volatility': cond_vol,
        'conditional_variance': cond_vol ** 2,
        'standardized_residuals': std_resid,
        'log_likelihood': result.loglikelihood,
        'aic': result.aic,
        'bic': result.bic,
        'forecast_1step_vol': forecast_vol,
        'forecast_1step_var': forecast_variance,
    }


def save_test_case(name: str, returns: np.ndarray, result: dict):
    """Save test case data to CSV files."""
    prefix = OUTPUT_DIR / name

    # Save input returns
    pd.DataFrame({'returns': returns}).to_csv(
        f"{prefix}_input.csv", index=False)

    # Save parameters
    params_df = pd.DataFrame([result['params']])
    params_df.to_csv(f"{prefix}_params.csv", index=False)

    # Save variance and volatility series
    variance_df = pd.DataFrame({
        'conditional_variance': result['conditional_variance'],
        'conditional_volatility': result['conditional_volatility'],
        'standardized_residuals': result['standardized_residuals']
    })
    variance_df.to_csv(f"{prefix}_variance.csv", index=False)

    # Save metrics
    metrics_df = pd.DataFrame([{
        'log_likelihood': result['log_likelihood'],
        'aic': result['aic'],
        'bic': result['bic'],
        'forecast_1step_vol': result['forecast_1step_vol'],
        'forecast_1step_var': result['forecast_1step_var']
    }])
    metrics_df.to_csv(f"{prefix}_metrics.csv", index=False)

    print(f"Saved test case: {name}")
    print(f"  Parameters: {result['params']}")
    print(f"  AIC: {result['aic']:.4f}, BIC: {result['bic']:.4f}")
    print(f"  Persistence: {sum(v for k,v in result['params'].items() if 'alpha' in k or 'beta' in k):.4f}")
    print()


def main():
    print("Generating GARCH test data for C++ validation...\n")

    # Test case 1: GARCH(1,1) with simulated data
    print("=" * 60)
    print("Test Case 1: GARCH(1,1) with simulated returns")
    print("=" * 60)
    returns_sim = generate_simulated_returns(n=1000, seed=42)
    result_11 = fit_garch_model(returns_sim, p=1, q=1, dist='normal')
    save_test_case("garch_11_simulated", returns_sim, result_11)

    # Test case 2: GARCH(1,1) with smaller sample
    print("=" * 60)
    print("Test Case 2: GARCH(1,1) with 500 observations")
    print("=" * 60)
    returns_500 = generate_simulated_returns(n=500, seed=123)
    result_11_500 = fit_garch_model(returns_500, p=1, q=1, dist='normal')
    save_test_case("garch_11_small", returns_500, result_11_500)

    # Test case 3: GARCH(2,1) for testing higher order
    print("=" * 60)
    print("Test Case 3: GARCH(2,1) with simulated returns")
    print("=" * 60)
    result_21 = fit_garch_model(returns_sim, p=2, q=1, dist='normal')
    save_test_case("garch_21_simulated", returns_sim, result_21)

    # Test case 4: GARCH(1,1) with Student's t distribution
    print("=" * 60)
    print("Test Case 4: GARCH(1,1) with Student-t errors")
    print("=" * 60)
    result_11_t = fit_garch_model(returns_sim, p=1, q=1, dist='t')
    save_test_case("garch_11_studentt", returns_sim, result_11_t)

    # Test case 5: High persistence GARCH
    print("=" * 60)
    print("Test Case 5: High persistence returns")
    print("=" * 60)
    np.random.seed(456)
    # Generate more persistent GARCH
    omega = 0.000005
    alpha = 0.05
    beta = 0.93
    n = 1000
    sigma2_hp = np.zeros(n)
    eps_hp = np.zeros(n)
    sigma2_hp[0] = omega / (1 - alpha - beta)
    for t in range(1, n):
        sigma2_hp[t] = omega + alpha * eps_hp[t-1]**2 + beta * sigma2_hp[t-1]
        eps_hp[t] = np.sqrt(sigma2_hp[t]) * np.random.randn()
    returns_hp = eps_hp
    result_hp = fit_garch_model(returns_hp, p=1, q=1, dist='normal')
    save_test_case("garch_11_high_persistence", returns_hp, result_hp)

    print("=" * 60)
    print("Test data generation complete!")
    print(f"Output directory: {OUTPUT_DIR}")
    print("=" * 60)


if __name__ == "__main__":
    main()
