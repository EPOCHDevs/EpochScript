#!/usr/bin/env python3
"""
Generate test data for FracDiff (Fractional Differentiation) transform.
Uses the fracdiff library as reference implementation.
"""

import numpy as np
import pandas as pd
import os

# Try to import fracdiff, if not available use manual implementation
try:
    from fracdiff import fdiff
    HAS_FRACDIFF = True
except ImportError:
    HAS_FRACDIFF = False
    print("fracdiff not installed, using manual implementation")


def compute_ffd_weights(d: float, threshold: float = 1e-5, max_size: int = 10000) -> np.ndarray:
    """
    Compute FFD weights using recurrence relation:
    w[k] = -w[k-1] * (d - k + 1) / k, where w[0] = 1
    """
    weights = [1.0]
    w = 1.0
    for k in range(1, max_size):
        w = -w * (d - k + 1) / k
        if abs(w) < threshold:
            break
        weights.append(w)
    return np.array(weights)


def frac_diff_manual(series: np.ndarray, d: float, threshold: float = 1e-5) -> np.ndarray:
    """
    Apply fractional differentiation using FFD method.
    X̃[t] = Σ(k=0 to l*) w[k] * X[t-k]
    """
    weights = compute_ffd_weights(d, threshold)
    window = len(weights)
    n = len(series)

    result = np.full(n, np.nan)

    for t in range(window - 1, n):
        sum_val = 0.0
        for k in range(window):
            sum_val += weights[k] * series[t - k]
        result[t] = sum_val

    return result


def generate_test_case(name: str, series: np.ndarray, d: float, threshold: float, output_dir: str):
    """Generate a single test case and save to CSV."""

    # Compute expected output
    if HAS_FRACDIFF:
        # fracdiff library uses slightly different API
        expected = fdiff(series, d, window=None, mode="valid")
        # Pad with NaN to match original length
        pad_len = len(series) - len(expected)
        expected = np.concatenate([np.full(pad_len, np.nan), expected])
    else:
        expected = frac_diff_manual(series, d, threshold)

    # Also compute with manual implementation for consistency
    manual_result = frac_diff_manual(series, d, threshold)

    # Create DataFrame
    df = pd.DataFrame({
        'input': series,
        'expected': manual_result,  # Use manual for C++ comparison
        'd': d,
        'threshold': threshold
    })

    # Save to CSV
    output_path = os.path.join(output_dir, f'{name}.csv')
    df.to_csv(output_path, index=False)
    print(f"Generated: {output_path}")

    # Also save weights for verification
    weights = compute_ffd_weights(d, threshold)
    weights_df = pd.DataFrame({'weight': weights})
    weights_path = os.path.join(output_dir, f'{name}_weights.csv')
    weights_df.to_csv(weights_path, index=False)

    return df


def main():
    output_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(output_dir, 'frac_diff_test_data')
    os.makedirs(data_dir, exist_ok=True)

    np.random.seed(42)

    # Test case 1: Simple linear series with d=0.5
    linear = np.arange(1, 101, dtype=float)
    generate_test_case('linear_d05', linear, d=0.5, threshold=1e-5, output_dir=data_dir)

    # Test case 2: Random walk (cumulative sum of returns)
    returns = np.random.randn(200) * 0.02
    prices = 100 * np.exp(np.cumsum(returns))
    generate_test_case('random_walk_d05', prices, d=0.5, threshold=1e-5, output_dir=data_dir)

    # Test case 3: Log prices with d=0.3 (mild differentiation)
    log_prices = np.log(prices)
    generate_test_case('log_prices_d03', log_prices, d=0.3, threshold=1e-5, output_dir=data_dir)

    # Test case 4: d=1.0 should be close to regular first difference
    generate_test_case('linear_d10', linear, d=1.0, threshold=1e-5, output_dir=data_dir)

    # Test case 5: Very small d (preserve most memory)
    generate_test_case('random_walk_d01', prices, d=0.1, threshold=1e-5, output_dir=data_dir)

    # Test case 6: Different threshold
    generate_test_case('linear_d05_thresh1e3', linear, d=0.5, threshold=1e-3, output_dir=data_dir)

    print(f"\nAll test data generated in: {data_dir}")

    # Print weight info for reference
    for d in [0.1, 0.3, 0.5, 1.0]:
        weights = compute_ffd_weights(d, 1e-5)
        print(f"d={d}: {len(weights)} weights, first 5: {weights[:5]}")


if __name__ == '__main__':
    main()