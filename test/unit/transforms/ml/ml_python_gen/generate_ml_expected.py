#!/usr/bin/env python3
"""
Generate ML test data for classification and regression.
Uses sklearn for expected outputs from LightGBM and LIBLINEAR.
"""
import os
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.svm import LinearSVR
from sklearn.preprocessing import StandardScaler

# Optional: LightGBM for expected outputs (may not be installed)
try:
    import lightgbm as lgb
    HAS_LIGHTGBM = True
except ImportError:
    HAS_LIGHTGBM = False
    print("Warning: lightgbm not installed, skipping LightGBM expected outputs")


def make_classification_data(n_samples: int = 500, seed: int = 42) -> pd.DataFrame:
    """Generate linearly separable binary classification data."""
    rng = np.random.default_rng(seed)

    n_pos = n_samples // 2
    n_neg = n_samples - n_pos

    # Class 0: lower momentum, higher volatility
    x0_f1 = rng.normal(-1.5, 0.8, n_neg)
    x0_f2 = rng.normal(2.0, 0.6, n_neg)
    x0_f3 = rng.normal(0, 1.0, n_neg)

    # Class 1: higher momentum, lower volatility
    x1_f1 = rng.normal(1.5, 0.8, n_pos)
    x1_f2 = rng.normal(-1.0, 0.6, n_pos)
    x1_f3 = rng.normal(0, 1.0, n_pos)

    X = np.vstack([
        np.column_stack([x0_f1, x0_f2, x0_f3]),
        np.column_stack([x1_f1, x1_f2, x1_f3])
    ])
    y = np.array([0] * n_neg + [1] * n_pos)

    # Shuffle
    indices = rng.permutation(len(X))
    X, y = X[indices], y[indices]

    # UTC timestamp index
    dt_index = pd.date_range("2020-01-01", periods=len(X), freq="D", tz="UTC")
    index_col = dt_index.strftime("%Y-%m-%dT%H:%M:%SZ")

    return pd.DataFrame({
        "index": index_col,
        "momentum": X[:, 0],
        "volatility": X[:, 1],
        "noise": X[:, 2],
        "target": y.astype(int),
    })


def make_regression_data(n_samples: int = 500, seed: int = 42) -> pd.DataFrame:
    """Generate regression data with linear + noise relationship."""
    rng = np.random.default_rng(seed)

    # Features
    x1 = rng.normal(0, 1.5, n_samples)  # main signal
    x2 = rng.normal(0, 1.0, n_samples)  # secondary signal
    x3 = rng.normal(0, 1.0, n_samples)  # noise

    # Target: linear combination + noise
    noise = rng.normal(0, 0.5, n_samples)
    y = 2.0 * x1 + 0.5 * x2 + noise

    dt_index = pd.date_range("2020-01-01", periods=n_samples, freq="D", tz="UTC")
    index_col = dt_index.strftime("%Y-%m-%dT%H:%M:%SZ")

    return pd.DataFrame({
        "index": index_col,
        "signal_1": x1,
        "signal_2": x2,
        "noise": x3,
        "target": y,
    })


def generate_logistic_expected(df: pd.DataFrame) -> pd.DataFrame:
    """Generate expected outputs using sklearn LogisticRegression (L2)."""
    features = ["momentum", "volatility", "noise"]
    X = df[features].values
    y = df["target"].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = LogisticRegression(penalty="l2", C=1.0, solver="lbfgs", max_iter=1000)
    model.fit(X_scaled, y)

    predictions = model.predict(X_scaled)
    probabilities = model.predict_proba(X_scaled)[:, 1]
    decision_values = model.decision_function(X_scaled)

    return pd.DataFrame({
        "index": df["index"].values,
        "prediction": predictions.astype(int),
        "probability": probabilities,
        "decision_value": decision_values,
    })


def generate_svr_expected(df: pd.DataFrame) -> pd.DataFrame:
    """Generate expected outputs using sklearn LinearSVR (L2)."""
    features = ["signal_1", "signal_2", "noise"]
    X = df[features].values
    y = df["target"].values

    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    model = LinearSVR(C=1.0, epsilon=0.1, max_iter=10000, random_state=123)
    model.fit(X_scaled, y)

    predictions = model.predict(X_scaled)

    return pd.DataFrame({
        "index": df["index"].values,
        "prediction": predictions,
    })


def generate_lightgbm_classifier_expected(df: pd.DataFrame) -> pd.DataFrame:
    """Generate expected outputs using LightGBM classifier."""
    if not HAS_LIGHTGBM:
        return None

    features = ["momentum", "volatility", "noise"]
    X = df[features].values
    y = df["target"].values

    # Z-score normalization
    X_mean = X.mean(axis=0)
    X_std = X.std(axis=0)
    X_scaled = (X - X_mean) / X_std

    train_data = lgb.Dataset(X_scaled, label=y)
    params = {
        "objective": "binary",
        "metric": "binary_logloss",
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "learning_rate": 0.1,
        "verbose": -1,
        "seed": 123,
    }

    model = lgb.train(params, train_data, num_boost_round=100)

    probabilities = model.predict(X_scaled)
    predictions = (probabilities > 0.5).astype(int)

    return pd.DataFrame({
        "index": df["index"].values,
        "prediction": predictions,
        "probability": probabilities,
    })


def generate_lightgbm_regressor_expected(df: pd.DataFrame) -> pd.DataFrame:
    """Generate expected outputs using LightGBM regressor."""
    if not HAS_LIGHTGBM:
        return None

    features = ["signal_1", "signal_2", "noise"]
    X = df[features].values
    y = df["target"].values

    # Z-score
    X_mean = X.mean(axis=0)
    X_std = X.std(axis=0)
    X_scaled = (X - X_mean) / X_std

    train_data = lgb.Dataset(X_scaled, label=y)
    params = {
        "objective": "regression",
        "metric": "rmse",
        "boosting_type": "gbdt",
        "num_leaves": 31,
        "learning_rate": 0.1,
        "verbose": -1,
        "seed": 123,
    }

    model = lgb.train(params, train_data, num_boost_round=100)
    predictions = model.predict(X_scaled)

    return pd.DataFrame({
        "index": df["index"].values,
        "prediction": predictions,
    })


def main():
    script_dir = os.path.abspath(os.path.dirname(__file__))
    out_dir = os.path.join(script_dir, "ml_test_data")
    os.makedirs(out_dir, exist_ok=True)

    # Classification data
    cls_df = make_classification_data()
    cls_df.to_csv(os.path.join(out_dir, "classification_input.csv"), index=False)
    print("Wrote classification_input.csv")

    logistic_expected = generate_logistic_expected(cls_df)
    logistic_expected.to_csv(os.path.join(out_dir, "classification_expected_logistic.csv"), index=False)
    print("Wrote classification_expected_logistic.csv")

    if HAS_LIGHTGBM:
        lgb_cls_expected = generate_lightgbm_classifier_expected(cls_df)
        if lgb_cls_expected is not None:
            lgb_cls_expected.to_csv(os.path.join(out_dir, "classification_expected_lightgbm.csv"), index=False)
            print("Wrote classification_expected_lightgbm.csv")

    # Regression data
    reg_df = make_regression_data()
    reg_df.to_csv(os.path.join(out_dir, "regression_input.csv"), index=False)
    print("Wrote regression_input.csv")

    svr_expected = generate_svr_expected(reg_df)
    svr_expected.to_csv(os.path.join(out_dir, "regression_expected_svr.csv"), index=False)
    print("Wrote regression_expected_svr.csv")

    if HAS_LIGHTGBM:
        lgb_reg_expected = generate_lightgbm_regressor_expected(reg_df)
        if lgb_reg_expected is not None:
            lgb_reg_expected.to_csv(os.path.join(out_dir, "regression_expected_lightgbm.csv"), index=False)
            print("Wrote regression_expected_lightgbm.csv")

    print("\nDone generating ML test data!")


if __name__ == "__main__":
    main()
