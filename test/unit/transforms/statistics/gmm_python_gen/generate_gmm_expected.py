#!/usr/bin/env python3
"""
Generate GMM test data with known cluster structure.
Uses sklearn GMM for expected outputs.
"""
import os
import numpy as np
import pandas as pd
from sklearn.mixture import GaussianMixture

def make_clustered_data(n_samples: int = 300, n_features: int = 2,
                        n_clusters: int = 3, seed: int = 42) -> pd.DataFrame:
    """Generate data with known cluster structure."""
    rng = np.random.default_rng(seed)

    # Define cluster centers - well separated
    centers_2 = [[-3.0, -3.0], [3.0, 3.0]]
    centers_3 = [[-3.0, -3.0], [0.0, 3.0], [3.0, -1.0]]
    centers_4 = [[-3.0, -3.0], [3.0, 3.0], [-3.0, 3.0], [3.0, -3.0]]

    if n_clusters == 2:
        centers = centers_2
    elif n_clusters == 3:
        centers = centers_3
    else:
        centers = centers_4[:n_clusters]

    # Generate samples per cluster
    samples_per_cluster = n_samples // n_clusters
    data = []

    for center in centers:
        cluster_data = rng.normal(loc=center, scale=0.8,
                                   size=(samples_per_cluster, n_features))
        data.append(cluster_data)

    X = np.vstack(data)

    # Shuffle
    indices = rng.permutation(len(X))
    X = X[indices]

    # UTC timestamp index
    dt_index = pd.date_range("2020-01-01", periods=len(X), freq="D", tz="UTC")
    index_col = dt_index.strftime("%Y-%m-%dT%H:%M:%SZ")

    df = pd.DataFrame({"index": index_col})
    for i in range(n_features):
        df[f"feature_{i}"] = X[:, i]

    return df

def generate_expected(df_in: pd.DataFrame, n_components: int) -> pd.DataFrame:
    """Run sklearn GMM and generate expected outputs."""
    features = [c for c in df_in.columns if c.startswith("feature_")]
    X = df_in[features].values

    gmm = GaussianMixture(n_components=n_components,
                          covariance_type="full",
                          max_iter=300,
                          tol=1e-10,
                          random_state=123)
    gmm.fit(X)

    labels = gmm.predict(X)
    probs = gmm.predict_proba(X)
    log_likelihood = gmm.score_samples(X)

    out = pd.DataFrame({
        "index": df_in["index"].values,
        "component": labels.astype(int),
        "log_likelihood": log_likelihood,
    })

    for i in range(n_components):
        out[f"component_{i}_prob"] = probs[:, i]

    return out

def main():
    script_dir = os.path.abspath(os.path.dirname(__file__))
    out_dir = os.path.join(script_dir, "gmm_test_data")
    os.makedirs(out_dir, exist_ok=True)

    for n_components in (2, 3, 4):
        df_in = make_clustered_data(n_samples=300, n_clusters=n_components)
        expected = generate_expected(df_in, n_components)

        input_path = os.path.join(out_dir, f"gmm_input_{n_components}.csv")
        expected_path = os.path.join(out_dir, f"gmm_expected_{n_components}.csv")

        df_in.to_csv(input_path, index=False)
        expected.to_csv(expected_path, index=False)
        print(f"Wrote {input_path} and {expected_path}")

if __name__ == "__main__":
    main()
