# Cointegration Plan (C++ only)

This outlines how to add native cointegration tooling (pair + multi-asset) into the existing transform stack (ADF/KPSS live here).

## Goals
- Provide a Johansen VECM transform for k≥2 assets to produce cointegration vectors and spreads.
- Expose outputs the runtime can trade on: spreads, eigenvalues, trace/max statistics, and critical values.
- Reuse existing stationarity pieces (ADF/KPSS, zscore, half-life) for validation and signal generation.
- Keep everything C++/Armadillo (no Python bridge).

## Components to add
- **Transform: `johansen_coint`**
  - Inputs: multi-column log-price frame (one column per asset).
  - Options: `lag_p` (>=1), `det_order` (none/const/trend), `rank_max`, `num_vectors_out` (how many β to emit).
  - Outputs:
    - `beta`: cointegration vectors (columns) and maybe a normalized variant (e.g., scale on first asset).
    - `lambda`: eigenvalues.
    - `trace_stat`, `max_stat`, and corresponding critical values (5%, 1%).
    - `spread_i` columns (Y * β_i) for downstream ADF/KPSS/zscore/half-life.
- **Helper transform: `half_life_ar1`**
  - Input: single spread series.
  - Output: estimated half-life (from AR(1) phi) and optional p-value of phi<0.

## Implementation outline (Armadillo/LAPACK)
1) **Prep**: take log prices; difference to ΔY; build lagged levels and differences per VECM formulation.
2) **OLS blocks**:
   - Regress ΔY on lagged ΔY and dummies → residuals R0.
   - Regress lagged levels on lagged ΔY and dummies → residuals R1.
3) **Covariances**: S00 = R0'R0/T, S11 = R1'R1/T, S01 = R0'R1/T, S10 = S01'.
4) **Eigenproblem**: solve `inv(S11) * S10 * inv(S00) * S01` for eigenvalues/vectors (use `eig_sym` or generalized eig via LAPACK for stability). Sort descending.
5) **Test stats**:
   - Trace: `-T * sum_{i=r}^{k-1} log(1 - λ_i)`.
   - Max-eig: `-T * log(1 - λ_r)`.
   - Provide critical values table (Johansen 1995) for det_order {none/const/trend}.
6) **Outputs**:
   - β = eigenvectors (k×r); optionally normalize by fixing first element to 1.
   - Spreads: Y * β[:, i].
   - Stats + crit values for rank decision; include chosen rank if user passes `rank_max`.

## Metadata/registration
- Add C++ metadata entry (not YAML) alongside other components; set `requiresTimeFrame=true`, category `Statistics`, inputs multi-slot with `allowMultipleConnections=false` per asset (or a single SLOT expecting multi-column frame, consistent with cross-sectional handling).
- Register in `src/transforms/components/registration.cpp`.
- Mark as non-cross-sectional (operates on one asset-frame containing multiple columns); the orchestrator will pass per-asset frames, so ensure the compiler can feed a multi-asset frame when needed. If needed, add a dedicated cross-sectional flag/path similar to `cs_zscore`.

## Testing
- **Synthetic rank-1**: x = RW, y = x + noise, z = x + 2*noise → expect rank=1, β close to [1, -1, 0] (up to scale) and [1, 0, -0.5].
- **Comparison fixture**: fixed seed data with statsmodels outputs precomputed (checked in as CSV + JSON) to validate eigenvalues/trace/max within tolerance.
- **Half-life**: AR(1) synthetic to confirm half-life formula and sign handling.
- **Integration**: pipeline test: prices → johansen_coint → spread → ADF/KPSS → zscore to ensure graphs compile and run.

## Usage pattern (runtime graph)
```
prices -> johansen_coint -> spread_0
spread_0 -> adf_stationary_check
spread_0 -> kpss_stationary_check
spread_0 -> half_life_ar1
spread_0 -> zscore(window=W) -> entry/exit logic
```
Filter trades when ADF rejects unit root, KPSS does not reject stationarity, and half-life < threshold.

## Open decisions
- Normalization of β (first element=1 vs unit norm).
- How many spreads to emit by default (`num_vectors_out` vs inferred rank).
- Whether to expose rank selection inside the transform or leave to user via outputs + thresholds.
