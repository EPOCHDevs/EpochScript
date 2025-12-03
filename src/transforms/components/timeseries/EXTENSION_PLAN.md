# Time Series Extension Plan

This document outlines planned extensions for GARCH and ARIMA model families.
Implementation deferred - proceeding with rolling variants first.

---

## GARCH Extensions

### 1. EGARCH (Exponential GARCH)

**Model:**
```
log(σ²_t) = ω + Σ αᵢ g(z_{t-i}) + Σ βⱼ log(σ²_{t-j})

where g(z) = θz + γ(|z| - E[|z|])
      z_t = ε_t / σ_t  (standardized residuals)
```

**Key Features:**
- Models log-variance → no positivity constraints needed
- Captures asymmetry via θ parameter (leverage effect)
- γ captures magnitude effect
- For normal dist: E[|z|] = √(2/π)

**Implementation Notes:**
- Variance recursion on log scale
- Exponentiate for actual variance
- No stationarity constraint (always stable)
- Python validation: `arch_model(vol='EGARCH')`

---

### 2. TARCH / GJR-GARCH (Threshold ARCH)

**Model:**
```
σ²_t = ω + Σ αᵢ ε²_{t-i} + Σ γᵢ ε²_{t-i} I(ε_{t-i} < 0) + Σ βⱼ σ²_{t-j}

where I(·) is indicator function for negative shocks
```

**Key Features:**
- Explicit asymmetric response to positive/negative shocks
- γ > 0 means negative shocks increase volatility more (leverage)
- Stationarity: Σαᵢ + 0.5·Σγᵢ + Σβⱼ < 1

**Implementation Notes:**
- Simple extension of GARCH variance recursion
- Add indicator term for negative residuals
- Python validation: `arch_model(vol='GARCH', o=1)` for GJR

---

### 3. FIGARCH (Fractionally Integrated GARCH)

**Model:**
```
σ²_t = ω/(1-β(L)) + [1 - (1-β(L))⁻¹ φ(L)(1-L)^d] ε²_t

where d ∈ (0, 1) is fractional differencing parameter
```

**Key Features:**
- Long memory in volatility
- Hyperbolic decay of autocorrelations
- d = 0 → GARCH, d = 1 → IGARCH
- Captures volatility persistence better than GARCH

**Implementation Notes:**
- Requires truncated infinite AR representation
- Use Chung (1999) approximation
- More complex estimation - may need specialized optimizer
- Python validation: `arch_model(vol='FIGARCH')`

---

## ARIMA Extensions

### 1. SARIMA (Seasonal ARIMA)

**Model:**
```
Φ(Lˢ) φ(L) (1-L)^d (1-Lˢ)^D y_t = c + Θ(Lˢ) θ(L) ε_t

ARIMA(p,d,q)(P,D,Q)[s]

where:
  φ(L) = 1 - φ₁L - ... - φₚLᵖ        (non-seasonal AR)
  θ(L) = 1 + θ₁L + ... + θ_qL^q      (non-seasonal MA)
  Φ(Lˢ) = 1 - Φ₁Lˢ - ... - Φ_PLˢᴾ   (seasonal AR)
  Θ(Lˢ) = 1 + Θ₁Lˢ + ... + Θ_QLˢᵠ   (seasonal MA)
  s = seasonal period (e.g., 12 for monthly, 4 for quarterly)
```

**Key Features:**
- Handles seasonal patterns in data
- Multiplicative seasonal structure
- Common for financial calendar effects

**Implementation Notes:**
- Extend differencing to handle seasonal differencing
- Modify CSS objective to include seasonal lags
- Seasonal stationarity check via seasonal companion matrix
- Python validation: `SARIMAX` from statsmodels

---

### 2. ARIMAX (ARIMA with eXogenous variables)

**Model:**
```
φ(L)(1-L)^d y_t = c + β'X_t + θ(L) ε_t

where X_t is vector of exogenous regressors
```

**Key Features:**
- Include external predictors
- Useful for incorporating fundamental data
- Can model transfer function relationships

**Implementation Notes:**
- Extend input to accept multiple columns
- Include regression coefficients in optimization
- Two approaches:
  1. Regression with ARIMA errors
  2. Transfer function model
- Python validation: `SARIMAX(exog=X)` from statsmodels

---

## Implementation Priority

When implementing, suggested order:

1. **EGARCH** - Most commonly used, simpler than FIGARCH
2. **TARCH/GJR** - Simple extension, widely used for equity
3. **SARIMA** - Important for calendar-based strategies
4. **ARIMAX** - Useful for multi-factor models
5. **FIGARCH** - Most complex, specialized use cases

---

## Static to Reporter Conversion Plan

### Current Status
- **Rolling GARCH/ARIMA**: Production-ready, output rolling forecasts per timestamp
- **Static GARCH/ARIMA**: REMOVED - See rationale below

### Rationale for Removal
Static transforms that fit on the entire dataset introduce design complications:
1. **Forward Bias**: Model diagnostics (AIC, BIC) computed on full data leak future info
2. **No Walk-Forward**: Static models don't support proper out-of-sample validation
3. **Limited Use Case**: Only useful for research/exploratory analysis, not trading systems

### Future: Reporter Transforms
When research/exploratory use cases are needed, implement as **Reporter Transforms**:

```
garch_report:
  - Generates visualization cards, not output columns
  - Model summary card (parameters, AIC, BIC)
  - Conditional volatility line chart
  - Standardized residuals QQ-plot
  - Forecast fan chart

arima_report:
  - Model summary card (parameters, diagnostics)
  - Fitted vs Actual line chart
  - Residual ACF/PACF plots
  - Forecast with confidence intervals
```

### Production Pattern
For production trading systems, always use **rolling variants**:
- `rolling_garch`: Walk-forward volatility estimation
- `rolling_arima`: Walk-forward forecasting
- These output predictions per-timestamp without lookahead bias

---

## Common Infrastructure Needs

All extensions will leverage:
- Existing L-BFGS-B optimizer (`optimizer/lbfgs_optimizer.h`)
- Armadillo matrix operations
- ITransform base class pattern
- Python test data generation framework

---

## References

- Bollerslev, T. (1986). Generalized Autoregressive Conditional Heteroskedasticity
- Nelson, D. (1991). Conditional Heteroskedasticity in Asset Returns: A New Approach (EGARCH)
- Glosten, Jagannathan, Runkle (1993). On the Relation between Expected Value and Volatility (GJR)
- Baillie, Bollerslev, Mikkelsen (1996). Fractionally Integrated GARCH
- Box, Jenkins, Reinsel (2015). Time Series Analysis (ARIMA/SARIMA)
