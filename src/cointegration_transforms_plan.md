constarint [look at hmm oUI think# Cointegration Testing Transforms Implementation Plan

## Overview

Add cointegration testing transforms for pairs trading and statistical arbitrage strategies. These transforms enable identification of cointegrated pairs from the US equity universe by testing for long-term equilibrium relationships.

## Transforms to Implement

### 1. Rolling ADF Test (`rolling_adf`)

**Purpose**: Test stationarity of a spread series (prerequisite for mean-reversion)

**Inputs**:
- `SLOT`: Price/spread series (Decimal)

**Options**:
- `window`: Rolling window size (default: 252)
- `adf_lag`: Number of lagged differences (default: 1)
- `deterministic`: "c" (constant) or "ct" (constant + trend)
- `significance`: Significance level for is_stationary (default: 0.05)

**Outputs**:
- `adf_stat`: ADF test statistic
- `p_value`: MacKinnon p-value approximation
- `critical_1pct`, `critical_5pct`, `critical_10pct`: Critical values
- `is_stationary`: Boolean (p_value < significance)

**Implementation**: Extend existing `StationaryCheckVisitor` with p-value computation using MacKinnon (2010) tables.

---

### 2. Engle-Granger Cointegration (`engle_granger`)

**Purpose**: Two-step cointegration test for pairs trading

**Algorithm**:
1. OLS regression: `y = alpha + beta*x + residuals`
2. ADF test on residuals with Engle-Granger critical values

**Inputs**:
- `y`: Dependent variable (Decimal) - typically the asset to trade
- `x`: Independent variable (Decimal) - the hedge asset

**Options**:
- `window`: Rolling window size (default: 252)
- `adf_lag`: Lag for ADF test (default: 1)
- `significance`: Significance level (default: 0.05)

**Outputs**:
- `hedge_ratio`: OLS slope (beta) - number of shares of x per share of y
- `intercept`: OLS intercept (alpha)
- `spread`: Residual series (y - beta*x - alpha)
- `adf_stat`: ADF statistic on residuals
- `p_value`: Cointegration p-value (MacKinnon cointegration tables)
- `critical_1pct`, `critical_5pct`, `critical_10pct`: Critical values
- `half_life`: Mean-reversion half-life from AR(1) fit
- `is_cointegrated`: Boolean (p_value < significance)

**Critical Note**: Engle-Granger uses MORE STRINGENT critical values than standard ADF because residuals are estimated.

---

### 3. Johansen Cointegration (`johansen_N`)

**Purpose**: Multivariate cointegration for basket trading (3+ assets)

**Algorithm**: Vector Error Correction Model (VECM) reduced rank regression
1. Compute lagged differences and levels
2. OLS regressions to get residuals R0, R1
3. Solve generalized eigenvalue problem
4. Compute trace and max-eigenvalue statistics

**Inputs** (Template-based, N = 2-5 assets):
- `asset_0`, `asset_1`, ..., `asset_{N-1}`: Price series (Decimal)

**Options**:
- `window`: Rolling window size (default: 252)
- `lag_p`: VECM lag order (default: 1)
- `det_order`: Deterministic terms - "none", "const", "trend"

**Outputs**:
- `rank`: Estimated cointegration rank (0 to N-1)
- `trace_stat_0`, ..., `trace_stat_{N-2}`: Trace statistics
- `max_stat_0`, ..., `max_stat_{N-2}`: Max-eigenvalue statistics
- `eigval_0`, ..., `eigval_{N-2}`: Sorted eigenvalues
- `beta_0_0`, `beta_0_1`, ...: Cointegration vector weights
- `spread_0`, `spread_1`, ...: Computed spreads (Y * beta)

**Implementation**: Use Armadillo for matrix operations (existing integration in `dataframe_armadillo_utils.h`).

---

### 4. Half-Life AR(1) (`half_life_ar1`)

**Purpose**: Estimate mean-reversion speed of a spread series

**Algorithm**: Fit AR(1) model: `spread_t = phi * spread_{t-1} + epsilon`
- Half-life = -log(2) / log(phi)

**Inputs**:
- `SLOT`: Spread series (Decimal)

**Options**:
- `window`: Rolling window size (default: 60)

**Outputs**:
- `half_life`: Mean-reversion half-life in periods
- `ar1_coef`: AR(1) coefficient (phi)
- `is_mean_reverting`: Boolean (0 < phi < 1)

**Implementation**: Simple OLS regression of spread on lagged spread.

---

## File Structure

```
src/transforms/components/hosseinmoein/statistics/
├── mackinnon_tables.h          # ADF critical values and p-values
├── johansen_tables.h           # Johansen critical value tables
├── rolling_adf.h               # Rolling ADF stationarity test
├── engle_granger.h             # Two-step cointegration
├── johansen.h                  # Multivariate cointegration (template)
└── cointegration_metadata.h    # C++ metadata definitions
```

---

## Implementation Details

### MacKinnon Critical Value Tables (`mackinnon_tables.h`)

```cpp
namespace epoch_script::transform::mackinnon {

// Standard ADF critical values (no cointegration)
struct ADFCriticalValues {
    static double get_critical_value(size_t sample_size,
                                     const std::string& deterministic,
                                     double significance);
    static double get_pvalue(double tau, size_t sample_size,
                            const std::string& deterministic);
};

// Engle-Granger cointegration critical values (more stringent)
struct CointegrationCriticalValues {
    static double get_critical_value(size_t sample_size,
                                     size_t n_variables,
                                     double significance);
    static double get_pvalue(double tau, size_t sample_size,
                            size_t n_variables);
};

} // namespace
```

### Engle-Granger Transform Pattern

```cpp
class EngleGranger final : public ITransform {
public:
    explicit EngleGranger(const TransformConfiguration &config);

    [[nodiscard]] epoch_frame::DataFrame
    TransformData(epoch_frame::DataFrame const &df) const override;

private:
    int64_t m_window;
    size_t m_adf_lag;
    double m_significance;

    // Following LinearFit pattern: accumulate vectors, concat at end
    struct WindowResult {
        double hedge_ratio, intercept, spread, adf_stat;
        double p_value, cv_1pct, cv_5pct, cv_10pct, half_life;
        bool is_cointegrated;
    };
};
```

### Johansen Template Pattern (following HMM)

```cpp
template <size_t N_ASSETS>
class JohansenCointegration final : public ITransform {
    static_assert(N_ASSETS >= 2 && N_ASSETS <= 5);

    // Dynamic output generation based on N_ASSETS
    // Uses Armadillo for eigenvalue decomposition
};

// Type aliases for registration
using Johansen2 = JohansenCointegration<2>;  // Pairs
using Johansen3 = JohansenCointegration<3>;  // Triplets
using Johansen4 = JohansenCointegration<4>;
using Johansen5 = JohansenCointegration<5>;
```

---

## Registration

Update `src/transforms/components/registration.cpp`:

```cpp
#include "hosseinmoein/statistics/rolling_adf.h"
#include "hosseinmoein/statistics/engle_granger.h"
#include "hosseinmoein/statistics/johansen.h"
#include "hosseinmoein/statistics/cointegration_metadata.h"

// In InitializeTransforms():
REGISTER_TRANSFORM(rolling_adf, RollingADF);
REGISTER_TRANSFORM(engle_granger, EngleGranger);
REGISTER_TRANSFORM(johansen_2, Johansen2);
REGISTER_TRANSFORM(johansen_3, Johansen3);
REGISTER_TRANSFORM(johansen_4, Johansen4);
REGISTER_TRANSFORM(johansen_5, Johansen5);
```

---

## Testing Strategy

### Unit Tests
- Synthetic cointegrated series with known hedge ratios
- Compare outputs against statsmodels reference values
- Edge cases: insufficient data, perfect collinearity, trending series

### Test Fixtures
Generate Python reference values:
```python
from statsmodels.tsa.stattools import coint, adfuller
from statsmodels.tsa.vector_ar.vecm import coint_johansen

# Store expected outputs as CSV/JSON in test/test_data/cointegration/
```

### Validation Tolerances
- Eigenvalues: 1e-6
- Test statistics: 1e-4
- P-values: 1e-3
- Critical values: exact match

---

## Downstream Usage Example

```
prices_y ─────┐
              ├─► engle_granger ─┬─► spread ──────► zscore ──► entry_signals
prices_x ─────┘                  │
                                 ├─► hedge_ratio ─► position_sizing
                                 │
                                 ├─► is_cointegrated ─► trade_filter
                                 │
                                 └─► half_life ─► holding_period
```

---

## Critical Files to Read Before Implementation

1. `src/transforms/components/hosseinmoein/statistics/stationary_check.h` - Existing ADF/KPSS
2. `src/transforms/components/hosseinmoein/statistics/linear_fit.h` - Multi-output pattern
3. `src/transforms/components/hosseinmoein/statistics/rolling_corr.h` - Two-input rolling pattern
4. `src/transforms/components/statistics/dataframe_armadillo_utils.h` - Armadillo integration
5. `src/transforms/components/datetime/datetime_metadata.h` - C++ metadata pattern

---

## Transforms Summary

| Transform | Inputs | Key Outputs |
|-----------|--------|-------------|
| `rolling_adf` | spread series | adf_stat, p_value, is_stationary |
| `engle_granger` | y, x series | hedge_ratio, spread, p_value, is_cointegrated |
| `johansen_2` - `johansen_5` | 2-5 assets | rank, trace_stats, beta vectors, spreads |
| `half_life_ar1` | spread series | half_life, ar1_coefficient |

## Implementation Order (All Transforms in Parallel)

1. **Phase 1**: MacKinnon + Johansen critical value tables (foundation)
2. **Phase 2**: All transforms in parallel:
   - Rolling ADF with p-values
   - Engle-Granger (without embedded half-life)
   - Johansen templates (2-5 assets)
   - Half-Life AR(1) as separate transform
3. **Phase 3**: Tests and validation against statsmodels