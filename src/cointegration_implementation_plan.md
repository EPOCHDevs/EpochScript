# Cointegration Implementation Plan

## Overview

This document outlines the phased implementation plan for cointegration testing transforms and supporting infrastructure.

**Related Documents**:
- `src/cointegration_transforms_plan.md` - Transform specifications and API design
- `src/pairs_trading_orchestration_ideas.md` - Future orchestration extensions
- `src/cointegration_plotkind_ideas.md` - Visualization concepts

---

## Phase 1: Foundation (Non-Blocking, Parallel)

All tasks in this phase can be executed simultaneously.

### 1.1 MacKinnon Critical Value Tables

**File**: `src/transforms/components/hosseinmoein/statistics/mackinnon_tables.h`

**Purpose**: ADF and cointegration critical values + p-value computation

**Contents**:
```cpp
namespace epoch_script::transform::mackinnon {
    // Standard ADF critical values
    struct ADFCriticalValues {
        static double get_critical_value(size_t T, std::string det, double sig);
        static double get_pvalue(double tau, size_t T, std::string det);
    };

    // Engle-Granger cointegration critical values (more stringent)
    struct CointegrationCriticalValues {
        static double get_critical_value(size_t T, size_t n_vars, double sig);
        static double get_pvalue(double tau, size_t T, size_t n_vars);
    };
}
```

**Reference**: MacKinnon (2010) "Critical Values for Cointegration Tests"

**Dependencies**: None

---

### 1.2 Johansen Critical Value Tables

**File**: `src/transforms/components/hosseinmoein/statistics/johansen_tables.h`

**Purpose**: Trace and max-eigenvalue critical values for Johansen test

**Contents**:
```cpp
namespace epoch_script::transform::johansen {
    struct JohansenCriticalValues {
        // Indexed by [det_order][k_minus_r][significance]
        static double get_trace_cv(int det_order, int k_minus_r, double sig);
        static double get_max_eigen_cv(int det_order, int k_minus_r, double sig);
    };
}
```

**Reference**: Johansen (1995), Osterwald-Lenum (1992)

**Dependencies**: None

---

### 1.3 Asset Scalar Transform (`asset_ref`)

**Key Insight**: Asset Scalar vs Regular Scalar

| | **Scalar** | **Asset Scalar** |
|---|---|---|
| **Known at** | Compile time | Orchestrator init |
| **Value** | Same for all assets | Different per asset |
| **Computed by** | Compiler (inlined away) | Orchestrator (before graph runs) |
| **Stored in** | `literal_inputs` | `cache[asset_id][output_id]` |

**Files**:
- `src/transforms/components/utility/asset_ref.h` - Transform definition
- `src/transforms/components/utility/asset_ref_metadata.h` - Metadata
- `src/transforms/runtime/orchestrator.cpp` - Pre-computation logic

**How It Works**:

```
COMPILE TIME:
  Regular Scalar: 1.5 → inlined, node removed
  Asset Scalar: asset_ref("is_spy", ticker="SPY") → node KEPT, marked AssetScalar

ORCHESTRATOR INIT (before graph execution):
  For each AssetScalar node:
    For each asset_id in session:
      bool value = EvaluateAssetFilter(asset_id, filterConfig)
      cache[asset_id][output_id] = Scalar(value)  // Stored as bool

RUNTIME (GatherInputs):
  Treats it like any other cached value - already computed, just use it
```

**Specification**:
```cpp
// Category marks it for special handling
struct AssetRefMetadata : TransformsMetaData {
    .id = "asset_ref",
    .category = TransformCategory::AssetScalar,  // NEW CATEGORY
    .inputs = {},  // No inputs
    .options = {
        {"ticker", String, optional},           // "AAPL" or "AAPL,MSFT" or regex
        {"asset_class", Select, optional},      // Equity, ETF, Crypto, Futures
        {"sector", String, optional},           // "Technology"
        {"industry", String, optional},         // "Semiconductors"
        {"futures_category", Select, optional}, // Commodity, Index, Currency
    },
    .outputs = {
        {Boolean, "match", "Asset Matches Filter"}
    }
};

// Helper function for evaluation
namespace epoch_script::transform {
bool EvaluateAssetFilter(const std::string& assetId,
                         const AssetFilterConfig& config);
}
```

**Orchestrator Change** (`orchestrator.cpp`):
```cpp
void Orchestrator::InitializeAssetScalars() {
    for (auto& node : m_nodes) {
        if (node.category == TransformCategory::AssetScalar) {
            for (const auto& asset_id : m_asset_ids) {
                bool value = EvaluateAssetFilter(asset_id, node.options);
                m_cache->StoreScalar(asset_id, node.outputId, value);
            }
        }
    }
}
```

**Dependencies**: None (can implement in parallel with tables)

---

## Phase 2: Core Transforms (Partial Dependencies)

### 2.1 Half-Life AR(1)

**File**: `src/transforms/components/hosseinmoein/statistics/half_life_ar1.h`

**Purpose**: Estimate mean-reversion speed from AR(1) coefficient

**Specification**:
- **Inputs**: `SLOT` (spread series)
- **Options**: `window` (default: 60)
- **Outputs**: `half_life`, `ar1_coef`, `is_mean_reverting`
- **Formula**: `half_life = -log(2) / log(phi)` where phi is AR(1) coefficient

**Pattern Reference**:
- `src/transforms/components/hosseinmoein/statistics/linear_fit.h`

**Dependencies**: None (can start immediately)

---

### 2.2 Rolling ADF

**File**: `src/transforms/components/hosseinmoein/statistics/rolling_adf.h`

**Purpose**: Rolling ADF stationarity test with p-values

**Specification**:
- **Inputs**: `SLOT` (price/spread series)
- **Options**: `window`, `adf_lag`, `deterministic`, `significance`
- **Outputs**: `adf_stat`, `p_value`, `critical_1pct`, `critical_5pct`, `critical_10pct`, `is_stationary`

**Pattern Reference**:
- `src/transforms/components/hosseinmoein/statistics/stationary_check.h` (existing ADF)
- `src/transforms/components/hosseinmoein/statistics/linear_fit.h` (multi-output)

**Dependencies**: MacKinnon tables (1.1)

---

### 2.3 Engle-Granger Cointegration

**File**: `src/transforms/components/hosseinmoein/statistics/engle_granger.h`

**Purpose**: Two-step cointegration test for pairs

**Specification**:
- **Inputs**: `y` (dependent), `x` (independent)
- **Options**: `window`, `adf_lag`, `significance`
- **Outputs**: `hedge_ratio`, `intercept`, `spread`, `adf_stat`, `p_value`, `critical_1pct`, `critical_5pct`, `critical_10pct`, `is_cointegrated`

**Algorithm**:
1. OLS: `y = alpha + beta*x + residuals`
2. ADF test on residuals with cointegration critical values

**Pattern Reference**:
- `src/transforms/components/hosseinmoein/statistics/rolling_corr.h` (two-input pattern)
- `src/transforms/components/hosseinmoein/statistics/linear_fit.h` (OLS + multi-output)

**Dependencies**: MacKinnon cointegration tables (1.1)

---

### 2.4 Johansen Cointegration

**File**: `src/transforms/components/hosseinmoein/statistics/johansen.h`

**Purpose**: Multivariate cointegration test (2-5 assets)

**Specification**:
- **Inputs**: `asset_0`, `asset_1`, ... `asset_{N-1}` (template-based)
- **Options**: `window`, `lag_p`, `det_order`
- **Outputs**: `rank`, `trace_stat_*`, `max_stat_*`, `eigval_*`, `beta_*_*`, `spread_*`

**Implementation**: Template class with specializations for N=2,3,4,5

**Pattern Reference**:
- `src/transforms/components/statistics/hmm.h` (template pattern, Armadillo)
- `src/transforms/components/statistics/dataframe_armadillo_utils.h` (matrix ops)

**Dependencies**: Johansen tables (1.2)

---

## Phase 3: Integration (Sequential, Blocking)

### 3.1 Extended Input Mapping

**Files to Modify**:
- `include/epoch_script/transforms/core/input_mapping.h`

**Change**: Add `assetRef` field to InputMapping

```cpp
struct InputMapping {
    // Existing fields...

    // NEW: Asset reference for pair support
    std::optional<AssetRef> assetRef;
};

struct AssetRef {
    enum class Mode { Explicit, Role, Filter };
    Mode mode;
    std::string value;  // "AAPL" or "dependent" or "asset_ref_node_id"
};
```

---

### 3.2 GatherInputs Update

**File to Modify**: `src/transforms/runtime/execution/intermediate_storage.cpp`

**Change**: Handle `assetRef` in `GatherInputs()`

```cpp
// In GatherInputs, after resolving nodeRef:
if (inputMapping.assetRef) {
    auto targetAsset = ResolveAssetRef(inputMapping.assetRef);
    // Fetch data for targetAsset instead of current asset
}
```

---

### 3.3 Registration

**File to Modify**: `src/transforms/components/registration.cpp`

**Add**:
```cpp
#include "hosseinmoein/statistics/rolling_adf.h"
#include "hosseinmoein/statistics/engle_granger.h"
#include "hosseinmoein/statistics/johansen.h"
#include "hosseinmoein/statistics/half_life_ar1.h"
#include "utility/asset_ref.h"

// In InitializeTransforms():
REGISTER_TRANSFORM(asset_ref, AssetRef);
REGISTER_TRANSFORM(rolling_adf, RollingADF);
REGISTER_TRANSFORM(engle_granger, EngleGranger);
REGISTER_TRANSFORM(half_life_ar1, HalfLifeAR1);
REGISTER_TRANSFORM(johansen_2, Johansen2);
REGISTER_TRANSFORM(johansen_3, Johansen3);
REGISTER_TRANSFORM(johansen_4, Johansen4);
REGISTER_TRANSFORM(johansen_5, Johansen5);
```

---

### 3.4 Metadata

**File**: `src/transforms/components/hosseinmoein/statistics/cointegration_metadata.h`

**Contents**: C++ metadata for all cointegration transforms

**Pattern Reference**: `src/transforms/components/datetime/datetime_metadata.h`

---

## Phase 4: Validation (Blocking)

### 4.1 Python Reference Fixtures

**Directory**: `test/test_data/cointegration/`

**Files to Create**:
- `synthetic_cointegrated_pair.csv` - Known hedge ratio
- `adf_expected.json` - statsmodels.adfuller reference
- `engle_granger_expected.json` - statsmodels.coint reference
- `johansen_expected.json` - statsmodels.coint_johansen reference

**Generator Script**: `test/tools/generate_cointegration_fixtures.py`

---

### 4.2 Unit Tests

**Directory**: `test/unit/transforms/hosseinmoein/statistics/`

**Files to Create**:
- `rolling_adf_test.cpp`
- `engle_granger_test.cpp`
- `johansen_test.cpp`
- `half_life_ar1_test.cpp`
- `asset_ref_test.cpp`

**Pattern Reference**: `test/unit/transforms/hosseinmoein/rolling_corr_test.cpp`

---

### 4.3 Integration Tests

**Directory**: `test/integration/`

**Test**: Full pipeline - prices → engle_granger → zscore → signals

---

## Phase 5: Polish (Non-Blocking)

### 5.1 PlotKind

**Files**:
- `include/epoch_script/transforms/core/metadata.h` - Add `spread_zscore` to enum
- `src/chart_metadata/plot_kinds/builders/spread_zscore_builder.h`
- `src/chart_metadata/plot_kinds/registry.cpp` - Register builder

**Reference**: `src/cointegration_plotkind_ideas.md`

---

## Dependency Graph

```
Phase 1 (parallel)
├── mackinnon_tables.h ─────────────────┐
├── johansen_tables.h ──────────────────┼──┐
└── asset_ref.h ────────────────────────┼──┼──┐
                                        │  │  │
Phase 2 (partial deps)                  │  │  │
├── half_life_ar1.h (no deps) ──────────┼──┼──┤
├── rolling_adf.h ◄─────────────────────┘  │  │
├── engle_granger.h ◄──────────────────────┘  │
└── johansen.h ◄───────────────────────────┘  │
                                              │
Phase 3 (sequential)                          │
├── InputMapping update ◄─────────────────────┘
├── GatherInputs update
├── registration.cpp
└── cointegration_metadata.h

Phase 4 (blocking)
├── Python fixtures
├── Unit tests
└── Integration tests

Phase 5 (optional)
├── spread_zscore PlotKind
└── Documentation
```

---

## File Locations Summary

| Component | Path |
|-----------|------|
| MacKinnon tables | `src/transforms/components/hosseinmoein/statistics/mackinnon_tables.h` |
| Johansen tables | `src/transforms/components/hosseinmoein/statistics/johansen_tables.h` |
| Asset ref | `src/transforms/components/utility/asset_ref.h` |
| Rolling ADF | `src/transforms/components/hosseinmoein/statistics/rolling_adf.h` |
| Engle-Granger | `src/transforms/components/hosseinmoein/statistics/engle_granger.h` |
| Half-life AR1 | `src/transforms/components/hosseinmoein/statistics/half_life_ar1.h` |
| Johansen | `src/transforms/components/hosseinmoein/statistics/johansen.h` |
| Metadata | `src/transforms/components/hosseinmoein/statistics/cointegration_metadata.h` |
| Registration | `src/transforms/components/registration.cpp` |
| Input mapping | `include/epoch_script/transforms/core/input_mapping.h` |
| Intermediate storage | `src/transforms/runtime/execution/intermediate_storage.cpp` |
| Tests | `test/unit/transforms/hosseinmoein/statistics/` |
| Test data | `test/test_data/cointegration/` |

---

## Critical Files to Read First

Before implementing, read these for patterns:

1. `src/transforms/components/hosseinmoein/statistics/stationary_check.h` - Existing ADF
2. `src/transforms/components/hosseinmoein/statistics/linear_fit.h` - Multi-output rolling
3. `src/transforms/components/hosseinmoein/statistics/rolling_corr.h` - Two-input pattern
4. `src/transforms/components/statistics/dataframe_armadillo_utils.h` - Matrix ops
5. `src/transforms/components/statistics/hmm.h` - Template pattern
6. `src/transforms/components/datetime/datetime_metadata.h` - C++ metadata
7. `src/transforms/components/scalar.h` - Scalar caching pattern