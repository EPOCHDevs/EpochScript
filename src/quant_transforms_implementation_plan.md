# Quant Finance Transforms Implementation Plan

## Overview

Implement 7 core quant finance transforms using HosseinMoein DataFrame (upgraded via CPM) and mlpack.

| Transform | Library | Complexity | Priority |
|-----------|---------|------------|----------|
| **ARIMA** | HMDF `ARIMAVisitor` | O(n·p·q) | Tier 1 |
| **HWES (Holt-Winters)** | HMDF `HWESForecastVisitor` | O(n) | Tier 1 |
| **PCA** | mlpack `PCA` | O(n·d²) | Tier 1 |
| **K-Means** | HMDF `KMeansVisitor<K>` | O(n·k·i) | Tier 2 |
| **DBSCAN** | HMDF `DBSCANVisitor` | O(n log n) | Tier 2 |
| **Spectral Clustering** | HMDF `SpectralClusteringVisitor<K>` | O(n³) | Tier 2 |
| **ICA/RADICAL** | mlpack `Radical` | O(n·d²) | Tier 3 |

### Templated K Values (for K-Means, Spectral)
- `kmeans_2`, `kmeans_3`, `kmeans_4`, `kmeans_5`
- `spectral_2`, `spectral_3`, `spectral_4`, `spectral_5`

---

## BLOCKING PHASE 0: DataFrame CPM Upgrade

**Must complete before any transforms can be implemented.**

### Step 0.1: Create `cmake/DataFrame.cmake`

```cmake
# DataFrame.cmake - HosseinMoein DataFrame from GitHub master via CPM
include_guard()

if(NOT COMMAND CPMAddPackage)
    include(${CMAKE_CURRENT_LIST_DIR}/CPM.cmake)
endif()

CPMAddPackage(
  NAME DataFrame
  GIT_REPOSITORY https://github.com/hosseinmoein/DataFrame.git
  GIT_TAG master
  OPTIONS
    "HMDF_TESTING OFF"
    "HMDF_EXAMPLES OFF"
    "HMDF_BENCHMARKS OFF"
)

if(DataFrame_ADDED)
  add_library(DataFrame_CPM INTERFACE)
  target_include_directories(DataFrame_CPM INTERFACE ${DataFrame_SOURCE_DIR}/include)
  add_library(DataFrame::DataFrame_CPM ALIAS DataFrame_CPM)
  message(STATUS "DataFrame configured via CPM from master branch")
endif()
```

### Step 0.2: Update `CMakeLists.txt`

1. Add after line 60: `include(${PROJECT_SOURCE_DIR}/cmake/DataFrame.cmake)`
2. Comment out line 63: `# find_package(DataFrame CONFIG REQUIRED)`
3. Change line 101: `DataFrame::DataFrame` → `DataFrame::DataFrame_CPM`

### Step 0.3: Update `vcpkg.json`

Remove `"dataframe"` from dependencies array.

### Step 0.4: Rebuild and verify

```bash
rm -rf cmake-build-release-test
cmake -B cmake-build-release-test -DBUILD_TEST=ON
cmake --build cmake-build-release-test --target epoch_script
```

---

## NON-BLOCKING PHASE 1: Time Series Forecasting (can run in parallel)

### Phase 1A: ARIMA Transform

**File**: `src/transforms/components/hosseinmoein/statistics/arima.h`

**Options**:
- `periods` (Integer, default: 3) - Forecast periods ahead
- `autoreg_order` (Integer, default: 1) - AR order (p)
- `diff` (Integer, default: 0) - Differencing order (d)
- `mav_order` (Integer, default: 1) - MA order (q)

**Inputs**: Single series via `x`

**Outputs**: `forecast`, `residual`

### Phase 1B: HWES (Holt-Winters) Transform

**File**: `src/transforms/components/hosseinmoein/statistics/hwes.h`

**Options**:
- `periods` (Integer, default: 1) - Forecast periods
- `season_length` (Integer, default: 12) - Seasonal cycle length
- `alpha` (Decimal, default: 0.2) - Level smoothing
- `beta` (Decimal, default: 0.1) - Trend smoothing
- `gamma` (Decimal, default: 0.1) - Seasonal smoothing
- `season_type` (String, default: "additive") - "additive" or "multiplicative"

**Inputs**: Single series via `x`

**Outputs**: `forecast`, `seasonal_factor`

---

## NON-BLOCKING PHASE 2: Clustering Transforms (can run in parallel)

All clustering transforms support:
- `lookback_window` (0 = full dataset, >0 = rolling window)
- `compute_zscore` (boolean, default: true)
- Multi-input via SLOT with `allowMultipleConnections: true`

### Phase 2A: K-Means Transform (K = 2, 3, 4, 5)

**File**: `src/transforms/components/hosseinmoein/statistics/kmeans.h`

```cpp
template <size_t K>
class KMeansTransform final : public ITransform {
  static_assert(K >= 2 && K <= 5, "KMeans supports 2-5 clusters");
  // Uses hmdf::KMeansVisitor<K, double, int64_t>
};

using KMeans2Transform = KMeansTransform<2>;
using KMeans3Transform = KMeansTransform<3>;
using KMeans4Transform = KMeansTransform<4>;
using KMeans5Transform = KMeansTransform<5>;
```

**Options**:
- `max_iterations` (Integer, default: 1000)
- `compute_zscore` (Boolean, default: true)
- `lookback_window` (Integer, default: 0)

**Outputs**: `cluster_label`, `cluster_0_dist`, `cluster_1_dist`, ..., `cluster_{K-1}_dist`

### Phase 2B: DBSCAN Transform

**File**: `src/transforms/components/hosseinmoein/statistics/dbscan.h`

**Options**:
- `epsilon` (Decimal, default: 0.5) - Neighborhood radius
- `min_points` (Integer, default: 5) - Min points for core
- `compute_zscore` (Boolean, default: true)
- `lookback_window` (Integer, default: 0)

**Outputs**: `cluster_label`, `is_anomaly`, `cluster_count`

### Phase 2C: Spectral Clustering Transform (K = 2, 3, 4, 5)

**File**: `src/transforms/components/hosseinmoein/statistics/spectral.h`

```cpp
template <size_t K>
class SpectralTransform final : public ITransform {
  static_assert(K >= 2 && K <= 5, "Spectral supports 2-5 clusters");
  // Uses hmdf::SpectralClusteringVisitor<K, double, int64_t>
};

using Spectral2Transform = SpectralTransform<2>;
using Spectral3Transform = SpectralTransform<3>;
using Spectral4Transform = SpectralTransform<4>;
using Spectral5Transform = SpectralTransform<5>;
```

**Options**:
- `sigma` (Decimal, default: 1.0) - RBF kernel bandwidth
- `max_iterations` (Integer, default: 1000)
- `compute_zscore` (Boolean, default: true)
- `lookback_window` (Integer, default: 0)

**Outputs**: Same as K-Means

---

## NON-BLOCKING PHASE 3: Dimensionality Reduction (can run in parallel)

### Phase 3A: PCA Transform

**File**: `src/transforms/components/statistics/pca.h`

**Options**:
- `n_components` (Integer, default: 0) - 0 = keep all or use variance_retained
- `variance_retained` (Decimal, default: 0.0) - 0.95 = keep 95% variance
- `scale_data` (Boolean, default: true)
- `lookback_window` (Integer, default: 0)

**Inputs**: Multiple series via SLOT

**Outputs**: `pc_0`, `pc_1`, ..., `explained_variance_ratio`

### Phase 3B: ICA (RADICAL) Transform

**File**: `src/transforms/components/statistics/ica.h`

**Options**:
- `noise_std_dev` (Decimal, default: 0.175)
- `replicates` (Integer, default: 30)
- `angles` (Integer, default: 150)
- `lookback_window` (Integer, default: 0)

**Inputs**: Multiple series via SLOT

**Outputs**: `ic_0`, `ic_1`, ...

---

## BLOCKING PHASE 4: Registration & Metadata

**Must complete after all transforms are implemented.**

### Step 4.1: Update `registration.cpp`

```cpp
#include "hosseinmoein/statistics/arima.h"
#include "hosseinmoein/statistics/hwes.h"
#include "hosseinmoein/statistics/kmeans.h"
#include "hosseinmoein/statistics/dbscan.h"
#include "hosseinmoein/statistics/spectral.h"
#include "statistics/pca.h"
#include "statistics/ica.h"

// Time Series
REGISTER_TRANSFORM(arima, ARIMATransform);
REGISTER_TRANSFORM(hwes, HWESTransform);

// Clustering (K = 2, 3, 4, 5)
REGISTER_TRANSFORM(kmeans_2, KMeans2Transform);
REGISTER_TRANSFORM(kmeans_3, KMeans3Transform);
REGISTER_TRANSFORM(kmeans_4, KMeans4Transform);
REGISTER_TRANSFORM(kmeans_5, KMeans5Transform);

REGISTER_TRANSFORM(dbscan, DBSCANTransform);

REGISTER_TRANSFORM(spectral_2, Spectral2Transform);
REGISTER_TRANSFORM(spectral_3, Spectral3Transform);
REGISTER_TRANSFORM(spectral_4, Spectral4Transform);
REGISTER_TRANSFORM(spectral_5, Spectral5Transform);

// Dimensionality Reduction
REGISTER_TRANSFORM(pca, PCATransform);
REGISTER_TRANSFORM(ica, ICATransform);
```

### Step 4.2: Add YAML Metadata to `files/transforms.yaml`

Add entries for all 12 transforms (arima, hwes, kmeans_2-5, dbscan, spectral_2-5, pca, ica).

### Step 4.3: Build and Test

```bash
cmake --build cmake-build-release-test
./cmake-build-release-test/bin/epoch_test_runner
```

---

## Execution Summary

```
BLOCKING PHASE 0: DataFrame CPM Upgrade
    ↓
    ├── NON-BLOCKING PHASE 1A: ARIMA ──────────────┐
    ├── NON-BLOCKING PHASE 1B: HWES ───────────────┤
    ├── NON-BLOCKING PHASE 2A: K-Means (2,3,4,5) ──┤
    ├── NON-BLOCKING PHASE 2B: DBSCAN ─────────────┤  (all parallel)
    ├── NON-BLOCKING PHASE 2C: Spectral (2,3,4,5) ─┤
    ├── NON-BLOCKING PHASE 3A: PCA ────────────────┤
    └── NON-BLOCKING PHASE 3B: ICA ────────────────┘
                        ↓
BLOCKING PHASE 4: Registration & Metadata
```

---

## Critical Files

### Files to Create:
- `cmake/DataFrame.cmake`
- `src/transforms/components/hosseinmoein/statistics/arima.h`
- `src/transforms/components/hosseinmoein/statistics/hwes.h`
- `src/transforms/components/hosseinmoein/statistics/kmeans.h`
- `src/transforms/components/hosseinmoein/statistics/dbscan.h`
- `src/transforms/components/hosseinmoein/statistics/spectral.h`
- `src/transforms/components/statistics/pca.h`
- `src/transforms/components/statistics/ica.h`

### Files to Modify:
- `CMakeLists.txt` (lines 60, 63, 101)
- `vcpkg.json` (remove "dataframe")
- `src/transforms/components/registration.cpp`
- `files/transforms.yaml`

### Reference Files:
- `src/transforms/components/statistics/hmm.h` - Multi-input pattern
- `src/transforms/components/hosseinmoein/statistics/rolling_corr.h` - HMDF visitor pattern
- `src/transforms/components/statistics/dataframe_armadillo_utils.h` - DataFrame ↔ Armadillo
