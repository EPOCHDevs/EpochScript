# Task 02: Foundation - Category, Directory Structure, Registration

## Objective
Set up the Portfolio category infrastructure: enum, directories, and registration hooks.

## Prerequisites
- Task 01 (OSQP setup) - can be parallel

## Steps

### 1. Add Portfolio to TransformCategory Enum

**File:** `/home/adesola/EpochLab/EpochScript/include/epoch_script/transforms/core/metadata.h`

Change line 26 from:
```cpp
            ML);
```
To:
```cpp
            ML,
            Portfolio);
```

### 2. Create Directory Structure

```bash
mkdir -p /home/adesola/EpochLab/EpochScript/src/transforms/components/portfolio/{covariance,risk,optimizers,rolling,utils}
mkdir -p /home/adesola/EpochLab/EpochScript/test/unit/transforms/portfolio/{covariance,allocation,risk_measures,integration,portfolio_python_gen/portfolio_test_data}
```

### 3. Create register.h

**File:** `src/transforms/components/portfolio/register.h`

```cpp
#pragma once

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Forward declarations - implementations added in later tasks
// #include "optimizers/equal_weight.h"
// #include "portfolio_metadata.h"

namespace epoch_script::transform::portfolio {

/**
 * @brief Register all portfolio optimization transforms
 *
 * Called from registration.cpp during initialization.
 */
inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // NAIVE ALLOCATORS (Task 04)
    // =========================================================================
    // epoch_script::transform::Register<EqualWeightOptimizer>("equal_weight");
    // epoch_script::transform::Register<InverseVolatilityOptimizer>("inv_vol_weight");

    // =========================================================================
    // CLUSTERING-BASED OPTIMIZERS (Task 07-08)
    // =========================================================================
    // epoch_script::transform::Register<HRPOptimizer>("hrp");
    // epoch_script::transform::Register<HERCOptimizer>("herc");

    // =========================================================================
    // QUADRATIC PROGRAMMING OPTIMIZERS (Task 10-12)
    // =========================================================================
    // epoch_script::transform::Register<MeanVarianceOptimizer>("mean_variance");
    // epoch_script::transform::Register<RiskParityOptimizer>("risk_parity");
    // epoch_script::transform::Register<MaxDiversificationOptimizer>("max_diversification");

    // =========================================================================
    // BAYESIAN OPTIMIZERS (Task 13)
    // =========================================================================
    // epoch_script::transform::Register<BlackLittermanOptimizer>("black_litterman");

    // =========================================================================
    // ROLLING OPTIMIZERS (Task 14)
    // =========================================================================
    // epoch_script::transform::Register<RollingHRPOptimizer>("rolling_hrp");
    // epoch_script::transform::Register<RollingMeanVarianceOptimizer>("rolling_mean_variance");
    // epoch_script::transform::Register<RollingRiskParityOptimizer>("rolling_risk_parity");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================
    // for (const auto& metadata : MakeAllPortfolioMetaData()) {
    //     metaRegistry.Register(metadata);
    // }
}

} // namespace epoch_script::transform::portfolio
```

### 4. Create Empty portfolio_metadata.h

**File:** `src/transforms/components/portfolio/portfolio_metadata.h`

```cpp
#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transform::portfolio {

// =============================================================================
// Common Option Builders (to be filled in Task 04+)
// =============================================================================

inline epoch_script::transforms::MetaDataOptionList MakeCommonPortfolioOptions() {
    return {};  // Populated in later tasks
}

// =============================================================================
// Combined Metadata Function
// =============================================================================

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeAllPortfolioMetaData() {
    std::vector<epoch_script::transforms::TransformsMetaData> all;
    // Populated as each optimizer is implemented
    return all;
}

} // namespace epoch_script::transform::portfolio
```

### 5. Update registration.cpp

**File:** `/home/adesola/EpochLab/EpochScript/src/transforms/components/registration.cpp`

Add include near other includes:
```cpp
#include "portfolio/register.h"
```

Add call in `InitializeTransforms()` function (near line 380):
```cpp
// Portfolio Optimization Transforms
portfolio::Register();
```

### 6. Create Test CMakeLists.txt

**File:** `test/unit/transforms/portfolio/CMakeLists.txt`

```cmake
# Portfolio optimization transform tests
# Subdirectories added as tests are implemented

# add_subdirectory(covariance)
# add_subdirectory(allocation)
# add_subdirectory(risk_measures)
# add_subdirectory(integration)

# Test data directory definition
target_compile_definitions(epoch_script_test PRIVATE
    -DPORTFOLIO_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/portfolio_python_gen/portfolio_test_data")
```

### 7. Update Parent Test CMakeLists.txt

**File:** `test/CMakeLists.txt`

Add:
```cmake
add_subdirectory(unit/transforms/portfolio)
```

## Verification

```bash
cd /home/adesola/EpochLab/EpochScript
cmake --build cmake-build-release-test --target epoch_script
# Should compile without errors
```

## Deliverables
- [ ] `Portfolio` added to `TransformCategory` enum
- [ ] Directory structure created
- [ ] `register.h` created with commented-out placeholders
- [ ] `portfolio_metadata.h` created with stubs
- [ ] `registration.cpp` updated to call `portfolio::Register()`
- [ ] Test CMakeLists.txt files created
- [ ] Project compiles successfully

## Files Created/Modified
| File | Action |
|------|--------|
| `include/epoch_script/transforms/core/metadata.h` | Modified (add Portfolio enum) |
| `src/transforms/components/portfolio/register.h` | Created |
| `src/transforms/components/portfolio/portfolio_metadata.h` | Created |
| `src/transforms/components/registration.cpp` | Modified (add include + call) |
| `test/unit/transforms/portfolio/CMakeLists.txt` | Created |
| `test/CMakeLists.txt` | Modified (add subdirectory) |
