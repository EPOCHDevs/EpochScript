# Task 16: Comprehensive Unit Tests

## Objective
Implement all unit tests for portfolio optimization category with Python baseline validation.

## Prerequisites
- Tasks 04-14 (All implementations)
- Task 15 (Python baselines)

## Test File Structure

```
test/unit/transforms/portfolio/
├── portfolio_test_utils.h         # Shared utilities
├── covariance/
│   ├── sample_cov_test.cpp
│   ├── ledoit_wolf_test.cpp
│   └── rmt_test.cpp
├── allocation/
│   ├── equal_weight_test.cpp
│   ├── inv_vol_test.cpp
│   ├── hrp_test.cpp
│   ├── herc_test.cpp
│   ├── mvo_test.cpp
│   ├── risk_parity_test.cpp
│   ├── max_div_test.cpp
│   └── black_litterman_test.cpp
├── risk_measures/
│   └── risk_measures_test.cpp
└── integration/
    ├── cross_validation_test.cpp
    └── rolling_test.cpp
```

## Test Utilities

### `portfolio_test_utils.h`
```cpp
namespace portfolio_test {

DataFrame load_returns(const std::string& file);
arma::vec load_expected_weights(const std::string& file);
arma::mat load_expected_cov(const std::string& file);

bool weights_approx_equal(const arma::vec& a, const arma::vec& b,
                          double rtol = 0.05, double atol = 0.02);

bool verify_weights_sum(const arma::vec& w, double tol = 1e-6);
bool verify_long_only(const arma::vec& w);
bool verify_bounds(const arma::vec& w, double min, double max);

} // namespace portfolio_test
```

## Tolerance Guidelines

| Test Type | rtol | atol | Rationale |
|-----------|------|------|-----------|
| Equal Weight | 0.001 | 1e-6 | Deterministic |
| Inv Volatility | 0.02 | 0.01 | Vol estimation |
| HRP/HERC | 0.05 | 0.02 | Clustering variance |
| MVO/RP/MaxDiv | 0.02 | 0.01 | QP solver |
| Black-Litterman | 0.05 | 0.02 | View confidence |
| Covariance | 0.02 | 1e-4 | Matrix elements |

## Test Categories

### 1. Basic Functionality Tests
- Correct output dimensions
- Weights sum to 1
- Non-negative (long-only)
- Within bounds

### 2. Cross-Validation Tests
- Compare against PyPortfolioOpt baselines
- Compare against Riskfolio-Lib baselines

### 3. Edge Case Tests
- Single asset (weight = 1.0)
- Two assets (minimum for clustering)
- High correlation (near-singular cov)
- Rank-deficient covariance
- Zero-variance asset

### 4. Integration Tests
- Full pipeline: returns -> optimizer -> weights
- Rolling optimizer time-varying output
- Multiple covariance methods with same optimizer

## Example Test: HRP Cross-Validation

```cpp
TEST_CASE("HRP vs PyPortfolioOpt", "[portfolio][hrp][cross-validation]") {
    auto returns_df = portfolio_test::load_returns("returns_10.csv");
    auto expected_weights = portfolio_test::load_expected_weights("hrp_ward_expected.csv");

    auto cfg = run_op("hrp", "hrp_cv",
        {{ARG, {/* all 10 asset inputs */}}},
        {{"linkage", MetaDataOptionDefinition{"ward"}},
         {"covariance_method", MetaDataOptionDefinition{"sample"}}},
        tf);

    auto t = MAKE_TRANSFORM(cfg);
    auto out = t->TransformData(returns_df);
    auto actual_weights = extract_weights(out);

    REQUIRE(portfolio_test::weights_approx_equal(
        actual_weights, expected_weights, 0.05, 0.02));
}
```

## CMakeLists.txt Updates

```cmake
# test/unit/transforms/portfolio/CMakeLists.txt
add_subdirectory(covariance)
add_subdirectory(allocation)
add_subdirectory(risk_measures)
add_subdirectory(integration)

target_compile_definitions(epoch_script_test PRIVATE
    -DPORTFOLIO_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/portfolio_python_gen/portfolio_test_data")
```

## Running Tests

```bash
cd /home/adesola/EpochLab/EpochScript

# Build
cmake --build cmake-build-release-test --target epoch_script_test

# Run all portfolio tests
./cmake-build-release-test/bin/epoch_script_test "[portfolio]"

# Run specific category
./cmake-build-release-test/bin/epoch_script_test "[portfolio][hrp]"
./cmake-build-release-test/bin/epoch_script_test "[portfolio][covariance]"
./cmake-build-release-test/bin/epoch_script_test "[cross-validation]"
```

## Deliverables
- [ ] `portfolio_test_utils.h` with shared utilities
- [ ] All covariance tests
- [ ] All allocation tests (8 optimizers)
- [ ] Risk measure tests
- [ ] Cross-validation tests against Python
- [ ] Integration tests
- [ ] All tests pass
