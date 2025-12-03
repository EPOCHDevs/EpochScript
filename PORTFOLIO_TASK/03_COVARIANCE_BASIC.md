# Task 03: Basic Covariance Estimation - Interface and Sample Covariance

## Objective
Create the covariance estimator interface and implement sample covariance using Armadillo.

## Prerequisites
- Task 02 (Foundation)

## Steps

### 1. Create Covariance Estimator Interface

**File:** `src/transforms/components/portfolio/covariance/covariance_estimator.h`

```cpp
#pragma once

#include <armadillo>
#include <memory>
#include <string>
#include <cmath>

namespace epoch_script::transform::portfolio {

/**
 * @brief Covariance estimation methods
 */
enum class CovarianceMethod {
    Sample,         // Standard sample covariance
    LedoitWolf,     // Shrinkage to scaled identity
    Oracle,         // Oracle Approximating Shrinkage
    EWMA,           // Exponentially weighted
    Gerber,         // Gerber robust statistic
    RMT             // Random Matrix Theory denoising
};

/**
 * @brief Configuration for covariance estimation
 */
struct CovarianceConfig {
    CovarianceMethod method{CovarianceMethod::Sample};
    double ewma_span{60};              // For EWMA: decay span in periods
    double shrinkage_target{0.0};      // Manual shrinkage (0 = auto-compute)
    bool denoise{false};               // Apply RMT denoising post-estimation
    bool detone{false};                // Remove market factor (first eigenvector)
    int rmt_components{0};             // Number of components to keep (0 = auto)
    double gerber_threshold{0.5};      // Gerber statistic threshold
};

/**
 * @brief Abstract base for covariance estimators
 *
 * All implementations expect:
 * - Input: T x N matrix (T observations, N assets)
 * - Output: N x N covariance matrix (symmetric, positive semi-definite)
 */
class ICovarianceEstimator {
public:
    virtual ~ICovarianceEstimator() = default;

    /**
     * @brief Estimate covariance matrix from returns
     * @param returns T x N matrix (rows = time, cols = assets)
     * @return N x N covariance matrix
     */
    [[nodiscard]] virtual arma::mat Estimate(const arma::mat& returns) const = 0;

    /**
     * @brief Get correlation matrix from returns
     * @param returns T x N matrix
     * @return N x N correlation matrix
     */
    [[nodiscard]] virtual arma::mat EstimateCorrelation(const arma::mat& returns) const {
        arma::mat cov = Estimate(returns);
        arma::vec std_dev = arma::sqrt(cov.diag());

        // Avoid division by zero
        for (size_t i = 0; i < std_dev.n_elem; ++i) {
            if (std_dev(i) < 1e-10) {
                std_dev(i) = 1e-10;
            }
        }

        arma::mat corr = cov;
        for (size_t i = 0; i < cov.n_rows; ++i) {
            for (size_t j = 0; j < cov.n_cols; ++j) {
                corr(i, j) = cov(i, j) / (std_dev(i) * std_dev(j));
            }
        }
        return corr;
    }

    /**
     * @brief Compute distance matrix for hierarchical clustering
     *
     * Uses the standard transformation: d_ij = sqrt((1 - corr_ij) / 2)
     * This maps correlation [-1, 1] to distance [0, 1]
     *
     * @param returns T x N matrix
     * @return N x N distance matrix
     */
    [[nodiscard]] arma::mat ComputeDistanceMatrix(const arma::mat& returns) const {
        arma::mat corr = EstimateCorrelation(returns);
        arma::mat dist = arma::sqrt((1.0 - corr) / 2.0);

        // Ensure diagonal is zero (no self-distance)
        dist.diag().zeros();

        // Ensure non-negative (numerical precision)
        dist = arma::clamp(dist, 0.0, 1.0);

        return dist;
    }

    /**
     * @brief Get estimator name for logging/debugging
     */
    [[nodiscard]] virtual std::string GetName() const = 0;
};

/**
 * @brief Parse covariance method from string
 */
inline CovarianceMethod ParseCovarianceMethod(const std::string& s) {
    if (s == "sample") return CovarianceMethod::Sample;
    if (s == "ledoit_wolf") return CovarianceMethod::LedoitWolf;
    if (s == "oracle") return CovarianceMethod::Oracle;
    if (s == "ewma") return CovarianceMethod::EWMA;
    if (s == "gerber") return CovarianceMethod::Gerber;
    if (s == "rmt") return CovarianceMethod::RMT;
    return CovarianceMethod::Sample;  // Default
}

// Forward declaration - factory implemented after all estimators
class CovarianceEstimatorFactory;

} // namespace epoch_script::transform::portfolio
```

### 2. Implement Sample Covariance Estimator

**File:** `src/transforms/components/portfolio/covariance/sample_covariance.h`

```cpp
#pragma once

#include "covariance_estimator.h"

namespace epoch_script::transform::portfolio {

/**
 * @brief Standard sample covariance estimator
 *
 * Computes the unbiased sample covariance matrix:
 *   Σ = (1/(T-1)) * (X - μ)' * (X - μ)
 *
 * where X is T x N returns matrix and μ is the column-wise mean.
 */
class SampleCovarianceEstimator : public ICovarianceEstimator {
public:
    explicit SampleCovarianceEstimator(bool annualize = false, int periods_per_year = 252)
        : m_annualize(annualize), m_periods_per_year(periods_per_year) {}

    [[nodiscard]] arma::mat Estimate(const arma::mat& returns) const override {
        if (returns.n_rows < 2) {
            throw std::runtime_error("SampleCovarianceEstimator: Need at least 2 observations");
        }

        // arma::cov computes unbiased (n-1) sample covariance
        arma::mat cov = arma::cov(returns);

        if (m_annualize) {
            cov *= m_periods_per_year;
        }

        return cov;
    }

    [[nodiscard]] std::string GetName() const override {
        return "SampleCovariance";
    }

private:
    bool m_annualize;
    int m_periods_per_year;
};

} // namespace epoch_script::transform::portfolio
```

### 3. Create Covariance Estimator Factory (Initial)

**File:** `src/transforms/components/portfolio/covariance/covariance_factory.h`

```cpp
#pragma once

#include "covariance_estimator.h"
#include "sample_covariance.h"
// Additional includes added in Task 05:
// #include "ledoit_wolf.h"
// #include "ewma_covariance.h"
// #include "gerber_statistic.h"
// #include "rmt_denoising.h"

#include <memory>
#include <stdexcept>

namespace epoch_script::transform::portfolio {

/**
 * @brief Factory for creating covariance estimators
 */
class CovarianceEstimatorFactory {
public:
    /**
     * @brief Create a covariance estimator based on configuration
     * @param config Covariance configuration
     * @return Unique pointer to estimator
     */
    [[nodiscard]] static std::unique_ptr<ICovarianceEstimator>
    Create(const CovarianceConfig& config) {
        switch (config.method) {
            case CovarianceMethod::Sample:
                return std::make_unique<SampleCovarianceEstimator>();

            case CovarianceMethod::LedoitWolf:
                // TODO: Task 05
                throw std::runtime_error("LedoitWolf not yet implemented");

            case CovarianceMethod::Oracle:
                // TODO: Task 05
                throw std::runtime_error("Oracle shrinkage not yet implemented");

            case CovarianceMethod::EWMA:
                // TODO: Task 05
                throw std::runtime_error("EWMA covariance not yet implemented");

            case CovarianceMethod::Gerber:
                // TODO: Task 05
                throw std::runtime_error("Gerber statistic not yet implemented");

            case CovarianceMethod::RMT:
                // TODO: Task 05
                throw std::runtime_error("RMT denoising not yet implemented");

            default:
                return std::make_unique<SampleCovarianceEstimator>();
        }
    }

    /**
     * @brief Create estimator from string method name
     */
    [[nodiscard]] static std::unique_ptr<ICovarianceEstimator>
    Create(const std::string& method) {
        CovarianceConfig config;
        config.method = ParseCovarianceMethod(method);
        return Create(config);
    }
};

} // namespace epoch_script::transform::portfolio
```

### 4. Create Unit Test for Sample Covariance

**File:** `test/unit/transforms/portfolio/covariance/sample_cov_test.cpp`

```cpp
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include "../../../../src/transforms/components/portfolio/covariance/sample_covariance.h"
#include "../../../../src/transforms/components/portfolio/covariance/covariance_factory.h"

using namespace epoch_script::transform::portfolio;
using Catch::Matchers::WithinRel;

TEST_CASE("SampleCovarianceEstimator basic functionality", "[portfolio][covariance]") {
    // Create known returns matrix
    // 2 assets, 5 observations
    arma::mat returns = {
        {0.01, 0.02},
        {0.02, 0.01},
        {-0.01, 0.03},
        {0.03, -0.01},
        {0.00, 0.02}
    };

    SampleCovarianceEstimator estimator;
    arma::mat cov = estimator.Estimate(returns);

    SECTION("Output dimensions") {
        REQUIRE(cov.n_rows == 2);
        REQUIRE(cov.n_cols == 2);
    }

    SECTION("Symmetry") {
        REQUIRE_THAT(cov(0, 1), WithinRel(cov(1, 0), 1e-10));
    }

    SECTION("Positive diagonal") {
        REQUIRE(cov(0, 0) > 0);
        REQUIRE(cov(1, 1) > 0);
    }

    SECTION("Matches arma::cov") {
        arma::mat expected = arma::cov(returns);
        for (size_t i = 0; i < 2; ++i) {
            for (size_t j = 0; j < 2; ++j) {
                REQUIRE_THAT(cov(i, j), WithinRel(expected(i, j), 1e-10));
            }
        }
    }
}

TEST_CASE("SampleCovarianceEstimator correlation", "[portfolio][covariance]") {
    // Perfect positive correlation
    arma::mat returns_pos = {
        {0.01, 0.01},
        {0.02, 0.02},
        {0.03, 0.03},
        {0.04, 0.04}
    };

    SampleCovarianceEstimator estimator;
    arma::mat corr = estimator.EstimateCorrelation(returns_pos);

    SECTION("Perfect correlation = 1.0") {
        REQUIRE_THAT(corr(0, 1), WithinRel(1.0, 1e-6));
    }

    SECTION("Diagonal = 1.0") {
        REQUIRE_THAT(corr(0, 0), WithinRel(1.0, 1e-10));
        REQUIRE_THAT(corr(1, 1), WithinRel(1.0, 1e-10));
    }
}

TEST_CASE("SampleCovarianceEstimator distance matrix", "[portfolio][covariance]") {
    arma::mat returns = {
        {0.01, 0.02, -0.01},
        {0.02, 0.01, 0.03},
        {-0.01, 0.03, -0.02},
        {0.03, -0.01, 0.01},
        {0.00, 0.02, 0.00}
    };

    SampleCovarianceEstimator estimator;
    arma::mat dist = estimator.ComputeDistanceMatrix(returns);

    SECTION("Zero diagonal") {
        for (size_t i = 0; i < dist.n_rows; ++i) {
            REQUIRE_THAT(dist(i, i), WithinRel(0.0, 1e-10));
        }
    }

    SECTION("Symmetry") {
        for (size_t i = 0; i < dist.n_rows; ++i) {
            for (size_t j = i + 1; j < dist.n_cols; ++j) {
                REQUIRE_THAT(dist(i, j), WithinRel(dist(j, i), 1e-10));
            }
        }
    }

    SECTION("Bounded [0, 1]") {
        REQUIRE(dist.min() >= 0.0);
        REQUIRE(dist.max() <= 1.0);
    }
}

TEST_CASE("CovarianceEstimatorFactory", "[portfolio][covariance]") {
    SECTION("Create sample estimator") {
        auto estimator = CovarianceEstimatorFactory::Create("sample");
        REQUIRE(estimator != nullptr);
        REQUIRE(estimator->GetName() == "SampleCovariance");
    }

    SECTION("Default is sample") {
        auto estimator = CovarianceEstimatorFactory::Create("unknown");
        REQUIRE(estimator->GetName() == "SampleCovariance");
    }
}
```

### 5. Update Test CMakeLists.txt

**File:** `test/unit/transforms/portfolio/covariance/CMakeLists.txt`

```cmake
target_sources(epoch_script_test PRIVATE
    sample_cov_test.cpp
)
```

**Update parent:** `test/unit/transforms/portfolio/CMakeLists.txt`
```cmake
add_subdirectory(covariance)
```

## Verification

```bash
cd /home/adesola/EpochLab/EpochScript
cmake --build cmake-build-release-test --target epoch_script_test
./cmake-build-release-test/bin/epoch_script_test "[portfolio][covariance]"
```

## Deliverables
- [ ] `covariance_estimator.h` - Interface with correlation and distance methods
- [ ] `sample_covariance.h` - Sample covariance implementation
- [ ] `covariance_factory.h` - Factory with sample + stubs for others
- [ ] `sample_cov_test.cpp` - Unit tests
- [ ] All tests pass

## Files Created
| File | Description |
|------|-------------|
| `src/transforms/components/portfolio/covariance/covariance_estimator.h` | Interface |
| `src/transforms/components/portfolio/covariance/sample_covariance.h` | Sample cov impl |
| `src/transforms/components/portfolio/covariance/covariance_factory.h` | Factory |
| `test/unit/transforms/portfolio/covariance/sample_cov_test.cpp` | Tests |
| `test/unit/transforms/portfolio/covariance/CMakeLists.txt` | Test build |
