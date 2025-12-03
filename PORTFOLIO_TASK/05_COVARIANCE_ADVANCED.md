# Task 05: Advanced Covariance Estimation

## Objective
Implement advanced covariance estimation methods: Ledoit-Wolf, EWMA, Gerber, and RMT denoising.

## Prerequisites
- Task 03 (Covariance Basic)

## Steps

### 1. Implement Ledoit-Wolf Shrinkage

**File:** `src/transforms/components/portfolio/covariance/ledoit_wolf.h`

```cpp
#pragma once

#include "covariance_estimator.h"
#include <cmath>

namespace epoch_script::transform::portfolio {

/**
 * @brief Ledoit-Wolf Shrinkage Covariance Estimator
 *
 * Shrinks sample covariance towards a structured target:
 *   Σ_shrunk = δ*F + (1-δ)*S
 *
 * where:
 *   S = sample covariance
 *   F = shrinkage target (scaled identity matrix)
 *   δ = optimal shrinkage intensity (auto-computed)
 *
 * Reference: Ledoit & Wolf (2004)
 *            "A Well-Conditioned Estimator for Large-Dimensional Covariance Matrices"
 */
class LedoitWolfEstimator : public ICovarianceEstimator {
public:
    explicit LedoitWolfEstimator(double manual_shrinkage = 0.0)
        : m_manual_shrinkage(manual_shrinkage) {}

    [[nodiscard]] arma::mat Estimate(const arma::mat& returns) const override {
        const size_t T = returns.n_rows;
        const size_t N = returns.n_cols;

        if (T < 2) {
            throw std::runtime_error("LedoitWolfEstimator: Need at least 2 observations");
        }

        // Center the returns
        arma::mat X = returns.each_row() - arma::mean(returns, 0);

        // Sample covariance (biased, using T not T-1)
        arma::mat S = (X.t() * X) / T;

        // Shrinkage target: scaled identity (average variance)
        double mu = arma::trace(S) / N;
        arma::mat F = mu * arma::eye<arma::mat>(N, N);

        // Compute optimal shrinkage intensity
        double delta = m_manual_shrinkage > 0.0 ?
            m_manual_shrinkage : ComputeOptimalShrinkage(X, S, mu);

        // Shrunk covariance
        return delta * F + (1.0 - delta) * S;
    }

    [[nodiscard]] std::string GetName() const override {
        return "LedoitWolf";
    }

private:
    double m_manual_shrinkage;

    [[nodiscard]] double ComputeOptimalShrinkage(
        const arma::mat& X,
        const arma::mat& S,
        double mu) const {

        const size_t T = X.n_rows;
        const size_t N = X.n_cols;

        // Delta formula from Ledoit-Wolf (2004)
        // Estimates optimal shrinkage intensity

        double sum_sq = 0.0;
        for (size_t t = 0; t < T; ++t) {
            arma::mat x_t = X.row(t).t();
            arma::mat m_t = x_t * x_t.t() - S;
            sum_sq += arma::accu(m_t % m_t);  // Frobenius norm squared
        }
        double pi_hat = sum_sq / T;

        // Gamma: squared Frobenius norm of (S - F)
        arma::mat diff = S - mu * arma::eye<arma::mat>(N, N);
        double gamma = arma::accu(diff % diff);

        // Optimal shrinkage (bounded to [0, 1])
        double delta = std::min(1.0, std::max(0.0, (pi_hat - gamma) / (T * gamma)));

        return delta;
    }
};

} // namespace epoch_script::transform::portfolio
```

### 2. Implement EWMA Covariance

**File:** `src/transforms/components/portfolio/covariance/ewma_covariance.h`

```cpp
#pragma once

#include "covariance_estimator.h"

namespace epoch_script::transform::portfolio {

/**
 * @brief Exponentially Weighted Moving Average Covariance
 *
 * Weights recent observations more heavily:
 *   Σ_t = λ * Σ_{t-1} + (1-λ) * r_t * r_t'
 *
 * where λ = 1 - 2/(span+1) is the decay factor.
 *
 * RiskMetrics default: span=60 (λ ≈ 0.97)
 */
class EWMACovarianceEstimator : public ICovarianceEstimator {
public:
    explicit EWMACovarianceEstimator(double span = 60.0)
        : m_span(span), m_lambda(1.0 - 2.0 / (span + 1.0)) {}

    [[nodiscard]] arma::mat Estimate(const arma::mat& returns) const override {
        const size_t T = returns.n_rows;
        const size_t N = returns.n_cols;

        if (T < 2) {
            throw std::runtime_error("EWMACovarianceEstimator: Need at least 2 observations");
        }

        // Initialize with first observation
        arma::rowvec r0 = returns.row(0);
        arma::mat cov = r0.t() * r0;

        // Exponential weighting
        for (size_t t = 1; t < T; ++t) {
            arma::rowvec r_t = returns.row(t);
            cov = m_lambda * cov + (1.0 - m_lambda) * (r_t.t() * r_t);
        }

        return cov;
    }

    [[nodiscard]] std::string GetName() const override {
        return "EWMA";
    }

private:
    double m_span;
    double m_lambda;
};

} // namespace epoch_script::transform::portfolio
```

### 3. Implement Gerber Statistic

**File:** `src/transforms/components/portfolio/covariance/gerber_statistic.h`

```cpp
#pragma once

#include "covariance_estimator.h"

namespace epoch_script::transform::portfolio {

/**
 * @brief Gerber Statistic Covariance Estimator
 *
 * Robust correlation measure that filters out noise by only counting
 * co-movements when both assets move significantly.
 *
 * Gerber correlation:
 *   ρ_ij = (n_concordant - n_discordant) / n_significant
 *
 * where movements are counted only if |r| > threshold * σ
 *
 * Reference: Gerber, Markov, Meden (2015)
 */
class GerberCovarianceEstimator : public ICovarianceEstimator {
public:
    explicit GerberCovarianceEstimator(double threshold = 0.5)
        : m_threshold(threshold) {}

    [[nodiscard]] arma::mat Estimate(const arma::mat& returns) const override {
        // First compute Gerber correlation, then scale by volatilities
        arma::mat gerber_corr = ComputeGerberCorrelation(returns);

        // Get standard deviations
        arma::vec std_dev = arma::stddev(returns, 0, 0).t();

        // Convert correlation to covariance
        arma::mat cov(returns.n_cols, returns.n_cols);
        for (size_t i = 0; i < returns.n_cols; ++i) {
            for (size_t j = 0; j < returns.n_cols; ++j) {
                cov(i, j) = gerber_corr(i, j) * std_dev(i) * std_dev(j);
            }
        }

        return cov;
    }

    [[nodiscard]] arma::mat EstimateCorrelation(const arma::mat& returns) const override {
        return ComputeGerberCorrelation(returns);
    }

    [[nodiscard]] std::string GetName() const override {
        return "Gerber";
    }

private:
    double m_threshold;

    [[nodiscard]] arma::mat ComputeGerberCorrelation(const arma::mat& returns) const {
        const size_t T = returns.n_rows;
        const size_t N = returns.n_cols;

        // Compute thresholds per asset
        arma::vec std_dev = arma::stddev(returns, 0, 0).t();
        arma::vec thresholds = m_threshold * std_dev;

        // Classify each return as +1 (up), -1 (down), 0 (insignificant)
        arma::mat signs(T, N, arma::fill::zeros);
        for (size_t i = 0; i < N; ++i) {
            for (size_t t = 0; t < T; ++t) {
                double r = returns(t, i);
                if (r > thresholds(i)) {
                    signs(t, i) = 1.0;
                } else if (r < -thresholds(i)) {
                    signs(t, i) = -1.0;
                }
            }
        }

        // Compute Gerber correlation
        arma::mat corr(N, N, arma::fill::ones);
        for (size_t i = 0; i < N; ++i) {
            for (size_t j = i + 1; j < N; ++j) {
                double concordant = 0.0;
                double discordant = 0.0;
                double significant = 0.0;

                for (size_t t = 0; t < T; ++t) {
                    double s_i = signs(t, i);
                    double s_j = signs(t, j);

                    // Only count if both are significant
                    if (s_i != 0 && s_j != 0) {
                        significant += 1.0;
                        if (s_i == s_j) {
                            concordant += 1.0;
                        } else {
                            discordant += 1.0;
                        }
                    }
                }

                double rho = significant > 0 ?
                    (concordant - discordant) / significant : 0.0;

                corr(i, j) = rho;
                corr(j, i) = rho;
            }
        }

        return corr;
    }
};

} // namespace epoch_script::transform::portfolio
```

### 4. Implement RMT Denoising (Using Spectra)

**File:** `src/transforms/components/portfolio/covariance/rmt_denoising.h`

```cpp
#pragma once

#include "covariance_estimator.h"
#include "sample_covariance.h"
#include <Spectra/SymEigsSolver.h>
#include <Spectra/MatOp/DenseSymMatProd.h>

namespace epoch_script::transform::portfolio {

/**
 * @brief Random Matrix Theory Denoising Covariance Estimator
 *
 * Uses Marchenko-Pastur distribution to identify noise eigenvalues
 * and filters them out, keeping only signal eigenvalues.
 *
 * Reference: Laloux, Cizeau, Bouchaud, Potters (1999)
 *            "Noise Dressing of Financial Correlation Matrices"
 *
 * Marchenko-Pastur bounds:
 *   λ_max = (1 + sqrt(1/q))^2
 *   λ_min = (1 - sqrt(1/q))^2
 *
 * where q = T/N (ratio of observations to assets)
 */
class RMTDenoisingEstimator : public ICovarianceEstimator {
public:
    explicit RMTDenoisingEstimator(bool detone = false)
        : m_detone(detone) {}

    [[nodiscard]] arma::mat Estimate(const arma::mat& returns) const override {
        const size_t T = returns.n_rows;
        const size_t N = returns.n_cols;

        // Start with sample covariance
        SampleCovarianceEstimator base_estimator;
        arma::mat cov = base_estimator.Estimate(returns);

        // Convert to correlation for eigenvalue analysis
        arma::vec std_dev = arma::sqrt(cov.diag());
        arma::mat corr(N, N);
        for (size_t i = 0; i < N; ++i) {
            for (size_t j = 0; j < N; ++j) {
                corr(i, j) = cov(i, j) / (std_dev(i) * std_dev(j));
            }
        }

        // Compute eigenvalues using Spectra
        Spectra::DenseSymMatProd<double> op(corr);
        int ncv = std::min(static_cast<int>(N), static_cast<int>(2 * N - 1));
        Spectra::SymEigsSolver<Spectra::DenseSymMatProd<double>> solver(op, N - 1, ncv);

        solver.init();
        int nconv = solver.compute(Spectra::SortRule::LargestAlge);

        if (solver.info() != Spectra::CompInfo::Successful) {
            // Fall back to sample covariance
            return cov;
        }

        arma::vec eigenvalues(N);
        arma::mat eigenvectors(N, N);

        // Get eigenvalues and eigenvectors
        Eigen::VectorXd evals = solver.eigenvalues();
        Eigen::MatrixXd evecs = solver.eigenvectors();

        for (size_t i = 0; i < N - 1; ++i) {
            eigenvalues(i) = evals(i);
            for (size_t j = 0; j < N; ++j) {
                eigenvectors(j, i) = evecs(j, i);
            }
        }
        eigenvalues(N - 1) = 1.0;  // Smallest eigenvalue estimate
        eigenvectors.col(N - 1).ones();
        eigenvectors.col(N - 1) /= std::sqrt(static_cast<double>(N));

        // Marchenko-Pastur threshold
        double q = static_cast<double>(T) / N;
        double lambda_max = std::pow(1.0 + std::sqrt(1.0 / q), 2);

        // Filter eigenvalues
        double mean_noise = 0.0;
        int noise_count = 0;
        for (size_t i = 0; i < N; ++i) {
            if (eigenvalues(i) < lambda_max) {
                mean_noise += eigenvalues(i);
                noise_count++;
            }
        }
        mean_noise = noise_count > 0 ? mean_noise / noise_count : 1.0;

        // Replace noise eigenvalues with average
        arma::vec filtered_eigenvalues = eigenvalues;
        for (size_t i = 0; i < N; ++i) {
            if (eigenvalues(i) < lambda_max) {
                filtered_eigenvalues(i) = mean_noise;
            }
        }

        // Detone: remove market factor (largest eigenvalue)
        if (m_detone && N > 1) {
            filtered_eigenvalues(0) = mean_noise;
        }

        // Reconstruct correlation matrix
        arma::mat denoised_corr = eigenvectors *
            arma::diagmat(filtered_eigenvalues) *
            eigenvectors.t();

        // Ensure diagonal is 1
        for (size_t i = 0; i < N; ++i) {
            denoised_corr(i, i) = 1.0;
        }

        // Convert back to covariance
        arma::mat denoised_cov(N, N);
        for (size_t i = 0; i < N; ++i) {
            for (size_t j = 0; j < N; ++j) {
                denoised_cov(i, j) = denoised_corr(i, j) * std_dev(i) * std_dev(j);
            }
        }

        return denoised_cov;
    }

    [[nodiscard]] std::string GetName() const override {
        return m_detone ? "RMT_Detoned" : "RMT";
    }

private:
    bool m_detone;
};

} // namespace epoch_script::transform::portfolio
```

### 5. Update Covariance Factory

**Update:** `src/transforms/components/portfolio/covariance/covariance_factory.h`

```cpp
#pragma once

#include "covariance_estimator.h"
#include "sample_covariance.h"
#include "ledoit_wolf.h"
#include "ewma_covariance.h"
#include "gerber_statistic.h"
#include "rmt_denoising.h"

namespace epoch_script::transform::portfolio {

class CovarianceEstimatorFactory {
public:
    [[nodiscard]] static std::unique_ptr<ICovarianceEstimator>
    Create(const CovarianceConfig& config) {
        switch (config.method) {
            case CovarianceMethod::Sample:
                return std::make_unique<SampleCovarianceEstimator>();

            case CovarianceMethod::LedoitWolf:
                return std::make_unique<LedoitWolfEstimator>(config.shrinkage_target);

            case CovarianceMethod::Oracle:
                // OAS uses same structure as Ledoit-Wolf with different target
                return std::make_unique<LedoitWolfEstimator>(config.shrinkage_target);

            case CovarianceMethod::EWMA:
                return std::make_unique<EWMACovarianceEstimator>(config.ewma_span);

            case CovarianceMethod::Gerber:
                return std::make_unique<GerberCovarianceEstimator>(config.gerber_threshold);

            case CovarianceMethod::RMT:
                return std::make_unique<RMTDenoisingEstimator>(config.detone);

            default:
                return std::make_unique<SampleCovarianceEstimator>();
        }
    }

    [[nodiscard]] static std::unique_ptr<ICovarianceEstimator>
    Create(const std::string& method) {
        CovarianceConfig config;
        config.method = ParseCovarianceMethod(method);
        return Create(config);
    }
};

} // namespace epoch_script::transform::portfolio
```

### 6. Create Unit Tests

**File:** `test/unit/transforms/portfolio/covariance/advanced_cov_test.cpp`

```cpp
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include "../../../../src/transforms/components/portfolio/covariance/ledoit_wolf.h"
#include "../../../../src/transforms/components/portfolio/covariance/ewma_covariance.h"
#include "../../../../src/transforms/components/portfolio/covariance/gerber_statistic.h"
#include "../../../../src/transforms/components/portfolio/covariance/rmt_denoising.h"
#include "../../../../src/transforms/components/portfolio/covariance/covariance_factory.h"

using namespace epoch_script::transform::portfolio;
using Catch::Matchers::WithinRel;

namespace {

arma::mat make_test_returns(int T = 100, int N = 5, int seed = 42) {
    arma::arma_rng::set_seed(seed);
    return arma::randn(T, N) * 0.02;  // ~2% daily vol
}

bool is_symmetric(const arma::mat& m, double tol = 1e-10) {
    return arma::approx_equal(m, m.t(), "absdiff", tol);
}

bool is_positive_semidefinite(const arma::mat& m) {
    arma::vec eigvals = arma::eig_sym(m);
    return eigvals.min() >= -1e-10;
}

} // anonymous namespace

TEST_CASE("Ledoit-Wolf shrinkage", "[portfolio][covariance][ledoit_wolf]") {
    arma::mat returns = make_test_returns();
    LedoitWolfEstimator estimator;
    arma::mat cov = estimator.Estimate(returns);

    SECTION("Symmetric") {
        REQUIRE(is_symmetric(cov));
    }

    SECTION("Positive semi-definite") {
        REQUIRE(is_positive_semidefinite(cov));
    }

    SECTION("Shrinkage reduces condition number") {
        SampleCovarianceEstimator sample_est;
        arma::mat sample_cov = sample_est.Estimate(returns);

        double cond_sample = arma::cond(sample_cov);
        double cond_lw = arma::cond(cov);

        REQUIRE(cond_lw <= cond_sample);
    }
}

TEST_CASE("EWMA covariance", "[portfolio][covariance][ewma]") {
    arma::mat returns = make_test_returns();
    EWMACovarianceEstimator estimator(60.0);
    arma::mat cov = estimator.Estimate(returns);

    SECTION("Symmetric") {
        REQUIRE(is_symmetric(cov));
    }

    SECTION("Positive diagonal") {
        REQUIRE(arma::all(cov.diag() > 0));
    }
}

TEST_CASE("Gerber statistic", "[portfolio][covariance][gerber]") {
    arma::mat returns = make_test_returns();
    GerberCovarianceEstimator estimator(0.5);
    arma::mat cov = estimator.Estimate(returns);

    SECTION("Symmetric") {
        REQUIRE(is_symmetric(cov));
    }

    SECTION("Correlation bounded") {
        arma::mat corr = estimator.EstimateCorrelation(returns);
        REQUIRE(corr.min() >= -1.0);
        REQUIRE(corr.max() <= 1.0);
    }
}

TEST_CASE("RMT denoising", "[portfolio][covariance][rmt]") {
    // Need more observations for RMT to work well
    arma::mat returns = make_test_returns(500, 10);
    RMTDenoisingEstimator estimator;
    arma::mat cov = estimator.Estimate(returns);

    SECTION("Symmetric") {
        REQUIRE(is_symmetric(cov));
    }

    SECTION("Positive semi-definite") {
        REQUIRE(is_positive_semidefinite(cov));
    }
}

TEST_CASE("Factory creates all estimators", "[portfolio][covariance][factory]") {
    for (const auto& method : {"sample", "ledoit_wolf", "ewma", "gerber", "rmt"}) {
        DYNAMIC_SECTION("Method: " << method) {
            auto estimator = CovarianceEstimatorFactory::Create(method);
            REQUIRE(estimator != nullptr);

            arma::mat returns = make_test_returns(200, 5);
            arma::mat cov = estimator->Estimate(returns);

            REQUIRE(cov.n_rows == 5);
            REQUIRE(cov.n_cols == 5);
            REQUIRE(is_symmetric(cov));
        }
    }
}
```

## Verification

```bash
cd /home/adesola/EpochLab/EpochScript
cmake --build cmake-build-release-test --target epoch_script_test
./cmake-build-release-test/bin/epoch_script_test "[portfolio][covariance]"
```

## Deliverables
- [ ] `ledoit_wolf.h` - Ledoit-Wolf shrinkage estimator
- [ ] `ewma_covariance.h` - EWMA covariance estimator
- [ ] `gerber_statistic.h` - Gerber robust covariance
- [ ] `rmt_denoising.h` - RMT denoising with Spectra
- [ ] `covariance_factory.h` updated
- [ ] All tests pass

## Notes
- Spectra is already in vcpkg.json (line 17)
- RMT denoising needs T > N for Marchenko-Pastur to apply meaningfully
- Gerber threshold typically 0.5 (Riskfolio default)
