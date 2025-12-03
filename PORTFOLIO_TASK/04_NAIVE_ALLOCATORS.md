# Task 04: Naive Allocators - Equal Weight and Inverse Volatility

## Objective
Implement the two simplest portfolio allocation methods that serve as baselines.

## Prerequisites
- Task 02 (Foundation)
- Task 03 (Covariance Basic)

## Steps

### 1. Create Portfolio Optimizer Base Class

**File:** `src/transforms/components/portfolio/optimizers/portfolio_optimizer_base.h`

```cpp
#pragma once

#include <epoch_script/transforms/core/itransform.h>
#include "../statistics/dataframe_armadillo_utils.h"
#include "../covariance/covariance_estimator.h"
#include "../covariance/covariance_factory.h"
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <armadillo>

namespace epoch_script::transform::portfolio {

/**
 * @brief Common configuration for portfolio optimizers
 */
struct PortfolioOptimizerConfig {
    CovarianceConfig covariance;
    double min_weight{0.0};            // Minimum weight per asset
    double max_weight{1.0};            // Maximum weight per asset
    size_t min_observations{30};       // Minimum history required
    bool long_only{true};              // Only non-negative weights
};

/**
 * @brief CRTP Base class for portfolio optimizers
 *
 * Cross-sectional transform pattern:
 * - Input: Multi-column DataFrame (one column per asset, values are returns)
 * - Output: Multi-column DataFrame (one weight column per asset)
 *
 * Derived classes implement: Optimize(returns, cov, corr, dist) -> weights
 */
template<typename Derived>
class PortfolioOptimizerBase : public ITransform {
public:
    explicit PortfolioOptimizerBase(const TransformConfiguration& cfg)
        : ITransform(cfg) {
        ParseCommonOptions(cfg);
    }

    [[nodiscard]] epoch_frame::DataFrame
    TransformData(const epoch_frame::DataFrame& df) const override {
        const auto& derived = static_cast<const Derived&>(*this);

        // Get input column names (one per asset)
        const auto input_ids = GetInputIds();
        const size_t n_assets = input_ids.size();

        if (n_assets < 1) {
            throw std::runtime_error("Portfolio optimization requires at least 1 asset");
        }

        // Convert to Armadillo matrix (T x N)
        arma::mat returns = utils::MatFromDataFrame(df, input_ids);
        const size_t T = returns.n_rows;

        if (T < m_config.min_observations) {
            throw std::runtime_error(
                "Insufficient observations. Need " +
                std::to_string(m_config.min_observations) +
                ", got " + std::to_string(T));
        }

        // Compute covariance using configured estimator
        auto cov_estimator = CovarianceEstimatorFactory::Create(m_config.covariance);
        arma::mat cov = cov_estimator->Estimate(returns);
        arma::mat corr = cov_estimator->EstimateCorrelation(returns);
        arma::mat dist = cov_estimator->ComputeDistanceMatrix(returns);

        // Call derived class optimization
        arma::vec weights = derived.Optimize(returns, cov, corr, dist);

        // Apply constraints
        weights = ApplyConstraints(weights);

        // Build output DataFrame
        return BuildWeightOutput(df.index(), weights, input_ids);
    }

protected:
    PortfolioOptimizerConfig m_config;

    void ParseCommonOptions(const TransformConfiguration& cfg) {
        // Covariance method
        auto cov_method = cfg.GetOptionValue("covariance_method",
            MetaDataOptionDefinition{std::string("sample")}).GetSelectOption();
        m_config.covariance.method = ParseCovarianceMethod(cov_method);

        // Weight constraints
        m_config.min_weight = cfg.GetOptionValue("min_weight",
            MetaDataOptionDefinition{0.0}).GetDecimal();
        m_config.max_weight = cfg.GetOptionValue("max_weight",
            MetaDataOptionDefinition{1.0}).GetDecimal();

        // Minimum observations
        m_config.min_observations = static_cast<size_t>(
            cfg.GetOptionValue("min_observations",
                MetaDataOptionDefinition{30.0}).GetInteger());

        // Long only
        m_config.long_only = cfg.GetOptionValue("long_only",
            MetaDataOptionDefinition{true}).GetBool();

        // Denoising
        m_config.covariance.denoise = cfg.GetOptionValue("denoise",
            MetaDataOptionDefinition{false}).GetBool();
    }

    [[nodiscard]] arma::vec ApplyConstraints(arma::vec weights) const {
        // Long-only constraint
        if (m_config.long_only) {
            weights = arma::clamp(weights, 0.0, arma::datum::inf);
        }

        // Weight bounds
        weights = arma::clamp(weights, m_config.min_weight, m_config.max_weight);

        // Normalize to sum to 1
        double sum = arma::accu(weights);
        if (sum > 1e-10) {
            weights /= sum;
        } else {
            // Fallback to equal weight if all weights are zero
            weights.fill(1.0 / weights.n_elem);
        }

        return weights;
    }

    [[nodiscard]] epoch_frame::DataFrame BuildWeightOutput(
        const epoch_frame::IndexPtr& index,
        const arma::vec& weights,
        const std::vector<std::string>& input_ids) const {

        const size_t T = index->size();
        const size_t N = weights.n_elem;

        std::vector<std::string> output_columns;
        std::vector<arrow::ChunkedArrayPtr> output_arrays;

        // Create one weight column per asset
        // Weight is constant across all timestamps (static optimization)
        for (size_t i = 0; i < N; ++i) {
            std::vector<double> weight_vec(T, weights(i));

            // Extract asset name from input ID (remove "src#" prefix if present)
            std::string asset_name = input_ids[i];
            if (asset_name.find('#') != std::string::npos) {
                asset_name = asset_name.substr(asset_name.find('#') + 1);
            }

            output_columns.push_back(GetConfig().GetOutputId("weight_" + asset_name).GetColumnName());
            output_arrays.push_back(epoch_frame::factory::array::make_array(weight_vec));
        }

        return epoch_frame::make_dataframe(index, output_arrays, output_columns);
    }
};

} // namespace epoch_script::transform::portfolio
```

### 2. Implement Equal Weight Optimizer

**File:** `src/transforms/components/portfolio/optimizers/equal_weight.h`

```cpp
#pragma once

#include "portfolio_optimizer_base.h"

namespace epoch_script::transform::portfolio {

/**
 * @brief Equal Weight (1/N) Portfolio Allocation
 *
 * The simplest diversification approach: allocate 1/N to each of N assets.
 *
 * Properties:
 * - No estimation error (doesn't use returns/covariance)
 * - Robust baseline for comparison
 * - Often outperforms optimized portfolios out-of-sample
 *
 * Reference: DeMiguel, Garlappi, Uppal (2009)
 *            "Optimal Versus Naive Diversification"
 */
class EqualWeightOptimizer : public PortfolioOptimizerBase<EqualWeightOptimizer> {
public:
    explicit EqualWeightOptimizer(const TransformConfiguration& cfg)
        : PortfolioOptimizerBase<EqualWeightOptimizer>(cfg) {}

    /**
     * @brief Compute equal weights
     *
     * @param returns Not used
     * @param cov Not used
     * @param corr Not used
     * @param dist Not used
     * @return Vector of 1/N weights
     */
    [[nodiscard]] arma::vec Optimize(
        const arma::mat& returns,
        const arma::mat& cov,
        const arma::mat& corr,
        const arma::mat& dist) const {

        const size_t N = returns.n_cols;
        return arma::vec(N, arma::fill::value(1.0 / N));
    }
};

} // namespace epoch_script::transform::portfolio
```

### 3. Implement Inverse Volatility Optimizer

**File:** `src/transforms/components/portfolio/optimizers/inverse_volatility.h`

```cpp
#pragma once

#include "portfolio_optimizer_base.h"

namespace epoch_script::transform::portfolio {

/**
 * @brief Inverse Volatility Weight Portfolio Allocation
 *
 * Allocate weights inversely proportional to asset volatility:
 *   w_i = (1/σ_i) / Σ(1/σ_j)
 *
 * Properties:
 * - Risk-based allocation without correlation structure
 * - More stable than mean-variance (doesn't use expected returns)
 * - Common in risk parity strategies as a starting point
 *
 * Use Cases:
 * - When assets have similar expected returns
 * - As a simpler alternative to full risk parity
 * - When correlation estimates are unreliable
 */
class InverseVolatilityOptimizer : public PortfolioOptimizerBase<InverseVolatilityOptimizer> {
public:
    explicit InverseVolatilityOptimizer(const TransformConfiguration& cfg)
        : PortfolioOptimizerBase<InverseVolatilityOptimizer>(cfg) {}

    /**
     * @brief Compute inverse volatility weights
     *
     * @param returns T x N returns matrix (used for volatility)
     * @param cov N x N covariance matrix (diagonal used)
     * @param corr Not used
     * @param dist Not used
     * @return Vector of inverse-volatility weights
     */
    [[nodiscard]] arma::vec Optimize(
        const arma::mat& returns,
        const arma::mat& cov,
        const arma::mat& corr,
        const arma::mat& dist) const {

        const size_t N = cov.n_cols;

        // Extract volatilities from covariance diagonal
        arma::vec volatilities = arma::sqrt(cov.diag());

        // Handle zero or near-zero volatility
        for (size_t i = 0; i < N; ++i) {
            if (volatilities(i) < 1e-10) {
                volatilities(i) = 1e-10;
            }
        }

        // Inverse volatility
        arma::vec inv_vol = 1.0 / volatilities;

        // Normalize to sum to 1
        double sum = arma::accu(inv_vol);
        return inv_vol / sum;
    }
};

} // namespace epoch_script::transform::portfolio
```

### 4. Add Metadata for Naive Allocators

**Update:** `src/transforms/components/portfolio/portfolio_metadata.h`

```cpp
#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transform::portfolio {

using namespace epoch_script::transforms;

// =============================================================================
// Common Option Builders
// =============================================================================

inline MetaDataOptionList MakeCommonPortfolioOptions() {
    return {
        MetaDataOption{
            .id = "covariance_method",
            .name = "Covariance Estimation",
            .type = epoch_core::MetaDataOptionType::Select,
            .defaultValue = MetaDataOptionDefinition{std::string("sample")},
            .selectOptions = {
                {"Sample Covariance", "sample"},
                {"Ledoit-Wolf Shrinkage", "ledoit_wolf"},
                {"EWMA", "ewma"},
                {"Gerber Statistic", "gerber"},
                {"RMT Denoising", "rmt"}
            },
            .desc = "Method for estimating covariance matrix"
        },
        MetaDataOption{
            .id = "min_weight",
            .name = "Minimum Weight",
            .type = epoch_core::MetaDataOptionType::Decimal,
            .defaultValue = MetaDataOptionDefinition{0.0},
            .min = -1.0,
            .max = 1.0,
            .desc = "Minimum portfolio weight per asset (negative allows short)"
        },
        MetaDataOption{
            .id = "max_weight",
            .name = "Maximum Weight",
            .type = epoch_core::MetaDataOptionType::Decimal,
            .defaultValue = MetaDataOptionDefinition{1.0},
            .min = 0.0,
            .max = 1.0,
            .desc = "Maximum portfolio weight per asset"
        },
        MetaDataOption{
            .id = "min_observations",
            .name = "Minimum Observations",
            .type = epoch_core::MetaDataOptionType::Integer,
            .defaultValue = MetaDataOptionDefinition{30.0},
            .min = 10,
            .max = 1000,
            .desc = "Minimum number of observations required"
        },
        MetaDataOption{
            .id = "long_only",
            .name = "Long Only",
            .type = epoch_core::MetaDataOptionType::Boolean,
            .defaultValue = MetaDataOptionDefinition{true},
            .desc = "Constrain weights to be non-negative"
        }
    };
}

// =============================================================================
// Equal Weight Metadata
// =============================================================================

inline std::vector<TransformsMetaData> MakeEqualWeightMetaData() {
    return {{
        .id = "equal_weight",
        .category = epoch_core::TransformCategory::Portfolio,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Equal Weight (1/N)",
        .options = {},  // No options needed
        .isCrossSectional = true,
        .desc = "Naive diversification: allocate 1/N to each active asset. "
                "No optimization required. Robust baseline for comparison.",
        .inputs = {{epoch_core::IODataType::Decimal, "SLOT", "Asset Returns", true, false}},
        .outputs = {{epoch_core::IODataType::Decimal, "weight", "Portfolio Weight", true, false}},
        .atLeastOneInputRequired = true,
        .tags = {"portfolio", "allocation", "naive", "diversification", "1/n", "equal-weight"},
        .requiresTimeFrame = false,
        .strategyTypes = {"portfolio-construction", "benchmark", "passive"},
        .relatedTransforms = {"inv_vol_weight", "hrp", "risk_parity"},
        .usageContext = "Use as baseline or when optimization is unreliable. "
                        "Works well when assets have similar risk characteristics.",
        .limitations = "Ignores risk and return information. May overweight risky assets."
    }};
}

// =============================================================================
// Inverse Volatility Metadata
// =============================================================================

inline std::vector<TransformsMetaData> MakeInverseVolatilityMetaData() {
    auto options = MakeCommonPortfolioOptions();

    return {{
        .id = "inv_vol_weight",
        .category = epoch_core::TransformCategory::Portfolio,
        .plotKind = epoch_core::TransformPlotKind::panel_line,
        .name = "Inverse Volatility Weight",
        .options = options,
        .isCrossSectional = true,
        .desc = "Risk-based allocation: weight inversely proportional to volatility. "
                "w_i = (1/sigma_i) / sum(1/sigma_j). Doesn't use correlation structure.",
        .inputs = {{epoch_core::IODataType::Decimal, "SLOT", "Asset Returns", true, false}},
        .outputs = {{epoch_core::IODataType::Decimal, "weight", "Portfolio Weight", true, false}},
        .atLeastOneInputRequired = true,
        .tags = {"portfolio", "allocation", "risk-based", "volatility", "inverse-vol"},
        .requiresTimeFrame = false,
        .strategyTypes = {"portfolio-construction", "risk-based"},
        .relatedTransforms = {"equal_weight", "hrp", "risk_parity"},
        .usageContext = "Use when assets have similar expected returns but different volatilities. "
                        "Simpler than full risk parity.",
        .limitations = "Ignores correlation structure. May concentrate in low-vol assets."
    }};
}

// =============================================================================
// Combined Metadata Function
// =============================================================================

inline std::vector<TransformsMetaData> MakeAllPortfolioMetaData() {
    std::vector<TransformsMetaData> all;

    auto append = [&all](const auto& vec) {
        all.insert(all.end(), vec.begin(), vec.end());
    };

    append(MakeEqualWeightMetaData());
    append(MakeInverseVolatilityMetaData());
    // Add more as implemented in later tasks

    return all;
}

} // namespace epoch_script::transform::portfolio
```

### 5. Update register.h

**Update:** `src/transforms/components/portfolio/register.h`

Uncomment and add includes:
```cpp
#pragma once

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

#include "optimizers/equal_weight.h"
#include "optimizers/inverse_volatility.h"
#include "portfolio_metadata.h"

namespace epoch_script::transform::portfolio {

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // NAIVE ALLOCATORS
    epoch_script::transform::Register<EqualWeightOptimizer>("equal_weight");
    epoch_script::transform::Register<InverseVolatilityOptimizer>("inv_vol_weight");

    // METADATA REGISTRATION
    for (const auto& metadata : MakeAllPortfolioMetaData()) {
        metaRegistry.Register(metadata);
    }
}

} // namespace epoch_script::transform::portfolio
```

### 6. Create Unit Tests

**File:** `test/unit/transforms/portfolio/allocation/naive_allocators_test.cpp`

```cpp
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/transform_registry.h>

using namespace epoch_core;
using namespace epoch_frame;
using namespace epoch_script;
using namespace epoch_script::transform;
using Catch::Matchers::WithinRel;

namespace {

DataFrame make_test_returns() {
    // 5 assets, 100 observations
    std::vector<double> asset1(100), asset2(100), asset3(100), asset4(100), asset5(100);

    std::mt19937 rng(42);
    std::normal_distribution<> dist1(0.0001, 0.02);  // Low vol
    std::normal_distribution<> dist2(0.0001, 0.04);  // High vol
    std::normal_distribution<> dist3(0.0001, 0.03);  // Med vol
    std::normal_distribution<> dist4(0.0001, 0.025);
    std::normal_distribution<> dist5(0.0001, 0.035);

    for (int i = 0; i < 100; ++i) {
        asset1[i] = dist1(rng);
        asset2[i] = dist2(rng);
        asset3[i] = dist3(rng);
        asset4[i] = dist4(rng);
        asset5[i] = dist5(rng);
    }

    auto index = epoch_frame::factory::index::make_datetime_index(
        "2024-01-01", 100, epoch_frame::DateTimeResolution::Day);

    return epoch_frame::make_dataframe(index, {
        {"src#asset1", epoch_frame::factory::array::make_array(asset1)},
        {"src#asset2", epoch_frame::factory::array::make_array(asset2)},
        {"src#asset3", epoch_frame::factory::array::make_array(asset3)},
        {"src#asset4", epoch_frame::factory::array::make_array(asset4)},
        {"src#asset5", epoch_frame::factory::array::make_array(asset5)}
    });
}

double sum_weights(const DataFrame& df) {
    double sum = 0.0;
    for (const auto& col : df.column_names()) {
        auto vals = df[col].contiguous_array().to_vector<double>();
        sum += vals[0];  // Weights are constant, take first
    }
    return sum;
}

} // anonymous namespace

TEST_CASE("Equal Weight basic functionality", "[portfolio][equal_weight]") {
    const auto tf = EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    auto df = make_test_returns();

    auto cfg = run_op("equal_weight", "ew_test",
        {{ARG, {input_ref("src", "asset1"), input_ref("src", "asset2"),
                input_ref("src", "asset3"), input_ref("src", "asset4"),
                input_ref("src", "asset5")}}},
        {},
        tf);

    auto tbase = MAKE_TRANSFORM(cfg);
    auto t = dynamic_cast<ITransform*>(tbase.get());
    REQUIRE(t != nullptr);

    auto out = t->TransformData(df);

    SECTION("Output has correct dimensions") {
        REQUIRE(out.num_rows() == df.num_rows());
        REQUIRE(out.num_cols() == 5);  // One weight per asset
    }

    SECTION("All weights equal 0.2") {
        for (const auto& col : out.column_names()) {
            auto vals = out[col].contiguous_array().to_vector<double>();
            REQUIRE_THAT(vals[0], WithinRel(0.2, 1e-10));
        }
    }

    SECTION("Weights sum to 1.0") {
        REQUIRE_THAT(sum_weights(out), WithinRel(1.0, 1e-10));
    }
}

TEST_CASE("Inverse Volatility basic functionality", "[portfolio][inv_vol]") {
    const auto tf = EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    auto df = make_test_returns();

    auto cfg = run_op("inv_vol_weight", "iv_test",
        {{ARG, {input_ref("src", "asset1"), input_ref("src", "asset2"),
                input_ref("src", "asset3"), input_ref("src", "asset4"),
                input_ref("src", "asset5")}}},
        {{"covariance_method", MetaDataOptionDefinition{std::string("sample")}}},
        tf);

    auto tbase = MAKE_TRANSFORM(cfg);
    auto t = dynamic_cast<ITransform*>(tbase.get());
    REQUIRE(t != nullptr);

    auto out = t->TransformData(df);

    SECTION("Output has correct dimensions") {
        REQUIRE(out.num_rows() == df.num_rows());
        REQUIRE(out.num_cols() == 5);
    }

    SECTION("Weights sum to 1.0") {
        REQUIRE_THAT(sum_weights(out), WithinRel(1.0, 1e-6));
    }

    SECTION("Low vol asset has higher weight") {
        // asset1 has lowest vol (0.02), asset2 has highest (0.04)
        // So weight_asset1 > weight_asset2
        auto w1 = out["weight_asset1"].contiguous_array().to_vector<double>()[0];
        auto w2 = out["weight_asset2"].contiguous_array().to_vector<double>()[0];
        REQUIRE(w1 > w2);
    }

    SECTION("All weights positive") {
        for (const auto& col : out.column_names()) {
            auto vals = out[col].contiguous_array().to_vector<double>();
            REQUIRE(vals[0] > 0.0);
        }
    }
}
```

### 7. Update Test CMakeLists.txt

**File:** `test/unit/transforms/portfolio/allocation/CMakeLists.txt`

```cmake
target_sources(epoch_script_test PRIVATE
    naive_allocators_test.cpp
)
```

**Update parent:** `test/unit/transforms/portfolio/CMakeLists.txt`
```cmake
add_subdirectory(covariance)
add_subdirectory(allocation)
```

## Verification

```bash
cd /home/adesola/EpochLab/EpochScript
cmake --build cmake-build-release-test --target epoch_script_test
./cmake-build-release-test/bin/epoch_script_test "[portfolio][equal_weight]"
./cmake-build-release-test/bin/epoch_script_test "[portfolio][inv_vol]"
```

## Deliverables
- [ ] `portfolio_optimizer_base.h` - CRTP base class
- [ ] `equal_weight.h` - Equal weight optimizer
- [ ] `inverse_volatility.h` - Inverse volatility optimizer
- [ ] `portfolio_metadata.h` updated with both metadata
- [ ] `register.h` updated to register both transforms
- [ ] Unit tests pass

## Files Created/Modified
| File | Action |
|------|--------|
| `src/transforms/components/portfolio/optimizers/portfolio_optimizer_base.h` | Created |
| `src/transforms/components/portfolio/optimizers/equal_weight.h` | Created |
| `src/transforms/components/portfolio/optimizers/inverse_volatility.h` | Created |
| `src/transforms/components/portfolio/portfolio_metadata.h` | Updated |
| `src/transforms/components/portfolio/register.h` | Updated |
| `test/unit/transforms/portfolio/allocation/naive_allocators_test.cpp` | Created |
