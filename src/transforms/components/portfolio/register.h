#pragma once
// Portfolio Optimization transforms registration
// Provides transforms for portfolio construction and optimization
//
// Categories:
// 1. Naive Allocators - Simple allocation rules
//    - equal_weight: Equal weight (1/N) allocation
//    - inv_vol_weight: Inverse volatility allocation
// 2. Hierarchical - Clustering-based allocation
//    - hrp: Hierarchical Risk Parity
//    - herc: Hierarchical Equal Risk Contribution
// 3. Quadratic Programming - Optimization-based allocation
//    - mean_variance: Mean-Variance Optimization (Markowitz)
//    - risk_parity: Risk Parity / Risk Budgeting
//    - max_diversification: Maximum Diversification Ratio
// 4. Bayesian - View-adjusted allocation
//    - black_litterman: Black-Litterman Model
// 5. Rolling - Time-varying allocation
//    - rolling_hrp, rolling_mean_variance, rolling_risk_parity

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Metadata
#include "portfolio_metadata.h"

// Transform implementations - uncomment as each is implemented
// Task 04: Naive Allocators
// #include "optimizers/equal_weight.h"
// #include "optimizers/inverse_volatility.h"

// Task 07-08: Hierarchical
// #include "optimizers/hrp.h"
// #include "optimizers/herc.h"

// Task 10-12: Quadratic Programming
// #include "optimizers/mean_variance.h"
// #include "optimizers/risk_parity.h"
// #include "optimizers/max_diversification.h"

// Task 13: Bayesian
// #include "optimizers/black_litterman.h"

// Task 14: Rolling
// #include "rolling/rolling_hrp.h"
// #include "rolling/rolling_mean_variance.h"
// #include "rolling/rolling_risk_parity.h"

namespace epoch_script::transform::portfolio {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all portfolio optimization transforms
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // NAIVE ALLOCATORS (Task 04)
    // =========================================================================
    // Simple allocation rules that don't require optimization

    // equal_weight: Equal weight (1/N) allocation
    // Input: Cross-sectional DataFrame (one column per asset)
    // Output: Weight columns, each set to 1/N
    // Use for: Baseline benchmark, equal conviction portfolios
    // epoch_script::transform::Register<EqualWeightOptimizer>("equal_weight");

    // inv_vol_weight: Inverse volatility allocation
    // Input: Cross-sectional DataFrame (one column per asset returns)
    // Output: Weight columns proportional to 1/volatility
    // Use for: Volatility-targeting, naive risk parity approximation
    // epoch_script::transform::Register<InverseVolatilityOptimizer>("inv_vol_weight");

    // =========================================================================
    // HIERARCHICAL OPTIMIZERS (Task 07-08)
    // =========================================================================
    // Clustering-based allocation avoiding covariance matrix inversion

    // hrp: Hierarchical Risk Parity (Lopez de Prado)
    // Input: Cross-sectional DataFrame (returns)
    // Options: linkage (ward, single, complete, average), covariance_method
    // Output: Weight columns from recursive bisection
    // Use for: Diversified portfolios robust to estimation error
    // epoch_script::transform::Register<HRPOptimizer>("hrp");

    // herc: Hierarchical Equal Risk Contribution
    // Input: Cross-sectional DataFrame (returns)
    // Options: linkage, covariance_method, risk_measure
    // Output: Weight columns with equal risk contribution within clusters
    // Use for: Risk-budgeted hierarchical portfolios
    // epoch_script::transform::Register<HERCOptimizer>("herc");

    // =========================================================================
    // QUADRATIC PROGRAMMING OPTIMIZERS (Task 10-12)
    // =========================================================================
    // Optimization-based allocation using OSQP solver

    // mean_variance: Mean-Variance Optimization (Markowitz)
    // Input: Cross-sectional DataFrame (returns)
    // Options: objective (min_vol, max_sharpe, target_return), covariance_method
    // Output: Optimal weight columns
    // Use for: Classical portfolio optimization
    // epoch_script::transform::Register<MeanVarianceOptimizer>("mean_variance");

    // risk_parity: Risk Parity / Risk Budgeting
    // Input: Cross-sectional DataFrame (returns)
    // Options: covariance_method, risk_measure, risk_budget
    // Output: Weight columns with equal risk contribution
    // Use for: Balanced risk allocation across assets
    // epoch_script::transform::Register<RiskParityOptimizer>("risk_parity");

    // max_diversification: Maximum Diversification Ratio
    // Input: Cross-sectional DataFrame (returns)
    // Options: covariance_method
    // Output: Weight columns maximizing diversification ratio
    // Use for: Maximally diversified portfolios
    // epoch_script::transform::Register<MaxDiversificationOptimizer>("max_diversification");

    // =========================================================================
    // BAYESIAN OPTIMIZERS (Task 13)
    // =========================================================================
    // View-adjusted allocation combining prior and views

    // black_litterman: Black-Litterman Model
    // Input: Cross-sectional DataFrame (returns), view inputs
    // Options: tau, risk_aversion, covariance_method
    // Output: Weight columns from posterior optimization
    // Use for: Incorporating investment views into optimization
    // epoch_script::transform::Register<BlackLittermanOptimizer>("black_litterman");

    // =========================================================================
    // ROLLING OPTIMIZERS (Task 14)
    // =========================================================================
    // Time-varying allocation with lookback windows

    // rolling_hrp: Rolling Hierarchical Risk Parity
    // epoch_script::transform::Register<RollingHRPOptimizer>("rolling_hrp");

    // rolling_mean_variance: Rolling Mean-Variance Optimization
    // epoch_script::transform::Register<RollingMeanVarianceOptimizer>("rolling_mean_variance");

    // rolling_risk_parity: Rolling Risk Parity
    // epoch_script::transform::Register<RollingRiskParityOptimizer>("rolling_risk_parity");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // Register all portfolio metadata
    for (const auto& metadata : MakeAllPortfolioMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::portfolio
