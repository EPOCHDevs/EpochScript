#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transform::portfolio {

// =============================================================================
// Common Option Builders
// =============================================================================

/**
 * @brief Common covariance estimation method options
 * Used by all optimizers that require covariance matrix estimation
 */
inline std::vector<epoch_script::SelectOption> MakeCovarianceMethodOptions() {
    return {
        {"Sample Covariance", "sample"},
        {"Ledoit-Wolf Shrinkage", "ledoit_wolf"},
        {"Oracle Approximating Shrinkage", "oracle"},
        {"EWMA (Exponentially Weighted)", "ewma"},
        {"Gerber Statistic", "gerber"},
        {"RMT Denoising", "rmt"}
    };
}

/**
 * @brief Common risk measure options
 * Used by risk-aware optimizers (HERC, Risk Parity)
 */
inline std::vector<epoch_script::SelectOption> MakeRiskMeasureOptions() {
    return {
        {"Variance", "variance"},
        {"Standard Deviation", "std"},
        {"Mean Absolute Deviation", "mad"},
        {"CVaR (Conditional Value at Risk)", "cvar"},
        {"EVaR (Entropic Value at Risk)", "evar"},
        {"CDaR (Conditional Drawdown at Risk)", "cdar"},
        {"Max Drawdown", "max_drawdown"},
        {"Sortino Ratio (Downside)", "sortino"},
        {"Kurtosis", "kurtosis"},
        {"Worst Realization", "worst"}
    };
}

/**
 * @brief Linkage method options for hierarchical clustering
 * Used by HRP and HERC optimizers
 */
inline std::vector<epoch_script::SelectOption> MakeLinkageOptions() {
    return {
        {"Ward (Minimum Variance)", "ward"},
        {"Single (Nearest Point)", "single"},
        {"Complete (Farthest Point)", "complete"},
        {"Average (UPGMA)", "average"}
    };
}

/**
 * @brief MVO objective function options
 */
inline std::vector<epoch_script::SelectOption> MakeMVOObjectiveOptions() {
    return {
        {"Minimize Volatility", "min_vol"},
        {"Maximize Sharpe Ratio", "max_sharpe"},
        {"Target Return", "target_return"},
        {"Target Volatility", "target_vol"}
    };
}

// =============================================================================
// Metadata Factory Functions - Populated as transforms are implemented
// =============================================================================

/**
 * @brief Equal weight allocator metadata (Task 04)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeEqualWeightMetaData() {
    // Populated in Task 04
    return {};
}

/**
 * @brief Inverse volatility allocator metadata (Task 04)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeInverseVolatilityMetaData() {
    // Populated in Task 04
    return {};
}

/**
 * @brief HRP optimizer metadata (Task 07)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeHRPMetaData() {
    // Populated in Task 07
    return {};
}

/**
 * @brief HERC optimizer metadata (Task 08)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeHERCMetaData() {
    // Populated in Task 08
    return {};
}

/**
 * @brief Mean-Variance optimizer metadata (Task 10)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeMeanVarianceMetaData() {
    // Populated in Task 10
    return {};
}

/**
 * @brief Risk Parity optimizer metadata (Task 11)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeRiskParityMetaData() {
    // Populated in Task 11
    return {};
}

/**
 * @brief Max Diversification optimizer metadata (Task 12)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeMaxDiversificationMetaData() {
    // Populated in Task 12
    return {};
}

/**
 * @brief Black-Litterman optimizer metadata (Task 13)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeBlackLittermanMetaData() {
    // Populated in Task 13
    return {};
}

/**
 * @brief Rolling optimizer metadata (Task 14)
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeRollingPortfolioMetaData() {
    // Populated in Task 14
    return {};
}

// =============================================================================
// Combined Metadata Function
// =============================================================================

/**
 * @brief Collect all portfolio optimization metadata
 * Called from Register() to register all metadata with the transform registry
 */
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeAllPortfolioMetaData() {
    std::vector<epoch_script::transforms::TransformsMetaData> all;

    // Helper to append vectors
    auto append = [&all](const auto& vec) {
        all.insert(all.end(), vec.begin(), vec.end());
    };

    // Naive allocators (Task 04)
    append(MakeEqualWeightMetaData());
    append(MakeInverseVolatilityMetaData());

    // Hierarchical (Task 07-08)
    append(MakeHRPMetaData());
    append(MakeHERCMetaData());

    // QP-based (Task 10-12)
    append(MakeMeanVarianceMetaData());
    append(MakeRiskParityMetaData());
    append(MakeMaxDiversificationMetaData());

    // Bayesian (Task 13)
    append(MakeBlackLittermanMetaData());

    // Rolling (Task 14)
    append(MakeRollingPortfolioMetaData());

    return all;
}

}  // namespace epoch_script::transform::portfolio
