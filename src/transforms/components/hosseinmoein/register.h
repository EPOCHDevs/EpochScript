#pragma once
// HosseinMoein DataFrame-based transforms registration
// Provides statistical, cointegration, and volatility transforms
// using the HosseinMoein DataFrame library optimizations
//
// Submodules:
// 1. statistics/ - Correlation, cointegration, stationarity tests
// 2. indicators/ - Technical indicators (Hurst exponent)
// 3. volatility/ - Volatility estimators (Hodges-Tompkins, Ulcer Index)

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Statistics submodule
#include "statistics/rolling_corr.h"
#include "statistics/rolling_cov.h"
#include "statistics/beta.h"
#include "statistics/ewm_corr.h"
#include "statistics/ewm_cov.h"
#include "statistics/linear_fit.h"
#include "statistics/half_life_ar1.h"
#include "statistics/rolling_adf.h"
#include "statistics/engle_granger.h"
#include "statistics/johansen.h"
#include "statistics/frac_diff.h"
#include "statistics/cointegration_metadata.h"
#include "statistics/correlation_metadata.h"
#include "statistics/frac_diff_metadata.h"

// Indicators submodule
#include "indicators/hurst_exponent.h"

// Volatility submodule
#include "volatility/hodges_tompkins.h"
#include "volatility/ulcer_index.h"
#include "volatility/garman_klass.h"
#include "volatility/parkinson.h"
#include "volatility/yang_zhang.h"
#include "volatility/volatility_metadata.h"

namespace epoch_script::transform::hosseinmoein {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all HosseinMoein-based transforms
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // CORRELATION & COVARIANCE (Rolling and EWM)
    // =========================================================================
    // Pairwise statistical measures for pairs trading and risk management

    // rolling_corr: Rolling window Pearson correlation
    // Inputs: SLOT0, SLOT1 (two series)
    // Options: window
    // Outputs: correlation [-1, 1]
    // Use for: Pairs selection, correlation breakdown detection
    epoch_script::transform::Register<RollingCorr>("rolling_corr");

    // rolling_cov: Rolling window covariance
    // Inputs: SLOT0, SLOT1 (two series)
    // Options: window
    // Outputs: covariance
    // Use for: Portfolio risk, hedge ratio calculation
    epoch_script::transform::Register<RollingCov>("rolling_cov");

    // ewm_corr: Exponentially weighted correlation (recent data weighted more)
    // Inputs: SLOT0, SLOT1 (two series)
    // Options: halflife or span
    // Outputs: correlation [-1, 1]
    // Use for: Adaptive correlation that responds faster to recent data
    epoch_script::transform::Register<EWMCorr>("ewm_corr");

    // ewm_cov: Exponentially weighted covariance
    // Inputs: SLOT0, SLOT1 (two series)
    // Options: halflife or span
    // Outputs: covariance
    // Use for: Adaptive hedge ratios, dynamic risk models
    epoch_script::transform::Register<EWMCov>("ewm_cov");

    // =========================================================================
    // REGRESSION & BETA
    // =========================================================================
    // Linear regression and market factor exposure

    // beta: Rolling beta (regression slope vs benchmark)
    // Inputs: SLOT0 (asset), SLOT1 (benchmark/market)
    // Options: window
    // Outputs: beta
    // Use for: Market exposure measurement, beta-neutral strategies
    epoch_script::transform::Register<Beta>("beta");

    // linear_fit: Rolling OLS regression
    // Inputs: SLOT0 (dependent), SLOT1 (independent)
    // Options: window
    // Outputs: slope, intercept, r_squared, residual
    // Use for: Pairs trading spread, factor exposure, residual signals
    epoch_script::transform::Register<LinearFit>("linear_fit");

    // =========================================================================
    // COINTEGRATION TESTS
    // =========================================================================
    // Tests for long-run equilibrium relationships (pairs/stat-arb)

    // engle_granger: Two-step Engle-Granger cointegration test
    // Inputs: SLOT0, SLOT1 (two price series)
    // Outputs: spread, hedge_ratio, adf_stat, p_value, is_cointegrated
    // Use for: Pairs selection, spread construction
    epoch_script::transform::Register<EngleGranger>("engle_granger");

    // johansen: Johansen multivariate cointegration test
    // Inputs: SLOT0, SLOT1, ... (2+ price series)
    // Outputs: spread, hedge_ratios, trace_stat, p_value
    // Use for: Multi-asset cointegration (triangular arb, baskets)
    epoch_script::transform::Register<Johansen>("johansen");

    // half_life_ar1: Half-life of mean reversion from AR(1)
    // Input: spread or residual series
    // Outputs: half_life (in periods)
    // Use for: Pairs trading exit timing, position sizing
    epoch_script::transform::Register<HalfLifeAR1>("half_life_ar1");

    // rolling_adf: Rolling Augmented Dickey-Fuller stationarity test
    // Input: series (spread or returns)
    // Options: window
    // Outputs: adf_stat, p_value, is_stationary
    // Use for: Monitoring spread stationarity, regime detection
    epoch_script::transform::Register<RollingADF>("rolling_adf");

    // =========================================================================
    // FRACTIONAL DIFFERENTIATION
    // =========================================================================
    // Balance stationarity and memory preservation (Marcos Lopez de Prado)

    // frac_diff: Fractional differentiation of price series
    // Input: prices
    // Options: d (differencing order, 0 < d < 1)
    // Outputs: frac_diff_series
    // Use for: ML feature engineering that preserves memory
    epoch_script::transform::Register<FracDiff>("frac_diff");

    // =========================================================================
    // TECHNICAL INDICATORS
    // =========================================================================
    // Specialized market structure indicators

    // rolling_hurst_exponent: Rolling Hurst exponent (trend vs mean-reversion)
    // Input: returns or prices
    // Options: window
    // Outputs: hurst [0, 1] where H<0.5=mean-reverting, H>0.5=trending
    // Use for: Strategy selection, regime detection
    epoch_script::transform::Register<RollingHurstExponent>("rolling_hurst_exponent");

    // =========================================================================
    // VOLATILITY ESTIMATORS
    // =========================================================================
    // Alternative volatility measures beyond standard deviation

    // hodges_tompkins: Bias-corrected volatility estimator
    // Input: returns
    // Options: window
    // Outputs: volatility (annualized)
    // Use for: More accurate vol estimation with small samples
    epoch_script::transform::Register<HodgesTompkins>("hodges_tompkins");

    // ulcer_index: Downside volatility (pain index)
    // Input: prices
    // Options: window
    // Outputs: ulcer_index
    // Use for: Risk-adjusted returns (Martin ratio), drawdown risk
    epoch_script::transform::Register<UlcerIndex>("ulcer_index");

    // garman_klass: OHLC-based volatility estimator
    // Input: OHLC data
    // Options: period, trading_days
    // Outputs: annualized volatility
    // Use for: More efficient vol estimation than close-to-close (~8x)
    epoch_script::transform::Register<GarmanKlass>("garman_klass");

    // parkinson: High-Low range-based volatility
    // Input: High, Low prices
    // Options: period, trading_periods
    // Outputs: annualized volatility
    // Use for: Efficient vol estimation with just high/low data
    epoch_script::transform::Register<Parkinson>("parkinson");

    // yang_zhang: Overnight + intraday volatility estimator
    // Input: OHLC data
    // Options: period, trading_periods
    // Outputs: annualized volatility
    // Use for: Most accurate estimator when overnight gaps matter
    epoch_script::transform::Register<YangZhang>("yang_zhang");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // Cointegration transforms metadata
    for (const auto& metadata : MakeCointegrationMetaData()) {
        metaRegistry.Register(metadata);
    }

    // Fractional differentiation metadata
    for (const auto& metadata : MakeFracDiffMetaData()) {
        metaRegistry.Register(metadata);
    }

    // Volatility estimator metadata
    for (const auto& metadata : epoch_script::transforms::MakeVolatilityEstimatorMetaData()) {
        metaRegistry.Register(metadata);
    }

    // Correlation and statistical transforms metadata
    for (const auto& metadata : epoch_script::transforms::MakeCorrelationMetaData()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::hosseinmoein
