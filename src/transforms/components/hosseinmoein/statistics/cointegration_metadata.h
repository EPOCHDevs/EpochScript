//
// Cointegration Transforms Metadata
// Provides metadata for half_life_ar1, rolling_adf, engle_granger, and johansen transforms
//

#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transform {

// Function to create metadata for all cointegration transforms
inline std::vector<epoch_script::transforms::TransformsMetaData> MakeCointegrationMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  // ============================================================================
  // HalfLifeAR1 - Mean-reversion half-life estimation
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = "half_life_ar1",
    .category = epoch_core::TransformCategory::Statistical,
    .name = "Half-Life AR(1)",
    .options = {
      {.id = "window",
       .name = "Window Size",
       .type = epoch_core::MetaDataOptionType::Integer,
       .isRequired = true,
       .desc = "Rolling window size for half-life calculation. Minimum 20 recommended for statistical validity."}
    },
    .isCrossSectional = false,
    .desc = "Estimate mean-reversion half-life using AR(1) model. "
            "Fits y(t) = alpha + beta*y(t-1) + epsilon, then computes half_life = -ln(2)/ln(beta). "
            "Used in pairs trading to estimate how quickly a spread reverts to mean. "
            "Lower half-life indicates faster mean reversion.",
    .inputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "SLOT",
       .name = "Spread Series"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "half_life",
       .name = "Half-Life (bars)"},
      {.type = epoch_core::IODataType::Number,
       .id = "ar1_coef",
       .name = "AR(1) Coefficient"},
      {.type = epoch_core::IODataType::Number,
       .id = "is_mean_reverting",
       .name = "Is Mean Reverting (0 or 1)"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"cointegration", "mean-reversion", "pairs-trading", "half-life", "ar1", "statistics"},
    .requiresTimeFrame = false,
    .allowNullInputs = false,
    .strategyTypes = {"pairs_trading", "statistical_arbitrage", "mean_reversion"},
    .relatedTransforms = {"engle_granger", "rolling_adf", "johansen_2"},
    .assetRequirements = {"single-asset"},
    .usageContext = "Use after computing a spread (e.g., from Engle-Granger) to estimate how long positions should be held. "
                    "Half-life guides position sizing and stop-loss timing.",
    .limitations = "Assumes AR(1) process. May produce negative or very large values for non-mean-reverting series."
  });

  // ============================================================================
  // RollingADF - Rolling Augmented Dickey-Fuller test
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = "rolling_adf",
    .category = epoch_core::TransformCategory::Statistical,
    .name = "Rolling ADF Test",
    .options = {
      {.id = "window",
       .name = "Window Size",
       .type = epoch_core::MetaDataOptionType::Integer,
       .isRequired = true,
       .desc = "Rolling window size for ADF test. Minimum 50 recommended for reliable results."},
      {.id = "max_lags",
       .name = "Maximum Lags",
       .type = epoch_core::MetaDataOptionType::Integer,
       .isRequired = false,
       .desc = "Maximum lags for ADF regression. If not specified, uses floor(12*(n/100)^0.25)."},
      {.id = "deterministic",
       .name = "Deterministic Terms",
       .type = epoch_core::MetaDataOptionType::String,
       .isRequired = false,
       .desc = "Deterministic terms: 'nc' (no constant), 'c' (constant only, default), 'ct' (constant + trend)."}
    },
    .isCrossSectional = false,
    .desc = "Rolling Augmented Dickey-Fuller test for stationarity. "
            "Tests null hypothesis that series has a unit root (non-stationary). "
            "Returns test statistic, p-value, and critical values at 1%, 5%, 10% significance. "
            "Uses MacKinnon (1994, 2010) tables for p-value computation.",
    .inputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "SLOT",
       .name = "Time Series"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "adf_stat",
       .name = "ADF Statistic"},
      {.type = epoch_core::IODataType::Number,
       .id = "p_value",
       .name = "P-Value"},
      {.type = epoch_core::IODataType::Number,
       .id = "critical_1pct",
       .name = "1% Critical Value"},
      {.type = epoch_core::IODataType::Number,
       .id = "critical_5pct",
       .name = "5% Critical Value"},
      {.type = epoch_core::IODataType::Number,
       .id = "critical_10pct",
       .name = "10% Critical Value"},
      {.type = epoch_core::IODataType::Number,
       .id = "is_stationary",
       .name = "Is Stationary (0 or 1)"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"cointegration", "stationarity", "unit-root", "adf", "statistics", "mackinnon"},
    .requiresTimeFrame = false,
    .allowNullInputs = false,
    .strategyTypes = {"pairs_trading", "statistical_arbitrage", "mean_reversion"},
    .relatedTransforms = {"half_life_ar1", "engle_granger", "johansen_2"},
    .assetRequirements = {"single-asset"},
    .usageContext = "Use to test if a spread or residual series is stationary (mean-reverting). "
                    "P-value < 0.05 suggests stationarity at 5% significance level.",
    .limitations = "Requires sufficient data points. Small samples may give unreliable results."
  });

  // ============================================================================
  // EngleGranger - Two-step Engle-Granger cointegration test
  // ============================================================================
  metadataList.emplace_back(TransformsMetaData{
    .id = "engle_granger",
    .category = epoch_core::TransformCategory::Statistical,
    .name = "Engle-Granger Cointegration",
    .options = {
      {.id = "window",
       .name = "Window Size",
       .type = epoch_core::MetaDataOptionType::Integer,
       .isRequired = true,
       .desc = "Rolling window size for cointegration test. Minimum 100 recommended."},
      {.id = "max_lags",
       .name = "Maximum Lags",
       .type = epoch_core::MetaDataOptionType::Integer,
       .isRequired = false,
       .desc = "Maximum lags for ADF test on residuals. If not specified, auto-selected."},
      {.id = "deterministic",
       .name = "Deterministic Terms",
       .type = epoch_core::MetaDataOptionType::String,
       .isRequired = false,
       .desc = "Deterministic terms for ADF: 'nc', 'c' (default), 'ct'."}
    },
    .isCrossSectional = false,
    .desc = "Two-step Engle-Granger cointegration test for two time series. "
            "Step 1: Regress Y on X to get hedge ratio (beta) and residuals. "
            "Step 2: Test residuals for stationarity using ADF. "
            "If residuals are stationary, series are cointegrated.",
    .inputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "y",
       .name = "Dependent Series (Y)"},
      {.type = epoch_core::IODataType::Number,
       .id = "x",
       .name = "Independent Series (X)"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "hedge_ratio",
       .name = "Hedge Ratio (beta)"},
      {.type = epoch_core::IODataType::Number,
       .id = "intercept",
       .name = "Intercept (alpha)"},
      {.type = epoch_core::IODataType::Number,
       .id = "spread",
       .name = "Spread (Y - alpha - beta*X)"},
      {.type = epoch_core::IODataType::Number,
       .id = "adf_stat",
       .name = "ADF Statistic"},
      {.type = epoch_core::IODataType::Number,
       .id = "p_value",
       .name = "P-Value"},
      {.type = epoch_core::IODataType::Number,
       .id = "critical_1pct",
       .name = "1% Critical Value"},
      {.type = epoch_core::IODataType::Number,
       .id = "critical_5pct",
       .name = "5% Critical Value"},
      {.type = epoch_core::IODataType::Number,
       .id = "critical_10pct",
       .name = "10% Critical Value"},
      {.type = epoch_core::IODataType::Number,
       .id = "is_cointegrated",
       .name = "Is Cointegrated (0 or 1)"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"cointegration", "pairs-trading", "hedge-ratio", "spread", "engle-granger", "statistics"},
    .requiresTimeFrame = false,
    .allowNullInputs = false,
    .strategyTypes = {"pairs_trading", "statistical_arbitrage"},
    .relatedTransforms = {"half_life_ar1", "rolling_adf", "johansen_2"},
    .assetRequirements = {"multi-asset"},
    .usageContext = "Use to test if two asset prices are cointegrated and compute optimal hedge ratio. "
                    "The spread (Y - hedge_ratio * X) should be mean-reverting if cointegrated.",
    .limitations = "Only tests pairwise cointegration. For >2 series, use Johansen test. "
                   "Hedge ratio may vary over time; consider rolling estimation."
  });

  // ============================================================================
  // Johansen - Multi-variate cointegration test (N=2,3,4,5)
  // ============================================================================
  for (int n = 2; n <= 5; ++n) {
    std::string id = "johansen_" + std::to_string(n);
    std::string name = "Johansen Cointegration (N=" + std::to_string(n) + ")";
    std::string desc = "Johansen cointegration test for " + std::to_string(n) + " time series. "
                       "Tests for multiple cointegrating relationships using VECM framework. "
                       "Returns trace statistics, max eigenvalue statistics, and critical values. "
                       "More powerful than Engle-Granger for multiple series.";

    // Inputs: asset_0, asset_1, ... asset_{n-1}
    std::vector<IOMetaData> inputs;
    for (int i = 0; i < n; ++i) {
      inputs.push_back({
        .type = epoch_core::IODataType::Number,
        .id = "asset_" + std::to_string(i),
        .name = "Asset " + std::to_string(i)
      });
    }

    // Outputs for Johansen
    std::vector<IOMetaData> outputs;
    outputs.push_back({.type = epoch_core::IODataType::Number, .id = "rank", .name = "Cointegration Rank"});

    // Trace statistics, max eigenvalue statistics, eigenvalues, and betas for each variable
    for (int i = 0; i < n; ++i) {
      outputs.push_back({
        .type = epoch_core::IODataType::Number,
        .id = "trace_stat_" + std::to_string(i),
        .name = "Trace Stat " + std::to_string(i)
      });
    }
    for (int i = 0; i < n; ++i) {
      outputs.push_back({
        .type = epoch_core::IODataType::Number,
        .id = "max_stat_" + std::to_string(i),
        .name = "Max Eigenvalue Stat " + std::to_string(i)
      });
    }
    for (int i = 0; i < n; ++i) {
      outputs.push_back({
        .type = epoch_core::IODataType::Number,
        .id = "eigval_" + std::to_string(i),
        .name = "Eigenvalue " + std::to_string(i)
      });
    }
    for (int i = 0; i < n; ++i) {
      outputs.push_back({
        .type = epoch_core::IODataType::Number,
        .id = "beta_" + std::to_string(i),
        .name = "Beta " + std::to_string(i)
      });
    }
    outputs.push_back({.type = epoch_core::IODataType::Number, .id = "spread", .name = "Spread"});

    metadataList.emplace_back(TransformsMetaData{
      .id = id,
      .category = epoch_core::TransformCategory::Statistical,
      .name = name,
      .options = {
        {.id = "window",
         .name = "Window Size",
         .type = epoch_core::MetaDataOptionType::Integer,
         .isRequired = true,
         .desc = "Rolling window size for Johansen test. Minimum 100 recommended."},
        {.id = "lag_p",
         .name = "VAR Lag Order",
         .type = epoch_core::MetaDataOptionType::Integer,
         .isRequired = false,
         .desc = "Lag order for VAR model. Default: 1."},
        {.id = "det_order",
         .name = "Deterministic Order",
         .type = epoch_core::MetaDataOptionType::Integer,
         .isRequired = false,
         .desc = "Deterministic specification: -1 (no deterministic), 0 (constant), 1 (constant + trend)."}
      },
      .isCrossSectional = false,
      .desc = desc,
      .inputs = inputs,
      .outputs = outputs,
      .atLeastOneInputRequired = true,
      .tags = {"cointegration", "johansen", "vecm", "multivariate", "statistics"},
      .requiresTimeFrame = false,
      .allowNullInputs = false,
      .strategyTypes = {"pairs_trading", "statistical_arbitrage", "basket_trading"},
      .relatedTransforms = {"engle_granger", "half_life_ar1", "rolling_adf"},
      .assetRequirements = {"multi-asset"},
      .usageContext = "Use for testing cointegration among " + std::to_string(n) + " or more assets. "
                      "Determines number of cointegrating relationships and provides cointegrating vectors.",
      .limitations = "Computationally intensive. Requires sufficient data for reliable eigenvalue estimation."
    });
  }

  return metadataList;
}

} // namespace epoch_script::transform
