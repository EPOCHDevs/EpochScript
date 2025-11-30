//
// Fractional Differentiation Metadata
// Based on "Advances in Financial Machine Learning" by Marcos López de Prado
//

#pragma once

#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transform {

inline std::vector<epoch_script::transforms::TransformsMetaData> MakeFracDiffMetaData() {
  using namespace epoch_script::transforms;
  using namespace epoch_script;
  std::vector<TransformsMetaData> metadataList;

  metadataList.emplace_back(TransformsMetaData{
    .id = "frac_diff",
    .category = epoch_core::TransformCategory::Statistical,
    .plotKind = epoch_core::TransformPlotKind::panel_line,
    .name = "Fractional Differentiation (FFD)",
    .options = {
      {.id = "d",
       .name = "Differentiation Order",
       .type = epoch_core::MetaDataOptionType::Decimal,
       .defaultValue = MetaDataOptionDefinition(0.5),
       .isRequired = true,
       .selectOption = {},
       .min = 0.01,
       .max = 2.0,
       .step_size = 0.01,
       .desc = "Fractional differentiation order. Values between 0 and 1 balance stationarity and memory preservation."},
      {.id = "threshold",
       .name = "Weight Threshold",
       .type = epoch_core::MetaDataOptionType::Decimal,
       .defaultValue = MetaDataOptionDefinition(1e-5),
       .isRequired = false,
       .selectOption = {},
       .min = 1e-10,
       .max = 0.1,
       .step_size = 1e-6,
       .desc = "Minimum weight threshold. Weights below this are truncated, determining the effective window size."}
    },
    .isCrossSectional = false,
    .desc = "Fixed-Width Window Fractional Differentiation (FFD). "
            "Transforms a time series to achieve stationarity while preserving memory. "
            "Unlike integer differencing (d=1) which removes all memory, fractional differencing "
            "with d < 1 maintains predictive information. Based on López de Prado's 'Advances in Financial Machine Learning'.",
    .inputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "SLOT",
       .name = "Price Series"}
    },
    .outputs = {
      {.type = epoch_core::IODataType::Number,
       .id = "result",
       .name = "Fractionally Differentiated Series"}
    },
    .atLeastOneInputRequired = true,
    .tags = {"statistics", "stationarity", "memory", "fracdiff", "ffd", "machine-learning", "feature-engineering"},
    .requiresTimeFrame = false,
    .allowNullInputs = false,
    .strategyTypes = {"machine-learning", "feature-engineering", "statistical-arbitrage"},
    .relatedTransforms = {"rolling_adf", "hurst_exponent", "zscore"},
    .assetRequirements = {"single-asset"},
    .usageContext = "Use on log prices before feeding to ML models. Finds minimum d that makes series stationary "
                    "(check with ADF test) while preserving memory for prediction. "
                    "Typical workflow: test d values from 0.1-1.0, select smallest d where ADF rejects unit root.",
    .limitations = "Early values are NaN (window = number of weights above threshold). "
                   "Optimal d varies by asset and time period - should be recalibrated periodically. "
                   "d > 1 is over-differencing and may remove useful signal."
  });

  return metadataList;
}

} // namespace epoch_script::transform
