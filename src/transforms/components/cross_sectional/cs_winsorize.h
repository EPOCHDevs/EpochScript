#pragma once
//
// Cross-Sectional Winsorize Transform
//
// Caps extreme values at specified percentiles ACROSS assets at each timestamp.
// Different from regular winsorize which operates over TIME within each asset.
//
// Example:
//   clean_pe = cs_winsorize(lower=0.05, upper=0.95)(raw_pe)
//   // At each timestamp, P/E ratios are winsorized across all stocks
//

#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/scalar.h>
#include <arrow/compute/api.h>

namespace epoch_script::transform {

/**
 * Cross-Sectional Winsorize
 *
 * Caps extreme values at specified percentile cutoffs ACROSS assets.
 * At each timestamp:
 *   1. Calculate lower and upper percentile across all asset values
 *   2. Cap values below lower percentile to the lower percentile value
 *   3. Cap values above upper percentile to the upper percentile value
 *
 * Use Cases:
 *   - Normalize P/E ratios before cross-sectional ranking
 *   - Remove outlier returns before factor construction
 *   - Prepare data for cross-sectional regression
 */
class CSWinsorize final : public ITransform {
public:
  explicit CSWinsorize(const TransformConfiguration &config)
      : ITransform(config),
        m_lower(config.GetOptionValue("lower_limit").GetDecimal()),
        m_upper(config.GetOptionValue("upper_limit").GetDecimal()) {
    AssertFromFormat(m_lower >= 0.0 && m_lower < 1.0,
                     "lower_limit must be in [0, 1)");
    AssertFromFormat(m_upper > 0.0 && m_upper <= 1.0,
                     "upper_limit must be in (0, 1]");
    AssertFromFormat(m_lower < m_upper,
                     "lower_limit must be less than upper_limit");
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    if (df.empty() || df.num_cols() == 0) {
      throw std::runtime_error("CSWinsorize requires multi-column DataFrame");
    }

    // Apply row-wise (across columns/assets at each timestamp)
    // Use Arrow's built-in winsorize function (available in Arrow v21+)
    arrow::compute::WinsorizeOptions opts(m_lower, m_upper);

    auto winsorized = df.apply(
        [&opts](const Array &row_array) -> Array {
          // row_array contains values for all assets at this timestamp
          const Series row_series(row_array.value());

          auto result = arrow::compute::CallFunction(
              "winsorize", {row_series.contiguous_array().value()}, &opts);

          if (!result.ok()) {
            // If winsorize fails, return original
            return Array(row_series.array());
          }

          return Array(result->make_array());
        },
        AxisType::Row);

    return winsorized;
  }

private:
  double m_lower;
  double m_upper;
};

// Metadata for cs_winsorize transform
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeCSWinsorizeMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  metadataList.emplace_back(TransformsMetaData{
      .id = "cs_winsorize",
      .category = epoch_core::TransformCategory::Statistical,
      .plotKind = epoch_core::TransformPlotKind::Null,
      .name = "Cross-Sectional Winsorize",
      .options =
          {MetaDataOption{
               .id = "lower_limit",
               .name = "Lower Percentile",
               .type = epoch_core::MetaDataOptionType::Decimal,
               .defaultValue = MetaDataOptionDefinition(0.05),
               .desc = "Values below this percentile (across assets) are capped",
               .tuningGuidance = "Use 0.01-0.05 for light winsorization"},
           MetaDataOption{
               .id = "upper_limit",
               .name = "Upper Percentile",
               .type = epoch_core::MetaDataOptionType::Decimal,
               .defaultValue = MetaDataOptionDefinition(0.95),
               .desc = "Values above this percentile (across assets) are capped",
               .tuningGuidance = "Use 0.95-0.99 for light winsorization"}},
      .isCrossSectional = true,
      .desc = "Caps extreme values at specified percentile cutoffs ACROSS assets "
              "at each timestamp. Use before cs_zscore or cs_rank for robust normalization.",
      .inputs = {IOMetaDataConstants::DECIMAL_INPUT_METADATA},
      .outputs = {IOMetaDataConstants::DECIMAL_OUTPUT_METADATA},
      .tags = {"cross-sectional", "outliers", "normalization", "robust", "statistics"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .strategyTypes = {"research", "trading"},
      .relatedTransforms = {"cs_zscore", "cs_rank", "winsorize"},
      .assetRequirements = {"multi-asset"}});

  return metadataList;
}

} // namespace epoch_script::transform
