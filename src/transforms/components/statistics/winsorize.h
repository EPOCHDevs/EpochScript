#pragma once
//
// Time-series Winsorize Transform
//
// Caps extreme values at specified percentiles within each asset's history.
// Uses Arrow's winsorize compute function.
//
// Example:
//   clean_pe = winsorize(lower=0.05, upper=0.95)(raw_pe)
//   // Values below 5th percentile -> 5th percentile value
//   // Values above 95th percentile -> 95th percentile value
//

#include <epoch_script/transforms/core/itransform.h>
#include <arrow/compute/api.h>

namespace epoch_script::transform {

/**
 * Time-series Winsorize
 *
 * Caps extreme values at specified percentile cutoffs.
 * Applied per-column (per-asset) along the time axis.
 *
 * Options:
 *   lower_limit: Lower percentile cutoff (default: 0.05)
 *   upper_limit: Upper percentile cutoff (default: 0.95)
 */
class Winsorize final : public ITransform {
public:
  explicit Winsorize(const TransformConfiguration &config)
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
  TransformData(epoch_frame::DataFrame const &bars) const override {
    epoch_frame::Series input = bars[GetInputId()];

    // Use Arrow's built-in winsorize function (available in Arrow v21+)
    arrow::compute::WinsorizeOptions opts(m_lower, m_upper);

    auto result = arrow::compute::CallFunction(
        "winsorize", {input.array()->chunk(0)}, &opts);

    if (!result.ok()) {
      throw std::runtime_error("Failed to winsorize values: " +
                               result.status().ToString());
    }

    epoch_frame::Series output(
        input.index(),
        arrow::ChunkedArray::Make({result->make_array()}).ValueOrDie(),
        GetOutputId());

    return MakeResult(output);
  }

private:
  double m_lower;
  double m_upper;
};

// Metadata for winsorize transform
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeWinsorizeMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  metadataList.emplace_back(TransformsMetaData{
      .id = "winsorize",
      .category = epoch_core::TransformCategory::Statistical,
      .plotKind = epoch_core::TransformPlotKind::Null,
      .name = "Winsorize",
      .options =
          {MetaDataOption{
               .id = "lower_limit",
               .name = "Lower Percentile",
               .type = epoch_core::MetaDataOptionType::Decimal,
               .defaultValue = MetaDataOptionDefinition(0.05),
               .desc = "Values below this percentile are capped",
               .tuningGuidance = "Use 0.01-0.05 for light winsorization, 0.10 for aggressive"},
           MetaDataOption{
               .id = "upper_limit",
               .name = "Upper Percentile",
               .type = epoch_core::MetaDataOptionType::Decimal,
               .defaultValue = MetaDataOptionDefinition(0.95),
               .desc = "Values above this percentile are capped",
               .tuningGuidance = "Use 0.95-0.99 for light winsorization, 0.90 for aggressive"}},
      .desc = "Caps extreme values at specified percentile cutoffs. Useful for "
              "handling outliers without removing data points.",
      .inputs = {IOMetaDataConstants::DECIMAL_INPUT_METADATA},
      .outputs = {IOMetaDataConstants::DECIMAL_OUTPUT_METADATA},
      .tags = {"outliers", "normalization", "robust", "statistics"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .strategyTypes = {"research", "trading"},
      .assetRequirements = {"single-asset"}});

  return metadataList;
}

} // namespace epoch_script::transform
