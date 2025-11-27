#pragma once
//
// Forward Fill Transform
//
// Fills null values by propagating the last valid observation forward.
// Uses Arrow's fill_null_forward compute function via epoch_frame.
//
// Example:
//   filled_data = ffill()(sparse_data)
//   // [1, null, null, 2, null] -> [1, 1, 1, 2, 2]
//

#include <epoch_script/transforms/core/itransform.h>
#include "../type_tags.h"

namespace epoch_script::transform {

/**
 * Forward Fill (ffill)
 *
 * Propagates the last valid observation forward to fill nulls.
 * Commonly used to:
 *   - Fill gaps in irregularly sampled data
 *   - Align quarterly fundamentals to daily prices
 *   - Handle missing values before calculations
 *
 * Works on all data types (numeric, string, boolean, timestamp).
 */
template <typename TypeTag>
class TypedFfill : public ITransform {
public:
  explicit TypedFfill(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    epoch_frame::Series input = bars[GetInputId()];

    // Use epoch_frame's ffill which wraps Arrow's fill_null_forward
    epoch_frame::Series result = input.ffill();

    return MakeResult(result.rename(GetOutputId()));
  }
};

// Type aliases for different data types
using FfillString = TypedFfill<StringType>;
using FfillNumber = TypedFfill<NumberType>;
using FfillBoolean = TypedFfill<BooleanType>;

// Metadata for ffill transforms
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeFfillMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  // Number variant
  metadataList.emplace_back(TransformsMetaData{
      .id = "ffill_number",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::Null,
      .name = "Forward Fill (Number)",
      .options = {},
      .desc = "Fills null values by forward-propagating the last valid numeric observation.",
      .inputs = {IOMetaDataConstants::DECIMAL_INPUT_METADATA},
      .outputs = {IOMetaDataConstants::DECIMAL_OUTPUT_METADATA},
      .tags = {"null-handling", "fill", "interpolation", "data-cleaning"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .strategyTypes = {"data-preparation", "research"},
      .assetRequirements = {"single-asset"}});

  // String variant
  metadataList.emplace_back(TransformsMetaData{
      .id = "ffill_string",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::Null,
      .name = "Forward Fill (String)",
      .options = {},
      .desc = "Fills null values by forward-propagating the last valid string observation.",
      .inputs = {IOMetaDataConstants::STRING_INPUT_METADATA},
      .outputs = {IOMetaDataConstants::STRING_OUTPUT_METADATA},
      .tags = {"null-handling", "fill", "interpolation", "data-cleaning"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .strategyTypes = {"data-preparation", "research"},
      .assetRequirements = {"single-asset"}});

  // Boolean variant
  metadataList.emplace_back(TransformsMetaData{
      .id = "ffill_boolean",
      .category = epoch_core::TransformCategory::Trend,
      .plotKind = epoch_core::TransformPlotKind::Null,
      .name = "Forward Fill (Boolean)",
      .options = {},
      .desc = "Fills null values by forward-propagating the last valid boolean observation.",
      .inputs = {IOMetaDataConstants::BOOLEAN_INPUT_METADATA},
      .outputs = {IOMetaDataConstants::BOOLEAN_OUTPUT_METADATA},
      .tags = {"null-handling", "fill", "interpolation", "data-cleaning"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .strategyTypes = {"data-preparation", "research"},
      .assetRequirements = {"single-asset"}});

  return metadataList;
}

} // namespace epoch_script::transform
