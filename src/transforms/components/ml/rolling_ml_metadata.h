#pragma once
//
// Shared Metadata Options for Rolling ML Transforms
//
// Provides common metadata generation functions that all rolling transforms share.
//
#include <epoch_script/transforms/core/metadata.h>
#include <vector>

namespace epoch_script::transform {

/**
 * @brief Generate common options for all rolling ML transforms
 *
 * These options are shared by all rolling transforms:
 * - window_size: Training window size
 * - step_size: Rows to advance per iteration
 * - window_type: "rolling" or "expanding"
 * - min_training_samples: Minimum samples required
 */
inline std::vector<MetaDataOption> MakeRollingMLOptions() {
  return {
    MetaDataOption{
      .id = "window_size",
      .name = "Window Size",
      .type = epoch_core::MetaDataOptionType::Integer,
      .defaultValue = MetaDataOptionDefinition(252.0),
      .min = 20,
      .max = 10000,
      .desc = "Size of training window (for rolling) or minimum window (for expanding)"
    },
    MetaDataOption{
      .id = "step_size",
      .name = "Step Size",
      .type = epoch_core::MetaDataOptionType::Integer,
      .defaultValue = MetaDataOptionDefinition(1.0),
      .min = 1,
      .max = 100,
      .desc = "Number of rows to advance between each model retrain"
    },
    MetaDataOption{
      .id = "window_type",
      .name = "Window Type",
      .type = epoch_core::MetaDataOptionType::Select,
      .defaultValue = MetaDataOptionDefinition(std::string("rolling")),
      .selectOption = {
        {"Rolling (fixed window)", "rolling"},
        {"Expanding (cumulative)", "expanding"}
      },
      .desc = "Rolling uses fixed-size window; Expanding grows from start"
    },
    MetaDataOption{
      .id = "min_training_samples",
      .name = "Min Training Samples",
      .type = epoch_core::MetaDataOptionType::Integer,
      .defaultValue = MetaDataOptionDefinition(100.0),
      .min = 10,
      .max = 10000,
      .desc = "Minimum samples required before first prediction"
    }
  };
}

/**
 * @brief Add rolling options to an existing options list
 */
inline void AppendRollingOptions(std::vector<MetaDataOption>& options) {
  auto rolling_opts = MakeRollingMLOptions();
  options.insert(options.end(), rolling_opts.begin(), rolling_opts.end());
}

/**
 * @brief Combine transform-specific options with rolling options
 */
inline std::vector<MetaDataOption> CombineWithRollingOptions(
    std::vector<MetaDataOption> specific_options) {
  AppendRollingOptions(specific_options);
  return specific_options;
}

} // namespace epoch_script::transform
