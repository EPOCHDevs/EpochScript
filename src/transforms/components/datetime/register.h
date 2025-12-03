#pragma once
// Datetime transforms registration
// Provides datetime component extraction and manipulation
//
// Categories:
// 1. Extraction - Extract datetime components from timestamps
//    - index_datetime_extract: From bar index timestamps
//    - column_datetime_extract: From timestamp columns
// 2. Creation - Create timestamp values
//    - timestamp_scalar: Constant timestamp for comparisons
// 3. Calculation - Time difference calculations
//    - datetime_diff: Difference between two timestamps

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "index_datetime_extract.h"
#include "datetime_diff.h"
#include "timestamp_scalar.h"

// Metadata definitions
#include "datetime_metadata.h"

namespace epoch_script::transform::datetime {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all datetime transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // DATETIME EXTRACTION
    // =========================================================================
    // Extract year, month, day, hour, minute, second, day_of_week, etc.
    // from timestamps for time-based filtering and analysis.

    // index_datetime_extract: Extract component from bar index timestamps
    // No input required - operates on DataFrame index
    // Outputs: value (integer component)
    // Use for: seasonal strategies, weekday effects, intraday patterns
    epoch_script::transform::Register<IndexDatetimeExtract>("index_datetime_extract");

    // column_datetime_extract: Extract component from timestamp column
    // Input: timestamp column (e.g., observation_date, period_end)
    // Outputs: value (integer component)
    // Use for: fundamental timing, economic event analysis
    epoch_script::transform::Register<ColumnDatetimeExtract>("column_datetime_extract");

    // =========================================================================
    // TIMESTAMP CREATION
    // =========================================================================
    // Create constant timestamp values for comparisons and filtering

    // timestamp_scalar: Create constant timestamp from ISO string
    // Options: value ("YYYY-MM-DD" or "YYYY-MM-DD HH:MM:SS")
    // Outputs: value (timestamp)
    // Use for: date cutoffs, regime change markers, event filtering
    epoch_script::transform::Register<TimestampScalar>("timestamp_scalar");

    // =========================================================================
    // TIME CALCULATIONS
    // =========================================================================
    // Calculate differences between timestamps

    // datetime_diff: Time difference between two timestamps
    // Inputs: SLOT0 (from), SLOT1 (to)
    // Options: unit (days, hours, minutes, seconds, weeks, months, years)
    // Outputs: value (integer difference)
    // Use for: recency analysis, lag detection, staleness filtering
    epoch_script::transform::Register<DatetimeDiff>("datetime_diff");

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    for (const auto& metadata : MakeDatetimeTransforms()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::datetime
