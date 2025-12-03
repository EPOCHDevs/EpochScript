#pragma once
// String transforms registration
// Provides string manipulation and analysis operations
//
// Categories:
// 1. Case Transformations - Change string case
//    - string_case: Convert to upper/lower/title case
// 2. Trimming - Remove whitespace/characters
//    - string_trim: Trim leading/trailing characters
// 3. Search/Match - String containment checks
//    - string_contains: Check for substring presence
// 4. Character Type Checks - Validate string content
//    - string_check: Check if alpha/numeric/alphanumeric

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Transform implementations
#include "string_operations.h"
#include "string_enums.h"

namespace epoch_script::transform::string_ops {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all string transforms and metadata
// =============================================================================

inline void Register() {
    // auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // CASE TRANSFORMATIONS
    // =========================================================================
    // Convert string case for normalization and display.

    // string_case: Convert string case
    // Input: String series
    // Options: operation (upper, lower, title, capitalize, swapcase)
    // Outputs: String series with modified case
    // Use for: Normalizing text data, formatting display strings,
    //          case-insensitive comparisons
    epoch_script::transform::Register<StringCaseTransform>("string_case");

    // =========================================================================
    // TRIMMING
    // =========================================================================
    // Remove leading/trailing whitespace or specified characters.

    // string_trim: Trim whitespace or specific characters
    // Input: String series
    // Options: operation (trim, ltrim, rtrim), trim_chars (default: whitespace)
    // Outputs: Trimmed string series
    // Use for: Cleaning text data, removing padding,
    //          normalizing user input
    epoch_script::transform::Register<StringTrimTransform>("string_trim");

    // =========================================================================
    // SEARCH / CONTAINMENT
    // =========================================================================
    // Check for substring presence (returns boolean).

    // string_contains: Check if string contains pattern
    // Input: String series
    // Options: operation (contains, starts_with, ends_with), pattern
    // Outputs: Boolean series (true if pattern found)
    // Use for: Filtering by ticker patterns, sector matching,
    //          text-based classification
    epoch_script::transform::Register<StringContainsTransform>("string_contains");

    // =========================================================================
    // CHARACTER TYPE CHECKS
    // =========================================================================
    // Validate string content type (returns boolean).

    // string_check: Check character type
    // Input: String series
    // Options: operation (is_alpha, is_numeric, is_alphanumeric, is_lower, is_upper)
    // Outputs: Boolean series (true if check passes)
    // Use for: Data validation, input sanitization,
    //          format verification
    epoch_script::transform::Register<StringCheckTransform>("string_check");

}

}  // namespace epoch_script::transform::string_ops
