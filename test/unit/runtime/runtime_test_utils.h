#pragma once
/**
 * @file runtime_test_utils.h
 * @brief Utilities for runtime integration tests with CSV baseline comparison
 *
 * Provides helpers to:
 * - Compare DataFrames against CSV baselines
 * - Generate baselines on first run (when GENERATE_BASELINES env var is set)
 * - Compare arrays with numerical tolerance
 */

#include <catch2/catch_test_macros.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/serialization.h>
#include <arrow/api.h>
#include <filesystem>
#include <limits>
#include <spdlog/spdlog.h>

namespace epoch_script::runtime::test {

/**
 * @brief Get double value from array, handling nulls and type conversions
 */
inline std::pair<double, bool> GetDoubleValue(const epoch_frame::Array& arr, int64_t idx) {
    // Check if null first (using operator-> to access underlying arrow::Array)
    if (arr->IsNull(idx)) {
        return {std::numeric_limits<double>::quiet_NaN(), true};
    }

    auto type_id = arr->type_id();

    if (type_id == arrow::Type::DOUBLE) {
        auto view = arr.to_view<double>();
        return {view->Value(idx), false};
    } else if (type_id == arrow::Type::INT64) {
        auto view = arr.to_view<int64_t>();
        return {static_cast<double>(view->Value(idx)), false};
    } else if (type_id == arrow::Type::NA) {
        // All-null column read from CSV
        return {std::numeric_limits<double>::quiet_NaN(), true};
    } else if (type_id == arrow::Type::STRING || type_id == arrow::Type::LARGE_STRING) {
        // CSV might read numbers as strings - try to parse
        auto str_arr = std::static_pointer_cast<arrow::StringArray>(arr.value());
        std::string val = str_arr->GetString(idx);
        if (val.empty()) {
            return {std::numeric_limits<double>::quiet_NaN(), true};
        }
        try {
            return {std::stod(val), false};
        } catch (...) {
            return {std::numeric_limits<double>::quiet_NaN(), true};
        }
    }

    // Fallback - try double cast
    try {
        auto view = arr.to_view<double>();
        return {view->Value(idx), false};
    } catch (...) {
        return {std::numeric_limits<double>::quiet_NaN(), true};
    }
}

/**
 * @brief Compare two arrays with numerical tolerance
 *
 * @param actual Actual array from test
 * @param expected Expected array from baseline
 * @param rtol Relative tolerance (default 1%)
 * @param atol Absolute tolerance (default 1e-6)
 * @param skip_warmup Number of initial rows to skip (for rolling window warmup)
 * @return true if arrays match within tolerance
 */
inline bool ArraysApproxEqual(const epoch_frame::Array& actual,
                              const epoch_frame::Array& expected,
                              double rtol = 0.01,
                              double atol = 1e-6,
                              int64_t skip_warmup = 0) {
    if (actual.length() != expected.length()) {
        SPDLOG_ERROR("Length mismatch: actual={} vs expected={}",
                     actual.length(), expected.length());
        return false;
    }

    int mismatch_count = 0;
    for (int64_t i = skip_warmup; i < actual.length(); ++i) {
        auto [a, a_is_null] = GetDoubleValue(actual, i);
        auto [e, e_is_null] = GetDoubleValue(expected, i);

        // Both null/NaN is OK
        if ((a_is_null || std::isnan(a)) && (e_is_null || std::isnan(e))) continue;

        // One null/NaN, one not
        if (a_is_null || std::isnan(a) || e_is_null || std::isnan(e)) {
            if (mismatch_count < 5) {
                SPDLOG_ERROR("NaN mismatch at index {}: actual={}, expected={}", i, a, e);
            }
            mismatch_count++;
            continue;
        }

        // Check tolerance
        double diff = std::abs(a - e);
        double max_val = std::max(std::abs(a), std::abs(e));
        if (diff > atol && diff > rtol * max_val) {
            if (mismatch_count < 5) {
                SPDLOG_ERROR("Value mismatch at index {}: actual={}, expected={}, diff={}",
                             i, a, e, diff);
            }
            mismatch_count++;
        }
    }

    if (mismatch_count > 0) {
        SPDLOG_ERROR("Total mismatches: {} out of {}",
                     mismatch_count, actual.length() - skip_warmup);
    }
    return mismatch_count == 0;
}

/**
 * @brief Load expected baseline DataFrame from CSV
 *
 * @param csv_path Path to CSV file
 * @return DataFrame with data from CSV
 */
inline epoch_frame::DataFrame LoadExpectedCsv(const std::filesystem::path& csv_path) {
    auto result = epoch_frame::read_csv_file(csv_path.string(), epoch_frame::CSVReadOptions{});
    REQUIRE(result.ok());
    auto df = result.ValueOrDie();

    // Set index if present
    if (df.num_cols() > 0 && df.column_names()[0] == "index") {
        df = df.set_index("index");
    }
    return df;
}

/**
 * @brief Write DataFrame to CSV for baseline generation
 *
 * @param df DataFrame to write
 * @param csv_path Output path
 */
inline void WriteBaselineCsv(const epoch_frame::DataFrame& df,
                             const std::filesystem::path& csv_path) {
    std::filesystem::create_directories(csv_path.parent_path());
    auto status = epoch_frame::write_csv_file(df, csv_path.string());
    REQUIRE(status.ok());
    SPDLOG_INFO("Generated baseline: {} ({} rows, {} cols)",
                csv_path.string(), df.num_rows(), df.num_cols());
}

/**
 * @brief Check if baseline generation mode is enabled
 *
 * Set GENERATE_BASELINES=1 environment variable to generate baselines
 */
inline bool ShouldGenerateBaselines() {
    const char* env = std::getenv("GENERATE_BASELINES");
    return env != nullptr && std::string(env) == "1";
}

/**
 * @brief Compare DataFrame against CSV baseline or generate it
 *
 * If GENERATE_BASELINES=1, writes the actual DataFrame to CSV.
 * Otherwise, loads expected CSV and compares column by column.
 *
 * @param actual Actual DataFrame from test
 * @param baseline_name Name for the baseline (without .csv extension)
 * @param test_data_dir Directory containing expected CSVs
 * @param columns Columns to compare (empty = compare all)
 * @param rtol Relative tolerance
 * @param atol Absolute tolerance
 * @param skip_warmup Rows to skip at start
 */
inline void CompareOrGenerateBaseline(
    const epoch_frame::DataFrame& actual,
    const std::string& baseline_name,
    const std::filesystem::path& test_data_dir,
    const std::vector<std::string>& columns = {},
    double rtol = 0.01,
    double atol = 1e-6,
    int64_t skip_warmup = 0) {

    auto csv_path = test_data_dir / (baseline_name + ".csv");

    if (ShouldGenerateBaselines()) {
        WriteBaselineCsv(actual, csv_path);
        SPDLOG_WARN("Baseline generated - skipping comparison for: {}", baseline_name);
        return;
    }

    REQUIRE(std::filesystem::exists(csv_path));
    auto expected = LoadExpectedCsv(csv_path);

    INFO("Comparing against baseline: " << csv_path.string());
    REQUIRE(actual.num_rows() == expected.num_rows());

    // Determine columns to compare
    std::vector<std::string> cols_to_compare = columns;
    if (cols_to_compare.empty()) {
        cols_to_compare = expected.column_names();
    }

    for (const auto& col : cols_to_compare) {
        INFO("Column: " << col);
        REQUIRE(actual.contains(col));
        REQUIRE(expected.contains(col));

        auto actual_arr = actual[col].contiguous_array();
        auto expected_arr = expected[col].contiguous_array();

        REQUIRE(ArraysApproxEqual(actual_arr, expected_arr, rtol, atol, skip_warmup));
    }
}

/**
 * @brief Macro for easy baseline comparison in tests
 *
 * Usage:
 *   COMPARE_BASELINE(df, "engle_granger", {"eg#hedge_ratio", "eg#spread"});
 */
#define COMPARE_BASELINE(df, name, columns) \
    CompareOrGenerateBaseline(df, name, \
        std::filesystem::path(RUNTIME_TEST_DATA_DIR), columns)

#define COMPARE_BASELINE_WITH_WARMUP(df, name, columns, warmup) \
    CompareOrGenerateBaseline(df, name, \
        std::filesystem::path(RUNTIME_TEST_DATA_DIR), columns, 0.01, 1e-6, warmup)

} // namespace epoch_script::runtime::test
