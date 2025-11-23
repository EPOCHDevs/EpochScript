#include "coverage_tracker.h"
#include <algorithm>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <spdlog/spdlog.h>
#include <epoch_frame/series.h>

namespace epoch_script::runtime::test {

// ============================================================================
// Singleton Access
// ============================================================================

CoverageTracker& CoverageTracker::GetInstance() {
    static CoverageTracker instance;
    return instance;
}

// ============================================================================
// Record Execution Methods
// ============================================================================

void CoverageTracker::RecordExecution(
    const std::string& transform_name,
    const TimeFrameAssetDataFrameMap& outputs,
    int64_t execution_time_ms,
    bool passed,
    size_t asset_count,
    const std::string& timeframe
) {
    auto& metrics = metrics_[transform_name];

    if (metrics.transform_name.empty()) {
        metrics.transform_name = transform_name;
    }

    // Update counts
    metrics.test_count++;
    if (passed) {
        metrics.pass_count++;
    } else {
        metrics.fail_count++;
    }

    // Update execution time
    metrics.total_execution_time_ms += execution_time_ms;

    // Track scenarios
    metrics.asset_counts_tested.insert(asset_count);
    metrics.timeframes_tested.insert(timeframe);

    // Analyze outputs for data quality
    for (const auto& [tf, asset_map] : outputs) {
        for (const auto& [asset, df] : asset_map) {
            AnalyzeDataFrame(df, metrics.null_stats, metrics.value_stats, metrics.output_size_stats);
        }
    }
}

void CoverageTracker::RecordExecutionNoOutput(
    const std::string& transform_name,
    int64_t execution_time_ms,
    bool passed,
    size_t asset_count,
    const std::string& timeframe
) {
    auto& metrics = metrics_[transform_name];

    if (metrics.transform_name.empty()) {
        metrics.transform_name = transform_name;
    }

    // Update counts
    metrics.test_count++;
    if (passed) {
        metrics.pass_count++;
    } else {
        metrics.fail_count++;
    }

    // Update execution time
    metrics.total_execution_time_ms += execution_time_ms;

    // Track scenarios
    metrics.asset_counts_tested.insert(asset_count);
    metrics.timeframes_tested.insert(timeframe);
}

// ============================================================================
// DataFrame Analysis
// ============================================================================

void CoverageTracker::AnalyzeDataFrame(
    const epoch_frame::DataFrame& df,
    NullStatistics& null_stats,
    ValueStatistics& value_stats,
    OutputSizeStatistics& size_stats
) {
    size_t num_rows = df.num_rows();
    size_t num_cols = df.num_cols();

    if (num_rows == 0 || num_cols == 0) {
        null_stats.all_null_count++;
        return;
    }

    // Update size stats
    size_stats.Update(num_rows, num_cols);

    // Check for nulls and analyze values
    bool has_any_null = false;
    bool all_null = true;

    for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
        try {
            auto col_name = df.column_names().at(col_idx);
            auto col = df[col_name];

            // Check if column is numeric
            auto arrow_array = col.contiguous_array().value();
            if (!arrow_array) continue;

            size_t null_count = arrow_array->null_count();
            has_any_null |= (null_count > 0);
            all_null &= (null_count == num_rows);

            // Try to get numeric values for statistics using Arrow type traits
            if (arrow::is_floating(arrow_array->type()->id())) {
                // Handle all floating point types (float, double)
                auto double_array = std::static_pointer_cast<arrow::DoubleArray>(arrow_array);
                for (int64_t i = 0; i < double_array->length(); ++i) {
                    if (!double_array->IsNull(i)) {
                        double value = double_array->Value(i);
                        if (std::isfinite(value)) {
                            value_stats.Update(value);
                        }
                    }
                }
            } else if (arrow::is_integer(arrow_array->type()->id())) {
                // Handle all integer types (int8, int16, int32, int64, uint8, etc.)
                auto int_array = std::static_pointer_cast<arrow::Int64Array>(arrow_array);
                for (int64_t i = 0; i < int_array->length(); ++i) {
                    if (!int_array->IsNull(i)) {
                        value_stats.Update(static_cast<double>(int_array->Value(i)));
                    }
                }
            } else if (arrow_array->type()->id() == arrow::Type::BOOL) {
                auto bool_array = std::static_pointer_cast<arrow::BooleanArray>(arrow_array);
                for (int64_t i = 0; i < bool_array->length(); ++i) {
                    if (!bool_array->IsNull(i)) {
                        value_stats.Update(bool_array->Value(i) ? 1.0 : 0.0);
                    }
                }
            }
        } catch (const std::exception& e) {
            SPDLOG_WARN(e.what());
            // Skip columns that can't be analyzed
            continue;
        }
    }

    // Update null statistics
    if (all_null) {
        null_stats.all_null_count++;
    } else if (has_any_null) {
        null_stats.some_null_count++;
    } else {
        null_stats.no_null_count++;
    }
}

bool CoverageTracker::IsAllNull(const epoch_frame::DataFrame& df) const {
    if (df.num_rows() == 0 || df.num_cols() == 0) {
        return true;
    }

    for (size_t col_idx = 0; col_idx < df.num_cols(); ++col_idx) {
        try {
            auto col_name = df.column_names().at(col_idx);
            auto col = df[col_name];

            auto arrow_array = col.contiguous_array().value();
            if (!arrow_array) continue;

            if (arrow_array->null_count() < arrow_array->length()) {
                return false;  // Found at least one non-null value
            }
        } catch (const std::exception&) {
            continue;
        }
    }

    return true;
}

bool CoverageTracker::HasSomeNull(const epoch_frame::DataFrame& df) const {
    if (df.num_rows() == 0 || df.num_cols() == 0) {
        return false;
    }

    for (size_t col_idx = 0; col_idx < df.num_cols(); ++col_idx) {
        try {
            auto col_name = df.column_names().at(col_idx);
            auto col = df[col_name];

            auto arrow_array = col.contiguous_array().value();
            if (!arrow_array) continue;

            if (arrow_array->null_count() > 0) {
                return true;
            }
        } catch (const std::exception&) {
            continue;
        }
    }

    return false;
}

// ============================================================================
// Coverage Report Generation
// ============================================================================

CoverageTracker::CoverageReport CoverageTracker::GenerateReport() const {
    CoverageReport report;
    report.total_transforms = total_transforms_;
    report.tested_transforms = metrics_.size();
    report.metrics = metrics_;

    // Identify untested transforms (would need access to registry to determine)
    // For now, we can only report on what we've tested

    return report;
}

const CoverageTracker::TransformMetrics* CoverageTracker::GetMetrics(const std::string& transform_name) const {
    auto it = metrics_.find(transform_name);
    if (it != metrics_.end()) {
        return &it->second;
    }
    return nullptr;
}

void CoverageTracker::Reset() {
    metrics_.clear();
    total_transforms_ = 0;
}

// ============================================================================
// Coverage Report Helper Methods
// ============================================================================

std::vector<std::pair<std::string, size_t>>
CoverageTracker::CoverageReport::GetMostTestedTransforms(size_t limit) const {
    std::vector<std::pair<std::string, size_t>> result;
    result.reserve(metrics.size());

    for (const auto& [name, m] : metrics) {
        result.emplace_back(name, m.test_count);
    }

    // Sort by test count descending
    std::sort(result.begin(), result.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    if (result.size() > limit) {
        result.resize(limit);
    }

    return result;
}

std::vector<std::pair<std::string, double>>
CoverageTracker::CoverageReport::GetSlowestTransforms(size_t limit) const {
    std::vector<std::pair<std::string, double>> result;
    result.reserve(metrics.size());

    for (const auto& [name, m] : metrics) {
        if (m.test_count > 0) {
            result.emplace_back(name, m.AvgExecutionTimeMs());
        }
    }

    // Sort by execution time descending
    std::sort(result.begin(), result.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    if (result.size() > limit) {
        result.resize(limit);
    }

    return result;
}

std::vector<std::pair<std::string, double>>
CoverageTracker::CoverageReport::GetHighNullRateTransforms(double threshold) const {
    std::vector<std::pair<std::string, double>> result;

    for (const auto& [name, m] : metrics) {
        double null_rate = m.null_stats.AllNullPercent();
        if (null_rate >= threshold) {
            result.emplace_back(name, null_rate);
        }
    }

    // Sort by null rate descending
    std::sort(result.begin(), result.end(),
              [](const auto& a, const auto& b) { return a.second > b.second; });

    return result;
}

// ============================================================================
// JSON Serialization
// ============================================================================

void CoverageTracker::CoverageReport::WriteToFile(const fs::path& output_path) const {
    // Build JSON manually for better control
    std::ofstream file(output_path);
    if (!file.is_open()) {
        throw std::runtime_error("Failed to open file: " + output_path.string());
    }

    file << "{\n";
    file << "  \"summary\": {\n";
    file << "    \"total_transforms\": " << total_transforms << ",\n";
    file << "    \"tested_transforms\": " << tested_transforms << ",\n";
    file << "    \"untested_transforms\": " << (total_transforms - tested_transforms) << ",\n";
    file << "    \"coverage_percent\": " << std::fixed << std::setprecision(2) << CoveragePercent() << "\n";
    file << "  },\n";

    file << "  \"transforms\": {\n";
    bool first_transform = true;
    for (const auto& [name, m] : metrics) {
        if (!first_transform) {
            file << ",\n";
        }
        first_transform = false;

        file << "    \"" << name << "\": {\n";
        file << "      \"test_count\": " << m.test_count << ",\n";
        file << "      \"pass_count\": " << m.pass_count << ",\n";
        file << "      \"fail_count\": " << m.fail_count << ",\n";
        file << "      \"total_execution_time_ms\": " << m.total_execution_time_ms << ",\n";
        file << "      \"avg_execution_time_ms\": " << std::fixed << std::setprecision(2) << m.AvgExecutionTimeMs() << ",\n";

        file << "      \"null_stats\": {\n";
        file << "        \"all_null_count\": " << m.null_stats.all_null_count << ",\n";
        file << "        \"some_null_count\": " << m.null_stats.some_null_count << ",\n";
        file << "        \"no_null_count\": " << m.null_stats.no_null_count << ",\n";
        file << "        \"all_null_percent\": " << std::fixed << std::setprecision(2) << m.null_stats.AllNullPercent() << "\n";
        file << "      },\n";

        file << "      \"value_stats\": {\n";
        if (m.value_stats.count > 0) {
            file << "        \"min\": " << m.value_stats.min << ",\n";
            file << "        \"max\": " << m.value_stats.max << ",\n";
            file << "        \"mean\": " << std::fixed << std::setprecision(4) << m.value_stats.Mean() << ",\n";
            file << "        \"stddev\": " << std::fixed << std::setprecision(4) << m.value_stats.StdDev() << ",\n";
            file << "        \"count\": " << m.value_stats.count << "\n";
        } else {
            file << "        \"count\": 0\n";
        }
        file << "      },\n";

        file << "      \"output_size_stats\": {\n";
        file << "        \"avg_rows\": " << std::fixed << std::setprecision(2) << m.output_size_stats.AvgRows() << ",\n";
        file << "        \"avg_columns\": " << std::fixed << std::setprecision(2) << m.output_size_stats.AvgColumns() << "\n";
        file << "      },\n";

        file << "      \"asset_counts_tested\": [";
        bool first_count = true;
        std::vector<size_t> sorted_counts(m.asset_counts_tested.begin(), m.asset_counts_tested.end());
        std::sort(sorted_counts.begin(), sorted_counts.end());
        for (auto count : sorted_counts) {
            if (!first_count) file << ", ";
            file << count;
            first_count = false;
        }
        file << "],\n";

        file << "      \"timeframes_tested\": [";
        bool first_tf = true;
        std::vector<std::string> sorted_tf(m.timeframes_tested.begin(), m.timeframes_tested.end());
        std::sort(sorted_tf.begin(), sorted_tf.end());
        for (const auto& tf : sorted_tf) {
            if (!first_tf) file << ", ";
            file << "\"" << tf << "\"";
            first_tf = false;
        }
        file << "]\n";

        file << "    }";
    }
    file << "\n  }\n";
    file << "}\n";

    file.close();
}

// ============================================================================
// Print Report Summary
// ============================================================================

void CoverageTracker::CoverageReport::PrintSummary(std::ostream& os) const {
    os << "\n";
    os << "============================================================\n";
    os << "              Transform Coverage Report\n";
    os << "============================================================\n\n";

    // Summary statistics
    os << "SUMMARY:\n";
    os << "  Total Transforms:   " << total_transforms << "\n";
    os << "  Tested:             " << tested_transforms << " ("
       << std::fixed << std::setprecision(1) << CoveragePercent() << "%)\n";
    os << "  Untested:           " << (total_transforms - tested_transforms) << "\n\n";

    // Coverage threshold check (85%)
    bool passes_threshold = CoveragePercent() >= 85.0;
    os << "  Coverage Threshold: 85.0% ";
    os << (passes_threshold ? "[PASS ✓]" : "[FAIL ✗]") << "\n\n";

    // Most tested transforms
    auto most_tested = GetMostTestedTransforms(5);
    if (!most_tested.empty()) {
        os << "TOP 5 MOST TESTED:\n";
        for (size_t i = 0; i < most_tested.size(); ++i) {
            const auto& [name, count] = most_tested[i];
            auto it = metrics.find(name);
            if (it != metrics.end()) {
                os << "  " << (i + 1) << ". " << std::setw(30) << std::left << name
                   << " - " << count << " tests "
                   << "(avg " << std::fixed << std::setprecision(1)
                   << it->second.AvgExecutionTimeMs() << "ms)\n";
            }
        }
        os << "\n";
    }

    // Slowest transforms
    auto slowest = GetSlowestTransforms(5);
    if (!slowest.empty()) {
        os << "TOP 5 SLOWEST (by avg execution time):\n";
        for (size_t i = 0; i < slowest.size(); ++i) {
            const auto& [name, avg_time] = slowest[i];
            auto it = metrics.find(name);
            if (it != metrics.end()) {
                os << "  " << (i + 1) << ". " << std::setw(30) << std::left << name
                   << " - " << std::fixed << std::setprecision(1) << avg_time << "ms "
                   << "(" << it->second.test_count << " tests)\n";
            }
        }
        os << "\n";
    }

    // High null rate transforms
    auto high_null = GetHighNullRateTransforms(50.0);
    if (!high_null.empty()) {
        os << "DATA QUALITY WARNINGS (>50% all-null outputs):\n";
        for (const auto& [name, null_rate] : high_null) {
            os << "  - " << std::setw(30) << std::left << name
               << " - " << std::fixed << std::setprecision(1) << null_rate << "% all-null\n";
        }
        os << "\n";
    }

    // Untested transforms
    if (!untested_transforms.empty() && untested_transforms.size() <= 20) {
        os << "UNTESTED TRANSFORMS:\n";
        for (const auto& name : untested_transforms) {
            os << "  - " << name << "\n";
        }
        os << "\n";
    } else if (!untested_transforms.empty()) {
        os << "UNTESTED TRANSFORMS: " << untested_transforms.size() << " (too many to list)\n\n";
    }

    os << "============================================================\n";
    os << "\n";
}

} // namespace epoch_script::runtime::test
