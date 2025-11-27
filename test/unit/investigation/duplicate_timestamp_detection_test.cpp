//
// Unit test to detect duplicate timestamps in dividends data
// This test reproduces the scenario from the failing test case:
// corp_actions_short_interest_dividends_1d_research
//
#include "catch.hpp"
#include <epoch_script/data/factory.h>
#include <epoch_script/transforms/runtime/transform_manager/itransform_manager.h>
#include <epoch_script/data/database/database.h>
#include <epoch_data_sdk/dataloader/factory.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/serialization.h>
#include <arrow/compute/api.h>
#include <filesystem>
#include <iostream>
#include <set>

using namespace epoch_script::data;
using namespace epoch_script;
using namespace epoch_frame;
using namespace epoch_core;

// Helper function to check for duplicate timestamps in a DataFrame
struct DuplicateCheckResult {
    size_t total_rows = 0;
    size_t unique_timestamps = 0;
    std::vector<int64_t> duplicate_timestamps;
    bool has_duplicates() const { return !duplicate_timestamps.empty(); }
};

DuplicateCheckResult CheckDuplicatesInDataFrame(const DataFrame& df, const std::string& label) {
    DuplicateCheckResult result;
    result.total_rows = df.num_rows();

    auto index = df.index();
    if (!index) {
        std::cout << "[WARN] " << label << ": No index found\n";
        return result;
    }

    auto index_array = index->as_chunked_array();

    // Use Arrow's ValueCounts to detect duplicates
    auto vc_result = arrow::compute::ValueCounts(index_array);
    if (!vc_result.ok()) {
        std::cout << "[ERROR] " << label << ": ValueCounts failed: "
                  << vc_result.status().message() << "\n";
        return result;
    }

    auto vc_struct = vc_result.ValueOrDie();
    auto counts = std::static_pointer_cast<arrow::Int64Array>(
        vc_struct->GetFieldByName("counts"));
    auto values = vc_struct->GetFieldByName("values");

    result.unique_timestamps = counts->length();

    // Find duplicates
    for (int64_t i = 0; i < counts->length(); i++) {
        if (counts->Value(i) > 1) {
            auto val_scalar = values->GetScalar(i);
            if (val_scalar.ok()) {
                auto ts_scalar = std::static_pointer_cast<arrow::TimestampScalar>(*val_scalar);
                int64_t ts = ts_scalar->value;
                result.duplicate_timestamps.push_back(ts);

                // Convert to human-readable date
                auto dt = DateTime::fromtimestamp(ts, "UTC");
                std::cout << "[DUPLICATE] " << label << ": "
                          << dt.repr() << " appears " << counts->Value(i) << "x\n";
            }
        }
    }

    return result;
}

// Helper to check cache files for duplicates
void CheckCacheFiles(const std::filesystem::path& cache_dir) {
    std::cout << "\n=== Checking Cache Files ===\n";

    if (!std::filesystem::exists(cache_dir)) {
        std::cout << "[INFO] Cache directory doesn't exist: " << cache_dir << "\n";
        return;
    }

    size_t total_files = 0;
    size_t files_with_duplicates = 0;

    for (const auto& entry : std::filesystem::recursive_directory_iterator(cache_dir)) {
        if (entry.path().extension() == ".arrow") {
            total_files++;

            // Read the Arrow file
            auto read_result = epoch_frame::read_arrow(entry.path().string());
            if (!read_result.ok()) {
                std::cout << "[ERROR] Failed to read " << entry.path().filename() << "\n";
                continue;
            }

            auto df = read_result.MoveValueUnsafe();
            auto label = std::string("Cache: ") + entry.path().filename().string();
            auto check_result = CheckDuplicatesInDataFrame(df, label);

            if (check_result.has_duplicates()) {
                files_with_duplicates++;
                std::cout << "[FAIL] " << label << ": "
                          << check_result.total_rows << " rows, "
                          << check_result.unique_timestamps << " unique, "
                          << check_result.duplicate_timestamps.size() << " duplicates\n";
            } else if (check_result.total_rows > 0) {
                std::cout << "[PASS] " << label << ": "
                          << check_result.total_rows << " rows, "
                          << check_result.unique_timestamps << " unique, "
                          << "NO duplicates\n";
            }
        }
    }

    std::cout << "\nCache Summary: " << files_with_duplicates << "/" << total_files
              << " files have duplicates\n";
}

TEST_CASE("Dividends data has no duplicate timestamps", "[.investigation][duplicate_detection][dividends]") {
    std::cout << "\n========================================\n";
    std::cout << "DUPLICATE TIMESTAMP DETECTION TEST\n";
    std::cout << "========================================\n\n";

    // Set up test cache directory
    std::filesystem::path test_cache = "/tmp/epoch_duplicate_test_cache";
    std::filesystem::remove_all(test_cache);
    std::filesystem::create_directories(test_cache);

    std::cout << "[INFO] Test cache directory: " << test_cache << "\n";

    // Create EpochScript code that loads dividends (mimics failing test)
    std::string code = R"(
div = dividends(timeframe="1D")()
)";

    std::cout << "[INFO] EpochScript code:\n" << code << "\n";

    // Parse the script
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto transform_manager = epoch_script::runtime::CreateTransformManager(source);


    // Use DJIA30 index (expands to ~30 constituents)
    std::vector<std::string> asset_ids = {"DJIA30"};
    auto [dataloaderAssets, strategyAssets, continuationAssets] =
        epoch_script::data::factory::MakeAssets(CountryCurrency::USD, asset_ids, false);

    // Set up data configuration with 30 DOW assets
    DataModuleOption dataConfig{
        .loader = {
            .startDate = DateTime::from_date_str("2022-01-01").date(),
            .endDate = DateTime::from_date_str("2025-01-31").date(),
            .requests = {},
            .dataloaderAssets = dataloaderAssets,
            .strategyAssets = strategyAssets,
            .continuationAssets = continuationAssets,
            .sourcePath = std::nullopt,
            .cacheDir = test_cache.string()
        }
    };
    dataConfig.loader.AddRequest(DataCategory::Dividends);

    dataConfig.transformManager = std::move(transform_manager);

    std::cout << "[INFO] Loading data for " << strategyAssets.size() << " assets\n";

    // Create the database factory and run pipeline
    auto factory = epoch_script::data::factory::DataModuleFactory(std::move(dataConfig));
    auto database = factory.CreateDatabase();

    std::cout << "[INFO] Running pipeline...\n";
    database->RunPipeline();
    std::cout << "[INFO] Pipeline complete\n\n";

    // Check database for duplicates
    std::cout << "=== Checking Database Tables ===\n";

    auto transformedData = database->GetTransformedData();

    size_t total_assets_checked = 0;
    size_t assets_with_duplicates = 0;

    for (const auto& [timeframe, assetMap] : transformedData) {
        std::cout << "\nTimeframe: " << timeframe << "\n";

        for (const auto& [asset, dataframe] : assetMap) {
            total_assets_checked++;

            std::string label = std::string("DB: ") + asset.GetID() + " @ " + timeframe;
            auto check_result = CheckDuplicatesInDataFrame(dataframe, label);

            if (check_result.has_duplicates()) {
                assets_with_duplicates++;
                std::cout << "[FAIL] " << label << ": "
                          << check_result.total_rows << " rows, "
                          << check_result.unique_timestamps << " unique, "
                          << check_result.duplicate_timestamps.size() << " duplicates\n";
            } else if (check_result.total_rows > 0) {
                std::cout << "[PASS] " << label << ": "
                          << check_result.total_rows << " rows, "
                          << check_result.unique_timestamps << " unique\n";
            }
        }
    }

    std::cout << "\nDatabase Summary: " << assets_with_duplicates << "/"
              << total_assets_checked << " assets have duplicates\n";

    // Check cache files
    CheckCacheFiles(test_cache);

    // Final verdict
    std::cout << "\n========================================\n";
    if (assets_with_duplicates == 0) {
        std::cout << "✅ TEST PASSED: No duplicates found in database\n";
    } else {
        std::cout << "❌ TEST FAILED: Found duplicates in " << assets_with_duplicates
                  << " assets\n";
    }
    std::cout << "========================================\n\n";

    // Cleanup
    std::filesystem::remove_all(test_cache);

    // Assert no duplicates
    REQUIRE(assets_with_duplicates == 0);
}
