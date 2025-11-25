//
// EpochScript Standalone Test Runner
//
// Usage: epoch_test_runner "<output_dir>"
//
// This executable compiles EpochScript code and runs it, outputting results to structured directory.
// Reads code from code.epochscript and metadata from metadata.json.
// Used by the Python coverage agent for test generation.
//

#include <iostream>
#include <fstream>
#include <stdexcept>
#include <filesystem>
#include <glaze/glaze.hpp>

#include "common.h"
#include "transforms/compiler/ast_compiler.h"
#include "epoch_frame/factory/calendar_factory.h"
#include <epoch_script/strategy/registration.h>
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/strategy/introspection.h>
#include <epoch_script/transforms/core/registration.h>
#include <epoch_script/data/factory.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/serialization.h>
#include <arrow/compute/initialize.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/json/json.h>
#include <absl/log/initialize.h>
#include <epoch_data_sdk/model/asset/asset_database.hpp>
#include <epoch_core/macros.h>

namespace fs = std::filesystem;

namespace {

// Asset configurations for multi-asset testing
struct AssetConfiguration {
  std::string name;
  std::vector<std::string> assets;
};

static std::vector<AssetConfiguration> GetAssetConfigurations() {
  return {
      {"single_stock", {"AAPL-Stocks"}},
{"small_index", {"DJIA30"}},
{"large_index", {"SP500"}}

  };
}

// Metadata JSON structure for test case (code stored separately in code.epochscript)
struct MetadataJson {
  std::string name;
  std::string description;
  std::string role;
  std::string category;
  std::string asset_config;
};

// Helper: Normalize CompilationResult by sorting nodes by ID
epoch_script::CompilationResult NormalizeResult(
    epoch_script::CompilationResult result) {
  std::ranges::sort(result, [](const auto& a, const auto& b) {
    return a.id < b.id;
  });
  return result;
}


// Initialize EpochScript runtime
void InitializeRuntime() {
  absl::InitializeLog();
  GOOGLE_PROTOBUF_VERIFY_VERSION;

  auto arrowComputeStatus = arrow::compute::Initialize();
  if (!arrowComputeStatus.ok()) {
    std::stringstream errorMsg;
    errorMsg << "arrow compute initialized failed: " << arrowComputeStatus;
    throw std::runtime_error(errorMsg.str());
  }

  epoch_frame::calendar::CalendarFactory::instance().Init();

  // Load asset specifications from S3
  AssertFromFormat(
      data_sdk::asset::AssetSpecificationDatabase::GetInstance().IsInitialized(),
      "Failed to initialize Asset Specification Database.");

  // Register transform metadata
  epoch_script::transforms::RegisterTransformMetadata(
      epoch_script::DEFAULT_YAML_LOADER);

  // Initialize transforms registry
  epoch_script::transform::InitializeTransforms(
      epoch_script::DEFAULT_YAML_LOADER, {}, {});

  setenv("POLYGON_API_KEY", "ptMp4LUoa1sgSpTFS7v8diiVtnimqH46", 1);
  setenv("FRED_API_KEY", "b6561c96d3615458fcae0b57580664f3", 1);
}

// Cleanup runtime
void ShutdownRuntime() {
  google::protobuf::ShutdownProtobufLibrary();
}

// Write string to file
void WriteToFile(const std::string& content, const std::string& path) {
  std::ofstream file(path);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open file for writing: " + path);
  }
  file << content;
  file.close();
}

// Save graph.json
void SaveGraph(const epoch_script::CompilationResult& graph, const std::string& output_dir) {
  std::string graph_json;
  auto ec = glz::write_json(graph, graph_json);
  if (ec) {
    throw std::runtime_error("Failed to serialize graph to JSON");
  }

  std::string graph_path = output_dir + "/graph.json";
  WriteToFile(graph_json, graph_path);
}

// Save transformed data as parquet files
void SaveTransformedDataAsParquet(
    const std::string& output_dir,
    const std::string& profile_name,
    const auto& db_output_data
) {
  // For each timeframe in the transformed data
  for (const auto& [timeframe_key, asset_map] : db_output_data) {
    // Create directory: output_dir/{profile}/tables/{timeframe_key}/
    std::string timeframe_dir = output_dir + "/" + profile_name +
                                "/tables/" + timeframe_key + "/";
    fs::create_directories(timeframe_dir);

    // For each asset's dataframe
    for (const auto& [asset, dataframe] : asset_map) {
      std::string asset_filename = asset.GetID() + ".parquet.gzip";
      std::string output_path = std::filesystem::path(timeframe_dir) / asset_filename;

      // Configure parquet write options with gzip compression
      epoch_frame::ParquetWriteOptions options;
      options.compression = arrow::Compression::GZIP;
      options.include_index = true;

      // Write DataFrame as parquet using epoch_frame serialization
      auto status = epoch_frame::write_parquet(dataframe, output_path, options);
      if (!status.ok()) {
        throw std::runtime_error("Failed to write parquet for " + asset.GetID() +
                                 " at " + timeframe_key + ": " + status.ToString());
      }
    }
  }
}

// Convert TearSheet (protobuf) to JSON
std::string TearSheetToJson(const epoch_proto::TearSheet& tearsheet) {
  std::string jsonStr;

  google::protobuf::json::PrintOptions options;
  options.add_whitespace = true;
  options.preserve_proto_field_names = true;

  auto status = google::protobuf::json::MessageToJsonString(tearsheet, &jsonStr, options);

  if (!status.ok()) {
    throw std::runtime_error("Failed to convert TearSheet to JSON: " + std::string(status.message()));
  }

  return jsonStr;
}

// Save reports as JSON for each asset
void SaveReportsAsJson(
    const std::string& output_dir,
    const std::string& profile_name,
    const epoch_script::runtime::AssetReportMap& reports
) {
  if (reports.empty()) {
    return;
  }

  // Create directory: output_dir/{profile}/reports/
  std::string reports_dir = output_dir + "/" + profile_name + "/reports/";
  fs::create_directories(reports_dir);

  // Save each asset's report as JSON
  for (const auto& [asset_id, tearsheet] : reports) {
    std::string report_filename = asset_id + ".json";
    std::string output_path = std::filesystem::path(reports_dir) / report_filename;

    std::string json_content = TearSheetToJson(tearsheet);
    WriteToFile(json_content, output_path);
  }
}

// Serialize EventMarkerData to JSON using Glaze
struct EventMarkerDataJson {
  std::string title;
  std::string icon;
  std::vector<epoch_script::CardColumnSchema> schemas;
  std::map<std::string, std::vector<std::string>> data;  // Simplified data representation
  std::optional<size_t> pivot_index;
};

std::string EventMarkerDataToJson(const epoch_script::transform::EventMarkerData& event_marker) {
  EventMarkerDataJson json_data;
  json_data.title = event_marker.title;
  json_data.icon = epoch_core::IconWrapper::ToString(event_marker.icon);
  json_data.schemas = event_marker.schemas;
  json_data.pivot_index = event_marker.pivot_index;

  // Convert DataFrame to simple map representation
  for (const auto& col_name : event_marker.data.column_names()) {
    json_data.data[col_name] = {};
    // Store column info (just indicating it exists)
    json_data.data[col_name].push_back("<" + std::string(event_marker.data[col_name].dtype()->ToString()) + ">");
  }

  std::string json_str;
  auto ec = glz::write<glz::opts{.prettify = true}>(json_data, json_str);
  if (ec) {
    throw std::runtime_error("Failed to serialize EventMarkerData to JSON");
  }

  return json_str;
}

// Save event markers as JSON for each asset
void SaveEventMarkersAsJson(
    const std::string& output_dir,
    const std::string& profile_name,
    const epoch_script::runtime::AssetEventMarkerMap& event_markers
) {
  if (event_markers.empty()) {
    return;
  }

  // Create directory: output_dir/{profile}/event_markers/
  std::string markers_dir = output_dir + "/" + profile_name + "/event_markers/";
  fs::create_directories(markers_dir);

  // Save each asset's event markers as JSON array
  for (const auto& [asset_id, markers] : event_markers) {
    std::string marker_filename = asset_id + ".json";
    std::string output_path = std::filesystem::path(markers_dir) / marker_filename;

    // Serialize vector of event markers
    std::vector<std::string> marker_jsons;
    for (const auto& marker : markers) {
      marker_jsons.push_back(EventMarkerDataToJson(marker));
    }

    // Create JSON array
    std::string json_array = "[\n  " +
      [&]() {
        std::string result;
        for (size_t i = 0; i < marker_jsons.size(); ++i) {
          if (i > 0) result += ",\n  ";
          result += marker_jsons[i];
        }
        return result;
      }() +
      "\n]";

    WriteToFile(json_array, output_path);
  }
}

// Run test on EpochScript source with output directory
void RunTest(const std::string& source, const std::string& output_dir, const std::string& selected_asset_config) {
  // Compile EpochScript source
  auto compiler = std::make_unique<epoch_script::strategy::PythonSource>(source, false);

  // Extract compilation result
  auto compilation_result = compiler->GetCompilationResult();

  // Normalize by sorting nodes by ID
  auto normalized = NormalizeResult(compilation_result);

  // Save graph.json to output directory
  SaveGraph(normalized, output_dir);

  // Runtime execution with selected asset configuration
  auto all_asset_configs = GetAssetConfigurations();

  // Find the selected asset config
  auto it = std::ranges::find_if(all_asset_configs, [&](const auto& config) {
    return config.name == selected_asset_config;
  });

  if (it == all_asset_configs.end()) {
    throw std::runtime_error("Invalid asset_config: " + selected_asset_config +
                             ". Must be one of: single_stock, small_index, large_index");
  }

  const auto& asset_config = *it;

  // 1. Create StrategyConfig from test input
  epoch_script::strategy::StrategyConfig strategyConfig;
  strategyConfig.trade_signal.source = *compiler;
  strategyConfig.data.assets = epoch_script::strategy::AssetIDContainer(asset_config.assets);

  // Determine date range based on timeframe (10 years for daily, 1 year for intraday)
  bool is_intraday = epoch_script::strategy::IsIntradayCampaign(strategyConfig);

  auto start_date = is_intraday
      ? epoch_frame::DateTime::from_str("2024-01-01", "UTC", "%Y-%m-%d").date()
      : epoch_frame::DateTime::from_str("2015-01-01", "UTC", "%Y-%m-%d").date();

  auto end_date = epoch_frame::DateTime::from_str("2025-01-01", "UTC", "%Y-%m-%d").date();

  // 2. Create database using strategy-aware factory
  auto dataModuleOption = epoch_script::data::factory::MakeDataModuleOptionFromStrategy(
      epoch_core::CountryCurrency::USD,
      epoch_script::strategy::DatePeriodConfig{start_date, end_date},
      strategyConfig
  );

  auto factory = epoch_script::data::factory::DataModuleFactory(std::move(dataModuleOption));
  auto database = factory.CreateDatabase();

  // 3. Run database pipeline (load + transform data)
  database->RunPipeline();

  // 4. Get outputs directly from database
  auto db_output_data = database->GetTransformedData();
  auto reports = database->GetGeneratedReports();
  auto event_markers = database->GetGeneratedEventMarkers();

  // 5. Validate that at least one output was generated
  bool has_output = !db_output_data.empty() || !reports.empty() || !event_markers.empty();

  if (!has_output) {
    throw std::runtime_error("Runtime execution produced no outputs for asset config: " + asset_config.name);
  }

  // 6. Save all outputs
  SaveTransformedDataAsParquet(output_dir, asset_config.name, db_output_data);
  SaveReportsAsJson(output_dir, asset_config.name, reports);
  SaveEventMarkersAsJson(output_dir, asset_config.name, event_markers);
}

// Read code from code.epochscript file
std::string ReadCodeFromFile(const std::string& test_case_dir) {
  std::string code_path = test_case_dir + "/code.epochscript";

  std::ifstream file(code_path);
  if (!file.is_open()) {
    throw std::runtime_error("Failed to open code.epochscript: " + code_path);
  }

  // Read entire file into string
  return std::string((std::istreambuf_iterator<char>(file)),
                     std::istreambuf_iterator<char>());
}

}  // namespace

int main(int argc, char* argv[]) {
  if (argc != 2) {
    std::cerr << "Usage: epoch_test_runner \"<output_dir>\"\n";
    std::cerr << "\n";
    std::cerr << "Arguments:\n";
    std::cerr << "  output_dir  Directory containing test case files and where outputs will be saved\n";
    std::cerr << "\n";
    std::cerr << "Expected input:\n";
    std::cerr << "  {output_dir}/code.epochscript  EpochScript source code to compile and run\n";
    std::cerr << "  {output_dir}/metadata.json     Test case metadata (name, description, category)\n";
    std::cerr << "\n";
    std::cerr << "Outputs created in output_dir:\n";
    std::cerr << "  graph.json          Compiled graph nodes\n";
    std::cerr << "  {profile}/tables/{timeframe}/{asset}.parquet.gzip  Transform outputs\n";
    std::cerr << "  error.txt           Error message (if compilation/runtime fails)\n";
    return 2;
  }

  const std::string output_dir = argv[1];

  try {
    // Read EpochScript code from code.epochscript file
    std::string epochscript_code = ReadCodeFromFile(output_dir);

    // Read metadata.json to get asset_config
    std::string metadata_path = output_dir + "/metadata.json";
    std::ifstream metadata_file(metadata_path);
    if (!metadata_file.is_open()) {
      throw std::runtime_error("Failed to open metadata.json: " + metadata_path);
    }

    std::string metadata_json_str((std::istreambuf_iterator<char>(metadata_file)),
                                   std::istreambuf_iterator<char>());

    MetadataJson metadata;
    auto parse_error = glz::read_json(metadata, metadata_json_str);
    if (parse_error) {
      throw std::runtime_error("Failed to parse metadata.json: " + glz::format_error(parse_error, metadata_json_str));
    }

    // Initialize runtime
    InitializeRuntime();

    // Run test with output directory and selected asset config (throws on error)
    RunTest(epochscript_code, output_dir, metadata.asset_config);

    // Cleanup runtime
    ShutdownRuntime();

    return 0;

  } catch (const std::exception& e) {
    // Save error to error.txt
    std::string error_path = output_dir + "/error.txt";
    WriteToFile(e.what(), error_path);

    std::cerr << "Test failed: " << e.what() << "\n";
    return 1;
  }
}
