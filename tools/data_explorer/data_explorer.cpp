//
// Data Explorer Tool
// Loads all available DataCategory types and saves them as Arrow files for inspection
//

#include <iostream>
#include <string>
#include <filesystem>
#include <sstream>

#include <absl/log/initialize.h>
#include <google/protobuf/stubs/common.h>
#include <arrow/compute/initialize.h>
#include <epoch_frame/factory/calendar_factory.h>

#include <epoch_data_sdk/model/asset/asset.hpp>
#include <epoch_data_sdk/model/asset/asset_database.hpp>
#include <epoch_data_sdk/model/builder/asset_builder.hpp>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/serialization.h>

#include <epoch_script/transforms/core/registration.h>
#include <epoch_script/strategy/registration.h>
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/strategy/introspection.h>
#include <epoch_script/core/constants.h>

#include "transforms/compiler/ast_compiler.h"
#include <epoch_script/data/factory.h>

namespace fs = std::filesystem;
namespace asset = data_sdk::asset;

struct ExplorerConfig {
    std::string ticker = "AAPL";
    epoch_core::Exchange exchange = epoch_core::Exchange::NASDAQ;
    epoch_core::AssetClass asset_class = epoch_core::AssetClass::Stocks;
    std::string start_date = "2024-01-01";
    std::string end_date = "2024-12-31";
    std::string profile = "default";
    std::string output_dir = ".";
};

const auto DEFAULT_YAML_LOADER = [](std::string const &_path) {
    return YAML::LoadFile(std::filesystem::path{METADATA_FILES_DIR} / _path);
};


void PrintUsage(const char* prog_name) {
    std::cout << "Usage: " << prog_name << " [options]\n"
              << "Options:\n"
              << "  --ticker TICKER         Asset ticker (default: AAPL)\n"
              << "  --start-date YYYY-MM-DD Start date (default: 2024-01-01)\n"
              << "  --end-date YYYY-MM-DD   End date (default: 2024-12-31)\n"
              << "  --output-dir PATH       Output directory (default: current directory)\n"
              << "  --profile PROFILE       Data source profile (default: default)\n"
              << "                          Available profiles:\n"
              << "                            daily    - 1D market data + all economic indicators + corporate actions\n"
              << "                            intraday - 1Min market data + news\n"
              << "                            mixed    - Multi-timeframe (1Min/1D/1W/1ME/1QE) + all data sources\n"
              << "  --help                  Show this help\n";
}

ExplorerConfig ParseArgs(int argc, char* argv[]) {
    ExplorerConfig config;

    for (int i = 1; i < argc; i++) {
        std::string arg = argv[i];

        if (arg == "--help" || arg == "-h") {
            PrintUsage(argv[0]);
            exit(0);
        } else if (arg == "--ticker" && i + 1 < argc) {
            config.ticker = argv[++i];
        } else if (arg == "--start-date" && i + 1 < argc) {
            config.start_date = argv[++i];
        } else if (arg == "--end-date" && i + 1 < argc) {
            config.end_date = argv[++i];
        } else if (arg == "--output-dir" && i + 1 < argc) {
            config.output_dir = argv[++i];
        } else if (arg == "--profile" && i + 1 < argc) {
            config.profile = argv[++i];
        } else {
            std::cerr << "Unknown argument: " << arg << "\n";
            PrintUsage(argv[0]);
            exit(1);
        }
    }

    return config;
}

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
            options.index_label = "index";

            // Write DataFrame as parquet using epoch_frame serialization
            auto status = epoch_frame::write_parquet(dataframe, output_path, options);
            if (!status.ok()) {
                throw std::runtime_error("Failed to write parquet for " + asset.GetID() +
                                         " at " + timeframe_key + ": " + status.ToString());
            }

            std::cout << "✓ Saved " << output_path << " ("
                      << dataframe.num_rows() << " rows, "
                      << dataframe.num_cols() << " columns)\n";
        }
    }
}

int main(int argc, char* argv[]) {
    // Initialize runtime (same as test initialization)
    absl::InitializeLog();
    GOOGLE_PROTOBUF_VERIFY_VERSION;

    // Initialize Arrow compute subsystem
    auto arrowComputeStatus = arrow::compute::Initialize();
    if (!arrowComputeStatus.ok()) {
        std::stringstream errorMsg;
        errorMsg << "arrow compute initialized failed: " << arrowComputeStatus << std::endl;
        throw std::runtime_error(errorMsg.str());
    }

    // Initialize calendar factory
    epoch_frame::calendar::CalendarFactory::instance().Init();

    // Verify asset database initialization
    AssertFromFormat(
        data_sdk::asset::AssetSpecificationDatabase::GetInstance().IsInitialized(),
        "Failed to initialize Asset Specification Database.");

    // Register transforms
    epoch_script::transforms::RegisterTransformMetadata(DEFAULT_YAML_LOADER);
    epoch_script::transform::InitializeTransforms(DEFAULT_YAML_LOADER, {}, {});

    // Set API keys (using test keys for now)
    setenv("POLYGON_API_KEY", "ptMp4LUoa1sgSpTFS7v8diiVtnimqH46", 1);
    setenv("FRED_API_KEY", "b6561c96d3615458fcae0b57580664f3", 1);
    auto cache = std::filesystem::current_path() / "bin" / "cache";
    setenv("EPOCH_DATA_CACHE_DIR", cache.c_str(), 1);

    auto config = ParseArgs(argc, argv);

    std::cout << "\n=== Data Explorer ===\n";
    std::cout << "Ticker: " << config.ticker << "\n";
    std::cout << "Date Range: " << config.start_date << " to " << config.end_date << "\n";
    std::cout << "Profile: " << config.profile << "\n";
    std::cout << "Output Directory: " << config.output_dir << "\n\n";

    // Create output directory if it doesn't exist
    fs::create_directories(config.output_dir);

    try {
        // 1. Create PythonSource with profile-based EpochScript code (skip sink validation)
        std::string epochscript_code;

        if (config.profile == "daily") {
            // Profile 1: 1D only - comprehensive daily data
            epochscript_code = R"(
# Market data at daily timeframe
market_data = market_data_source(timeframe="1D")
vwap_daily = vwap(timeframe="1D")
trades_daily = trade_count(timeframe="1D")

# Index data
spy_index = common_indices(ticker="SPX", timeframe="1D")
vix_index = indices(ticker="VIX", timeframe="1D")

# Fundamental data (quarterly/annual)
balance_sheet_data = balance_sheet(timeframe="1D")
income_stmt_data = income_statement(timeframe="1D")
cash_flow_data = cash_flow(timeframe="1D")
ratios_data = financial_ratios(timeframe="1D")

# Economic indicators (all categories)
cpi = economic_indicator(category="CPI", timeframe="1D")
core_cpi = economic_indicator(category="CoreCPI", timeframe="1D")
pce = economic_indicator(category="PCE", timeframe="1D")
core_pce = economic_indicator(category="CorePCE", timeframe="1D")
fed_funds = economic_indicator(category="FedFunds", timeframe="1D")
treasury_3m = economic_indicator(category="Treasury3M", timeframe="1D")
treasury_2y = economic_indicator(category="Treasury2Y", timeframe="1D")
treasury_5y = economic_indicator(category="Treasury5Y", timeframe="1D")
treasury_10y = economic_indicator(category="Treasury10Y", timeframe="1D")
treasury_30y = economic_indicator(category="Treasury30Y", timeframe="1D")
unemployment = economic_indicator(category="Unemployment", timeframe="1D")
nonfarm_payrolls = economic_indicator(category="NonfarmPayrolls", timeframe="1D")
initial_claims = economic_indicator(category="InitialClaims", timeframe="1D")
gdp = economic_indicator(category="GDP", timeframe="1D")
industrial_production = economic_indicator(category="IndustrialProduction", timeframe="1D")
retail_sales = economic_indicator(category="RetailSales", timeframe="1D")
housing_starts = economic_indicator(category="HousingStarts", timeframe="1D")
consumer_sentiment = economic_indicator(category="ConsumerSentiment", timeframe="1D")
m2 = economic_indicator(category="M2", timeframe="1D")

# Corporate actions and events (with timeframe to satisfy requirement)
divs = dividends(timeframe="1D")
stock_splits = splits(timeframe="1D")
ticker_changes = ticker_events(timeframe="1D")
short_int = short_interest(timeframe="1D")
short_vol = short_volume(timeframe="1D")

# News (intraday timestamps)
news_data = news(timeframe="1D")
sentiment = finbert_sentiment(timeframe="1D")(news_data.description)
)";
        } else if (config.profile == "intraday") {
            // Profile 2: Intraday only - minute-level market data + news
            epochscript_code = R"(
# Market data at 1-minute timeframe
market_data = market_data_source(timeframe="1Min")
vwap_intraday = vwap(timeframe="1Min")
trades_intraday = trade_count(timeframe="1Min")

# Index data
spy_index = common_indices(ticker="SPX", timeframe="1Min")
vix_index = indices(ticker="VIX", timeframe="1Min")
)";
        } else if (config.profile == "mixed") {
            // Profile 3: Mixed timeframes - combination of intraday and lower-frequency data
            epochscript_code = R"(
# Market data at multiple timeframes
market_1min = market_data_source(timeframe="1Min")
vwap_1min = vwap(timeframe="1Min")
trades_1min = trade_count(timeframe="1Min")

market_daily = market_data_source(timeframe="1D")
vwap_daily = vwap(timeframe="1D")
trades_daily = trade_count(timeframe="1D")

market_weekly = market_data_source(timeframe="1W-FRI")
vwap_weekly = vwap(timeframe="1W-FRI")
trades_weekly = trade_count(timeframe="1W-FRI")

market_monthly = market_data_source(timeframe="1ME")
vwap_monthly = vwap(timeframe="1ME")
trades_monthly = trade_count(timeframe="1ME")

market_quarterly = market_data_source(timeframe="1QE")
vwap_quarterly = vwap(timeframe="1QE")
trades_quarterly = trade_count(timeframe="1QE")

# Index data at various timeframes
spy_daily = common_indices(ticker="SPX", timeframe="1D")
vix_daily = indices(ticker="VIX", timeframe="1D")

# Economic indicators at various frequencies
fed_funds_daily = economic_indicator(category="FedFunds", timeframe="1D")
treasury_10y_daily = economic_indicator(category="Treasury10Y", timeframe="1D")
initial_claims_weekly = economic_indicator(category="InitialClaims", timeframe="1W-FRI")
retail_sales_weekly = economic_indicator(category="RetailSales", timeframe="1W-FRI")
cpi_monthly = economic_indicator(category="CPI", timeframe="1ME")
unemployment_monthly = economic_indicator(category="Unemployment", timeframe="1ME")
gdp_quarterly = economic_indicator(category="GDP", timeframe="1QE")

# Corporate actions and events (with timeframe to satisfy requirement)
divs = dividends(timeframe="1D")
stock_splits = splits(timeframe="1D")
ticker_changes = ticker_events(timeframe="1D")
short_int = short_interest(timeframe="1D")
short_vol = short_volume(timeframe="1D")
news_data = news(timeframe="1D")
)";
        } else {
            // Default profile: empty (legacy behavior)
            epochscript_code = "";
        }

        auto compiler = std::make_unique<epoch_script::strategy::PythonSource>(
            epochscript_code,
            true  // skip_sink_validation = true
        );

        // 2. Create asset string for StrategyConfig
        std::string asset_string = config.ticker + "-" +
                                   epoch_core::AssetClassWrapper::ToLongFormString(config.asset_class);

        // 3. Create StrategyConfig from PythonSource
        epoch_script::strategy::StrategyConfig strategyConfig;
        strategyConfig.trade_signal.source = *compiler;
        strategyConfig.data.assets = epoch_script::strategy::AssetIDContainer({asset_string});

        // 4. Determine date range (use config dates)
        auto start_date = epoch_frame::DateTime::from_str(
            config.start_date, "UTC", "%Y-%m-%d").date();
        auto end_date = epoch_frame::DateTime::from_str(
            config.end_date, "UTC", "%Y-%m-%d").date();

        // 5. Create database using strategy-aware factory
        auto dataModuleOption = epoch_script::data::factory::MakeDataModuleOptionFromStrategy(
            epoch_core::CountryCurrency::USD,
            epoch_script::strategy::DatePeriodConfig{start_date, end_date},
            strategyConfig
        );

        auto factory = epoch_script::data::factory::DataModuleFactory(std::move(dataModuleOption));
        auto database = factory.CreateDatabase();

        // 6. Run database pipeline (load + transform data)
        std::cout << "Loading data and running pipeline...\n";
        database->RunPipeline();

        // 7. Get outputs directly from database
        auto db_output_data = database->GetTransformedData();
        auto reports = database->GetGeneratedReports();
        auto event_markers = database->GetGeneratedEventMarkers();

        // 8. Validate that at least one output was generated
        bool has_output = !db_output_data.empty() || !reports.empty() || !event_markers.empty();

        if (!has_output) {
            throw std::runtime_error("Pipeline execution produced no outputs");
        }

        // 9. Save transformed data as parquet files
        SaveTransformedDataAsParquet(config.output_dir, config.profile, db_output_data);

        std::cout << "\n=== Exploration Complete ===\n";
        std::cout << "Parquet files saved to: " << config.output_dir << "/" << config.profile << "/\n";

    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << "\n";
        return 1;
    }

    return 0;
}
