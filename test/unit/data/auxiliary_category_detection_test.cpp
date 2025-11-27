//
// Test for auto-detection of auxiliary data categories from transforms
//
#include "catch.hpp"
#include <epoch_script/data/factory.h>
#include "epoch_script/core/constants.h"
#include <epoch_script/transforms/core/transform_definition.h>
#include <epoch_data_sdk/dataloader/options.hpp>
#include "transforms/components/data_sources/data_category_mapper.h"

using namespace epoch_script::data;
using namespace epoch_script::data::factory;
using namespace epoch_script::transform;
using epoch_script::MetaDataArgDefinitionMapping;
using epoch_script::MetaDataOptionDefinition;

// Helper to create a minimal TransformConfiguration for testing
TransformConfiguration MakeTestTransformConfig(
    std::string const &transformType,
    epoch_core::TransformCategory category,
    MetaDataArgDefinitionMapping options = {}) {

  epoch_script::TransformDefinitionData data{
      .type = transformType,
      .id = transformType + "_test",
      .options = std::move(options),
      .timeframe = epoch_script::TimeFrame{"1d"},
      .inputs = {},
      .metaData = {
          .id = transformType,
          .category = category,
          .plotKind = epoch_core::TransformPlotKind::Null,
          .name = transformType,
          .options = {},
          .isCrossSectional = false,
          .desc = "Test transform",
          .inputs = {},
          .outputs = {},
          .atLeastOneInputRequired = false,
          .tags = {},
          .requiresTimeFrame = false,
          .requiredDataSources = {},
      }};

  return TransformConfiguration(
      epoch_script::TransformDefinition(std::move(data)));
}

TEST_CASE("GetDataCategoryForTransform", "[factory][transforms]") {
  SECTION("Maps balance_sheet to BalanceSheets") {
    auto result = epoch_script::data_sources::GetDataCategoryForTransform(
        epoch_script::polygon::BALANCE_SHEET);
    REQUIRE(result.has_value());
    REQUIRE(result.value() == DataCategory::BalanceSheets);
  }

  SECTION("Maps income_statement to IncomeStatements") {
    auto result = epoch_script::data_sources::GetDataCategoryForTransform(
        epoch_script::polygon::INCOME_STATEMENT);
    REQUIRE(result.has_value());
    REQUIRE(result.value() == DataCategory::IncomeStatements);
  }

  SECTION("Maps cash_flow to CashFlowStatements") {
    auto result = epoch_script::data_sources::GetDataCategoryForTransform(
        epoch_script::polygon::CASH_FLOW);
    REQUIRE(result.has_value());
    REQUIRE(result.value() == DataCategory::CashFlowStatements);
  }

  SECTION("Returns nullopt for non-mapped transforms") {
    auto result = epoch_script::data_sources::GetDataCategoryForTransform("unknown_transform");
    REQUIRE_FALSE(result.has_value());
  }

  SECTION("Maps news transforms") {
    auto result = epoch_script::data_sources::GetDataCategoryForTransform(
        epoch_script::polygon::NEWS);
    REQUIRE(result.has_value());
    REQUIRE(result.value() == DataCategory::News);
  }

  SECTION("Maps dividends transforms") {
    auto result = epoch_script::data_sources::GetDataCategoryForTransform(
        epoch_script::polygon::DIVIDENDS);
    REQUIRE(result.has_value());
    REQUIRE(result.value() == DataCategory::Dividends);
  }
}

// Helper to extract categories from DataRequest vector
std::set<DataCategory> GetCategorySet(
    std::vector<data_sdk::dataloader::DataRequest> const& requests) {
  std::set<DataCategory> result;
  for (auto const& req : requests) {
    result.insert(req.category);
  }
  return result;
}

TEST_CASE("ExtractAuxiliaryCategoriesFromTransforms", "[factory][transforms]") {
  SECTION("Extracts BalanceSheets from balance_sheet transform") {
    std::string code = "balance_sheet_data = balance_sheet(period=\"quarterly\", timeframe=\"1D\")";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 1);
    REQUIRE(requests[0].category == DataCategory::BalanceSheets);
  }

  SECTION("Extracts different financial categories") {
    std::string code = R"(
balance_sheet_data = balance_sheet(period="quarterly", timeframe="1D")
income_stmt_data = income_statement(period="quarterly", timeframe="1D")
cash_flow_data = cash_flow(period="quarterly", timeframe="1D")
)";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 3);
    auto categorySet = GetCategorySet(requests);
    REQUIRE(categorySet.count(DataCategory::BalanceSheets) == 1);
    REQUIRE(categorySet.count(DataCategory::IncomeStatements) == 1);
    REQUIRE(categorySet.count(DataCategory::CashFlowStatements) == 1);
  }

  SECTION("Ignores non-DataSource transforms") {
    std::string code = R"(
prices = market_data_source(timeframe="1D")()
sma_val = sma(period=20, timeframe="1D")(prices.c)
rsi_val = rsi(period=14, timeframe="1D")(prices.c)
)";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.empty());
  }

  SECTION("Mixed transforms - only extracts DataSource categories") {
    std::string code = R"(
prices = market_data_source(timeframe="1D")()
sma_val = sma(period=20, timeframe="1D")(prices.c)
balance_sheet_data = balance_sheet(period="quarterly", timeframe="1D")()
rsi_val = rsi(period=14, timeframe="1D")(prices.c)
income_stmt_data = income_statement(period="quarterly", timeframe="1D")()
)";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 2);
    auto categorySet = GetCategorySet(requests);
    REQUIRE(categorySet.count(DataCategory::BalanceSheets) == 1);
    REQUIRE(categorySet.count(DataCategory::IncomeStatements) == 1);
  }
}

TEST_CASE("ProcessConfigurations auto-detects auxiliary categories",
          "[factory][integration]") {
  SECTION("Auto-populates auxiliary categories from DataSource transforms") {
    // Create a DataModuleOption with only MinuteBars category
    DataModuleOption option{
        .loader = {.startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date(),
                   .endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date(),
                   .requests = {}}};
    option.loader.AddRequest(DataCategory::MinuteBars);

    // Create transform configurations with required period option
    MetaDataArgDefinitionMapping balanceSheetOptions = {
        {"period", MetaDataOptionDefinition{std::string("quarterly")}}
    };
    std::vector<std::unique_ptr<TransformConfiguration>> configs;
    configs.push_back(std::make_unique<TransformConfiguration>(
        MakeTestTransformConfig(epoch_script::polygon::BALANCE_SHEET,
                                epoch_core::TransformCategory::DataSource,
                                balanceSheetOptions)));
    configs.push_back(std::make_unique<TransformConfiguration>(
        MakeTestTransformConfig("sma", epoch_core::TransformCategory::Trend)));

    // Process configurations
    ProcessConfigurations(configs, epoch_script::TimeFrame{"1d"}, option);

    // Verify auxiliary categories were added to requests
    auto categories = option.loader.GetCategories();
    REQUIRE(categories.count(DataCategory::BalanceSheets) == 1);
    REQUIRE(categories.count(DataCategory::MinuteBars) == 1);
    REQUIRE(categories.size() == 2);
  }

  SECTION("Merges auto-detected with existing categories") {
    // Create a DataModuleOption with manually specified categories
    DataModuleOption option{
        .loader = {.startDate = epoch_frame::DateTime::from_date_str("2024-01-01").date(),
                   .endDate = epoch_frame::DateTime::from_date_str("2024-12-31").date(),
                   .requests = {}}};
    option.loader.AddRequest(DataCategory::MinuteBars);
    option.loader.AddRequest(DataCategory::News);

    // Create transform configurations with required period option
    MetaDataArgDefinitionMapping balanceSheetOptions = {
        {"period", MetaDataOptionDefinition{std::string("quarterly")}}
    };
    std::vector<std::unique_ptr<TransformConfiguration>> configs;
    configs.push_back(std::make_unique<TransformConfiguration>(
        MakeTestTransformConfig(epoch_script::polygon::BALANCE_SHEET,
                                epoch_core::TransformCategory::DataSource,
                                balanceSheetOptions)));

    // Process configurations
    ProcessConfigurations(configs, epoch_script::TimeFrame{"1d"}, option);

    // Verify all categories are present
    auto categories = option.loader.GetCategories();
    REQUIRE(categories.count(DataCategory::MinuteBars) == 1);
    REQUIRE(categories.count(DataCategory::News) == 1);
    REQUIRE(categories.count(DataCategory::BalanceSheets) == 1);
    REQUIRE(categories.size() == 3);
  }
}

TEST_CASE("Mixed data source categories", "[factory][integration]") {
  SECTION("Multiple different data source categories are detected") {
    std::string code = R"(
balance_sheet_data = balance_sheet(period="quarterly", timeframe="1D")
news_data = news(timeframe="1D")
divs = dividends(timeframe="1D")
)";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 3);

    auto categorySet = GetCategorySet(requests);
    REQUIRE(categorySet.count(DataCategory::BalanceSheets) == 1);
    REQUIRE(categorySet.count(DataCategory::News) == 1);
    REQUIRE(categorySet.count(DataCategory::Dividends) == 1);
  }
}

TEST_CASE("ExtractAuxiliaryCategoriesFromTransforms builds correct kwargs", "[factory][kwargs]") {
  using namespace data_sdk::dataloader;

  SECTION("BalanceSheets with quarterly period") {
    std::string code = R"(bs = balance_sheet(period="quarterly", timeframe="1D"))";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 1);
    REQUIRE(requests[0].category == DataCategory::BalanceSheets);
    REQUIRE(std::holds_alternative<BalanceSheetsKwargs>(requests[0].kwargs));
    auto& kwargs = std::get<BalanceSheetsKwargs>(requests[0].kwargs);
    REQUIRE(kwargs.timeframe == data_sdk::BalanceSheetTimeframe::quarterly);
  }

  SECTION("BalanceSheets with annual period") {
    std::string code = R"(bs = balance_sheet(period="annual", timeframe="1D"))";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 1);
    REQUIRE(requests[0].category == DataCategory::BalanceSheets);
    auto& kwargs = std::get<BalanceSheetsKwargs>(requests[0].kwargs);
    REQUIRE(kwargs.timeframe == data_sdk::BalanceSheetTimeframe::annual);
  }

  SECTION("IncomeStatements with TTM period") {
    std::string code = R"(inc = income_statement(period="trailing_twelve_months", timeframe="1D"))";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 1);
    REQUIRE(requests[0].category == DataCategory::IncomeStatements);
    REQUIRE(std::holds_alternative<FinancialsKwargs>(requests[0].kwargs));
    auto& kwargs = std::get<FinancialsKwargs>(requests[0].kwargs);
    REQUIRE(kwargs.timeframe == data_sdk::FinancialsTimeframe::trailing_twelve_months);
  }

  SECTION("CashFlow with quarterly period") {
    std::string code = R"(cf = cash_flow(period="quarterly", timeframe="1D"))";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 1);
    REQUIRE(requests[0].category == DataCategory::CashFlowStatements);
    REQUIRE(std::holds_alternative<FinancialsKwargs>(requests[0].kwargs));
    auto& kwargs = std::get<FinancialsKwargs>(requests[0].kwargs);
    REQUIRE(kwargs.timeframe == data_sdk::FinancialsTimeframe::quarterly);
  }

  SECTION("Dividends with dividend_type filter") {
    std::string code = R"(divs = dividends(dividend_type="CD", timeframe="1D"))";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 1);
    REQUIRE(requests[0].category == DataCategory::Dividends);
    REQUIRE(std::holds_alternative<DividendsKwargs>(requests[0].kwargs));
    auto& kwargs = std::get<DividendsKwargs>(requests[0].kwargs);
    REQUIRE(kwargs.dividend_type.has_value());
    REQUIRE(kwargs.dividend_type.value() == data_sdk::DividendType::CD);
  }

  SECTION("Dividends without dividend_type filter (all types)") {
    std::string code = R"(divs = dividends(timeframe="1D"))";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 1);
    REQUIRE(requests[0].category == DataCategory::Dividends);
    REQUIRE(std::holds_alternative<DividendsKwargs>(requests[0].kwargs));
    auto& kwargs = std::get<DividendsKwargs>(requests[0].kwargs);
    REQUIRE_FALSE(kwargs.dividend_type.has_value());  // No filter = all types
  }

  SECTION("Mixed financial statements with different periods") {
    std::string code = R"(
bs = balance_sheet(period="annual", timeframe="1D")
inc = income_statement(period="trailing_twelve_months", timeframe="1D")
cf = cash_flow(period="quarterly", timeframe="1D")
)";
    auto source = epoch_script::strategy::PythonSource(code, true);
    auto manager = epoch_script::runtime::CreateTransformManager(source);

    auto requests = ExtractAuxiliaryCategoriesFromTransforms(*manager->GetTransforms());

    REQUIRE(requests.size() == 3);

    // Find each request and verify kwargs
    for (auto const& req : requests) {
      if (req.category == DataCategory::BalanceSheets) {
        auto& kwargs = std::get<BalanceSheetsKwargs>(req.kwargs);
        REQUIRE(kwargs.timeframe == data_sdk::BalanceSheetTimeframe::annual);
      } else if (req.category == DataCategory::IncomeStatements) {
        auto& kwargs = std::get<FinancialsKwargs>(req.kwargs);
        REQUIRE(kwargs.timeframe == data_sdk::FinancialsTimeframe::trailing_twelve_months);
      } else if (req.category == DataCategory::CashFlowStatements) {
        auto& kwargs = std::get<FinancialsKwargs>(req.kwargs);
        REQUIRE(kwargs.timeframe == data_sdk::FinancialsTimeframe::quarterly);
      }
    }
  }
}
