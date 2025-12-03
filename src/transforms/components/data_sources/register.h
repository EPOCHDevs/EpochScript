#pragma once
// Data source transforms registration
// Provides access to external market and fundamental data feeds
//
// Categories:
// 1. Fundamentals (Polygon) - SEC filings and financial statements
//    - Balance Sheet, Income Statement, Cash Flow, Financial Ratios
// 2. Corporate Actions - Corporate events affecting share prices
//    - Dividends, Splits, Ticker Events
// 3. Reference Data - Cross-asset data feeds
//    - Indices, Stocks, FX Pairs, Crypto Pairs
// 4. Alternative Data - Non-traditional data sources
//    - News, Short Interest, Short Volume
// 5. Macroeconomic - Economic indicators and reference rates
//    - FRED (Federal Reserve Economic Data)
// 6. Regulatory - SEC filings and insider data
//    - SEC Form 4, Institutional Holdings

#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_registry.h>

// Data source implementations
#include "parametric_data_source.h"
#include "sec_data_source.h"

// Metadata definitions
#include "polygon_metadata.h"
#include "dividends_metadata.h"
#include "splits_metadata.h"
#include "news_metadata.h"
#include "ticker_events_metadata.h"
#include "short_interest_metadata.h"
#include "short_volume_metadata.h"
#include "fred_metadata.h"
#include "sec_metadata.h"
#include "reference_indices_metadata.h"
#include "reference_stocks_metadata.h"
#include "reference_fx_metadata.h"
#include "reference_crypto_metadata.h"

namespace epoch_script::transform::data_sources {

using namespace epoch_script::transform;
using TransformsMetaData = epoch_script::transforms::TransformsMetaData;

// =============================================================================
// Registration function - registers all data source transforms and metadata
// =============================================================================

inline void Register() {
    auto& metaRegistry = epoch_script::transforms::ITransformRegistry::GetInstance();

    // =========================================================================
    // FUNDAMENTAL DATA (Polygon - SEC Filings)
    // =========================================================================
    // Financial statements from company SEC filings (10-K, 10-Q)
    // Data has reporting lag - filings are published weeks after period end

    // balance_sheet: Assets, liabilities, equity metrics
    // Options: period (quarterly/annual)
    epoch_script::transform::Register<PolygonBalanceSheetTransform>("balance_sheet");

    // income_statement: Revenue, expenses, earnings metrics
    // Options: period (quarterly/annual/ttm)
    epoch_script::transform::Register<PolygonIncomeStatementTransform>("income_statement");

    // cash_flow: Operating, investing, financing cash flows
    // Options: period (quarterly/annual/ttm)
    epoch_script::transform::Register<PolygonCashFlowTransform>("cash_flow");

    // financial_ratios: Pre-calculated profitability, leverage, efficiency ratios
    // Options: period (quarterly/annual/ttm)
    epoch_script::transform::Register<PolygonFinancialRatiosTransform>("financial_ratios");

    // =========================================================================
    // CORPORATE ACTIONS
    // =========================================================================
    // Events that affect share prices or ownership structure

    // dividends: Dividend payments with ex-date, record date, pay date
    // Use for: dividend capture strategies, yield analysis
    epoch_script::transform::Register<PolygonDividendsTransform>("dividends");

    // splits: Stock splits and reverse splits
    // Use for: adjusting historical prices, detecting corporate events
    epoch_script::transform::Register<PolygonSplitsTransform>("splits");

    // ticker_events: Corporate calendar events (earnings, conferences)
    // Use for: event-driven strategies, earnings plays
    epoch_script::transform::Register<PolygonTickerEventsTransform>("ticker_events");

    // =========================================================================
    // ALTERNATIVE DATA
    // =========================================================================
    // Non-traditional data sources for alpha generation

    // news: Financial news with sentiment and relevance
    // Use for: sentiment strategies, news-based signals
    epoch_script::transform::Register<PolygonNewsTransform>("news");

    // short_interest: Short interest and days to cover
    // Use for: short squeeze detection, sentiment analysis
    epoch_script::transform::Register<PolygonShortInterestTransform>("short_interest");

    // short_volume: Daily short volume vs total volume
    // Use for: intraday sentiment, short activity monitoring
    epoch_script::transform::Register<PolygonShortVolumeTransform>("short_volume");

    // =========================================================================
    // REFERENCE DATA - Cross-Asset
    // =========================================================================
    // Access to other asset classes for correlation and macro analysis

    // common_indices / indices: Major market indices (SPY, QQQ, VIX, etc.)
    // Use for: market regime, beta hedging, correlation analysis
    epoch_script::transform::Register<ParametricDataSourceTransform>("common_indices");
    epoch_script::transform::Register<ParametricDataSourceTransform>("indices");

    // common_reference_stocks / reference_stocks: Additional stock tickers
    // Use for: sector analysis, peer comparison, pairs trading
    epoch_script::transform::Register<ParametricDataSourceTransform>("common_reference_stocks");
    epoch_script::transform::Register<ParametricDataSourceTransform>("reference_stocks");

    // common_fx_pairs / fx_pairs: Currency pairs (EUR/USD, etc.)
    // Use for: FX exposure, carry trade, currency correlation
    epoch_script::transform::Register<ParametricDataSourceTransform>("common_fx_pairs");
    epoch_script::transform::Register<ParametricDataSourceTransform>("fx_pairs");

    // common_crypto_pairs / crypto_pairs: Cryptocurrency pairs
    // Use for: crypto correlation, BTC/ETH as macro indicators
    epoch_script::transform::Register<ParametricDataSourceTransform>("common_crypto_pairs");
    epoch_script::transform::Register<ParametricDataSourceTransform>("crypto_pairs");

    // =========================================================================
    // MACROECONOMIC DATA (FRED)
    // =========================================================================
    // Federal Reserve Economic Data - economic indicators

    // economic_indicator: FRED time series (GDP, CPI, unemployment, etc.)
    // Use for: macro regime, economic cycle analysis
    // Note: Registration handled separately via MakeFREDDataSource()

    // =========================================================================
    // REGULATORY DATA (SEC)
    // =========================================================================
    // SEC filings and insider trading data

    // sec_insider_trading: Form 4 insider buys/sells
    // Use for: insider sentiment, smart money signals
    // Note: Registration handled separately via MakeSECDataSources()

    // =========================================================================
    // METADATA REGISTRATION
    // =========================================================================

    // Polygon fundamental data sources
    for (const auto& metadata : MakePolygonDataSources()) {
        metaRegistry.Register(metadata);
    }

    // Polygon indices reference data
    for (const auto& metadata : MakePolygonIndicesDataSources()) {
        metaRegistry.Register(metadata);
    }

    // FRED economic data
    for (const auto& metadata : MakeFREDDataSource()) {
        metaRegistry.Register(metadata);
    }

    // SEC regulatory data
    for (const auto& metadata : MakeSECDataSources()) {
        metaRegistry.Register(metadata);
    }

    // Reference data sources
    for (const auto& metadata : MakeReferenceStocksDataSources()) {
        metaRegistry.Register(metadata);
    }
}

}  // namespace epoch_script::transform::data_sources
