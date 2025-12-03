/**
 * @file fake_data_sources.h
 * @brief Shared fake data generators for orchestrator tests
 *
 * Provides factory functions to create realistic fake DataFrames for each
 * data source type supported by the system. Use these for unit testing
 * the orchestrator's timeseries execution path.
 *
 * All timestamps are in nanoseconds UTC.
 */

#pragma once

#include <epoch_frame/dataframe.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/array_factory.h>
#include <epoch_script/core/bar_attribute.h>
#include <arrow/type.h>
#include <arrow/builder.h>
#include <chrono>
#include <vector>
#include <string>
#include <cstdint>

namespace epoch_script::runtime::test {

using namespace std::chrono_literals;
using namespace epoch_frame;

// Base timestamp: 2020-01-01 00:00:00 UTC in nanoseconds
constexpr int64_t BASE_TIMESTAMP_NS = 1577836800000000000LL;  // 2020-01-01T00:00:00Z
constexpr int64_t DAY_NS = 86400000000000LL;  // nanoseconds in a day

/**
 * @brief Create nanosecond UTC timestamps starting from 2020-01-01
 * @param numDays Number of days
 * @param dayMultiplier Multiplier for day spacing (1 = daily, 7 = weekly, 90 = quarterly)
 */
inline std::vector<int64_t> CreateTimestampsNS(int numDays, int dayMultiplier = 1) {
    std::vector<int64_t> timestamps;
    timestamps.reserve(numDays);
    for (int i = 0; i < numDays; ++i) {
        timestamps.push_back(BASE_TIMESTAMP_NS + (i * dayMultiplier * DAY_NS));
    }
    return timestamps;
}

/**
 * @brief Create OHLCV market data (o, h, l, c, v columns)
 * Used by: market_data_source
 * @param closeValues Vector of close prices to derive OHLCV from
 *
 * Alternates between bullish (open < close) and bearish (open > close) candles
 * to ensure ML classifiers have both classes when using c >= o as labels.
 */
inline DataFrame CreateOHLCVData(const std::vector<double>& closeValues) {
    auto& C = EpochStratifyXConstants::instance();
    auto timestamps = CreateTimestampsNS(static_cast<int>(closeValues.size()));
    std::vector<double> opens, highs, lows, closes, volumes;

    for (size_t i = 0; i < closeValues.size(); ++i) {
        double close = closeValues[i];
        // Alternate between bullish (open < close) and bearish (open > close)
        // to ensure binary classification tests have both classes
        if (i % 3 == 0) {
            // Bearish candle: open > close
            opens.push_back(close * 1.01);
        } else {
            // Bullish candle: open < close
            opens.push_back(close * 0.99);
        }
        highs.push_back(close * 1.02);
        lows.push_back(close * 0.97);
        closes.push_back(close);
        volumes.push_back(1000000.0 + (i * 10000.0));
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    return make_dataframe<double>(index,
        {opens, highs, lows, closes, volumes},
        {C.OPEN(), C.HIGH(), C.LOW(), C.CLOSE(), C.VOLUME()});
}

/**
 * @brief Helper to create timestamp array from int64 nanoseconds
 */
inline arrow::ChunkedArrayPtr MakeTimestampArray(const std::vector<int64_t>& timestamps) {
    auto type = arrow::timestamp(arrow::TimeUnit::NANO, "UTC");
    arrow::TimestampBuilder builder(type, arrow::default_memory_pool());
    for (auto ts : timestamps) {
        auto status = builder.Append(ts);
        if (!status.ok()) throw std::runtime_error("Failed to append timestamp");
    }
    auto result = builder.Finish();
    if (!result.ok()) throw std::runtime_error("Failed to finish timestamp array");
    return std::make_shared<arrow::ChunkedArray>(result.MoveValueUnsafe());
}

/**
 * @brief Create dividend data with ALL required SDK columns
 * Used by: dividends data source (SDK prefix: "D:")
 * Required columns from SDK: ticker, id, cash_amount, currency, declaration_date,
 *                           record_date, pay_date, frequency, dividend_type
 * @param numRows Number of dividend events
 * @param baseCashAmount Base dividend amount per share
 */
inline DataFrame CreateDividendData(int numRows, double baseCashAmount = 0.25) {
    auto timestamps = CreateTimestampsNS(numRows, 90);  // Quarterly spacing (ex_dividend_date)

    // Build all columns required by SDK metadata
    std::vector<std::string> tickers, ids, currencies, dividendTypes;
    std::vector<double> cashAmounts;
    std::vector<int64_t> frequencies;
    std::vector<int64_t> declDates, recordDates, payDates;  // Timestamp columns as int64 nanoseconds

    for (int i = 0; i < numRows; ++i) {
        tickers.push_back("AAPL");
        ids.push_back("DIV_" + std::to_string(i));
        cashAmounts.push_back(baseCashAmount + (i * 0.01));
        currencies.push_back("USD");
        // Declaration date: 30 days before ex-dividend date
        declDates.push_back(timestamps[i] - (30 * DAY_NS));
        // Record date: 1 day before ex-dividend date
        recordDates.push_back(timestamps[i] - DAY_NS);
        // Pay date: 14 days after ex-dividend date
        payDates.push_back(timestamps[i] + (14 * DAY_NS));
        frequencies.push_back(4);  // Quarterly
        dividendTypes.push_back("CD");  // Consistent dividend
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");

    // Build mixed-type dataframe using Arrow arrays
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(tickers),
        factory::array::make_array(ids),
        factory::array::make_array(cashAmounts),
        factory::array::make_array(currencies),
        MakeTimestampArray(declDates),
        MakeTimestampArray(recordDates),
        MakeTimestampArray(payDates),
        factory::array::make_array(frequencies),
        factory::array::make_array(dividendTypes),
    };

    return make_dataframe(index, arrays,
        {"D:ticker", "D:id", "D:cash_amount", "D:currency", "D:declaration_date",
         "D:record_date", "D:pay_date", "D:frequency", "D:dividend_type"});
}

/**
 * @brief Create short volume data with ALL required SDK columns
 * Used by: short_volume data source (SDK prefix: "SV:")
 * Required: ticker, short_volume, total_volume, short_volume_ratio, exempt_volume, non_exempt_volume
 * @param numRows Number of trading days
 * @param baseVolume Base volume level
 */
inline DataFrame CreateShortVolumeData(int numRows, int64_t baseVolume = 1000000) {
    auto timestamps = CreateTimestampsNS(numRows);
    std::vector<std::string> tickers;
    std::vector<int32_t> shortVolumes, totalVolumes, exemptVolumes, nonExemptVolumes;
    std::vector<double> shortVolumeRatios;

    for (int i = 0; i < numRows; ++i) {
        tickers.push_back("AAPL");
        int32_t total = static_cast<int32_t>(baseVolume + (i * 10000));
        int32_t shortVol = static_cast<int32_t>(total * 0.3);  // ~30% short volume
        int32_t exemptVol = static_cast<int32_t>(shortVol * 0.1);  // ~10% of short is exempt
        shortVolumes.push_back(shortVol);
        totalVolumes.push_back(total);
        shortVolumeRatios.push_back(static_cast<double>(shortVol) / total * 100.0);
        exemptVolumes.push_back(exemptVol);
        nonExemptVolumes.push_back(shortVol - exemptVol);
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(tickers),
        factory::array::make_array(shortVolumes),
        factory::array::make_array(totalVolumes),
        factory::array::make_array(shortVolumeRatios),
        factory::array::make_array(exemptVolumes),
        factory::array::make_array(nonExemptVolumes),
    };
    return make_dataframe(index, arrays,
        {"SV:ticker", "SV:short_volume", "SV:total_volume", "SV:short_volume_ratio",
         "SV:exempt_volume", "SV:non_exempt_volume"});
}

/**
 * @brief Create short interest data with ALL required SDK columns
 * Used by: short_interest data source (SDK prefix: "SI:")
 * Required: ticker, short_interest, avg_daily_volume, days_to_cover
 * @param numRows Number of reporting periods (bi-weekly)
 * @param baseInterest Base short interest level
 */
inline DataFrame CreateShortInterestData(int numRows, int64_t baseInterest = 5000000) {
    auto timestamps = CreateTimestampsNS(numRows, 14);  // Bi-weekly spacing
    std::vector<std::string> tickers;
    std::vector<int32_t> shortInterests, avgDailyVolumes;
    std::vector<double> daysToCover;

    for (int i = 0; i < numRows; ++i) {
        tickers.push_back("AAPL");
        int32_t si = static_cast<int32_t>(baseInterest + (i * 100000));
        int32_t adv = 1000000;  // 1M avg daily volume
        shortInterests.push_back(si);
        avgDailyVolumes.push_back(adv);
        daysToCover.push_back(static_cast<double>(si) / adv);  // Days to cover
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(tickers),
        factory::array::make_array(shortInterests),
        factory::array::make_array(avgDailyVolumes),
        factory::array::make_array(daysToCover),
    };
    return make_dataframe(index, arrays,
        {"SI:ticker", "SI:short_interest", "SI:avg_daily_volume", "SI:days_to_cover"});
}

/**
 * @brief Create news data with all required SDK columns (strings)
 * Used by: news data source (SDK prefix: "N:")
 * Required: id, title, author, description, article_url, amp_url, image_url, tickers, keywords, etc.
 * @param numRows Number of news articles
 */
inline DataFrame CreateNewsData(int numRows) {
    auto timestamps = CreateTimestampsNS(numRows);
    std::vector<std::string> ids, titles, authors, descriptions, articleUrls, ampUrls, imageUrls;
    std::vector<std::string> tickers, keywords, publisherNames, publisherHomepages;

    for (int i = 0; i < numRows; ++i) {
        ids.push_back("NEWS_" + std::to_string(i));
        titles.push_back("Test news article " + std::to_string(i));
        authors.push_back("Author " + std::to_string(i));
        descriptions.push_back("Description of news article " + std::to_string(i));
        articleUrls.push_back("https://example.com/article/" + std::to_string(i));
        ampUrls.push_back("https://example.com/amp/" + std::to_string(i));
        imageUrls.push_back("https://example.com/img/" + std::to_string(i) + ".jpg");
        tickers.push_back("AAPL");
        keywords.push_back("earnings,finance");
        publisherNames.push_back("Test Publisher");
        publisherHomepages.push_back("https://example.com");
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(ids),
        factory::array::make_array(titles),
        factory::array::make_array(authors),
        factory::array::make_array(descriptions),
        factory::array::make_array(articleUrls),
        factory::array::make_array(ampUrls),
        factory::array::make_array(imageUrls),
        factory::array::make_array(tickers),
        factory::array::make_array(keywords),
        factory::array::make_array(publisherNames),
        factory::array::make_array(publisherHomepages),
    };
    return make_dataframe(index, arrays,
        {"N:id", "N:title", "N:author", "N:description", "N:article_url", "N:amp_url",
         "N:image_url", "N:tickers", "N:keywords", "N:publisher_name", "N:publisher_homepage"});
}

/**
 * @brief Create splits data with ALL required SDK columns
 * Used by: splits data source (SDK prefix: "S:")
 * Required: ticker, id, split_from, split_to, split_ratio
 * @param numRows Number of split events
 */
inline DataFrame CreateSplitsData(int numRows) {
    auto timestamps = CreateTimestampsNS(numRows, 365);  // Yearly spacing
    std::vector<std::string> tickers, ids;
    std::vector<double> splitFroms, splitTos, splitRatios;

    for (int i = 0; i < numRows; ++i) {
        tickers.push_back("AAPL");
        ids.push_back("SPLIT_" + std::to_string(i));
        double splitFrom = 1.0;
        double splitTo = static_cast<double>(2 + i);  // 2:1, 3:1, 4:1 splits
        splitFroms.push_back(splitFrom);
        splitTos.push_back(splitTo);
        splitRatios.push_back(splitTo / splitFrom);  // Ratio = split_to / split_from
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(tickers),
        factory::array::make_array(ids),
        factory::array::make_array(splitFroms),
        factory::array::make_array(splitTos),
        factory::array::make_array(splitRatios),
    };
    return make_dataframe(index, arrays,
        {"S:ticker", "S:id", "S:split_from", "S:split_to", "S:split_ratio"});
}

/**
 * @brief Create balance sheet data with ALL required SDK columns
 * Used by: balance_sheet data source (SDK prefix: "BS:")
 * SDK columns (in order): ticker, filing_date, period_end, fiscal_year, fiscal_quarter, timeframe,
 *                        accounts_payable, accrued_liabilities, aoci, cash, debt_current,
 *                        deferred_revenue, goodwill, intangible_assets_net, inventories,
 *                        lt_debt, ppe_net, receivables, retained_earnings
 * @param numRows Number of quarterly reports
 */
inline DataFrame CreateBalanceSheetData(int numRows, double baseCash = 10000000.0) {
    auto timestamps = CreateTimestampsNS(numRows, 90);  // Quarterly spacing
    std::vector<std::string> tickers, timeframes;
    std::vector<int64_t> filingDates, periodEnds;
    std::vector<int32_t> fiscalYears, fiscalQuarters;
    // All financial columns in SDK metadata order
    std::vector<double> accountsPayable, accruedLiabilities, aoci, cash, debtCurrent;
    std::vector<double> deferredRevenue, goodwill, intangibles, inventories;
    std::vector<double> ltDebt, ppeNet, receivables, retainedEarnings;

    for (int i = 0; i < numRows; ++i) {
        tickers.push_back("AAPL");
        timeframes.push_back("quarterly");
        filingDates.push_back(timestamps[i] + (30 * DAY_NS));  // Filing 30 days after period end
        periodEnds.push_back(timestamps[i]);
        fiscalYears.push_back(2020 + (i / 4));
        fiscalQuarters.push_back((i % 4) + 1);
        // Financial columns in SDK order
        accountsPayable.push_back(baseCash * 0.08);
        accruedLiabilities.push_back(baseCash * 0.03);  // ~3% of cash
        aoci.push_back(baseCash * -0.01);  // AOCI can be negative
        cash.push_back(baseCash * (1.0 + i * 0.05));
        debtCurrent.push_back(baseCash * 0.05);
        deferredRevenue.push_back(baseCash * 0.02);
        goodwill.push_back(baseCash * 0.2);
        intangibles.push_back(baseCash * 0.1);
        inventories.push_back(baseCash * 0.05);
        ltDebt.push_back(baseCash * 0.5 * (1.0 - i * 0.02));
        ppeNet.push_back(baseCash * 2.0);
        receivables.push_back(baseCash * 0.1);
        retainedEarnings.push_back(baseCash * 3.0);
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    // Arrays in SDK metadata column order
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(tickers),
        MakeTimestampArray(filingDates),
        MakeTimestampArray(periodEnds),
        factory::array::make_array(fiscalYears),
        factory::array::make_array(fiscalQuarters),
        factory::array::make_array(timeframes),
        factory::array::make_array(accountsPayable),
        factory::array::make_array(accruedLiabilities),
        factory::array::make_array(aoci),
        factory::array::make_array(cash),
        factory::array::make_array(debtCurrent),
        factory::array::make_array(deferredRevenue),
        factory::array::make_array(goodwill),
        factory::array::make_array(intangibles),
        factory::array::make_array(inventories),
        factory::array::make_array(ltDebt),
        factory::array::make_array(ppeNet),
        factory::array::make_array(receivables),
        factory::array::make_array(retainedEarnings),
    };
    // Column names in SDK metadata order
    return make_dataframe(index, arrays,
        {"BS:ticker", "BS:filing_date", "BS:period_end", "BS:fiscal_year", "BS:fiscal_quarter",
         "BS:timeframe", "BS:accounts_payable", "BS:accrued_liabilities", "BS:aoci", "BS:cash",
         "BS:debt_current", "BS:deferred_revenue", "BS:goodwill", "BS:intangible_assets_net",
         "BS:inventories", "BS:lt_debt", "BS:ppe_net", "BS:receivables", "BS:retained_earnings"});
}

/**
 * @brief Create income statement data with ALL required SDK columns
 * Used by: income_statement data source (SDK prefix: "IS:")
 * SDK columns (in order): ticker, filing_date, period_end, fiscal_year, fiscal_quarter, timeframe,
 *                        basic_eps, diluted_eps, revenue, cogs, gross_profit, operating_income,
 *                        net_income, rd, sga
 * @param numRows Number of quarterly reports
 */
inline DataFrame CreateIncomeStatementData(int numRows, double baseRevenue = 50000000.0) {
    auto timestamps = CreateTimestampsNS(numRows, 90);  // Quarterly spacing
    std::vector<std::string> tickers, timeframes;
    std::vector<int64_t> filingDates, periodEnds;
    std::vector<int32_t> fiscalYears, fiscalQuarters;
    // Financial columns in SDK order
    std::vector<double> basicEps, dilutedEps, revenues, cogs, grossProfits;
    std::vector<double> operatingIncomes, netIncomes, rd, sga;

    for (int i = 0; i < numRows; ++i) {
        tickers.push_back("AAPL");
        timeframes.push_back("quarterly");
        filingDates.push_back(timestamps[i] + (30 * DAY_NS));
        periodEnds.push_back(timestamps[i]);
        fiscalYears.push_back(2020 + (i / 4));
        fiscalQuarters.push_back((i % 4) + 1);
        double revenue = baseRevenue * (1.0 + i * 0.1);
        // Financial columns in SDK order
        basicEps.push_back(revenue * 0.15 / 100000000.0);
        dilutedEps.push_back(revenue * 0.15 / 100000000.0);
        revenues.push_back(revenue);
        cogs.push_back(revenue * 0.60);           // 60% COGS
        grossProfits.push_back(revenue * 0.40);   // 40% gross margin
        operatingIncomes.push_back(revenue * 0.25);
        netIncomes.push_back(revenue * 0.15);     // 15% net margin
        rd.push_back(revenue * 0.08);             // 8% R&D spend
        sga.push_back(revenue * 0.07);            // 7% SG&A spend
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    // Arrays in SDK metadata column order
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(tickers),
        MakeTimestampArray(filingDates),
        MakeTimestampArray(periodEnds),
        factory::array::make_array(fiscalYears),
        factory::array::make_array(fiscalQuarters),
        factory::array::make_array(timeframes),
        factory::array::make_array(basicEps),
        factory::array::make_array(dilutedEps),
        factory::array::make_array(revenues),
        factory::array::make_array(cogs),
        factory::array::make_array(grossProfits),
        factory::array::make_array(operatingIncomes),
        factory::array::make_array(netIncomes),
        factory::array::make_array(rd),
        factory::array::make_array(sga),
    };
    // Column names in SDK metadata order
    return make_dataframe(index, arrays,
        {"IS:ticker", "IS:filing_date", "IS:period_end", "IS:fiscal_year", "IS:fiscal_quarter",
         "IS:timeframe", "IS:basic_eps", "IS:diluted_eps", "IS:revenue", "IS:cogs",
         "IS:gross_profit", "IS:operating_income", "IS:net_income", "IS:rd", "IS:sga"});
}

/**
 * @brief Create cash flow data with ALL required SDK columns
 * Used by: cash_flow data source (SDK prefix: "CF:")
 * SDK columns (in order): ticker, filing_date, period_end, fiscal_year, fiscal_quarter, timeframe,
 *                        cfo, change_cash, change_assets, dda, dividends, lt_debt_issuances,
 *                        ncf_financing, ncf_investing, ncf_operating, net_income, capex
 * @param numRows Number of quarterly reports
 */
inline DataFrame CreateCashFlowData(int numRows, double baseCFO = 20000000.0) {
    auto timestamps = CreateTimestampsNS(numRows, 90);  // Quarterly spacing
    std::vector<std::string> tickers, timeframes;
    std::vector<int64_t> filingDates, periodEnds;
    std::vector<int32_t> fiscalYears, fiscalQuarters;
    // Financial columns in SDK order
    std::vector<double> cfos, changeCash, changeAssets, dda, dividends;
    std::vector<double> ltDebtIssuances, ncfFinancing, ncfInvesting, ncfOperating, netIncomes, capex;

    for (int i = 0; i < numRows; ++i) {
        tickers.push_back("AAPL");
        timeframes.push_back("quarterly");
        filingDates.push_back(timestamps[i] + (30 * DAY_NS));
        periodEnds.push_back(timestamps[i]);
        fiscalYears.push_back(2020 + (i / 4));
        fiscalQuarters.push_back((i % 4) + 1);
        double cfo = baseCFO * (1.0 + i * 0.05);
        // Financial columns in SDK order
        cfos.push_back(cfo);
        changeCash.push_back(cfo * 0.5);
        changeAssets.push_back(cfo * 0.1);
        dda.push_back(cfo * 0.2);
        dividends.push_back(-cfo * 0.1);
        ltDebtIssuances.push_back(cfo * 0.05);
        ncfFinancing.push_back(-cfo * 0.15);
        ncfInvesting.push_back(-cfo * 0.25);       // Investing activities
        ncfOperating.push_back(cfo * 0.95);        // Operating activities
        netIncomes.push_back(cfo * 0.8);           // Net income
        capex.push_back(-cfo * 0.3);
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    // Arrays in SDK metadata column order
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(tickers),
        MakeTimestampArray(filingDates),
        MakeTimestampArray(periodEnds),
        factory::array::make_array(fiscalYears),
        factory::array::make_array(fiscalQuarters),
        factory::array::make_array(timeframes),
        factory::array::make_array(cfos),
        factory::array::make_array(changeCash),
        factory::array::make_array(changeAssets),
        factory::array::make_array(dda),
        factory::array::make_array(dividends),
        factory::array::make_array(ltDebtIssuances),
        factory::array::make_array(ncfFinancing),
        factory::array::make_array(ncfInvesting),
        factory::array::make_array(ncfOperating),
        factory::array::make_array(netIncomes),
        factory::array::make_array(capex),
    };
    // Column names in SDK metadata order
    return make_dataframe(index, arrays,
        {"CF:ticker", "CF:filing_date", "CF:period_end", "CF:fiscal_year", "CF:fiscal_quarter",
         "CF:timeframe", "CF:cfo", "CF:change_cash", "CF:change_assets", "CF:dda",
         "CF:dividends", "CF:lt_debt_issuances", "CF:ncf_financing", "CF:ncf_investing",
         "CF:ncf_operating", "CF:net_income", "CF:capex"});
}

/**
 * @brief Create financial ratios data with ALL required SDK columns
 * Used by: financial_ratios data source (SDK prefix: "R:")
 * Required SDK columns: ticker, average_volume, cash, current, debt_to_equity, dividend_yield,
 *                       earnings_per_share, enterprise_value, ev_to_ebitda, ev_to_sales,
 *                       free_cash_flow, market_cap, price, price_to_book, price_to_cash_flow,
 *                       price_to_earnings, price_to_free_cash_flow, price_to_sales, quick,
 *                       return_on_assets, return_on_equity
 * @param numRows Number of data points
 * @param basePE Base P/E ratio
 */
inline DataFrame CreateFinancialRatiosData(int numRows, double basePE = 20.0) {
    auto timestamps = CreateTimestampsNS(numRows);
    std::vector<std::string> tickers;
    std::vector<double> avgVolumes, cash, current, debtEquity, divYield, eps, ev, evEbitda, evSales;
    std::vector<double> fcf, marketCap, price, pb, pcf, pe, pfcf, ps, quick, roa, roe;

    for (int i = 0; i < numRows; ++i) {
        tickers.push_back("AAPL");
        avgVolumes.push_back(50000000.0 + i * 100000);      // ~50M avg volume
        cash.push_back(1.5 + (i % 3) * 0.1);                // Cash ratio ~1.5
        current.push_back(2.0 + (i % 3) * 0.1);             // Current ratio ~2.0
        debtEquity.push_back(0.5 + (i % 5) * 0.05);         // D/E ~0.5-0.7
        divYield.push_back(1.5 + (i % 4) * 0.1);            // ~1.5-1.8% yield
        eps.push_back(5.0 + (i % 10) * 0.2);                // EPS ~$5-7
        ev.push_back(2000000000000.0 + i * 10000000000.0);  // ~$2T+ enterprise value
        evEbitda.push_back(15.0 + (i % 5) * 0.5);           // EV/EBITDA ~15-17
        evSales.push_back(6.0 + (i % 4) * 0.3);             // EV/Sales ~6-7
        fcf.push_back(80000000000.0 + i * 1000000000.0);    // ~$80B+ FCF
        marketCap.push_back(2500000000000.0 + i * 50000000000.0);  // ~$2.5T+ market cap
        price.push_back(170.0 + i * 2.0);                   // ~$170+ stock price
        pb.push_back(35.0 + (i % 6) * 2.0);                 // P/B ~35-45
        pcf.push_back(20.0 + (i % 5) * 1.0);                // P/CF ~20-25
        pe.push_back(basePE + (i % 10) - 5);                // P/E varies around base
        pfcf.push_back(25.0 + (i % 4) * 1.5);               // P/FCF ~25-30
        ps.push_back(6.5 + (i % 5) * 0.4);                  // P/S ~6.5-8.5
        quick.push_back(1.3 + (i % 3) * 0.1);               // Quick ratio ~1.3-1.5
        roa.push_back(20.0 + (i % 5) * 1.0);                // ROA ~20-25%
        roe.push_back(150.0 + (i % 6) * 5.0);               // ROE ~150-175%
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        factory::array::make_array(tickers),
        factory::array::make_array(avgVolumes),
        factory::array::make_array(cash),
        factory::array::make_array(current),
        factory::array::make_array(debtEquity),
        factory::array::make_array(divYield),
        factory::array::make_array(eps),
        factory::array::make_array(ev),
        factory::array::make_array(evEbitda),
        factory::array::make_array(evSales),
        factory::array::make_array(fcf),
        factory::array::make_array(marketCap),
        factory::array::make_array(price),
        factory::array::make_array(pb),
        factory::array::make_array(pcf),
        factory::array::make_array(pe),
        factory::array::make_array(pfcf),
        factory::array::make_array(ps),
        factory::array::make_array(quick),
        factory::array::make_array(roa),
        factory::array::make_array(roe),
    };
    return make_dataframe(index, arrays,
        {"R:ticker", "R:average_volume", "R:cash", "R:current", "R:debt_to_equity",
         "R:dividend_yield", "R:earnings_per_share", "R:enterprise_value", "R:ev_to_ebitda",
         "R:ev_to_sales", "R:free_cash_flow", "R:market_cap", "R:price", "R:price_to_book",
         "R:price_to_cash_flow", "R:price_to_earnings", "R:price_to_free_cash_flow",
         "R:price_to_sales", "R:quick", "R:return_on_assets", "R:return_on_equity"});
}

/**
 * @brief Create FRED economic indicator data with ALL required SDK columns
 * Used by: economic_indicator data source (SDK prefix: "ECON:{category}:")
 * Required SDK columns: observation_date, value, revision
 * @param category The economic indicator category (e.g., "CPI", "UNRATE")
 * @param numRows Number of observations (monthly/quarterly)
 * @param baseValue Base indicator value
 */
inline DataFrame CreateEconomicIndicatorData(const std::string& category, int numRows, double baseValue = 3.0) {
    auto timestamps = CreateTimestampsNS(numRows, 30);  // Monthly spacing (published_at index)
    std::vector<double> values;
    std::vector<int64_t> observationDates;  // When the economic observation occurred
    std::vector<int64_t> revisions;         // Revision number (1 = initial release)

    for (int i = 0; i < numRows; ++i) {
        values.push_back(baseValue + (i % 6) * 0.1);  // Value fluctuates
        // Observation date is typically 1 month before publication
        observationDates.push_back(timestamps[i] - (30 * DAY_NS));
        revisions.push_back(1);  // All initial releases (revision = 1)
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");

    // Build all 3 required ECON columns with category prefix
    std::string prefix = "ECON:" + category + ":";
    std::vector<arrow::ChunkedArrayPtr> arrays = {
        MakeTimestampArray(observationDates),
        factory::array::make_array(values),
        factory::array::make_array(revisions),
    };
    return make_dataframe(index, arrays,
        {prefix + "observation_date", prefix + "value", prefix + "revision"});
}

/**
 * @brief Create ticker events data (TE:event_type_id column) - returns dummy numeric
 * Used by: ticker_events data source (SDK prefix: "TE:")
 * @param numRows Number of events
 */
inline DataFrame CreateTickerEventsData(int numRows) {
    auto timestamps = CreateTimestampsNS(numRows, 180);  // Semi-annual spacing
    std::vector<double> eventTypeIds;

    for (int i = 0; i < numRows; ++i) {
        eventTypeIds.push_back(i % 2 == 0 ? 1.0 : 2.0);  // 1=name_change, 2=ticker_change
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    return make_dataframe<double>(index, {eventTypeIds}, {"TE:event_type_id"});
}

/**
 * @brief Create SEC Form 13F holdings data
 * Used by: form13f_holdings data source
 * @param numRows Number of quarterly filings
 */
inline DataFrame CreateForm13FData(int numRows, int64_t baseShares = 1000000) {
    auto timestamps = CreateTimestampsNS(numRows, 90);  // Quarterly spacing
    std::vector<double> shares;
    std::vector<double> values;
    std::vector<double> institutionIds;

    for (int i = 0; i < numRows; ++i) {
        double shareCount = static_cast<double>(baseShares + (i * 100000));
        shares.push_back(shareCount);
        values.push_back(shareCount * 150.0);  // Assume $150/share
        institutionIds.push_back(static_cast<double>(i + 1));
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    return make_dataframe<double>(index,
        {shares, values, institutionIds},
        {"shares", "value", "institution_id"});
}

/**
 * @brief Create insider trading data
 * Used by: insider_trading data source
 * @param numRows Number of insider transactions
 */
inline DataFrame CreateInsiderTradingData(int numRows, int64_t baseShares = 10000) {
    auto timestamps = CreateTimestampsNS(numRows, 7);  // Weekly spacing
    std::vector<double> shares;
    std::vector<double> prices;
    std::vector<double> transactionCodeIds;
    std::vector<double> ownerIds;

    for (int i = 0; i < numRows; ++i) {
        shares.push_back(static_cast<double>(baseShares + (i * 1000)));
        prices.push_back(100.0 + (i * 2.0));
        transactionCodeIds.push_back(i % 3 == 0 ? 1.0 : 2.0);  // 1=Purchase, 2=Sale
        ownerIds.push_back(static_cast<double>(i + 1));
    }

    auto index = factory::index::make_datetime_index(timestamps, "index", "UTC");
    return make_dataframe<double>(index,
        {shares, prices, transactionCodeIds, ownerIds},
        {"shares", "price", "transaction_code_id", "owner_id"});
}

} // namespace epoch_script::runtime::test