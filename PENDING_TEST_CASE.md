Plan: Testing frac_diff with Test Coverage Framework

Test Case Structure

Each test case folder contains:
- code.epochscript - The script to execute
- execution.log - Logs with pass/fail status
- graph.json - Transform graph
- metadata.json - Test metadata
- small_index/ or single_stock/ - Data scope

Simple Testing Plan for frac_diff

1. Create a test case folder:
   scripts/test_coverage/output/test_cases/statistical/frac_diff_stationarity_research/

2. Write code.epochscript:
# Basic frac_diff test
src = market_data_source(timeframe="1D")()
log_close = log()(src.c)

# Fractional diff with d=0.5 for stationarity while preserving memory
frac_d = frac_diff(d=0.5, threshold=1e-5)(log_close)

# Compare with regular diff
reg_diff = roc(period=1)(log_close)

# Reports
numeric_cards_report(agg="last", category="FracDiff", title="FracDiff(0.5) Last")(frac_d)
numeric_cards_report(agg="mean", category="FracDiff", title="FracDiff(0.5) Mean")(frac_d)
histogram_chart_report(title="FracDiff Distribution", bins=30, category="FracDiff")(frac_d)
line_chart_report(title="FracDiff vs RegDiff", category="Comparison")(frac_d, reg_diff)

3. Run the test:
   ./cmake-build-release-test/bin/epoch_test_runner \
   scripts/test_coverage/output/test_cases/statistical/frac_diff_stationarity_research/

4. Check results:
- execution.log should show Status: PASSED
- Return code should be 0