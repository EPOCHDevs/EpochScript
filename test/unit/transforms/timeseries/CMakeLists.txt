# Timeseries transform tests (GARCH, ARIMA, Rolling)

target_sources(epoch_script_test PRIVATE
    garch_test.cpp
    arima_test.cpp
    rolling_ts_test.cpp
)

target_compile_definitions(epoch_script_test PRIVATE
    -DGARCH_TEST_DATA_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/garch_test_data"
    -DARIMA_TEST_DATA_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/arima_test_data"
)

# Copy GARCH test data
file(COPY "${CMAKE_CURRENT_LIST_DIR}/garch_python_gen/garch_test_data/"
     DESTINATION "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/garch_test_data"
     FILES_MATCHING PATTERN "*.csv")

# Copy ARIMA test data
file(COPY "${CMAKE_CURRENT_LIST_DIR}/arima_python_gen/arima_test_data/"
     DESTINATION "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/arima_test_data"
     FILES_MATCHING PATTERN "*.csv")
