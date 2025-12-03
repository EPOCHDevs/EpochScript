target_sources(epoch_script_test PRIVATE indicators_test.cpp volatility_test.cpp donchian_channel_test.cpp
 ichimoku_test.cpp
 linear_fit_test.cpp
zscore_test.cpp
cointegration_test.cpp
frac_diff_test.cpp
#  kpss_adf_test.cpp  # Disabled: API incompatibility with hmdf library (not NodeReference-related)
)

# Cointegration test data directory definition
target_compile_definitions(epoch_script_test PRIVATE
    -DCOINTEGRATION_TEST_DATA_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/cointegration_test_data"
    -DFRAC_DIFF_TEST_DATA_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/frac_diff_test_data")

# Copy cointegration test data
file(COPY "${CMAKE_CURRENT_LIST_DIR}/cointegration_python_gen/cointegration_test_data/"
     DESTINATION "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/cointegration_test_data"
     FILES_MATCHING PATTERN "*.csv")

# Copy frac_diff test data
file(COPY "${CMAKE_CURRENT_LIST_DIR}/frac_diff_python_gen/frac_diff_test_data/"
     DESTINATION "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/frac_diff_test_data"
     FILES_MATCHING PATTERN "*.csv")

if(NOT EXISTS "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_data/hmdf")
    # Copy test data directory to runtime bin
    file(COPY "${CMAKE_CURRENT_LIST_DIR}/test_data/hmdf"
            DESTINATION "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_data"
            PATTERN "*")

    message(STATUS "Copied test data to: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_data/hmdf")
endif()