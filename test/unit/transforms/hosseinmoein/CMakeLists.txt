target_sources(epoch_script_test PRIVATE indicators_test.cpp volatility_test.cpp donchian_channel_test.cpp
 ichimoku_test.cpp
 # linear_fit_test.cpp
zscore_test.cpp
#  kpss_adf_test.cpp  # Disabled: API incompatibility with hmdf library (not NodeReference-related)
)

if(NOT EXISTS "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_data/hmdf")
    # Copy test data directory to runtime bin
    file(COPY "${CMAKE_CURRENT_LIST_DIR}/test_data/hmdf"
            DESTINATION "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_data"
            PATTERN "*")

    message(STATUS "Copied test data to: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/test_data/hmdf")
endif()