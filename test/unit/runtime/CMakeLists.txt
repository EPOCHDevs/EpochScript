# Runtime orchestrator unit tests
target_sources(epoch_script_test PRIVATE
    orchestrator_initialization_test.cpp
    orchestrator_error_handling_test.cpp
    orchestrator_parallel_execution_test.cpp
    orchestrator_report_caching_test.cpp
    orchestrator_report_generation_test.cpp
    orchestrator_event_marker_caching_test.cpp
    orchestrator_graph_topologies_real_test.cpp
    orchestrator_basic_all_datasource.cpp
    orchestrator_cross_sectional.cpp
    orchestrator_ml_integration_test.cpp
    pairs_trading_test.cpp
    ml_strategy_test.cpp
    events/orchestrator_event_bridge_test.cpp
)

target_include_directories(epoch_script_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Runtime test data directory definition for CSV baseline comparison
target_compile_definitions(epoch_script_test PRIVATE
    -DRUNTIME_TEST_DATA_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/runtime_test_data"
)

# Copy expected_data CSVs to build directory at configure time
file(GLOB_RECURSE RUNTIME_CSV_FILES
    "${CMAKE_CURRENT_LIST_DIR}/expected_data/*.csv"
)
if(RUNTIME_CSV_FILES)
    file(COPY "${CMAKE_CURRENT_LIST_DIR}/expected_data/"
         DESTINATION "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/runtime_test_data"
         FILES_MATCHING PATTERN "*.csv")
    message(STATUS "Copied runtime test data to: ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/runtime_test_data")
endif()
