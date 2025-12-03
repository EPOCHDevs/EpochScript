# EpochScript Test Suite
add_executable(epoch_script_test catch_main.cpp)
target_link_libraries(epoch_script_test PRIVATE
    epoch_script
    Catch2::Catch2
    yaml-cpp::yaml-cpp
    trompeloeil::trompeloeil
    protobuf::libprotobuf
)
target_include_directories(epoch_script_test PRIVATE
    ../src
    .
)

# Helper tool to generate graph.json from EpochScript source
add_executable(generate_graph integration/tools/generate_graph.cpp)
target_link_libraries(generate_graph PRIVATE epoch_script)
target_include_directories(generate_graph PRIVATE ../src .)
set_target_properties(generate_graph PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Helper tool to dump expected runtime artifacts for a test case
add_executable(dump_expected integration/tools/dump_expected.cpp)
target_link_libraries(dump_expected PRIVATE epoch_script)
target_include_directories(dump_expected PRIVATE ../src .)
set_target_properties(dump_expected PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Link integration common helpers (CSV loader, JSON/PB helpers)
target_sources(dump_expected PRIVATE
    integration/common/csv_data_loader.cpp
    integration/common/tearsheet_comparator.cpp
    integration/common/event_marker_comparator.cpp
)

# Provide METADATA_FILES_DIR define (used by DEFAULT_YAML_LOADER)
target_compile_definitions(dump_expected PUBLIC
    -DMETADATA_FILES_DIR="${METADATA_FILES_DIR}")

# Helper tool to dump transform metadata to JSON for test coverage
add_executable(dump_transform_metadata dump_transform_metadata.cpp)
target_link_libraries(dump_transform_metadata PRIVATE epoch_script)
target_include_directories(dump_transform_metadata PRIVATE ../src .)
set_target_properties(dump_transform_metadata PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
target_compile_definitions(dump_transform_metadata PUBLIC
    -DMETADATA_FILES_DIR="${METADATA_FILES_DIR}")

# Standalone test runner for coverage agent
add_executable(epoch_test_runner epoch_test_runner.cpp)
target_link_libraries(epoch_test_runner PRIVATE epoch_script epoch_event_viewer)
target_include_directories(epoch_test_runner PRIVATE ../src .)
set_target_properties(epoch_test_runner PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
target_compile_definitions(epoch_test_runner PUBLIC
    -DMETADATA_FILES_DIR="${METADATA_FILES_DIR}")

# Standalone repro executable for debugging reindex issues
add_executable(repro_reindex repro_reindex.cpp)
target_link_libraries(repro_reindex PRIVATE epoch_script)
target_include_directories(repro_reindex PRIVATE ../src .)
set_target_properties(repro_reindex PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")
target_compile_definitions(repro_reindex PUBLIC
    -DMETADATA_FILES_DIR="${METADATA_FILES_DIR}")

# Set runtime output directory to bin (where test resources are copied)
set_target_properties(epoch_script_test PROPERTIES
    RUNTIME_OUTPUT_DIRECTORY "${CMAKE_BINARY_DIR}/bin")

# Add unit tests (includes shared utilities in unit/common)
add_subdirectory(unit)

# Add integration tests (includes mocks in integration/mocks)
add_subdirectory(integration)

target_compile_definitions(epoch_script_test PUBLIC
    -DMETADATA_FILES_DIR="${METADATA_FILES_DIR}"
    -DRESAMPLE_FILES="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/resample_test_files"
    -DTRANSFORMS_TEST_CASES_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/transforms_test_cases")

target_compile_definitions(generate_graph PUBLIC
    -DMETADATA_FILES_DIR="${METADATA_FILES_DIR}")

MESSAGE("METADATA_FILES_DIR ${METADATA_FILES_DIR}")
