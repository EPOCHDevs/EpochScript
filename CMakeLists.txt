cmake_minimum_required(VERSION 3.26 FATAL_ERROR) # 3.26 For now until XGBOOST Removes FindCUDA

# Add option for shared vs static libraries (default to shared for development)
set(GIT_TOKEN "" CACHE STRING "Default GIT TOKEN")
option(BUILD_SHARED_LIBS "Build libraries as shared instead of static" ON)
message(STATUS "Building SHARED libraries as ${BUILD_SHARED_LIBS}")
if(BUILD_SHARED_LIBS)
    message(STATUS "Building SHARED libraries")
    set(CMAKE_POSITION_INDEPENDENT_CODE ON)
else()
    message(STATUS "Building STATIC libraries")
    set(CMAKE_POSITION_INDEPENDENT_CODE OFF)
endif()

# Set binary and library output directories to keep build files separate from source
set(PROJECT_VERSION 0.1.0)
set(PROJECT_VERSION_MAJOR 0.1.0)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)

# Enable vcpkg manifest mode (uses vcpkg.json)
set(DEPENDENCY_DIR "/usr/local" CACHE STRING "Dependency directory")
set(CMAKE_TOOLCHAIN_FILE $ENV{HOME}/vcpkg/scripts/buildsystems/vcpkg.cmake CACHE STRING "Vcpkg toolchain file")
set(VCPKG_MANIFEST_MODE ON)
set(VCPKG_FEATURE_FLAGS "versions")

option(BUILD_TEST OFF)
option(ENABLE_COVERAGE "Enable code coverage reporting" OFF)
########################################################################################################################

project(EpochScript LANGUAGES C CXX)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

if (GIT_TOKEN)
    SET(REPO_URL "https://${GIT_TOKEN}:@github.com")
    message("REPO URL: https://************:@github.com")
else ()
    SET(REPO_URL "https://github.com")
    message("REPO URL: ${REPO_URL}")
endif ()


if (BUILD_TEST)
    # Find Catch2 for both tests and benchmarks
    find_package(Catch2 3 REQUIRED)
    find_package(trompeloeil CONFIG REQUIRED)

    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -O0 --coverage")
    enable_testing()
endif()

include(${PROJECT_SOURCE_DIR}/cmake/EpochDashboard.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/EpochDataSDK.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/Tulip.cmake)
# Tree-sitter setup via CPM
include(${PROJECT_SOURCE_DIR}/cmake/TreeSitter.cmake)

find_package(yaml-cpp CONFIG REQUIRED)
find_package(DataFrame CONFIG REQUIRED)
find_package(Armadillo CONFIG REQUIRED)
find_package(TBB CONFIG REQUIRED)
find_package(Protobuf CONFIG REQUIRED)
find_package(OpenMP REQUIRED)
find_package(AWSSDK CONFIG COMPONENTS dynamodb  sagemaker-runtime REQUIRED)

add_library(epoch_script "")
add_library(epoch::script ALIAS epoch_script)

# Configure version for shared libraries
set_target_properties(epoch_script PROPERTIES
        VERSION ${PROJECT_VERSION}
        SOVERSION ${PROJECT_VERSION_MAJOR}
        WINDOWS_EXPORT_ALL_SYMBOLS ON
)

# For shared libraries, export all symbols by default (Linux-friendly approach)
if(BUILD_SHARED_LIBS)
    set_target_properties(epoch_script PROPERTIES
            CXX_VISIBILITY_PRESET default
            VISIBILITY_INLINES_HIDDEN OFF
    )
endif()

target_include_directories(epoch_script
        PUBLIC
        ${PROJECT_SOURCE_DIR}/include
        PRIVATE
        ${PROJECT_SOURCE_DIR}/src
)

target_compile_options(epoch_script PRIVATE -Wall -Wextra -Werror)
target_link_libraries(epoch_script PUBLIC
        cpp-tree-sitter
        tree-sitter-python
        aws-cpp-sdk-dynamodb
        aws-cpp-sdk-sagemaker-runtime
        armadillo
        DataFrame::DataFrame
       epoch::dashboard
       epoch::data_sdk
       yaml-cpp::yaml-cpp
       libmpdec++
       libmpdec
       tulip_indicators
       TBB::tbb)

# Link LIBLINEAR and LightGBM (no find_package configs available in vcpkg)
# WARNING: OpenMP MUST come AFTER LightGBM - static library link order matters!
find_library(LIBLINEAR_LIB NAMES liblinear PATHS ${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib REQUIRED)
find_library(LIGHTGBM_LIB NAMES _lightgbm PATHS ${VCPKG_INSTALLED_DIR}/${VCPKG_TARGET_TRIPLET}/lib REQUIRED)
target_link_libraries(epoch_script PUBLIC ${LIBLINEAR_LIB} ${LIGHTGBM_LIB} OpenMP::OpenMP_CXX)


add_subdirectory(src)
add_subdirectory(tools)

# Copy test files and define METADATA_FILES_DIR early
# Use the configurable SCRIPT_METADATA_FILES_DIR option from main CMakeLists.txt
set(METADATA_FILES_DIR ${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/files)
file(COPY ${PROJECT_SOURCE_DIR}/files DESTINATION ${CMAKE_RUNTIME_OUTPUT_DIRECTORY})

if (BUILD_TEST)
    add_subdirectory(test)
    add_subdirectory(benchmark)
endif()

# Installation target for test cases (for use by StratifyX test coverage framework)
option(INSTALL_TEST_CASES "Install test cases for external usage" OFF)
if(INSTALL_TEST_CASES)
    set(TEST_CASES_INSTALL_DIR "${CMAKE_INSTALL_PREFIX}/share/epochscript/test_cases" CACHE PATH "Test cases installation directory")

    # Install test cases, excluding those with error.txt
    install(DIRECTORY ${PROJECT_SOURCE_DIR}/scripts/test_coverage/output/test_cases/
            DESTINATION ${TEST_CASES_INSTALL_DIR}
            COMPONENT test_cases
            FILES_MATCHING
            PATTERN "*.epochscript"
            PATTERN "*.json"
            PATTERN "error.txt" EXCLUDE
            PATTERN "[ATTENTION]*" EXCLUDE)

    message(STATUS "Test cases will be installed to: ${TEST_CASES_INSTALL_DIR}")
endif()