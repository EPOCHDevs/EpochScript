# EpochDataSDK.cmake
#
# This is a helper file to include EpochDataSDK
include(FetchContent)

set(EPOCH_DATA_SDK_REPOSITORY "${REPO_URL}/EPOCHDevs/EpochDataSDK.git" CACHE STRING "EpochDataSDK repository URL")
set(EPOCH_DATA_SDK_TAG "main" CACHE STRING "EpochDataSDK Git tag to use")

FetchContent_Declare(
    EpochDataSDK
    GIT_REPOSITORY ${EPOCH_DATA_SDK_REPOSITORY}
    GIT_TAG ${EPOCH_DATA_SDK_TAG}
)

FetchContent_MakeAvailable(EpochDataSDK)

message(STATUS "EpochDataSDK fetched and built from source")
