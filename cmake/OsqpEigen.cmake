# OsqpEigen.cmake - OSQP quadratic programming with Eigen wrapper setup using CPM
# Provides OsqpEigen::OsqpEigen target for portfolio optimization (MVO, Risk Parity, etc.)

include_guard()

# Download CPM if not already included
if(NOT COMMAND CPMAddPackage)
    include(${CMAKE_CURRENT_LIST_DIR}/CPM.cmake)
endif()

# Force static linking for OSQP (override BUILD_SHARED_LIBS)
set(BUILD_SHARED_LIBS_BACKUP ${BUILD_SHARED_LIBS})
set(BUILD_SHARED_LIBS OFF CACHE BOOL "Force static OSQP" FORCE)

# Eigen3 is required - should be available via vcpkg or system
find_package(Eigen3 CONFIG QUIET)
if(NOT TARGET Eigen3::Eigen)
    # Try to find system Eigen
    find_package(Eigen3 REQUIRED)
endif()

# Download OSQP (the core solver) - v1.0.0 with new API
CPMAddPackage(
    NAME osqp
    GIT_REPOSITORY https://github.com/osqp/osqp.git
    GIT_TAG v1.0.0
    OPTIONS
        "OSQP_BUILD_SHARED_LIB OFF"
        "OSQP_BUILD_DEMO_EXE OFF"
        "OSQP_BUILD_UNITTESTS OFF"
        "CMAKE_BUILD_TYPE Release"
)

# Create osqp::osqpstatic and osqp::osqp aliases (osqp-eigen v0.11.0 expects namespaced targets)
# OSQP v1.0.0 creates 'osqpstatic' and 'osqp' targets without namespace
if(TARGET osqpstatic AND NOT TARGET osqp::osqpstatic)
    add_library(osqp::osqpstatic ALIAS osqpstatic)
    message(STATUS "Created alias osqp::osqpstatic -> osqpstatic")
endif()
if(TARGET osqp AND NOT TARGET osqp::osqp)
    add_library(osqp::osqp ALIAS osqp)
    message(STATUS "Created alias osqp::osqp -> osqp")
endif()
# If only static was built, also create osqp::osqp pointing to static
if(TARGET osqp::osqpstatic AND NOT TARGET osqp::osqp)
    add_library(osqp::osqp ALIAS osqpstatic)
    message(STATUS "Created alias osqp::osqp -> osqpstatic (shared not built)")
endif()

# Download OSQP-Eigen (Eigen wrapper)
# v0.11.0 is compatible with OSQP v1.0.0
CPMAddPackage(
    NAME OsqpEigen
    GIT_REPOSITORY https://github.com/robotology/osqp-eigen.git
    GIT_TAG v0.11.0
    OPTIONS
        "BUILD_TESTING OFF"
        "OSQP_EIGEN_DEBUG_OUTPUT OFF"
        "CMAKE_BUILD_TYPE Release"
)

# Restore original BUILD_SHARED_LIBS setting
set(BUILD_SHARED_LIBS ${BUILD_SHARED_LIBS_BACKUP} CACHE BOOL "Restored after OSQP" FORCE)

# Verify target exists
if(TARGET OsqpEigen::OsqpEigen)
    message(STATUS "OsqpEigen configured via CPM (target: OsqpEigen::OsqpEigen)")
elseif(TARGET OsqpEigen)
    # Create alias if not present
    add_library(OsqpEigen::OsqpEigen ALIAS OsqpEigen)
    message(STATUS "OsqpEigen configured via CPM (target: OsqpEigen, aliased)")
else()
    message(FATAL_ERROR "OsqpEigen target not found after CPM configuration")
endif()
