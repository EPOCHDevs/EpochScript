# Portfolio optimization transform tests
# Subdirectories added as tests are implemented

# Test subdirectories - uncomment as implemented
# add_subdirectory(covariance)
# add_subdirectory(allocation)
# add_subdirectory(risk_measures)
# add_subdirectory(integration)

# Test data directory definition
target_compile_definitions(epoch_script_test PRIVATE
    PORTFOLIO_TEST_DATA_DIR="${CMAKE_CURRENT_SOURCE_DIR}/portfolio_python_gen/portfolio_test_data")
