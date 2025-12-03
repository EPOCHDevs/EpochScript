# ML tests temporarily disabled - missing AWS SDK dependencies
target_sources(epoch_script_test PRIVATE
    sagemaker_sentiment_test.cpp
    lightgbm_test.cpp
    liblinear_test.cpp
    ml_preprocess_test.cpp
    rolling_ml_test.cpp
    rolling_window_iterator_test.cpp)

target_compile_definitions(epoch_script_test PRIVATE
    -DML_TEST_DATA_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ml_test_data")

file(COPY "${CMAKE_CURRENT_LIST_DIR}/ml_python_gen/ml_test_data/"
     DESTINATION "${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/ml_test_data"
     FILES_MATCHING PATTERN "*.csv")