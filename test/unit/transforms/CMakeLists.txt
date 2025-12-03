target_sources(epoch_script_test PRIVATE
transform_metadata_factory.cpp
transforms_test.cpp trade_executor_test.cpp
        # typed_transforms_test.cpp
flag_schema_validation_test.cpp
template_expansion_test.cpp)


add_subdirectory(cummulative)
add_subdirectory(cross_sectional)
add_subdirectory(data_sources)
add_subdirectory(datetime)
add_subdirectory(futures)
add_subdirectory(hosseinmoein)
add_subdirectory(ml)
add_subdirectory(operators)
add_subdirectory(price_actions)
add_subdirectory(string)
add_subdirectory(tulip)
add_subdirectory(volatility)
add_subdirectory(statistics)
add_subdirectory(timeseries)
add_subdirectory(utility)
add_subdirectory(portfolio)


target_compile_definitions(epoch_script_test PUBLIC -DMETADATA_FILES_DIR="${CMAKE_RUNTIME_OUTPUT_DIRECTORY}/files")
