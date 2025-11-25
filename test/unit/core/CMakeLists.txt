# Core component unit tests

target_sources(epoch_script_test PRIVATE
    sql_statement_test.cpp
    symbol_test.cpp
    timeframe_test.cpp
    metadata_options_test.cpp
    constant_value_test.cpp
)
