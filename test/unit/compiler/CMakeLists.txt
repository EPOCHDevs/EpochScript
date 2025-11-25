# Compiler unit tests
target_sources(epoch_script_test PRIVATE
    ast_compiler_test.cpp
    cse_optimizer_test.cpp
    scalar_inlining_pass_test.cpp
    test_scalar_timeframe_resolution.cpp
    boolean_type_cast_test.cpp
    test_intraday_only_default.cpp
    any_type_resolution_test.cpp
    boolean_select_validation_test.cpp
    ternary_operator_test.cpp
)
