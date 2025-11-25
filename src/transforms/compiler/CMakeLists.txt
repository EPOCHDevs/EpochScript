# EpochScript - Python Algorithm Compiler for StratifyX
# Compiles Python-like algorithm syntax to nodes/edges graphs

# Link tree-sitter and Python grammar
target_link_libraries(epoch_script PUBLIC cpp-tree-sitter tree-sitter-python)

# Add source files from this directory
target_sources(epoch_script PRIVATE
    ast_compiler.cpp
    ast_visitor.cpp
    constant_folder.cpp
    constructor_parser.cpp
    cse_optimizer.cpp
    expression_compiler.cpp
    node_builder.cpp
    option_validator.cpp
    scalar_inlining_pass.cpp
    special_parameter_handler.cpp
    timeframe_resolver.cpp
    type_checker.cpp
)

# Add subdirectories
add_subdirectory(parser)
add_subdirectory(validators)
add_subdirectory(error_formatting)