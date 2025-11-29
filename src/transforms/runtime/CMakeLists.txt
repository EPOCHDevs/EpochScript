# EpochScript Runtime - Transform Orchestrator and Execution Engine
# Manages dataflow graph execution using TBB

# Add runtime sources to parent target (epoch_script)
target_sources(epoch_script PRIVATE
    orchestrator.cpp
    execution/execution_node.cpp
    execution/intermediate_storage.cpp
    transform_manager/transform_manager.cpp
    # Event system for progress tracking and callbacks
    events/event_dispatcher.cpp
    events/transform_progress_emitter.cpp
)

# Runtime needs access to execution headers
target_include_directories(epoch_script PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}
)
