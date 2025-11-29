# Runtime orchestrator unit tests
target_sources(epoch_script_test PRIVATE
    orchestrator_initialization_test.cpp
    orchestrator_error_handling_test.cpp
    orchestrator_parallel_execution_test.cpp
    orchestrator_report_caching_test.cpp
    orchestrator_report_generation_test.cpp
    orchestrator_event_marker_caching_test.cpp
    orchestrator_graph_topologies_real_test.cpp
    orchestrator_basic_all_datasource.cpp
    orchestrator_cross_sectional.cpp
    # Event system unit tests
    events/cancellation_token_test.cpp
    events/event_filter_test.cpp
    events/event_dispatcher_test.cpp
    events/transform_progress_emitter_test.cpp
    events/orchestrator_events_integration_test.cpp
)

target_include_directories(epoch_script_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}
)
