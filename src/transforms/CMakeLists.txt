
target_sources(epoch_script PRIVATE
    metadata.cpp
    registration.cpp
    tulip_charts.cpp
    tulip_indicators.cpp
    transform_definition.cpp
    core/itransform_progress.cpp
)
add_subdirectory(compiler)
add_subdirectory(runtime)
add_subdirectory(components)