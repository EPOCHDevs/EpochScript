#pragma once
#include <string>
#include <vector>
#include <optional>
#include <epoch_frame/dataframe.h>
#include <epoch_script/core/metadata_options.h>
#include <epoch_script/core/constants.h>

namespace epoch_script::transform {

  // Event marker data structure for chart annotations
  struct EventMarkerData {
    std::string title;
    epoch_core::Icon icon;
    std::vector<epoch_script::CardColumnSchema> schemas;
    epoch_frame::DataFrame data;
    std::optional<size_t> pivot_index;  // Index in schemas array pointing to Timestamp column for chart navigation

    EventMarkerData() : icon(epoch_core::Icon::Info) {}
    EventMarkerData(std::string title_,
                 std::vector<epoch_script::CardColumnSchema> schemas_,
                 epoch_frame::DataFrame data_,
                 std::optional<size_t> pivot_index_ = std::nullopt,
                 epoch_core::Icon icon_ = epoch_core::Icon::Info)
        : title(std::move(title_)),
          icon(icon_),
          schemas(std::move(schemas_)),
          data(std::move(data_)),
          pivot_index(pivot_index_) {}
  };

} // namespace epoch_script::transform
