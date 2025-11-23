#pragma once
#include <string>
#include <unordered_map>
#include <vector>
#include <optional>
#include <epoch_frame/dataframe.h>
#include <epoch_protos/tearsheet.pb.h>
#include <epoch_dashboard/tearsheet/tearsheet_builder.h>
#include <epoch_script/transforms/core/event_marker_data.h>

// Forward declarations
namespace epoch_script::transform {
    struct ITransformBase;
}

namespace epoch_script::runtime {
    using AssetID = std::string;
    using AssetDataFrameMap = std::unordered_map<AssetID, epoch_frame::DataFrame>;
    using TimeFrameAssetDataFrameMap = std::unordered_map<std::string, AssetDataFrameMap>;

    using AssetReportMap = std::unordered_map<AssetID, epoch_proto::TearSheet>;
    using AssetEventMarkerMap = std::unordered_map<AssetID, std::vector<epoch_script::transform::EventMarkerData>>;

    // Result structure for stateless transform execution
    // Both types are now complete via includes
    struct TransformResult {
        epoch_frame::DataFrame data;
        std::optional<epoch_tearsheet::DashboardBuilder> dashboard;
        std::optional<epoch_script::transform::EventMarkerData> event_marker;
    };
}
