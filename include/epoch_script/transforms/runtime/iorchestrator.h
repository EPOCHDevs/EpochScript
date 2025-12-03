#pragma once
//
// Created by dewe on 1/26/23.
//
#include <epoch_script/transforms/runtime/types.h>
#include <epoch_script/transforms/components/event_markers/event_marker.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/runtime/transform_manager/itransform_manager.h>
#include <epoch_data_sdk/events/all.h>
#include "epoch_protos/tearsheet.pb.h"

namespace epoch_script::runtime {
    struct IDataFlowOrchestrator {
        using Ptr = std::unique_ptr<IDataFlowOrchestrator>;

        virtual TimeFrameAssetDataFrameMap
        ExecutePipeline(TimeFrameAssetDataFrameMap data,
                        data_sdk::events::ScopedProgressEmitter& emitter) = 0;

        virtual AssetReportMap GetGeneratedReports() const = 0;

        virtual AssetEventMarkerMap GetGeneratedEventMarkers() const = 0;

        virtual ~IDataFlowOrchestrator() = default;
    };

    std::unique_ptr<IDataFlowOrchestrator> CreateDataFlowRuntimeOrchestrator(
        const std::set<std::string>& assetIdList,
        epoch_script::runtime::ITransformManagerPtr transformManager);
} // namespace epoch_script::runtime