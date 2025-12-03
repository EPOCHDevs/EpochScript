#pragma once

#include <epoch_script/data/aliases.h>
#include "epoch_frame/dataframe.h"
#include "epoch_script/core/time_frame.h"
#include <epoch_data_sdk/events/all.h>
#include <vector>

namespace epoch_script::data {
    struct IResampler {
        using OutputType = StringAssetDataFrameMap;

        virtual ~IResampler() = default;

        virtual std::vector<
            std::tuple<TimeFrameNotation, asset::Asset, epoch_frame::DataFrame>>
        Build(AssetDataFrameMap const &,
              data_sdk::events::ScopedProgressEmitter& emitter) const = 0;
    };

    using IResamplerPtr = std::unique_ptr<IResampler>;

    struct Resampler final : IResampler {
        explicit Resampler(const std::vector<epoch_script::TimeFrame>& timeFrames, bool isIntraday)
            : m_isIntraday(isIntraday) {
            epoch_script::TimeFrameSet timeFrameSet;
            for (auto const &timeFrame : timeFrames) {
                if (timeFrameSet.contains(timeFrame)) {
                    continue;
                }
                timeFrameSet.insert(timeFrame);
                m_timeFrames.emplace_back(timeFrame);
            }
        }

        std::vector<
            std::tuple<TimeFrameNotation, asset::Asset, epoch_frame::DataFrame>>
        Build(AssetDataFrameMap const &,
              data_sdk::events::ScopedProgressEmitter& emitter) const override;

    private:
        std::vector<epoch_script::TimeFrame> m_timeFrames;
        bool m_isIntraday;

        epoch_frame::DataFrame
        AdjustTimestamps(asset::Asset const &, epoch_frame::IndexPtr const &,
                         epoch_frame::DataFrame const &, bool isIntraday) const;
    };
} // namespace epoch_script::data