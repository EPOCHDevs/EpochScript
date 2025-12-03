#pragma once
//
// Created by dewe on 4/14/23.
//
#include "epoch_core/enum_wrapper.h"
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/series_factory.h>

CREATE_ENUM(AggType, AllOf, AnyOf, NoneOf, Sum, Average, Min, Max, IsEqual,
            IsUnique);

namespace epoch_script::transform {

template <epoch_core::AggType agg_type>
struct AggregateTransform final : ITransform {
  explicit AggregateTransform(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    auto data = bars[GetInputIds()];
    epoch_frame::Series result;
    if constexpr (agg_type == epoch_core::AggType::Min) {
      result = data.min(epoch_frame::AxisType::Column);
    } else if constexpr (agg_type == epoch_core::AggType::Max) {
      result = data.max(epoch_frame::AxisType::Column);
    } else {
      auto columns = bars.column_names();
      auto first = columns.front();

      auto start = bars[columns[0]];
      if constexpr (agg_type == epoch_core::AggType::IsEqual ||
                    agg_type == epoch_core::AggType::IsUnique) {
        start = epoch_frame::make_series<bool>(
            bars.index(), std::vector<bool>(data.num_rows(), true));
      }

      result = std::accumulate(
          columns.begin() + 1, columns.end(), start,
          [&](auto &&acc, const std::string &col) {
            if constexpr (agg_type == epoch_core::AggType::AllOf) {
              acc = acc && bars[col];
            } else if constexpr (agg_type == epoch_core::AggType::AnyOf ||
                                 agg_type == epoch_core::AggType::NoneOf) {
              acc = acc || bars[col];
            } else if constexpr (agg_type == epoch_core::AggType::Sum ||
                                 agg_type == epoch_core::AggType::Average) {
              acc = acc + bars[col];
            } else if constexpr (agg_type == epoch_core::AggType::IsEqual) {
              acc = acc && (bars[first] == bars[col]);
            } else if constexpr (agg_type == epoch_core::AggType::IsUnique) {
              acc = acc && (bars[first] != bars[col]);
            }

            else {
              throw std::runtime_error("Invalid aggregate type");
            }
            return acc;
          });

      if constexpr (agg_type == epoch_core::AggType::Average) {
        result = result / epoch_frame::Scalar{columns.size() * 1.0};
      }
      if constexpr (agg_type == epoch_core::AggType::NoneOf) {
        result = !result;
      }
    }
    return epoch_frame::make_dataframe(bars.index(), {result.array()},
                                       {GetOutputId()});
  }
};

using AllOfAggregateTransform = AggregateTransform<epoch_core::AggType::AllOf>;
using AnyOfAggregateTransform = AggregateTransform<epoch_core::AggType::AnyOf>;
using NoneOfAggregateTransform =
    AggregateTransform<epoch_core::AggType::NoneOf>;
using SumAggregateTransform = AggregateTransform<epoch_core::AggType::Sum>;
using AverageAggregateTransform =
    AggregateTransform<epoch_core::AggType::Average>;
using MinAggregateTransform = AggregateTransform<epoch_core::AggType::Min>;
using MaxAggregateTransform = AggregateTransform<epoch_core::AggType::Max>;
using AllEqualAggregateTransform =
    AggregateTransform<epoch_core::AggType::IsEqual>;
using AllUniqueAggregateTransform =
    AggregateTransform<epoch_core::AggType::IsUnique>;
} // namespace epoch_script::transform
