//
// Created by adesola on 10/31/25.
//

#pragma once
#include "../common_utils.h"
#include "epoch_frame/common.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/scalar.h"
#include <epoch_script/transforms/core/itransform.h>
#include <DataFrame/DataFrameStatsVisitors.h>
#include <DataFrame/DataFrameTypes.h>

#include <vector>

namespace epoch_script::transform {

// Exponentially weighted moving correlation using HMDF ExponentiallyWeightedCorrVisitor
// Inputs: x, y
// Options: span (window span for EWM, >= 1)
// Outputs: correlation
class EWMCorr final : public ITransform {
public:
  explicit EWMCorr(const TransformConfiguration &config)
      : ITransform(config),
        m_span(config.GetOptionValue("span").GetInteger()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    const Series x = df[GetInputId("x")];
    const Series y = df[GetInputId("y")];

    // Use exponentially weighted correlation visitor
    // Using span decay spec: decay = 2 / (1 + span)
    hmdf::ExponentiallyWeightedCorrVisitor<double, int64_t> visitor(
        hmdf::exponential_decay_spec::span,
        static_cast<double>(m_span)
    );

    const SeriesSpan<> xs{x};
    const SeriesSpan<> ys{y};
    run_visit(x, visitor, xs, ys);

    // get_result() returns a vector
    const auto &corr_vec = visitor.get_result();

    return make_dataframe(
        df.index(),
        {factory::array::make_array(corr_vec)},
        {GetOutputId("result")}
    );
  }

private:
  int64_t m_span;
};

} // namespace epoch_script::transform
