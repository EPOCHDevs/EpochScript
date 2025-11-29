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

#include <vector>

namespace epoch_script::transform {

// Rolling correlation (Pearson) using HMDF CorrVisitor
// Inputs: x, y
// Options: window
// Outputs: correlation
class RollingCorr final : public ITransform {
public:
  explicit RollingCorr(const TransformConfiguration &config)
      : ITransform(config),
        m_window(config.GetOptionValue("window").GetInteger()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    const Series x = df[GetInputId("x")];
    const Series y = df[GetInputId("y")];

    const Series correlation =
        x.rolling_apply({.window_size = m_window}).apply([&](const Series &xw) {
          const Series yw = y.loc(xw.index());

          // Use Pearson correlation visitor
          hmdf::CorrVisitor<double, int64_t> visitor(
              hmdf::correlation_type::pearson,
              false,  // biased = false (use n-1 for sample correlation)
              true,   // skip_nan = true
              false   // stable_algo = false
          );

          const SeriesSpan<> xs{xw};
          const SeriesSpan<> ys{yw};
          run_visit(xw, visitor, xs, ys);

          return Scalar{visitor.get_result()};
        });

    return correlation.to_frame(GetOutputId("result"));
  }

private:
  int64_t m_window;
};

} // namespace epoch_script::transform
