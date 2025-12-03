//
// Created by adesola on 5/16/25.
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
#include <DataFrame/DataFrameFinancialVisitors.h>

#include <vector>

namespace epoch_script::transform {

// Rolling linear regression fit (y on x) using HMDF LinearFit visitor
// Inputs: x, y
// Options: window
// Outputs: fitted (back-of-window), residual (y - fitted)
class LinearFit final : public ITransform {
public:
  explicit LinearFit(const TransformConfiguration &config)
      : ITransform(config),
        m_window(config.GetOptionValue("window").GetInteger()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    const Series x = df[GetInputId("x")];
    const Series y = df[GetInputId("y")];

    // Create driver DataFrame for rolling_apply (same pattern as engle_granger)
    auto driver_df = make_dataframe(df.index(), {x.array(), y.array()},
                                    {GetInputId("x"), GetInputId("y")});

    auto out_df = driver_df.rolling_apply({.window_size = m_window})
                      .apply([&](const DataFrame &win) {
                        const Series xw = win[GetInputId("x")];
                        const Series yw = win[GetInputId("y")];

                        hmdf::linfit_v<double, int64_t> visitor;
                        const SeriesSpan<> xs{xw};
                        const SeriesSpan<> ys{yw};
                        run_visit(xw, visitor, xs, ys);

                        double slope = visitor.get_slope();
                        double intercept = visitor.get_intercept();
                        double residual = visitor.get_residual();

                        auto win_idx = factory::index::make_datetime_index(
                            {xw.index()->at(-1).to_datetime()}, "", "UTC");
                        return make_dataframe(
                            win_idx,
                            {factory::array::make_array(std::vector<double>{slope}),
                             factory::array::make_array(std::vector<double>{intercept}),
                             factory::array::make_array(std::vector<double>{residual})},
                            {GetOutputId("slope"), GetOutputId("intercept"),
                             GetOutputId("residual")});
                      });

    // rolling_apply reindexes to the driver index, padding warm-up rows with nulls
    return out_df;
  }

private:
  int64_t m_window;
};

} // namespace epoch_script::transform
