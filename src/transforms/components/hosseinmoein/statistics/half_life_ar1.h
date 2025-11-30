//
// Half-Life AR(1) Transform
// Estimates mean-reversion speed from AR(1) coefficient
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

#include <cmath>
#include <vector>

namespace epoch_script::transform {

/**
 * Rolling Half-Life AR(1) Transform
 *
 * Estimates the half-life of mean reversion from an AR(1) model:
 *   y_t = phi * y_{t-1} + epsilon_t
 *
 * Half-life formula: HL = -log(2) / log(phi)
 *
 * For a mean-reverting series (0 < phi < 1), the half-life indicates
 * how many periods it takes for deviations to decay by half.
 *
 * Inputs: SLOT (spread or price series to test for mean reversion)
 * Options: window (rolling window size, default 60)
 * Outputs:
 *   - half_life: Mean reversion half-life in periods
 *   - ar1_coef: AR(1) coefficient (phi)
 *   - is_mean_reverting: Boolean, true if 0 < phi < 1
 */
class HalfLifeAR1 final : public ITransform {
public:
  explicit HalfLifeAR1(const TransformConfiguration &config)
      : ITransform(config),
        m_window(config.GetOptionValue("window").GetInteger()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    const Series input = df[GetInputId()];

    auto driver_df = make_dataframe(df.index(), {input.array()}, {GetInputId()});

    auto out_df = driver_df.rolling_apply({.window_size = m_window})
                      .apply([&](const DataFrame &win) {
                        const Series w = win[GetInputId()];

                        auto arr = w.contiguous_array().template to_view<double>();
                        const double *data = arr->raw_values();
                        size_t n = w.size();

                        double phi = std::numeric_limits<double>::quiet_NaN();
                        double hl = std::numeric_limits<double>::quiet_NaN();
                        int64_t is_mr = 0;

                        if (n >= 3) {
                          double sum_y = 0.0, sum_y_lag = 0.0;
                          double sum_yy = 0.0, sum_y_lag_sq = 0.0, sum_cross = 0.0;
                          size_t count = n - 1;

                          for (size_t i = 1; i < n; ++i) {
                            double y_t = data[i];
                            double y_lag = data[i - 1];

                            if (std::isnan(y_t) || std::isnan(y_lag)) {
                              continue;
                            }

                            sum_y += y_t;
                            sum_y_lag += y_lag;
                            sum_yy += y_t * y_t;
                            sum_y_lag_sq += y_lag * y_lag;
                            sum_cross += y_t * y_lag;
                          }

                          double mean_y = sum_y / count;
                          double mean_y_lag = sum_y_lag / count;

                          double cov = (sum_cross / count) - mean_y * mean_y_lag;
                          double var_lag = (sum_y_lag_sq / count) - mean_y_lag * mean_y_lag;

                          phi = (var_lag > 1e-10) ? cov / var_lag : 0.0;

                          is_mr = (phi > 0.0 && phi < 1.0) ? 1 : 0;

                          if (is_mr && phi > 1e-10) {
                            hl = -std::log(2.0) / std::log(phi);
                            hl = std::min(hl, static_cast<double>(m_window * 10));
                          }
                        }

                        auto win_idx = factory::index::make_datetime_index(
                            {w.index()->at(-1).to_datetime()}, "", "UTC");
                        return make_dataframe(
                            win_idx,
                            {factory::array::make_array(std::vector<double>{hl}),
                             factory::array::make_array(std::vector<double>{phi}),
                             factory::array::make_array(std::vector<int64_t>{is_mr})},
                            {GetOutputId("half_life"), GetOutputId("ar1_coef"),
                             GetOutputId("is_mean_reverting")});
                      });

    return out_df;
  }

private:
  int64_t m_window;
};

} // namespace epoch_script::transform
