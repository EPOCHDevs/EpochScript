//
// Fractional Differentiation (FFD - Fixed-Width Window Fracdiff)
// Based on "Advances in Financial Machine Learning" by Marcos López de Prado
// Reference: https://www.risklab.ai/research/financial-data-science/fractional_differentiation
//

#pragma once
#include "../common_utils.h"
#include "epoch_frame/common.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/scalar.h"
#include <epoch_script/transforms/core/itransform.h>

#include <cmath>
#include <vector>

namespace epoch_script::transform {

namespace frac_diff_detail {

// Compute FFD weights using the recurrence relation:
// w[k] = -w[k-1] * (d - k + 1) / k, where w[0] = 1
// Returns weights until they fall below threshold
inline std::vector<double> compute_ffd_weights(double d, double threshold,
                                                size_t max_size = 10000) {
  std::vector<double> weights;
  weights.reserve(std::min(max_size, size_t(1000)));

  double w = 1.0;
  weights.push_back(w);

  for (size_t k = 1; k < max_size; ++k) {
    w = -w * (d - static_cast<double>(k) + 1.0) / static_cast<double>(k);
    if (std::abs(w) < threshold) {
      break;
    }
    weights.push_back(w);
  }

  return weights;
}

} // namespace frac_diff_detail

// Fixed-Width Window Fractional Differentiation (FFD)
// Processes time-series to be stationary while preserving memory
// Input: price series (typically log prices)
// Options: d (differentiation order), threshold (weight cutoff)
// Output: fractionally differentiated series
class FracDiff final : public ITransform {
public:
  explicit FracDiff(const TransformConfiguration &config)
      : ITransform(config),
        m_d(config.GetOptionValue("d").GetDecimal()),
        m_threshold(config.GetOptionValue("threshold").GetDecimal()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    // Compute weights on-the-fly (stateless)
    const auto weights = frac_diff_detail::compute_ffd_weights(m_d, m_threshold);
    const size_t window = weights.size();

    const Series input = df[GetInputId()];

    // Apply FFD using rolling window
    // X̃[t] = Σ(k=0 to l*) w[k] * X[t-k]
    const Series result =
        input.rolling_apply({.window_size = static_cast<int64_t>(window),
                             .min_periods = static_cast<int64_t>(window)})
            .apply([&weights, window](const Series &win) {
              const auto arr = win.contiguous_array();
              const auto view = arr.to_view<double>();

              const auto len = static_cast<size_t>(view->length());
              if (len < window) {
                return Scalar{std::numeric_limits<double>::quiet_NaN()};
              }

              double sum = 0.0;
              const size_t last_idx = len - 1;

              for (size_t k = 0; k < window && k <= last_idx; ++k) {
                double val = view->GetView(last_idx - k);
                if (std::isnan(val)) {
                  return Scalar{std::numeric_limits<double>::quiet_NaN()};
                }
                sum += weights[k] * val;
              }

              return Scalar{sum};
            });

    return result.to_frame(GetOutputId("result"));
  }

private:
  double m_d;
  double m_threshold;
};

} // namespace epoch_script::transform