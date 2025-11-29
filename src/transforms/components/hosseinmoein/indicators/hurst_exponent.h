//
// Created by adesola on 4/16/25.
//

#pragma once

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"
#include <DataFrame/DataFrameFinancialVisitors.h>
#pragma GCC diagnostic pop
#include "../common_utils.h"
#include <epoch_script/transforms/core/itransform.h>

#include <epoch_frame/factory/dataframe_factory.h>

using namespace epoch_frame;

namespace epoch_script::transform {
class HurstExponent final : public ITransform {
public:
  explicit HurstExponent(const TransformConfiguration &config)
      : ITransform(config),
        m_min_window(config.GetOptionValue("min_period").GetInteger()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    const auto series =
        df[GetInputId()]
            .expanding_apply({.min_periods = static_cast<double>(m_min_window)})
            .apply([&](Series const &x) {
              hmdf::HurstExponentVisitor<double> visitor({1, 2, 4, 8});
              const SeriesSpan span{x};
              run_visit(x, visitor, span);
              return Scalar{visitor.get_result()};
            });

    return series.to_frame(GetOutputId("result"));
  }

private:
  int64_t m_min_window;
};

class RollingHurstExponent final : public ITransform {
public:
  explicit RollingHurstExponent(const TransformConfiguration &config)
      : ITransform(config),
        m_window(config.GetOptionValue("window").GetInteger()),
        m_lagGrid(lagGrid(m_window)) {
    if (m_lagGrid.empty()) {
      SPDLOG_WARN("No lag grid found for window size {}", m_window);
      m_lagGrid = {1};
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    const auto series =
        df[GetInputId()]
            .rolling_apply({.window_size = m_window})
            .apply([&](Series const &x) {
              auto lagGrid = m_lagGrid;
              hmdf::HurstExponentVisitor<double> visitor{std::move(lagGrid)};
              const SeriesSpan<> span{x};
              run_visit(x, visitor, span);
              return Scalar{visitor.get_result()};
            });

    return series.to_frame(GetOutputId("result"));
  }

  static hmdf::HurstExponentVisitor<double>::RangeVec
  lagGrid(ino64_t w, int64_t base = 2, double maxFrac = 0.25) {
    const auto maxLag = static_cast<int64_t>(std::ceil(w * maxFrac));
    const auto kMax = static_cast<int64_t>(std::log(maxLag) / std::log(base));
    hmdf::HurstExponentVisitor<double>::RangeVec result;
    result.reserve(kMax);
    for (int64_t i = 0; i < kMax; ++i) {
      result.emplace_back(std::pow(base, i));
    }
    return result;
  }

private:
  int64_t m_window;
  hmdf::HurstExponentVisitor<double>::RangeVec m_lagGrid;
};
} // namespace epoch_script::transform