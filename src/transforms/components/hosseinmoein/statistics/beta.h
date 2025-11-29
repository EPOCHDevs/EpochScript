//
// Created by Claude Code on 11/5/25.
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

// Rolling beta calculation
// Beta = Cov(asset, market) / Var(market)
// Inputs: asset_returns, market_returns
// Options: window
// Outputs: beta
class Beta final : public ITransform {
public:
  explicit Beta(const TransformConfiguration &config)
      : ITransform(config),
        m_window(config.GetOptionValue("window").GetInteger()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    const Series asset_returns = df[GetInputId("asset_returns")];
    const Series market_returns = df[GetInputId("market_returns")];

    // Calculate rolling beta using BetaVisitor
    // Beta = Cov(asset, market) / Var(market)
    const Series beta =
        asset_returns.rolling_apply({.window_size = m_window})
            .apply([&](const Series &asset_window) {
              const Series market_window = market_returns.loc(asset_window.index());

              // Use beta visitor which calculates cov/var internally
              hmdf::BetaVisitor<double, int64_t> visitor(
                  false,  // biased = false (use n-1 for sample statistics)
                  true,   // skip_nan = true
                  false   // stable_algo = false
              );

              const SeriesSpan<> asset_span{asset_window};
              const SeriesSpan<> market_span{market_window};
              run_visit(asset_window, visitor, asset_span, market_span);

              return Scalar{visitor.get_result()};
            });

    return beta.to_frame(GetOutputId("result"));
  }

private:
  int64_t m_window;
};

} // namespace epoch_script::transform
