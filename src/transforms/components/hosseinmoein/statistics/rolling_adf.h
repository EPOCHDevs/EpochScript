 //
// Rolling ADF (Augmented Dickey-Fuller) Transform
// Stationarity test with p-values using MacKinnon critical values
//
// Uses custom Armadillo-based ADF implementation that matches statsmodels
//

#pragma once
#include "../common_utils.h"
#include "adf_core.h"
#include "mackinnon_tables.h"
#include "epoch_frame/common.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/scalar.h"
#include <epoch_script/transforms/core/itransform.h>

#include <cmath>
#include <vector>

// Deterministic term specification for ADF test
CREATE_ENUM(ADFDeterministic, nc, c, ct);

namespace epoch_script::transform {

using epoch_core::ADFDeterministic;

/**
 * Rolling ADF (Augmented Dickey-Fuller) Transform
 *
 * Tests for stationarity using the ADF test over a rolling window.
 * Provides test statistics, p-values, and critical values.
 *
 * The null hypothesis H0: series has a unit root (non-stationary)
 * Reject H0 (conclude stationary) if test statistic < critical value
 *
 * Inputs: SLOT (price/spread series to test)
 * Options:
 *   - window: Rolling window size (default 60)
 *   - adf_lag: Number of lags for ADF test (default 1)
 *   - deterministic: nc (none), c (constant), ct (constant+trend)
 *   - significance: Significance level for is_stationary (default 0.05)
 *
 * Outputs:
 *   - adf_stat: ADF test statistic (tau)
 *   - p_value: Approximate p-value
 *   - critical_1pct: 1% critical value
 *   - critical_5pct: 5% critical value
 *   - critical_10pct: 10% critical value
 *   - is_stationary: Boolean, true if stat < critical value at significance
 */
class RollingADF final : public ITransform {
public:
  explicit RollingADF(const TransformConfiguration &config)
      : ITransform(config),
        m_window(config.GetOptionValue("window").GetInteger()),
        m_adfLag(config.GetOptionValue("adf_lag").GetInteger()),
        m_deterministic(
            config.GetOptionValue("deterministic").GetSelectOption<ADFDeterministic>()),
        m_significance(config.GetOptionValue("significance").GetDecimal()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    const Series input = df[GetInputId()];

    // Determine which critical value to use for is_stationary
    int sig_idx = 1; // Default 5%
    if (std::abs(m_significance - 0.01) < 1e-6) {
      sig_idx = 0;
    } else if (std::abs(m_significance - 0.10) < 1e-6) {
      sig_idx = 2;
    }

    std::string det_str = epoch_core::ADFDeterministicWrapper::ToString(m_deterministic);

    auto driver_df = make_dataframe(df.index(), {input.array()}, {GetInputId()});

    auto out_df = driver_df.rolling_apply({.window_size = m_window})
                      .apply([&](const DataFrame &win) {
                        const Series w = win[GetInputId()];

                        // Extract values to vector for our ADF implementation
                        auto arr = w.contiguous_array().to_view<double>();
                        std::vector<double> values;
                        values.reserve(arr->length());
                        for (int64_t i = 0; i < arr->length(); ++i) {
                          values.push_back(arr->Value(i));
                        }

                        // Use our custom Armadillo-based ADF that matches statsmodels
                        auto result = adf::compute_adf(values, static_cast<int>(m_adfLag), det_str);
                        double tau = result.adf_stat;

                        // Get critical values from MacKinnon tables
                        size_t T = w.size();
                        auto cvs =
                            mackinnon::ADFCriticalValues::get_all_critical_values(T, det_str);

                        // Compute p-value
                        double pval = mackinnon::ADFCriticalValues::get_pvalue(tau, T, det_str);

                        // Determine stationarity at chosen significance level
                        bool stationary = tau < cvs[sig_idx];

                        auto win_idx = factory::index::make_datetime_index(
                            {w.index()->at(-1).to_datetime()}, "", "UTC");
                        return make_dataframe(
                            win_idx,
                            {factory::array::make_array(std::vector<double>{tau}),
                             factory::array::make_array(std::vector<double>{pval}),
                             factory::array::make_array(std::vector<double>{cvs[0]}),
                             factory::array::make_array(std::vector<double>{cvs[1]}),
                             factory::array::make_array(std::vector<double>{cvs[2]}),
                             factory::array::make_array(
                                 std::vector<int64_t>{stationary ? 1 : 0})},
                            {GetOutputId("adf_stat"), GetOutputId("p_value"),
                             GetOutputId("critical_1pct"), GetOutputId("critical_5pct"),
                             GetOutputId("critical_10pct"), GetOutputId("is_stationary")});
                      });

    return out_df;
  }

private:
  int64_t m_window;
  int64_t m_adfLag;
  ADFDeterministic m_deterministic;
  double m_significance;
};

} // namespace epoch_script::transform
