//
// Engle-Granger Two-Step Cointegration Transform
// Tests for cointegration between two price series
//

#pragma once
#include "../common_utils.h"
#include "mackinnon_tables.h"
#include "epoch_frame/common.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/scalar.h"
#include <epoch_script/transforms/core/itransform.h>
#include <DataFrame/DataFrameFinancialVisitors.h>

#include <cmath>
#include <vector>

namespace epoch_script::transform {

/**
 * Engle-Granger Two-Step Cointegration Transform
 *
 * Tests for cointegration between two price series using the Engle-Granger
 * two-step procedure:
 *   Step 1: OLS regression y = alpha + beta*x + residuals (find hedge ratio)
 *   Step 2: ADF test on residuals (test for stationarity of spread)
 *
 * The null hypothesis H0: No cointegration (residuals have unit root)
 * Reject H0 (conclude cointegrated) if ADF statistic < critical value
 *
 * Note: Uses cointegration-specific critical values (MacKinnon 2010) which
 * are more stringent than standard ADF critical values.
 *
 * Inputs:
 *   - y: Dependent variable (e.g., price of asset A)
 *   - x: Independent variable (e.g., price of asset B)
 *
 * Options:
 *   - window: Rolling window size (default 60)
 *   - adf_lag: Number of lags for ADF test on residuals (default 1)
 *   - significance: Significance level for is_cointegrated (default 0.05)
 *
 * Outputs:
 *   - hedge_ratio: OLS beta coefficient (y = alpha + beta*x)
 *   - intercept: OLS alpha coefficient
 *   - spread: Residual series (y - alpha - beta*x)
 *   - adf_stat: ADF test statistic on residuals
 *   - p_value: Approximate p-value (using cointegration critical values)
 *   - critical_1pct: 1% critical value for cointegration
 *   - critical_5pct: 5% critical value for cointegration
 *   - critical_10pct: 10% critical value for cointegration
 *   - is_cointegrated: Boolean, true if cointegrated at significance level
 */
class EngleGranger final : public ITransform {
public:
  explicit EngleGranger(const TransformConfiguration &config)
      : ITransform(config),
        m_window(config.GetOptionValue("window").GetInteger()),
        m_adfLag(config.GetOptionValue("adf_lag").GetInteger()),
        m_significance(config.GetOptionValue("significance").GetDecimal()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    const Series y = df[GetInputId("y")];
    const Series x = df[GetInputId("x")];

    // Determine which critical value to use for is_cointegrated
    int sig_idx = 1; // Default 5%
    if (std::abs(m_significance - 0.01) < 1e-6) {
      sig_idx = 0;
    } else if (std::abs(m_significance - 0.10) < 1e-6) {
      sig_idx = 2;
    }

    auto driver_df = make_dataframe(df.index(), {x.array(), y.array()},
                                    {GetInputId("x"), GetInputId("y")});

    auto out_df = driver_df.rolling_apply({.window_size = m_window})
                      .apply([&](const DataFrame &win) {
                        const Series xw = win[GetInputId("x")];
                        const Series yw = win[GetInputId("y")];

                        // Step 1: OLS regression y = alpha + beta*x
                        hmdf::linfit_v<double, int64_t> fit_visitor;
                        const SeriesSpan<> xs{xw};
                        const SeriesSpan<> ys{yw};
                        run_visit(xw, fit_visitor, xs, ys);

                        double beta = fit_visitor.get_slope();
                        double alpha = fit_visitor.get_intercept();

                        // Compute spread (residual at end of window)
                        auto y_arr = yw.contiguous_array().template to_view<double>();
                        auto x_arr = xw.contiguous_array().template to_view<double>();
                        size_t n = xw.size();
                        double last_y = y_arr->raw_values()[n - 1];
                        double last_x = x_arr->raw_values()[n - 1];
                        double spread = last_y - alpha - beta * last_x;

                        // Step 2: ADF test on residuals
                        std::vector<double> residuals(n);
                        for (size_t i = 0; i < n; ++i) {
                          residuals[i] = y_arr->raw_values()[i] - alpha -
                                         beta * x_arr->raw_values()[i];
                        }

                        double tau = ComputeADFStatistic(residuals, m_adfLag);

                        // Get cointegration critical values (n_vars = 2 for Engle-Granger)
                        size_t T = n;
                        auto cvs =
                            mackinnon::CointegrationCriticalValues::get_all_critical_values(T, 2);

                        double pval =
                            mackinnon::CointegrationCriticalValues::get_pvalue(tau, T, 2);

                        bool cointegrated = tau < cvs[sig_idx];

                        auto win_idx = factory::index::make_datetime_index(
                            {xw.index()->at(-1).to_datetime()}, "", "UTC");
                        return make_dataframe(
                            win_idx,
                            {factory::array::make_array(std::vector<double>{beta}),
                             factory::array::make_array(std::vector<double>{alpha}),
                             factory::array::make_array(std::vector<double>{spread}),
                             factory::array::make_array(std::vector<double>{tau}),
                             factory::array::make_array(std::vector<double>{pval}),
                             factory::array::make_array(std::vector<double>{cvs[0]}),
                             factory::array::make_array(std::vector<double>{cvs[1]}),
                             factory::array::make_array(std::vector<double>{cvs[2]}),
                             factory::array::make_array(
                                 std::vector<int64_t>{cointegrated ? 1 : 0})},
                            {GetOutputId("hedge_ratio"), GetOutputId("intercept"),
                             GetOutputId("spread"), GetOutputId("adf_stat"),
                             GetOutputId("p_value"), GetOutputId("critical_1pct"),
                             GetOutputId("critical_5pct"),
                             GetOutputId("critical_10pct"),
                             GetOutputId("is_cointegrated")});
                      });

    // rolling_apply reindexes to the driver index, padding warm-up rows with nulls
    return out_df;
  }

private:
  int64_t m_window;
  int64_t m_adfLag;
  double m_significance;

  /**
   * Compute ADF test statistic on residuals
   * ADF regression: Δy_t = γ*y_{t-1} + sum(φ_i*Δy_{t-i}) + ε_t
   * Test statistic: t-stat for γ
   */
  static double ComputeADFStatistic(const std::vector<double> &y,
                                    int64_t lag) {
    size_t n = y.size();
    if (n < static_cast<size_t>(lag + 3)) {
      return std::numeric_limits<double>::quiet_NaN();
    }

    // Compute first differences
    std::vector<double> dy(n - 1);
    for (size_t i = 1; i < n; ++i) {
      dy[i - 1] = y[i] - y[i - 1];
    }

    // Build regression matrices for ADF
    // Δy_t = γ*y_{t-1} + sum(φ_i*Δy_{t-i}) + ε_t
    // Observations start at index (lag + 1) to have all lagged differences

    size_t start_idx = static_cast<size_t>(lag);
    size_t obs_count = dy.size() - start_idx;

    if (obs_count < 5) {
      return std::numeric_limits<double>::quiet_NaN();
    }

    // Number of regressors: 1 (y_{t-1}) + lag (lagged differences)
    size_t k = 1 + static_cast<size_t>(lag);

    // Build X matrix and Y vector
    std::vector<double> Y(obs_count);
    std::vector<std::vector<double>> X(obs_count, std::vector<double>(k));

    for (size_t t = 0; t < obs_count; ++t) {
      size_t idx = t + start_idx;
      Y[t] = dy[idx];

      // y_{t-1} (lagged level)
      X[t][0] = y[idx]; // y at time corresponding to dy[idx]

      // Lagged differences
      for (int64_t j = 1; j <= lag; ++j) {
        X[t][j] = dy[idx - j];
      }
    }

    // Simple OLS for gamma coefficient and its t-statistic
    // Using manual computation for clarity

    // X'X
    std::vector<std::vector<double>> XtX(k, std::vector<double>(k, 0.0));
    for (size_t i = 0; i < k; ++i) {
      for (size_t j = 0; j < k; ++j) {
        for (size_t t = 0; t < obs_count; ++t) {
          XtX[i][j] += X[t][i] * X[t][j];
        }
      }
    }

    // X'Y
    std::vector<double> XtY(k, 0.0);
    for (size_t i = 0; i < k; ++i) {
      for (size_t t = 0; t < obs_count; ++t) {
        XtY[i] += X[t][i] * Y[t];
      }
    }

    // Solve (X'X)^{-1} * X'Y for beta
    // For ADF, we only need gamma (first coefficient) and its standard error

    // Invert XtX using simple Gauss-Jordan for small matrices
    auto XtX_inv = InvertMatrix(XtX);
    if (XtX_inv.empty()) {
      return std::numeric_limits<double>::quiet_NaN();
    }

    // Compute beta = (X'X)^{-1} * X'Y
    std::vector<double> beta(k, 0.0);
    for (size_t i = 0; i < k; ++i) {
      for (size_t j = 0; j < k; ++j) {
        beta[i] += XtX_inv[i][j] * XtY[j];
      }
    }

    double gamma = beta[0];

    // Compute residuals and SSE
    double sse = 0.0;
    for (size_t t = 0; t < obs_count; ++t) {
      double fitted = 0.0;
      for (size_t j = 0; j < k; ++j) {
        fitted += X[t][j] * beta[j];
      }
      double resid = Y[t] - fitted;
      sse += resid * resid;
    }

    // Standard error of gamma
    double sigma2 = sse / (obs_count - k);
    double se_gamma = std::sqrt(sigma2 * XtX_inv[0][0]);

    if (se_gamma < 1e-10) {
      return std::numeric_limits<double>::quiet_NaN();
    }

    // ADF t-statistic
    return gamma / se_gamma;
  }

  /**
   * Simple matrix inversion using Gauss-Jordan elimination
   */
  static std::vector<std::vector<double>>
  InvertMatrix(const std::vector<std::vector<double>> &A) {
    size_t n = A.size();
    if (n == 0 || A[0].size() != n) {
      return {};
    }

    // Augmented matrix [A | I]
    std::vector<std::vector<double>> aug(n, std::vector<double>(2 * n, 0.0));
    for (size_t i = 0; i < n; ++i) {
      for (size_t j = 0; j < n; ++j) {
        aug[i][j] = A[i][j];
      }
      aug[i][n + i] = 1.0;
    }

    // Gauss-Jordan elimination
    for (size_t col = 0; col < n; ++col) {
      // Find pivot
      size_t pivot_row = col;
      double max_val = std::abs(aug[col][col]);
      for (size_t row = col + 1; row < n; ++row) {
        if (std::abs(aug[row][col]) > max_val) {
          max_val = std::abs(aug[row][col]);
          pivot_row = row;
        }
      }

      if (max_val < 1e-10) {
        return {}; // Singular matrix
      }

      // Swap rows
      if (pivot_row != col) {
        std::swap(aug[col], aug[pivot_row]);
      }

      // Scale pivot row
      double scale = aug[col][col];
      for (size_t j = 0; j < 2 * n; ++j) {
        aug[col][j] /= scale;
      }

      // Eliminate column
      for (size_t row = 0; row < n; ++row) {
        if (row != col) {
          double factor = aug[row][col];
          for (size_t j = 0; j < 2 * n; ++j) {
            aug[row][j] -= factor * aug[col][j];
          }
        }
      }
    }

    // Extract inverse
    std::vector<std::vector<double>> inv(n, std::vector<double>(n));
    for (size_t i = 0; i < n; ++i) {
      for (size_t j = 0; j < n; ++j) {
        inv[i][j] = aug[i][n + j];
      }
    }

    return inv;
  }
};

} // namespace epoch_script::transform
