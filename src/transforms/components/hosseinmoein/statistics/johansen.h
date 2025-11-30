//
// Johansen Cointegration Transform
// Multivariate cointegration test using Johansen's procedure
//

#pragma once
#include "../common_utils.h"
#include "../../statistics/dataframe_armadillo_utils.h"
#include "johansen_tables.h"
#include "epoch_frame/common.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/datetime.h"
#include "epoch_frame/factory/array_factory.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include "epoch_frame/factory/index_factory.h"
#include "epoch_frame/scalar.h"
#include <epoch_script/transforms/core/itransform.h>

#include <armadillo>
#include <cmath>
#include <vector>

namespace epoch_script::transform {

/**
 * Johansen Cointegration Test Transform
 *
 * Tests for cointegration among multiple time series using the Johansen
 * maximum likelihood procedure. Determines the cointegration rank and
 * estimates cointegrating vectors (beta).
 *
 * Template parameter N specifies the number of variables (2-5)
 *
 * Test Hypotheses:
 *   Trace test: H0: rank <= r vs H1: rank > r
 *   Max-eigenvalue test: H0: rank = r vs H1: rank = r + 1
 *
 * Inputs: asset_0, asset_1, ... asset_{N-1}
 *
 * Options:
 *   - window: Rolling window size (default 60)
 *   - lag_p: VAR lag order (default 1)
 *   - det_order: Deterministic specification (-1, 0, or 1, default 1)
 *   - significance: Significance level (default 0.05)
 *
 * Outputs (for N variables):
 *   - rank: Estimated cointegration rank
 *   - trace_stat_0 ... trace_stat_{N-1}: Trace test statistics
 *   - max_stat_0 ... max_stat_{N-1}: Max-eigenvalue test statistics
 *   - eigval_0 ... eigval_{N-1}: Eigenvalues
 *   - For N=2: beta_0, beta_1, spread (normalized cointegrating vector)
 */
template <size_t N_VARS>
class JohansenTransform final : public ITransform {
public:
  static_assert(N_VARS >= 2 && N_VARS <= 5, "Johansen supports 2-5 variables");

  explicit JohansenTransform(const TransformConfiguration &config)
      : ITransform(config),
        m_window(config.GetOptionValue("window").GetInteger()),
        m_lag_p(config.GetOptionValue("lag_p").GetInteger()),
        m_det_order(config.GetOptionValue("det_order").GetInteger()),
        m_significance(config
                           .GetOptionValue(
                               "significance",
                               epoch_script::MetaDataOptionDefinition{0.05})
                           .GetDecimal()) {
    // Validate det_order
    johansen::GetCaseIndex(m_det_order);
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    using namespace epoch_frame;

    // Collect all input series and build driver DataFrame
    std::vector<arrow::ChunkedArrayPtr> input_arrays;
    std::vector<std::string> input_names;
    for (size_t i = 0; i < N_VARS; ++i) {
      std::string name = GetInputId("asset_" + std::to_string(i));
      input_arrays.push_back(df[name].array());
      input_names.push_back(name);
    }

    auto driver_df = make_dataframe(df.index(), input_arrays, input_names);

    // Build output column names
    std::vector<std::string> out_names;
    out_names.push_back(GetOutputId("rank"));
    for (size_t i = 0; i < N_VARS; ++i) {
      out_names.push_back(GetOutputId("trace_stat_" + std::to_string(i)));
    }
    for (size_t i = 0; i < N_VARS; ++i) {
      out_names.push_back(GetOutputId("max_stat_" + std::to_string(i)));
    }
    for (size_t i = 0; i < N_VARS; ++i) {
      out_names.push_back(GetOutputId("eigval_" + std::to_string(i)));
    }
    for (size_t i = 0; i < N_VARS; ++i) {
      out_names.push_back(GetOutputId("beta_" + std::to_string(i)));
    }
    out_names.push_back(GetOutputId("spread"));

    auto out_df = driver_df.rolling_apply({.window_size = m_window})
                      .apply([&](const DataFrame &win) {
                        // Build Y matrix from window
                        arma::mat Y(win.num_rows(), N_VARS);
                        for (size_t i = 0; i < N_VARS; ++i) {
                          Series wi = win[input_names[i]];
                          auto arr = wi.contiguous_array().template to_view<double>();
                          for (size_t t = 0; t < win.num_rows(); ++t) {
                            Y(t, i) = arr->raw_values()[t];
                          }
                        }

                        // Run Johansen test
                        JohansenResult result = ComputeJohansen(Y, m_lag_p, m_det_order);

                        // Compute rank
                        int64_t rank = johansen::JohansenCriticalValues::estimate_rank_trace(
                            result.trace_stats, N_VARS, m_det_order, m_significance);

                        // Compute spread using first cointegrating vector (if rank >= 1)
                        double spread = std::numeric_limits<double>::quiet_NaN();
                        if (!result.beta.empty() && result.eigenvalues[0] > 1e-10) {
                          spread = 0.0;
                          for (size_t i = 0; i < N_VARS; ++i) {
                            spread += result.beta[i] * Y(Y.n_rows - 1, i);
                          }
                        }

                        // Build output arrays
                        std::vector<arrow::ChunkedArrayPtr> out_arrays;
                        out_arrays.push_back(factory::array::make_array(std::vector<int64_t>{rank}));

                        for (size_t i = 0; i < N_VARS; ++i) {
                          out_arrays.push_back(factory::array::make_array(
                              std::vector<double>{result.trace_stats[i]}));
                        }
                        for (size_t i = 0; i < N_VARS; ++i) {
                          out_arrays.push_back(factory::array::make_array(
                              std::vector<double>{result.max_stats[i]}));
                        }
                        for (size_t i = 0; i < N_VARS; ++i) {
                          out_arrays.push_back(factory::array::make_array(
                              std::vector<double>{result.eigenvalues[i]}));
                        }
                        for (size_t i = 0; i < N_VARS; ++i) {
                          out_arrays.push_back(factory::array::make_array(
                              std::vector<double>{result.beta.size() > i ? result.beta[i] : 0.0}));
                        }
                        out_arrays.push_back(factory::array::make_array(std::vector<double>{spread}));

                        auto win_idx = factory::index::make_datetime_index(
                            {win.index()->at(-1).to_datetime()}, "", "UTC");
                        return make_dataframe(win_idx, out_arrays, out_names);
                      });

    return out_df;
  }

private:
  int64_t m_window;
  int64_t m_lag_p;
  int m_det_order;
  double m_significance;

  struct JohansenResult {
    std::vector<double> trace_stats;
    std::vector<double> max_stats;
    std::vector<double> eigenvalues;
    std::vector<double> beta; // First cointegrating vector (normalized)
  };

  /**
   * Compute Johansen cointegration test
   *
   * VECM representation: ΔY_t = Π*Y_{t-1} + Γ_1*ΔY_{t-1} + ... + c + ε_t
   * where Π = αβ' captures the cointegration relationships
   *
   * Steps:
   * 1. Regress ΔY_t on ΔY_{t-1},...,ΔY_{t-p+1} and get residuals R0
   * 2. Regress Y_{t-1} on ΔY_{t-1},...,ΔY_{t-p+1} and get residuals R1
   * 3. Compute S00, S11, S01 matrices from residuals
   * 4. Solve eigenvalue problem |λS11 - S01'S00^{-1}S01| = 0
   * 5. Compute trace and max-eigenvalue statistics
   */
  JohansenResult ComputeJohansen(const arma::mat &Y, int64_t p,
                                 int det_order) const {
    JohansenResult result;
    result.trace_stats.resize(N_VARS, 0.0);
    result.max_stats.resize(N_VARS, 0.0);
    result.eigenvalues.resize(N_VARS, 0.0);
    result.beta.resize(N_VARS, 0.0);

    size_t T = Y.n_rows;
    size_t k = Y.n_cols; // Should equal N_VARS

    if (T < static_cast<size_t>(p + 3) || T < k + 5) {
      return result; // Not enough observations
    }

    // Compute first differences
    arma::mat dY = arma::diff(Y, 1, 0); // T-1 x k

    // Build lagged levels and differences
    // Y_{t-1} for t = p+1,...,T
    size_t n_obs = T - static_cast<size_t>(p) - 1;
    if (n_obs < k + 2) {
      return result;
    }

    // Z0 = ΔY_t (dependent variable)
    // Z1 = Y_{t-1} (lagged levels)
    // Z2 = [ΔY_{t-1}, ..., ΔY_{t-p+1}, constant] (other regressors)

    arma::mat Z0(n_obs, k);
    arma::mat Z1(n_obs, k);

    // Number of lagged difference regressors
    size_t n_lagged_diff = (p > 1) ? static_cast<size_t>(p - 1) * k : 0;
    size_t n_det = (det_order >= 0) ? 1 : 0; // Constant term
    size_t n_z2 = n_lagged_diff + n_det;

    arma::mat Z2;
    if (n_z2 > 0) {
      Z2.set_size(n_obs, n_z2);
    }

    for (size_t t = 0; t < n_obs; ++t) {
      size_t idx = t + static_cast<size_t>(p);

      // Z0: ΔY_t
      Z0.row(t) = dY.row(idx);

      // Z1: Y_{t-1}
      Z1.row(t) = Y.row(idx);

      // Z2: lagged differences and constant
      if (n_z2 > 0) {
        size_t col = 0;
        for (int64_t lag = 1; lag < p; ++lag) {
          for (size_t j = 0; j < k; ++j) {
            Z2(t, col++) = dY(idx - lag, j);
          }
        }
        if (det_order >= 0) {
          Z2(t, col) = 1.0; // Constant
        }
      }
    }

    // Residual regression: project Z0 and Z1 onto orthogonal complement of Z2
    arma::mat R0, R1;

    if (n_z2 > 0 && Z2.n_cols > 0) {
      // M = I - Z2(Z2'Z2)^{-1}Z2'
      arma::mat Z2tZ2_inv;
      if (!arma::inv_sympd(Z2tZ2_inv, Z2.t() * Z2)) {
        // Fallback to pseudo-inverse
        Z2tZ2_inv = arma::pinv(Z2.t() * Z2);
      }

      arma::mat M = arma::eye(n_obs, n_obs) - Z2 * Z2tZ2_inv * Z2.t();
      R0 = M * Z0;
      R1 = M * Z1;
    } else {
      R0 = Z0;
      R1 = Z1;
    }

    // Compute moment matrices
    arma::mat S00 = R0.t() * R0 / n_obs;
    arma::mat S11 = R1.t() * R1 / n_obs;
    arma::mat S01 = R0.t() * R1 / n_obs;
    arma::mat S10 = R1.t() * R0 / n_obs;

    // Solve generalized eigenvalue problem
    // |λS11 - S01'S00^{-1}S01| = 0

    arma::mat S00_inv;
    if (!arma::inv_sympd(S00_inv, S00)) {
      S00_inv = arma::pinv(S00);
    }

    arma::mat S11_inv;
    if (!arma::inv_sympd(S11_inv, S11)) {
      S11_inv = arma::pinv(S11);
    }

    // Form the matrix for eigenvalue problem: S11^{-1} * S10 * S00^{-1} * S01
    arma::mat A = S11_inv * S10 * S00_inv * S01;

    // Compute eigenvalues and eigenvectors
    arma::vec eigval_real;
    arma::mat eigvec;

    // Use eigen decomposition
    arma::cx_vec eigval_cx;
    arma::cx_mat eigvec_cx;

    if (!arma::eig_gen(eigval_cx, eigvec_cx, A)) {
      return result; // Eigendecomposition failed
    }

    // Extract real parts and sort in descending order
    eigval_real = arma::real(eigval_cx);
    eigvec = arma::real(eigvec_cx);

    // Clamp eigenvalues to [0, 1) for stability
    for (size_t i = 0; i < eigval_real.n_elem; ++i) {
      eigval_real(i) = std::max(0.0, std::min(eigval_real(i), 1.0 - 1e-10));
    }

    // Sort eigenvalues in descending order
    arma::uvec sort_idx = arma::sort_index(eigval_real, "descend");
    eigval_real = eigval_real(sort_idx);
    eigvec = eigvec.cols(sort_idx);

    // Store eigenvalues
    for (size_t i = 0; i < k && i < N_VARS; ++i) {
      result.eigenvalues[i] = eigval_real(i);
    }

    // Compute trace statistics: -T * sum_{i=r+1}^{k} ln(1 - λ_i)
    for (size_t r = 0; r < k; ++r) {
      double trace = 0.0;
      for (size_t i = r; i < k; ++i) {
        if (eigval_real(i) > 1e-10 && eigval_real(i) < 1.0) {
          trace -= std::log(1.0 - eigval_real(i));
        }
      }
      result.trace_stats[r] = static_cast<double>(n_obs) * trace;
    }

    // Compute max-eigenvalue statistics: -T * ln(1 - λ_{r+1})
    for (size_t r = 0; r < k; ++r) {
      if (eigval_real(r) > 1e-10 && eigval_real(r) < 1.0) {
        result.max_stats[r] =
            -static_cast<double>(n_obs) * std::log(1.0 - eigval_real(r));
      }
    }

    // Extract first cointegrating vector (normalized so first element = 1)
    if (eigval_real(0) > 1e-10) {
      arma::vec beta_vec = eigvec.col(0);
      double normalizer = beta_vec(0);
      if (std::abs(normalizer) > 1e-10) {
        beta_vec /= normalizer;
      }
      for (size_t i = 0; i < k && i < N_VARS; ++i) {
        result.beta[i] = beta_vec(i);
      }
    }

    return result;
  }
};

// Specialized Johansen transforms for 2-5 variables
using Johansen2Transform = JohansenTransform<2>;
using Johansen3Transform = JohansenTransform<3>;
using Johansen4Transform = JohansenTransform<4>;
using Johansen5Transform = JohansenTransform<5>;

} // namespace epoch_script::transform
