# Task 11: Risk Parity Optimization

## Objective
Implement Risk Parity (Equal Risk Contribution) optimization.

## Prerequisites
- Task 01 (OSQP Setup)
- Task 03 (Covariance)
- Task 09 (Risk Measures)

## Risk Parity Definition

**Goal:** Each asset contributes equally to portfolio risk.

**Risk Contribution:**
```
RC_i = w_i * (Σw)_i / σ_p
where σ_p = sqrt(w'Σw)
```

**Target:**
```
RC_i = σ_p / N  for all i
```

## Algorithm Options

### 1. Newton-Raphson Iteration (Simpler)
```cpp
while (not converged) {
    sigma_w = cov * w;
    port_vol = sqrt(dot(w, sigma_w));
    rc = (w % sigma_w) / port_vol;
    target_rc = port_vol / N;

    grad = 2 * (sigma_w % (rc - target_rc)) / port_vol;
    w = w - step_size * grad;
    w = project_to_simplex(w);
}
```

### 2. OSQP Formulation (With Constraints)
Log-barrier formulation:
```
minimize    -b' * log(w)
subject to  Σw_i = 1
            w ≥ ε
```
where b is the risk budget vector (default: 1/N for each).

## File to Create

### `src/transforms/components/portfolio/optimizers/risk_parity.h`

```cpp
class RiskParityOptimizer : public PortfolioOptimizerBase<RiskParityOptimizer> {
public:
    arma::vec Optimize(const arma::mat& returns, const arma::mat& cov,
                       const arma::mat& corr, const arma::mat& dist) const {
        if (m_use_osqp) {
            return SolveOSQP(cov);
        } else {
            return SolveNewton(cov);
        }
    }

private:
    arma::vec m_risk_budget;  // Default: equal (1/N)
    int m_max_iterations{1000};
    double m_tolerance{1e-8};
};
```

## Metadata Options
- `risk_measure`: variance (default), cvar, mad
- `risk_budget`: custom budget vector (optional)
- `covariance_method`, `min_weight`, `max_weight`

## Cross-Validation
Compare against Riskfolio-Lib `Portfolio.rp_optimization()`:
- Tolerance: rtol=0.03, atol=0.015

## Deliverables
- [ ] `risk_parity.h` with Newton and OSQP methods
- [ ] Support for custom risk budgets
- [ ] Metadata and registration
- [ ] Unit tests with Riskfolio-Lib baseline
