# Task 14: Rolling Portfolio Optimizers

## Objective
Implement rolling/walk-forward versions of HRP, MVO, and Risk Parity.

## Prerequisites
- Task 07 (HRP)
- Task 10 (MVO)
- Task 11 (Risk Parity)

## Rolling Window Pattern

**Existing Pattern:** `src/transforms/components/ml/rolling_ml_base.h`

**Concept:**
1. At each rebalance point, use trailing `window_size` observations
2. Compute optimal weights using chosen optimizer
3. Apply weights to next `step_size` periods
4. Output time-varying weight columns

## Base Class

### `src/transforms/components/portfolio/rolling/rolling_portfolio_base.h`

```cpp
template<typename Derived>
class RollingPortfolioBase : public ITransform {
public:
    explicit RollingPortfolioBase(const TransformConfiguration& cfg)
        : ITransform(cfg) {
        m_window_size = cfg.GetOptionValue("window_size", 252).GetInteger();
        m_step_size = cfg.GetOptionValue("step_size", 21).GetInteger();  // Monthly
    }

    DataFrame TransformData(const DataFrame& df) const override {
        // Use RollingWindowIterator pattern
        RollingWindowIterator iterator(n_rows, m_window_size, m_step_size);

        // For each window:
        iterator.ForEach([&](const WindowSpec& window) {
            arma::mat train_returns = GetWindowReturns(window);
            arma::vec weights = static_cast<const Derived&>(*this)
                .OptimizeWindow(train_returns);

            // Fill prediction period with these weights
            FillWeights(weights, window.predict_start, window.predict_end);
        });

        return BuildOutput();
    }

protected:
    size_t m_window_size;
    size_t m_step_size;
};
```

## Rolling Implementations

### `rolling/rolling_hrp.h`
```cpp
class RollingHRPOptimizer : public RollingPortfolioBase<RollingHRPOptimizer> {
    arma::vec OptimizeWindow(const arma::mat& returns) const {
        HRPOptimizer static_hrp(GetConfig());
        return static_hrp.Optimize(returns, ...);
    }
};
```

### `rolling/rolling_mean_variance.h`
```cpp
class RollingMeanVarianceOptimizer : public RollingPortfolioBase<RollingMeanVarianceOptimizer> {
    arma::vec OptimizeWindow(const arma::mat& returns) const {
        MeanVarianceOptimizer static_mvo(GetConfig());
        return static_mvo.Optimize(returns, ...);
    }
};
```

### `rolling/rolling_risk_parity.h`
```cpp
class RollingRiskParityOptimizer : public RollingPortfolioBase<RollingRiskParityOptimizer> {
    arma::vec OptimizeWindow(const arma::mat& returns) const {
        RiskParityOptimizer static_rp(GetConfig());
        return static_rp.Optimize(returns, ...);
    }
};
```

## Metadata Options (Common)
- `window_size`: 252 (default, ~1 year)
- `step_size`: 21 (default, ~monthly rebalance)
- `window_type`: rolling (default), expanding
- Plus all options from base optimizer

## Output Format
- Same columns as static optimizer (weight_asset1, weight_asset2, ...)
- Values change at each rebalance point
- NaN for initial warm-up period

## Deliverables
- [ ] `rolling_portfolio_base.h` - CRTP base class
- [ ] `rolling_hrp.h` - Rolling HRP
- [ ] `rolling_mean_variance.h` - Rolling MVO
- [ ] `rolling_risk_parity.h` - Rolling Risk Parity
- [ ] Metadata and registration
- [ ] Integration tests with walk-forward validation
