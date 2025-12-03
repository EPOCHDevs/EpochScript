# Task 09: Risk Measures Suite

## Objective
Implement 10+ risk measures for portfolio optimization and risk budgeting.

## Prerequisites
- Task 02 (Foundation)

## Risk Measures to Implement

| Measure | Formula | Empyrical Reuse |
|---------|---------|-----------------|
| Variance | w'Σw | No |
| MAD | E[|r - μ|] | No |
| CVaR | E[r | r < VaR_α] | Yes: `ConditionalValueAtRisk` |
| EVaR | Entropic VaR | No |
| CDaR | E[DD | DD > DD_α] | No |
| Sortino | σ_downside | Yes: `DownsideRisk` |
| Kurtosis | E[(r-μ)⁴]/σ⁴ | Yes: `Kurtosis` |
| Skewness | E[(r-μ)³]/σ³ | Yes: `Skew` |
| MaxDrawdown | max(DD) | Yes: `MaxDrawDown` |
| WR | min(r) | No |

## Interface

```cpp
// risk/risk_measure.h
class IRiskMeasure {
    virtual double ComputeRisk(const arma::vec& weights,
                               const arma::mat& returns,
                               const arma::mat* cov = nullptr) const = 0;

    virtual arma::vec ComputeRiskContribution(
        const arma::vec& weights,
        const arma::mat& returns,
        const arma::mat* cov = nullptr) const = 0;
};

class RiskMeasureFactory {
    static std::unique_ptr<IRiskMeasure> Create(RiskMeasure measure, double alpha = 0.05);
};
```

## Files to Create

```
src/transforms/components/portfolio/risk/
├── risk_measure.h           # Interface + enum
├── variance_risk.h          # w'Σw
├── mad_risk.h               # Mean Absolute Deviation
├── cvar_risk.h              # Uses empyrical::ConditionalValueAtRisk
├── evar_risk.h              # Entropic VaR
├── cdar_risk.h              # Conditional Drawdown at Risk
├── sortino_risk.h           # Uses empyrical::DownsideRisk
├── kurtosis_risk.h          # Uses empyrical::Kurtosis
├── max_drawdown_risk.h      # Uses empyrical::MaxDrawDown
├── worst_realization_risk.h # min(returns)
└── risk_factory.h           # Factory
```

## Deliverables
- [ ] All risk measure implementations
- [ ] Risk factory
- [ ] Unit tests for each measure
- [ ] Integration with empyrical where applicable
