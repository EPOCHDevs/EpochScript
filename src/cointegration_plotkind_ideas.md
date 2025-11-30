# Cointegration Visualization: PlotKind Ideas

## Current PlotKind System

### How It Works

Every transform has an optional `plotKind` that determines how outputs are rendered on TradingView-style charts.

```cpp
struct TransformsMetaData {
  TransformPlotKind plotKind;  // e.g., line, macd, bbands, ichimoku
  // ...
};
```

**Architecture**:
1. **PlotKind Enum**: 46 predefined visualization types
2. **Builder Registry**: Maps enum → builder class
3. **Builder Output**: Semantic field mapping (e.g., "signal" → actual column name)
4. **Renderer**: Consumes builder output for chart rendering

### Available PlotKind Categories

| Category | Examples | Axis Behavior |
|----------|----------|---------------|
| Single Line | `line`, `rsi`, `atr` | Own axis (panel below) |
| Multi-Line | `macd`, `stoch`, `aroon` | Own axis + multiple series |
| Bands | `bbands`, `keltner`, `donchian` | Overlay on price |
| Cloud/Fill | `ichimoku` | Overlay with filled regions |
| Patterns | `consolidation_box`, `double_top` | Overlay shapes |
| Events | `flag`, `zone` | Markers on chart |
| ML/Stats | `hmm` | Dynamic output count |

### Key Builder Methods

```cpp
class IPlotKindBuilder {
  // Map semantic names to actual columns
  virtual std::unordered_map<std::string, std::string> Build(config);

  // Validation
  virtual bool Validate(config);

  // Rendering layer (0=bottom, 100=top)
  virtual int GetZIndex();

  // Needs separate panel below price chart?
  virtual bool RequiresOwnAxis();

  // Default thresholds, colors
  virtual ConfigOptions GetDefaultConfigOptions();
};
```

---

## Cointegration Visualization Needs

### Outputs to Visualize

| Output | Best Visualization | Notes |
|--------|-------------------|-------|
| `spread` | Line in own panel | Like price, can be positive/negative |
| `zscore` | Line with bands | ±1, ±2, ±3 std dev levels |
| `hedge_ratio` | Line (slowly changing) | Should be stable if cointegrated |
| `half_life` | Numeric card or line | Bars to mean reversion |
| `p_value` | Color-coded line | Green < 0.05, red > 0.10 |
| `is_cointegrated` | Zone highlight | Boolean - shade background |
| `entry_long/short` | Flag markers | Entry/exit signals |

---

## Proposed PlotKinds for Cointegration

### 1. `spread_zscore` - Primary Visualization

**Purpose**: Show spread with z-score normalization and trading bands.

```
┌────────────────────────────────────────────────┐
│ Price Chart (AAPL vs SPY)                      │
│ ════════════════════════════════════════════   │
│  Candlesticks...                               │
├────────────────────────────────────────────────┤
│ Spread Z-Score                                 │
│ ────────────────                               │
│     +2 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ (short entry)│
│     +1 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─              │
│  ~~~0~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~(mean)     │
│     -1 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─              │
│     -2 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ (long entry) │
│                                                │
│  [Z-score line oscillates around 0]            │
└────────────────────────────────────────────────┘
```

**Builder Spec**:
```cpp
class SpreadZScoreBuilder : public IPlotKindBuilder {
  std::unordered_map<std::string, std::string> Build(config) {
    return {
      {"zscore", config.GetOutputId("zscore").GetColumnName()},
      {"upper_2", "+2.0"},  // Static threshold
      {"upper_1", "+1.0"},
      {"mean", "0.0"},
      {"lower_1", "-1.0"},
      {"lower_2", "-2.0"}
    };
  }

  bool RequiresOwnAxis() { return true; }
  int GetZIndex() { return 50; }

  ConfigOptions GetDefaultConfigOptions() {
    return {
      {"entry_threshold", 2.0},
      {"exit_threshold", 0.5},
      {"line_color", "blue"},
      {"band_color", "gray"}
    };
  }
};
```

**Registration**:
```cpp
// metadata.h - add to enum
spread_zscore,

// registry.cpp
REGISTER_PLOT_KIND(spread_zscore, SpreadZScoreBuilder);
```

---

### 2. `cointegration_status` - Regime Indicator

**Purpose**: Show cointegration validity with background shading.

```
┌────────────────────────────────────────────────┐
│ Price Chart                                    │
│ [Green background when cointegrated]           │
│ [Red background when not cointegrated]         │
│                                                │
│  ████████████░░░░░░░░████████████░░░░░░░░░░    │
│  ↑ coint    ↑ not    ↑ coint    ↑ not          │
├────────────────────────────────────────────────┤
│ P-Value                                        │
│     0.10 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ (threshold)  │
│     0.05 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─              │
│  ~~~~~~~~~~~~~~~~~~~~(p-value line)            │
└────────────────────────────────────────────────┘
```

**Builder Spec**:
```cpp
class CointegrationStatusBuilder : public IPlotKindBuilder {
  std::unordered_map<std::string, std::string> Build(config) {
    return {
      {"is_cointegrated", config.GetOutputId("is_cointegrated").GetColumnName()},
      {"p_value", config.GetOutputId("p_value").GetColumnName()},
      {"threshold_5pct", "0.05"},
      {"threshold_10pct", "0.10"}
    };
  }

  bool RequiresOwnAxis() { return true; }  // For p-value line

  ConfigOptions GetDefaultConfigOptions() {
    return {
      {"cointegrated_color", "rgba(0,255,0,0.1)"},
      {"not_cointegrated_color", "rgba(255,0,0,0.1)"},
      {"show_background", true}
    };
  }
};
```

---

### 3. `pairs_signals` - Entry/Exit Flags

**Purpose**: Mark entry and exit points on the chart.

```
┌────────────────────────────────────────────────┐
│ Price Chart                                    │
│                                                │
│     🔵 LONG          🔴 SHORT                   │
│      ↓                ↓                        │
│  ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~          │
│          ↑                ↑                    │
│         ⬜ EXIT          ⬜ EXIT                │
│                                                │
└────────────────────────────────────────────────┘
```

**Leverage Existing Flag Builder**:
```cpp
// Use existing flag builder with template substitution
.plotKind = flag,
.flagSchema = FlagSchema {
  .icon = "trending-up",  // or "trending-down"
  .color = SemanticColor::success,  // or danger
  .textTemplate = "{signal_type} @ Z={zscore:.2f}"
}
```

---

### 4. `hedge_ratio_stability` - Coefficient Tracking

**Purpose**: Monitor hedge ratio stability over time.

```
┌────────────────────────────────────────────────┐
│ Hedge Ratio                                    │
│                                                │
│  1.5 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─               │
│  1.2 ════════════════════════════ (mean)       │
│  1.0 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─               │
│  ~~~~~~~~~~~(hedge ratio line)~~~~~            │
│  0.8 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─               │
│                                                │
│  [Shaded band shows ±1 std dev]                │
└────────────────────────────────────────────────┘
```

**Builder Spec**:
```cpp
class HedgeRatioBuilder : public IPlotKindBuilder {
  std::unordered_map<std::string, std::string> Build(config) {
    return {
      {"hedge_ratio", config.GetOutputId("hedge_ratio").GetColumnName()},
      {"rolling_mean", config.GetOutputId("hedge_ratio_mean").GetColumnName()},
      {"upper_band", config.GetOutputId("hedge_ratio_upper").GetColumnName()},
      {"lower_band", config.GetOutputId("hedge_ratio_lower").GetColumnName()}
    };
  }

  bool RequiresOwnAxis() { return true; }
};
```

---

### 5. `half_life_indicator` - Mean Reversion Speed

**Purpose**: Show expected time to mean reversion.

```
┌────────────────────────────────────────────────┐
│ Half-Life (bars)                               │
│                                                │
│  50 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─                 │
│  ~~~~~~~~~~~~~~~(half-life line)~~~            │
│  20 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ (ideal range)  │
│  10 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─                 │
│                                                │
│  [Color: green if 5-30, yellow if 30-60, red]  │
└────────────────────────────────────────────────┘
```

---

## Composite Dashboard: `cointegration_dashboard`

For comprehensive pairs analysis, use **Dashboard** instead of PlotKind:

```cpp
std::optional<epoch_core::Dashboard> GetDashboard() const override {
  DashboardBuilder builder;

  // Row 1: Pair identification
  builder.AddRow()
    .AddCard("Pair", "AAPL / SPY")
    .AddCard("Hedge Ratio", hedge_ratio, "{:.3f}")
    .AddCard("Half-Life", half_life, "{:.1f} bars");

  // Row 2: Cointegration status
  builder.AddRow()
    .AddCard("P-Value", p_value, "{:.4f}")
    .AddCard("ADF Stat", adf_stat, "{:.2f}")
    .AddStatusCard("Status", is_cointegrated, "Cointegrated", "Not Cointegrated");

  // Row 3: Current position
  builder.AddRow()
    .AddCard("Z-Score", zscore, "{:+.2f}")
    .AddCard("Spread", spread, "{:.4f}")
    .AddSignalCard("Signal", signal);

  // Chart panel
  builder.AddChart()
    .AddSeries("Spread Z-Score", zscore_series)
    .AddHorizontalLine(2.0, "Entry Short", "red")
    .AddHorizontalLine(-2.0, "Entry Long", "green")
    .AddHorizontalLine(0, "Mean", "gray");

  return builder.Build();
}
```

**Rendered Dashboard**:
```
┌─────────────────────────────────────────────────────────┐
│ ┌─────────┐ ┌─────────────┐ ┌───────────────┐           │
│ │  PAIR   │ │ HEDGE RATIO │ │   HALF-LIFE   │           │
│ │AAPL/SPY │ │   1.234     │ │   15.2 bars   │           │
│ └─────────┘ └─────────────┘ └───────────────┘           │
│ ┌─────────┐ ┌─────────────┐ ┌───────────────┐           │
│ │ P-VALUE │ │  ADF STAT   │ │    STATUS     │           │
│ │  0.023  │ │   -3.45     │ │ ✓ COINTEGRATED│           │
│ └─────────┘ └─────────────┘ └───────────────┘           │
│ ┌─────────┐ ┌─────────────┐ ┌───────────────┐           │
│ │ Z-SCORE │ │   SPREAD    │ │    SIGNAL     │           │
│ │  -1.8   │ │   0.0234    │ │  🔵 LONG SOON │           │
│ └─────────┘ └─────────────┘ └───────────────┘           │
├─────────────────────────────────────────────────────────┤
│                    Z-Score Chart                        │
│  +2 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─              │
│   0 ════════════════════════════════════════            │
│  -2 ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─ ─              │
│      ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~               │
└─────────────────────────────────────────────────────────┘
```

---

## Implementation Recommendations

### Quick Win: Use Existing PlotKinds

For MVP, use existing PlotKinds without new builders:

| Output | Use This PlotKind | Notes |
|--------|-------------------|-------|
| `zscore` | `line` | Simple line in own panel |
| `spread` | `line` | Same as zscore |
| `hedge_ratio` | `line` | Stable line |
| `is_cointegrated` | `zone` | Background shading |
| `entry/exit` | `flag` | Event markers |

### Medium Effort: Custom `spread_zscore`

Create one new builder that combines:
- Z-score line
- Static threshold lines (±1, ±2)
- Optional background zones

**Files to modify**:
1. `include/epoch_script/transforms/core/metadata.h` - Add enum
2. `src/chart_metadata/plot_kinds/builders/spread_zscore_builder.h` - New builder
3. `src/chart_metadata/plot_kinds/registry.cpp` - Register

### Full Feature: Dashboard + Custom PlotKinds

For comprehensive pairs trading UI:
1. Add `cointegration_dashboard` as Dashboard type
2. Create 3-4 specialized builders
3. Support pair-level reports (not just per-asset)

---

## PlotKind Enum Additions

```cpp
// In metadata.h, add to TransformPlotKind enum:

// Cointegration visualization
spread_zscore,           // Z-score with bands
cointegration_status,    // P-value + background shading
hedge_ratio_stability,   // Hedge ratio with confidence bands
half_life_indicator,     // Mean reversion speed
pairs_composite,         // Multi-pane pairs dashboard
```

---

## Summary

| Approach | Effort | Best For |
|----------|--------|----------|
| Reuse `line` + `zone` + `flag` | 0 days | Quick prototype |
| New `spread_zscore` builder | 1 day | Clean z-score visualization |
| Full dashboard | 2-3 days | Production pairs trading UI |

**Recommendation**: Start with existing PlotKinds for MVP, then add `spread_zscore` builder once the transforms work.
