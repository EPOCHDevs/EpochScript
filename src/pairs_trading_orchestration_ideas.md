header# Pairs Trading Orchestration: Architecture Extension Ideas

## Current Architecture Summary

### How the Orchestrator Works Today

**Per-Asset Execution (Default)**:
- Each transform executes independently for each asset via `tbb::parallel_for_each()`
- Assets flow through the same graph but with separate data paths
- Outputs stored per-asset in cache: `cache[timeframe][asset_id][outputId]`

**Cross-Sectional Execution** (`isCrossSectional = true`):
1. Gather inputs from ALL assets in parallel
2. Rename columns to asset IDs (e.g., "AAPL", "MSFT")
3. Concatenate into single DataFrame (assets as columns, time as rows)
4. Execute transform ONCE on full matrix
5. Distribute results back to individual assets

**Key Limitation**: No concept of "which asset is which" - assets are an unordered collection.

---

## The Pairs Trading Problem

For cointegration/pairs trading, we need:
- **Explicit asset roles**: Asset Y (dependent) vs Asset X (independent/hedge)
- **Pair-specific outputs**: hedge_ratio applies to the PAIR, not individual assets
- **Ordered relationships**: y = beta*x + residual requires knowing which is which
- **Multi-pair support**: Run same strategy on many pairs (SPY/QQQ, GLD/GDX, etc.)

---

## Proposed Solutions

### Option A: Asset Reference Nodes (Recommended)

**Concept**: Add new node types that explicitly reference and label assets.

```
┌─────────────────────────────────────────────────────────┐
│  NEW NODE: asset_ref                                    │
│  ─────────────────                                      │
│  Inputs: data_source (e.g., close price)                │
│  Options:                                               │
│    - role: "dependent" | "independent" | "hedge"        │
│    - label: "asset_y" | "asset_x" | "asset_1"           │
│    - asset_filter: "AAPL" | "SPY" | regex pattern       │
│                                                         │
│  Outputs: labeled series with asset metadata attached   │
└─────────────────────────────────────────────────────────┘
```

**Usage in Graph**:
```
close_price ──► asset_ref(role="dependent", label="y") ──┐
                                                          ├──► engle_granger
close_price ──► asset_ref(role="independent", label="x")─┘
```

**Implementation**:
- New transform type: `AssetReference`
- Attaches metadata to Series: `{role, label, source_asset}`
- Downstream transforms can query: `GetInputAssetRole("y")`

**Pros**:
- Explicit, self-documenting graphs
- Works with existing cross-sectional infrastructure
- Flexible for any multi-asset pattern (not just pairs)

**Cons**:
- Requires new node type
- Slightly more verbose graphs

---

### Option B: Pair-Context Transform Wrapper

**Concept**: Wrap transforms in a "pair context" that handles asset binding.

```
┌─────────────────────────────────────────────────────────┐
│  NEW NODE: pair_context                                 │
│  ────────────────────                                   │
│  Options:                                               │
│    - asset_y: "AAPL" | asset_list                       │
│    - asset_x: "MSFT" | asset_list                       │
│    - mode: "single_pair" | "all_combinations"           │
│                                                         │
│  Children: nested transforms operate in pair context    │
└─────────────────────────────────────────────────────────┘
```

**Usage**:
```yaml
pair_context:
  asset_y: ["AAPL", "GOOGL", "MSFT"]
  asset_x: ["SPY"]
  mode: all_combinations  # Creates AAPL/SPY, GOOGL/SPY, MSFT/SPY
  children:
    - engle_granger
    - zscore
```

**Implementation**:
- New execution context that binds assets to roles
- Expands to multiple pair executions
- Results tagged with pair identifier: `"AAPL_SPY"`

**Pros**:
- Clean separation of pair definition from transform logic
- Easy to run many pairs
- Supports pair-level reporting

**Cons**:
- More complex execution model
- Nested graph structure

---

### Option C: Extended Input Mapping (Minimal Change)

**Concept**: Extend existing input mapping to include asset specification.

**Current**:
```cpp
InputMapping {
  .type = InputType::NodeRef,
  .nodeId = "close_price",
  .outputId = "result"
}
```

**Extended**:
```cpp
InputMapping {
  .type = InputType::NodeRef,
  .nodeId = "close_price",
  .outputId = "result",
  .assetSpec = AssetSpec {
    .mode = AssetSpecMode::Explicit,  // or Pattern, or Index
    .value = "AAPL"                   // or "asset_0", or regex
  }
}
```

**Implementation**:
- Modify `InputMapping` struct
- Update `GatherInputs()` to filter by asset
- Add asset resolution in `intermediate_storage.cpp`

**Pros**:
- Minimal architectural change
- Backward compatible (assetSpec defaults to "current asset")

**Cons**:
- Less visible in graph structure
- Harder to validate at compile time

---

### Option D: Cross-Sectional with Ordered Columns

**Concept**: Force deterministic column ordering and use positional references.

```
┌─────────────────────────────────────────────────────────┐
│  MODIFICATION: Cross-sectional execution                │
│  ───────────────────────────────────────                │
│  Before: Assets as unordered columns                    │
│  After: Assets sorted alphabetically OR by config       │
│                                                         │
│  New input syntax:                                      │
│    - "SLOT[0]" → first asset column                     │
│    - "SLOT[1]" → second asset column                    │
│    - "SLOT[AAPL]" → specific asset column               │
└─────────────────────────────────────────────────────────┘
```

**Implementation**:
- Sort assets before concatenation (alphabetical or by config order)
- Parse extended input IDs: `GetInputId("x[0]")` → first column
- Add column indexing to cross-sectional input gathering

**Pros**:
- Works with existing cross-sectional infrastructure
- Deterministic ordering for debugging

**Cons**:
- Positional references are fragile
- Adding/removing assets breaks references

---

## Recommended Approach: Hybrid A + C

Combine **Asset Reference Nodes** with **Extended Input Mapping**:

### Phase 1: Extended Input Mapping
Add `assetSpec` to `InputMapping` for explicit asset filtering:
```cpp
// In transform configuration
inputs:
  y: { nodeRef: "close", assetSpec: "AAPL" }
  x: { nodeRef: "close", assetSpec: "SPY" }
```

### Phase 2: Asset Reference Nodes
Add `asset_ref` node for complex scenarios:
```cpp
// In graph
asset_ref("ref_y", close, { role: "dependent", assets: ["AAPL", "GOOGL"] })
asset_ref("ref_x", close, { role: "hedge", assets: ["SPY"] })
engle_granger("coint", ref_y, ref_x)
```

### Phase 3: Pair-Level Output Distribution
Modify `DistributeCrossSectionalOutputs()` to support:
- **Pair-keyed storage**: `cache[timeframe]["AAPL_SPY"][outputId]`
- **Role-based routing**: hedge_ratio goes to dependent asset only
- **Broadcast modes**: spread goes to both assets

---

## Output Routing for Pairs

### Current Behavior
```
Cross-sectional transform outputs:
  - Single column → broadcast to all assets
  - Multi-column → each asset gets its column
```

### Needed for Pairs
```
Pair transform outputs:
  hedge_ratio   → store under pair key "AAPL_SPY"
  spread        → store under BOTH assets (for downstream per-asset logic)
  is_cointegrated → store under pair key
```

### Proposed: Output Routing Metadata
```cpp
struct OutputRoutingSpec {
  enum Mode {
    PerAsset,      // Each asset gets its value
    Broadcast,     // All assets get same value
    PairKey,       // Store under "asset1_asset2" key
    DependentOnly, // Only dependent asset gets value
    HedgeOnly      // Only hedge asset gets value
  };
  Mode mode;
  std::optional<std::string> customKey;
};
```

Add to transform metadata:
```cpp
.outputs = {
  {Decimal, "hedge_ratio", "Hedge Ratio", .routing = PairKey},
  {Decimal, "spread", "Spread", .routing = Broadcast},
  {Boolean, "is_cointegrated", "Cointegrated", .routing = PairKey}
}
```

---

## Multi-Pair Execution

### Scenario
Run cointegration on 100 pairs: (AAPL/SPY, GOOGL/SPY, MSFT/SPY, ...)

### Option 1: Explicit Enumeration
```yaml
pairs:
  - { y: "AAPL", x: "SPY" }
  - { y: "GOOGL", x: "SPY" }
  - { y: "MSFT", x: "SPY" }
```

### Option 2: Cartesian Product
```yaml
pair_generator:
  dependent_assets: ["AAPL", "GOOGL", "MSFT", "AMZN", ...]
  hedge_assets: ["SPY"]
  mode: cartesian  # All combinations
```

### Option 3: Universe-Based
```yaml
pair_generator:
  universe: "SP500"
  hedge: "SPY"
  filters:
    - sector != "Financials"
    - market_cap > 10B
```

### Implementation
New orchestration layer that:
1. Generates pair list from specification
2. Creates execution context per pair
3. Runs transform graph for each pair
4. Aggregates results with pair keys

---

## Pair-Level Reporting

### Current Reporter Model
```
Reports stored as:
  - Per-asset: cache[timeframe][asset_id][report]
  - Group-level: cache[timeframe]["ALL"][report]
```

### Needed for Pairs
```
Pair-level reports:
  - cache[timeframe]["AAPL_SPY"][report]
  - Aggregate: cache[timeframe]["ALL_PAIRS"][report]
```

### Proposed: Pair Reporter Transform
```cpp
class PairReporter : public ITransform {
  // Inputs: pair-level outputs (hedge_ratio, spread, etc.)
  // Outputs: TearSheet for the pair
  // Storage key: pair identifier
};
```

---

## Summary of Changes Needed

| Component | Change | Complexity |
|-----------|--------|------------|
| `InputMapping` | Add `assetSpec` field | Low |
| `intermediate_storage.cpp` | Filter by asset in GatherInputs | Medium |
| `metadata.h` | Add output routing spec | Low |
| `execution_node.cpp` | Handle routing in distribution | Medium |
| New: `asset_ref` transform | Create asset reference node | Medium |
| New: `pair_context` | Optional pair execution wrapper | High |
| `orchestrator.cpp` | Support pair-keyed storage | Medium |

---

## Next Steps

1. **Prototype Extended Input Mapping** (1-2 days)
   - Add `assetSpec` to InputMapping
   - Update GatherInputs to filter

2. **Implement Asset Reference Node** (1 day)
   - Simple transform that tags series with role

3. **Test with Engle-Granger** (1 day)
   - Validate pair execution works end-to-end

4. **Add Pair-Level Reporting** (1 day)
   - Extend reporter system for pair keys
