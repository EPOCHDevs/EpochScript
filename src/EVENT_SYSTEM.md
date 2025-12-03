# Unified Event System

## Overview

A domain-agnostic event system that flows from **EpochDataSDK** → **EpochScript** → **StratifyX** → **Frontend**, enabling:
- Single subscriber passed through all layers
- Deep cancellation propagation from JobManager
- Rich UI progress display (terminal, tree, collapsible stages)
- ML training progress with adaptive throttling

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          STRATIFYX (Master)                                  │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ JobManager                                                              │ │
│  │   └─ AbstractJob                                                        │ │
│  │       ├─ CancellationToken (master) ──────────────────────────────────┐│ │
│  │       ├─ GenericEventDispatcher (master) ◄────────────────────────────┐│ │
│  │       └─ SSEStream ◄── SSEEventSerializer                             ││ │
│  └────────────────────────────────────────────────────────────────────────┘│ │
└─────────────────────────────────────────────────────────────────────────────┘
         │ passes down                                    │ receives events
         ▼                                                │
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EPOCHSCRIPT                                         │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ Orchestrator                                                            │ │
│  │   ├─ SetExternalProgressEmitter(emitter)                               │ │
│  │   ├─ SetExternalCancellationToken(token)                               │ │
│  │   ├─ Creates ScopedProgressEmitter per transform                       │ │
│  │   └─ TBB Flow Graph executes transforms                                │ │
│  └────────────────────────────────────────────────────────────────────────┘│ │
│                              │                                               │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ Transforms (ML, Statistics, etc.)                                       │ │
│  │   ├─ Each has ScopedProgressEmitter child scope                        │ │
│  │   ├─ EmitProgress(), EmitEpochWithMetrics(), ThrowIfCancelled()       │ │
│  │   └─ Events flow up to master dispatcher                               │ │
│  └────────────────────────────────────────────────────────────────────────┘│ │
└─────────────────────────────────────────────────────────────────────────────┘
         │ also passes to
         ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          EPOCHDATASDK                                        │
│  ┌────────────────────────────────────────────────────────────────────────┐ │
│  │ ApiCacheDataLoader                                                      │ │
│  │   ├─ SetProgressEmitter(emitter)                                       │ │
│  │   ├─ SetCancellationToken(token)                                       │ │
│  │   ├─ Emits DataLoad events                                             │ │
│  │   └─ Checks cancellation during fetch                                  │ │
│  └────────────────────────────────────────────────────────────────────────┘│ │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Implementation Status

### Phase 1: Core Infrastructure (EpochDataSDK) ✅ COMPLETED

| File | Status | Description |
|------|--------|-------------|
| `include/epoch_data_sdk/common/event_path.h` | ✅ | EventPath class for hierarchical paths |
| `include/epoch_data_sdk/common/generic_event_types.h` | ✅ | GenericEvent variant (5 event types) |
| `include/epoch_data_sdk/common/generic_event_filter.h` | ✅ | Path & type filtering |
| `include/epoch_data_sdk/common/generic_event_dispatcher.h` | ✅ | IGenericEventDispatcher interface |
| `include/epoch_data_sdk/common/scoped_progress_emitter.h` | ✅ | ScopedProgressEmitter + MLProgressThrottler |

### Phase 2: StratifyX Integration ✅ COMPLETED

| File | Status | Description |
|------|--------|-------------|
| `EpochStratifyX/.../abstract_job.h` | ✅ | Enhanced with event system methods |
| `EpochStratifyX/.../sse_event_serializer.h` | ✅ | GenericEvent → SSE JSON serialization |

### Phase 3: EpochScript Integration ✅ COMPLETED

| File | Status | Description |
|------|--------|-------------|
| `src/transforms/runtime/orchestrator.h` | ✅ | Added SetExternalProgressEmitter/Token |
| `src/transforms/runtime/orchestrator.cpp` | ✅ | Implementation added |

### Phase 4: DataLoader Integration ⏳ PENDING

| File | Status | Description |
|------|--------|-------------|
| ApiCacheDataLoader | ⏳ | Add emitter/token setters |
| Resample | ⏳ | Add emitter/token setters |

### Phase 5-6: Frontend ⏳ PENDING (Backend testing first)

| File | Status | Description |
|------|--------|-------------|
| `types/events.ts` | ⏳ | TypeScript types |
| `hooks/useJobEvents.ts` | ⏳ | SSE subscription hook |
| `components/TreeView.tsx` | ⏳ | Collapsible tree |
| `components/TerminalView.tsx` | ⏳ | Log output |

---

## Core Components

### 1. EventPath (`event_path.h`)

Hierarchical path identification for events.

```cpp
// Path format: "scope:id/scope:id/..."
// Example: "job:abc123/stage:RunCampaign/pipeline:transforms/node:SMA_20/asset:AAPL"

class EventPath {
    struct Segment { std::string scope; std::string id; };

    // Construction
    EventPath(const std::string& scope, const std::string& id);
    static EventPath Parse(const std::string& path);

    // Navigation
    [[nodiscard]] EventPath Child(const std::string& scope, const std::string& id) const;
    [[nodiscard]] EventPath Parent() const;

    // Queries
    [[nodiscard]] bool IsDescendantOf(const EventPath& ancestor) const;
    [[nodiscard]] std::optional<std::string> GetSegment(const std::string& scope) const;
    [[nodiscard]] std::string ToString() const;
    [[nodiscard]] size_t Depth() const;
};

// Factory functions
EventPath MakeJobPath(const std::string& jobId);
EventPath MakeStagePath(const std::string& jobId, const std::string& stageName);
EventPath MakeNodePath(const std::string& jobId, const std::string& stageName,
                       const std::string& pipelineName, const std::string& nodeId);
```

### 2. Generic Event Types (`generic_event_types.h`)

```cpp
enum class OperationStatus : uint8_t {
    Pending, Started, InProgress, Completed, Failed, Cancelled, Skipped
};

enum class GenericEventType : uint8_t {
    Lifecycle, Progress, Metric, Summary, Log
};

struct EventBase {
    Timestamp timestamp;
    EventPath path;
    JsonMetadata context;  // Domain-specific key-value data
};

// 5 Event Types
struct LifecycleEvent : EventBase {
    OperationStatus status;
    std::string operation_type;  // "stage", "pipeline", "node", "data_load"
    std::string operation_name;
    std::optional<std::chrono::milliseconds> duration;
    std::optional<std::string> error_message;
    std::optional<size_t> items_succeeded, items_failed, items_total;
};

struct ProgressEvent : EventBase {
    std::optional<size_t> current, total;
    std::optional<double> progress_percent;
    std::string message;
    std::optional<std::string> unit;  // "epochs", "rows", "assets"
    // ML metrics stored in context: context["loss"], context["accuracy"], etc.
};

struct MetricEvent : EventBase {
    std::string metric_name;
    double value;
    std::optional<std::string> unit;
};

struct SummaryEvent : EventBase {
    double overall_progress_percent;
    size_t operations_completed, operations_total, operations_failed;
    std::vector<std::string> currently_running;
};

struct LogEvent : EventBase {
    enum class Level : uint8_t { Debug, Info, Warning, Error };
    Level level;
    std::string message;
    std::optional<std::string> source;
};

using GenericEvent = std::variant<LifecycleEvent, ProgressEvent, MetricEvent, SummaryEvent, LogEvent>;
```

### 3. GenericEventFilter (`generic_event_filter.h`)

```cpp
class GenericEventFilter {
    // Factory methods
    static GenericEventFilter All();
    static GenericEventFilter OnlyTypes(std::initializer_list<GenericEventType> types);
    static GenericEventFilter LifecycleOnly();
    static GenericEventFilter ProgressOnly();

    // Path-based filters (chainable)
    [[nodiscard]] GenericEventFilter WithPathPrefix(const EventPath& prefix) const;
    [[nodiscard]] GenericEventFilter AtOrBelowDepth(size_t maxDepth) const;
    [[nodiscard]] GenericEventFilter WithScope(const std::string& scope) const;
    [[nodiscard]] GenericEventFilter WithPredicate(Predicate pred) const;

    // Evaluation
    [[nodiscard]] bool Accepts(const GenericEvent& event) const;

    // Composition
    GenericEventFilter operator&(const GenericEventFilter& other) const;  // AND
    GenericEventFilter operator|(const GenericEventFilter& other) const;  // OR
};
```

### 4. IGenericEventDispatcher (`generic_event_dispatcher.h`)

```cpp
class IGenericEventDispatcher {
    virtual void Emit(GenericEvent event) = 0;
    virtual void Emit(GenericEvent event, const EventPath& path) = 0;

    virtual boost::signals2::connection Subscribe(
        GenericEventSlot handler,
        GenericEventFilter filter = GenericEventFilter::All()) = 0;

    // Type-safe subscription
    template <typename T>
    boost::signals2::connection SubscribeTo(
        std::function<void(const T&)> handler,
        GenericEventFilter additionalFilter = GenericEventFilter::All());

    // Convenience subscriptions
    boost::signals2::connection SubscribeToPath(GenericEventSlot handler, const EventPath& prefix);
    boost::signals2::connection SubscribeToLifecycle(std::function<void(const LifecycleEvent&)> handler);
    boost::signals2::connection SubscribeToProgress(std::function<void(const ProgressEvent&)> handler);
};

// Implementations
class GenericEventDispatcher : public IGenericEventDispatcher { ... };      // Standard
class ThreadSafeGenericEventDispatcher : public IGenericEventDispatcher { ... };  // Extra mutex
class NullGenericEventDispatcher : public IGenericEventDispatcher { ... };  // No-op

// Factory functions
IGenericEventDispatcherPtr MakeGenericEventDispatcher();
IGenericEventDispatcherPtr MakeThreadSafeGenericEventDispatcher();
IGenericEventDispatcherPtr MakeNullGenericEventDispatcher();
```

### 5. ScopedProgressEmitter (`scoped_progress_emitter.h`)

The primary interface for emitting events with hierarchical paths.

```cpp
class ScopedProgressEmitter {
    ScopedProgressEmitter(
        IGenericEventDispatcherPtr dispatcher,
        CancellationTokenPtr cancellationToken,
        EventPath basePath);

    // Scope management
    [[nodiscard]] ScopedProgressEmitter ChildScope(const std::string& scope, const std::string& id) const;
    [[nodiscard]] const EventPath& GetPath() const;
    [[nodiscard]] bool IsActive() const;

    // Context management
    void SetContext(const std::string& key, const glz::generic& value);
    void SetContext(const std::string& key, double value);
    void SetContext(const std::string& key, const std::string& value);

    // Cancellation
    [[nodiscard]] bool IsCancelled() const noexcept;
    void ThrowIfCancelled() const;

    // Lifecycle events
    void EmitStarted(const std::string& operation_type, const std::string& operation_name);
    void EmitCompleted(const std::string& operation_type, const std::string& operation_name);
    void EmitFailed(const std::string& operation_type, const std::string& operation_name, const std::string& error);
    void EmitCancelled(const std::string& operation_type, const std::string& operation_name);
    void EmitSkipped(const std::string& operation_type, const std::string& operation_name, const std::string& reason);

    // Progress events
    void EmitProgress(size_t current, size_t total, const std::string& message = "", const std::string& unit = "");
    void EmitProgressPercent(double percent, const std::string& message = "");
    void EmitEpoch(size_t epoch, size_t total_epochs, const std::string& message = "");
    void EmitEpochWithMetrics(size_t epoch, size_t total_epochs,
                              std::optional<double> loss = std::nullopt,
                              std::optional<double> accuracy = std::nullopt,
                              std::optional<double> learning_rate = std::nullopt);

    // Other events
    void EmitMetric(const std::string& name, double value, const std::string& unit = "");
    void EmitDebug(const std::string& message);
    void EmitInfo(const std::string& message);
    void EmitWarning(const std::string& message);
    void EmitError(const std::string& message);
    void EmitSummary(double overall_percent, size_t completed, size_t total, size_t failed = 0);

    // Timing
    void StartTiming();
    [[nodiscard]] std::chrono::milliseconds GetElapsed() const;
};

// RAII guard for automatic start/complete
class ScopedOperation {
    ScopedOperation(ScopedProgressEmitter& emitter, std::string operation_type, std::string operation_name);
    ~ScopedOperation();  // Emits Completed or Failed on destruction
    void SetFailed(const std::string& error);
};

// ML Progress throttling
class MLProgressThrottler {
    static size_t GetEmitInterval(size_t totalEpochs);  // 1/10/50/100 based on total
    [[nodiscard]] bool ShouldEmit(size_t currentEpoch) const;
};
```

### 6. AbstractJob Enhancement (`abstract_job.h`)

```cpp
class AbstractJob : public IJob {
public:
    // Enhanced Cancel - propagates to token
    Status Cancel(DBKeyIdentifier const &key) override {
        interrupt.test_and_set();
        if (m_cancellationToken) {
            m_cancellationToken->Cancel();
        }
        // Emit cancellation event
        if (m_genericDispatcher && !m_jobId.empty()) {
            LifecycleEvent cancelEvent;
            cancelEvent.path = MakeJobPath(m_jobId);
            cancelEvent.status = OperationStatus::Cancelled;
            cancelEvent.operation_type = "job";
            m_genericDispatcher->Emit(cancelEvent);
        }
        return MakeOkStatus();
    }

protected:
    // Initialize event system (call at start of Run())
    void InitializeEventSystem(const std::string& jobId);

    // Access event system
    [[nodiscard]] IGenericEventDispatcherPtr GetGenericDispatcher() const;
    [[nodiscard]] CancellationTokenPtr GetCancellationToken() const;

    // Create root emitter for this job
    [[nodiscard]] ScopedProgressEmitterPtr CreateRootEmitter();

    // Subscribe SSE stream
    boost::signals2::connection SubscribeToGenericEvents(GenericEventSlot handler);

    // Cancellation check
    [[nodiscard]] bool IsCancelled() const noexcept;
    void ThrowIfCancelled() const;

protected:
    std::string m_jobId{};
    IGenericEventDispatcherPtr m_genericDispatcher{};
    CancellationTokenPtr m_cancellationToken{};
};
```

### 7. SSEEventSerializer (`sse_event_serializer.h`)

```cpp
struct SSEEventEnvelope {
    std::string event_id;       // "job:abc/stage:Run/node:SMA"
    std::string event_type;     // "lifecycle", "progress", "metric", "summary", "log"
    std::string job_id;
    uint64_t sequence;
    std::string timestamp;      // ISO 8601
    std::optional<std::string> parent_id;
    std::string layer;          // "job", "stage", "pipeline", "node", "asset"

    // Lifecycle fields
    std::optional<std::string> status, operation_type, operation_name, error;
    std::optional<int64_t> duration_ms;
    std::optional<size_t> items_succeeded, items_failed, items_total;

    // Progress fields
    std::optional<size_t> current, total;
    std::optional<double> percent;
    std::optional<std::string> message, unit;

    // ML metrics
    std::optional<double> loss, accuracy, learning_rate, validation_loss, validation_accuracy;
    std::optional<size_t> epoch, total_epochs;

    // Metric, Summary, Log fields...
};

class SSEEventSerializer {
    [[nodiscard]] std::string Serialize(const GenericEvent& event);
    [[nodiscard]] uint64_t GetSequence() const noexcept;
    void ResetSequence() noexcept;
};
```

### 8. Orchestrator Enhancement (`orchestrator.h`)

```cpp
class DataFlowRuntimeOrchestrator {
public:
    // External event system integration
    void SetExternalProgressEmitter(ScopedProgressEmitterPtr emitter);
    void SetExternalCancellationToken(CancellationTokenPtr token);
    [[nodiscard]] ScopedProgressEmitterPtr GetExternalProgressEmitter() const;
    [[nodiscard]] CancellationTokenPtr GetExternalCancellationToken() const;
    [[nodiscard]] bool HasExternalEventSystem() const;

private:
    ScopedProgressEmitterPtr m_externalEmitter;
    CancellationTokenPtr m_externalCancellationToken;
};
```

---

## Usage Examples

### Job Execution with Event System

```cpp
void ResearchStudyJob::Run(DBKeyIdentifier const& key, bool enableProgressBar, const IJobPersister& persister) {
    // Initialize event system
    InitializeEventSystem(key.entityId);

    // Subscribe SSE stream to events
    auto sseSubscription = SubscribeToGenericEvents(
        [this](const GenericEvent& event) {
            auto json = m_sseSerializer.Serialize(event);
            m_sseStream->sendData(json);
        });

    // Create scoped emitters for each layer
    auto rootEmitter = CreateRootEmitter();

    // Data loading stage
    auto dataLoadEmitter = rootEmitter->ChildScope("stage", "DataLoading");
    dataLoadEmitter->EmitStarted("stage", "Loading market data");
    m_dataLoader->SetProgressEmitter(dataLoadEmitter->ChildScope("pipeline", "data_load"));
    m_dataLoader->SetCancellationToken(GetCancellationToken());
    // ... load data ...
    dataLoadEmitter->EmitCompleted("stage", "Loading market data");

    // Transform execution stage
    auto transformEmitter = rootEmitter->ChildScope("stage", "RunCampaign");
    transformEmitter->EmitStarted("stage", "Executing transforms");
    m_orchestrator->SetExternalProgressEmitter(transformEmitter->ChildScope("pipeline", "transforms"));
    m_orchestrator->SetExternalCancellationToken(GetCancellationToken());
    // ... execute pipeline ...
    transformEmitter->EmitCompleted("stage", "Executing transforms");
}
```

### Transform with ML Progress

```cpp
void RollingLightGBM::Execute(const ExecutionContext& ctx) {
    auto& emitter = *ctx.progressEmitter;
    MLProgressThrottler throttler(m_params.num_epochs);

    emitter.EmitStarted("transform", "LightGBM Training");

    for (size_t epoch = 1; epoch <= m_params.num_epochs; ++epoch) {
        emitter.ThrowIfCancelled();  // Check cancellation

        double loss = TrainEpoch();
        double accuracy = Evaluate();

        if (throttler.ShouldEmit(epoch)) {
            emitter.EmitEpochWithMetrics(epoch, m_params.num_epochs, loss, accuracy);
        }
    }

    emitter.EmitCompleted("transform", "LightGBM Training");
}
```

### Cancellation Flow

```
User clicks "Cancel" (Frontend)
         │
         ▼
HTTP POST /api/v1/jobs/{id}/cancel
         │
         ▼
JobManager::ExecuteCancelTask()
         │
         ▼
AbstractJob::Cancel()
    ├─ interrupt.test_and_set()
    └─ m_cancellationToken->Cancel()
              │
    ┌─────────┴─────────┐
    ▼                   ▼
Orchestrator        DataLoader
IsCancelled()       IsCancelled()
    │                   │
    ▼                   ▼
OperationCancelledException (bubbles up)
    │
    ▼
Emit LifecycleEvent(Cancelled) → SSEStream → Frontend
```

---

## Thread Safety

| Component | Mechanism | Notes |
|-----------|-----------|-------|
| GenericEventDispatcher | boost::signals2 | Thread-safe emit/subscribe |
| CancellationToken | std::atomic<bool> | Lock-free, memory_order_acquire/release |
| ScopedProgressEmitter | Immutable path + shared context | Thread-safe via design |
| SSEStream | std::mutex + condition_variable | Already production-ready |
| EventPath | Value type, immutable | Safe to copy across threads |

---

## SSE Throttling Strategy

| Event Type | Strategy | Interval |
|------------|----------|----------|
| Lifecycle (start/end) | Immediate | 0ms |
| Progress (same node) | Coalesce | 100ms |
| ML Metrics | Adaptive | 1/10/50/100 epochs |
| Summary | Periodic | 500ms |
| Log | Batch | 200ms / 50 lines |

### Adaptive ML Progress Throttling

```cpp
static size_t GetEmitInterval(size_t totalEpochs) {
    if (totalEpochs <= 20)   return 1;     // Every epoch
    if (totalEpochs <= 100)  return 10;    // Every 10
    if (totalEpochs <= 500)  return 50;    // Every 50
    return 100;                             // Every 100
}
```

---

## Frontend (Pending Backend Testing)

### TypeScript Types (events.ts)

```typescript
export type EventId = string;  // "job:abc/stage:Run/node:SMA"

export type OperationStatus =
  | 'pending' | 'started' | 'in_progress'
  | 'completed' | 'failed' | 'cancelled' | 'skipped';

export interface SSEEvent {
  eventId: EventId;
  type: 'lifecycle' | 'progress' | 'metric' | 'summary' | 'log';
  jobId: string;
  sequence: number;
  timestamp: string;
  parentId?: EventId;
  layer: string;

  // Lifecycle
  status?: OperationStatus;
  operationType?: string;
  operationName?: string;
  durationMs?: number;
  error?: string;

  // Progress + ML
  current?: number;
  total?: number;
  percent?: number;
  message?: string;
  loss?: number;
  accuracy?: number;
  epoch?: number;
  totalEpochs?: number;

  // Summary
  overallPercent?: number;
  completed?: number;
  failed?: number;
  currentlyRunning?: string[];

  // Log
  level?: 'debug' | 'info' | 'warn' | 'error';
}

export interface TreeNode {
  id: EventId;
  label: string;
  status: OperationStatus;
  progress?: { current: number; total: number; percent: number };
  mlMetrics?: { epoch?: number; loss?: number; accuracy?: number };
  children: EventId[];
  collapsed: boolean;
}
```

### React Hook (useJobEvents.ts)

```typescript
export function useJobEvents(jobId: string) {
  const [state, dispatch] = useReducer(jobReducer, createInitialState(jobId));

  useEffect(() => {
    const eventSource = new EventSource(`/api/v1/jobs/${jobId}/events/stream`);
    eventSource.onmessage = (e) => {
      const event = JSON.parse(e.data) as SSEEvent;
      dispatch({ type: 'EVENT', event });
    };
    return () => eventSource.close();
  }, [jobId]);

  const toggleCollapse = useCallback((nodeId: EventId) => {
    dispatch({ type: 'TOGGLE_COLLAPSE', nodeId });
  }, []);

  return { state, toggleCollapse };
}
```

### UI Wireframe (Split Layout)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│ Job Progress                                               [Cancel]         │
├─────────────────────────────────────────────────────────────────────────────┤
│ ████████████████████████████████░░░░░░░░░░░░░░░░░░ 62% (5/8 nodes)         │
├─────────────────────────────────────────────────────────────────────────────┤
│ ▼ Job: RunCampaign                                              [running]  │
│   ├─ ✓ DataLoading                                        [completed] 12s  │
│   │  ├─ ✓ FetchMarketData                                                  │
│   │  │  ├─ ✓ AAPL                                                    1.2s │
│   │  │  ├─ ✓ MSFT                                                    1.1s │
│   │  │  └─ ✓ GOOGL                                                   1.3s │
│   │  └─ ✓ CacheSync                                                  0.5s │
│   ├─ ✓ Resample                                           [completed] 2.3s │
│   ├─ ▶ Orchestrator                                           [running]    │
│   │  └─ ▼ Pipeline: main                                   5/8 nodes (62%) │
│   │     ├─ ✓ sma_20                                        [completed] 12ms │
│   │     ├─ ✓ rsi_14                                        [completed] 18ms │
│   │     ├─ ▶ rolling_lightgbm                                 [running]    │
│   │     │  │  Epoch 67/100 - Loss: 0.289, Acc: 82.4%                       │
│   │     │  │  ██████████████████░░░░░░░░░░ 67%                             │
│   │     ├─ ○ signal_generator                                 [pending]    │
│   │     └─ ○ tearsheet                                        [pending]    │
│   └─ ○ Finalization                                           [pending]    │
├═══════════════════════ TERMINAL (127 lines) ════════════════════════════════┤
│ 14:23:45.123 [INFO]  Starting pipeline with 8 nodes, 3 assets               │
│ 14:23:45.234 [INFO]  Node sma_20: Processing 3 assets in parallel           │
│ 14:23:47.891 [INFO]  Node rolling_lightgbm: Starting LightGBM training      │
│ 14:23:50.123 [DEBUG] rolling_lightgbm/AAPL: Epoch 67/100, loss=0.289        │
│ ▼ [Auto-scroll enabled]                                                      │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Files Modified/Created

### EpochDataSDK (New)
- `include/epoch_data_sdk/common/event_path.h` ✅
- `include/epoch_data_sdk/common/generic_event_types.h` ✅
- `include/epoch_data_sdk/common/generic_event_filter.h` ✅
- `include/epoch_data_sdk/common/generic_event_dispatcher.h` ✅
- `include/epoch_data_sdk/common/scoped_progress_emitter.h` ✅

### EpochScript (Modified)
- `src/transforms/runtime/orchestrator.h` ✅
- `src/transforms/runtime/orchestrator.cpp` ✅

### StratifyX (Modified/New)
- `cpp/src/campaign/singletons/abstract_job.h` ✅
- `cpp/src/campaign/controller/stratifyx/http/sse_event_serializer.h` ✅

### Frontend (Pending)
- `types/events.ts` ⏳
- `hooks/useJobEvents.ts` ⏳
- `components/JobProgress/TreeView.tsx` ⏳
- `components/JobProgress/TerminalView.tsx` ⏳
- `components/JobProgress/JobProgressPanel.tsx` ⏳

---

## Testing Checklist

- [ ] EventPath parsing and serialization
- [ ] GenericEventFilter accepts/rejects correctly
- [ ] GenericEventDispatcher emits to filtered subscribers
- [ ] ScopedProgressEmitter creates correct hierarchical paths
- [ ] CancellationToken propagates through layers
- [ ] SSEEventSerializer produces valid JSON
- [ ] AbstractJob initializes event system correctly
- [ ] Orchestrator receives and uses external emitter/token
- [ ] End-to-end: Job → Orchestrator → Transform → SSE → Console
