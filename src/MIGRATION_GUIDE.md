# Event System Extraction Plan

Goal: share the orchestrator event/progress plumbing with `EpochDataSDK` so loaders (e.g., `src/dataloader/api_cache_dataloader.cpp`) can emit/consume the same events while keeping `EpochScript` as the primary source. This is a phased migration plan; code moves happen in `EpochDataSDK/src/common` with headers exported under `EpochDataSDK/include`.

## 1) Inventory & Scope
- Components to migrate: `event_dispatcher` (filter + dispatcher), `orchestrator_events` (event types/variant), `cancellation_token`, and `transform_progress_emitter` (emitter + asset context guard).
- Consumers today: `DataFlowRuntimeOrchestrator`, execution nodes, and tests under `test/unit/runtime/events/*`.
- Dependencies: STL, `boost::signals2`, `glaze` (for JsonMetadata), `<chrono>`, `<map>`, `<optional>`, `<variant>`. Avoid Arrow/EpochScript types in the shared layer.

## 2) Extract a Shared Core in EpochDataSDK
- Create `EpochDataSDK/src/common/events/` with the four components above, namespaced `epoch_events` (or keep `epoch_script::runtime::events` but wrapped with a namespace alias exported via header).
- Make headers public in `EpochDataSDK/include/epoch_events/`:
  - `event_types.h` (was `orchestrator_events.h`)
  - `event_filter.h` + `event_dispatcher.h`
  - `cancellation_token.h`
  - `progress_emitter.h`
- Replace EpochScript-specific types in the shared layer:
  - Drop `TransformProgressEvent::transform_name` dependency on transforms; keep as `std::string`.
  - Keep `JsonMetadata` as `std::map<std::string, glz::generic>` (glaze already vendored in both repos).
  - Remove any `epoch_core` includes; use plain enums/strings in the shared layer.
- Add `CMake` target `epoch_events` in EpochDataSDK exporting headers; link `boost::signals2` and `glaze`.

## 3) Wire EpochScript to the Shared Core
- Add dependency on `epoch_events` target in `EpochScript` CMake.
- Replace includes:
  - `src/transforms/runtime/events/*.h` -> include from `epoch_events/…`.
  - Remove local implementations or wrap them with thin compatibility headers until full switch.
- Update namespaces or add `namespace epoch_script::runtime::events = epoch_events;` compatibility aliases to minimize churn.
- Adjust `ExecutionContext` and orchestrator code to use the shared types (no API change expected if aliases are used).

## 4) Enable DataSDK Usage (api_cache_dataloader.cpp)
- Include `epoch_events/event_types.h` and `epoch_events/event_dispatcher.h` in `api_cache_dataloader.cpp`.
- Instantiate `event_dispatcher` and emit events for per-asset/data-category loads:
  - Define a small adapter to populate `node_id`/`transform_name` equivalents for loaders (e.g., `loader_id`, `asset_id`, `category`).
  - Emit `TransformProgressEvent` for load progress and `PipelineStarted/Completed/Failed` for batch loads.
- Add unit tests in EpochDataSDK that subscribe to the dispatcher and assert ordering/filtering.

## 5) Validation & Clean-up
- In EpochScript: update tests to link against the shared core; delete the old event files once green.
- In EpochDataSDK: add coverage for loader progress events and cancellation token usage.
- Document the new public API and usage snippets in both repos’ README/docs.

## 6) Migration Checklist (Actionable)
1. Copy event headers/impl into `EpochDataSDK/src/common/events` (adjust namespaces/deps) and export headers.
2. Add `epoch_events` CMake target + install rules; verify include path from consumers.
3. Switch `EpochScript` includes to the shared headers with namespace aliasing; build + run event tests.
4. Implement loader-side event emission in `api_cache_dataloader.cpp`; add minimal tests/subscriber examples.
5. Remove duplicated event code from `EpochScript` (or leave thin wrappers) once both repos pass CI.

Use this guide as the source of truth while migrating; update it with any API deviations discovered during implementation.
