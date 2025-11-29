#pragma once
#include "iintermediate_storage.h"
#include "thread_safe_logger.h"
#include "../events/event_dispatcher.h"
#include "../events/cancellation_token.h"
#include <functional>

namespace epoch_script::runtime {

struct ExecutionContext {
  std::unique_ptr<IIntermediateStorage> cache;
  ILoggerPtr logger;

  // Event dispatcher for progress callbacks (optional, can be null)
  events::IEventDispatcherPtr eventDispatcher;

  // Cancellation token for stopping execution (optional, can be null)
  events::CancellationTokenPtr cancellationToken;

  // Node tracking for progress calculation
  std::atomic<size_t>* nodesCompleted{nullptr};
  std::atomic<size_t>* nodesFailed{nullptr};
  std::atomic<size_t>* nodesSkipped{nullptr};
  size_t totalNodes{0};

  // Running node tracking
  std::function<void(const std::string&)> onNodeStarted;
  std::function<void(const std::string&)> onNodeCompleted;

  // Helper to emit events safely (no-op if dispatcher is null)
  template<typename EventT>
  void EmitEvent(EventT&& event) const {
    if (eventDispatcher) {
      eventDispatcher->Emit(std::forward<EventT>(event));
    }
  }

  // Check if execution should be cancelled
  [[nodiscard]] bool IsCancelled() const {
    return cancellationToken && cancellationToken->IsCancelled();
  }

  // Throw if cancelled
  void ThrowIfCancelled() const {
    if (cancellationToken) {
      cancellationToken->ThrowIfCancelled();
    }
  }
};

} // namespace epoch_script::runtime
