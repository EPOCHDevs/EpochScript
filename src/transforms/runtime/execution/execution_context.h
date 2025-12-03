#pragma once
#include "iintermediate_storage.h"
#include "thread_safe_logger.h"
#include "../events/event_dispatcher.h"
#include "../events/cancellation_token.h"
#include <epoch_data_sdk/events/all.h>
#include <epoch_data_sdk/events/event_ids.h>
#include <functional>

namespace epoch_script::runtime {

struct ExecutionContext {
  std::unique_ptr<IIntermediateStorage> cache;
  ILoggerPtr logger;

  // Event dispatcher for progress callbacks (optional, can be null)
  events::IEventDispatcherPtr eventDispatcher;

  // External progress emitter for unified event system (optional, can be null)
  // When set, node events are emitted through this emitter to the ConsoleEventViewer
  data_sdk::events::ScopedProgressEmitter* externalEmitter{nullptr};

  // Cancellation token for stopping execution (optional, can be null)
  events::CancellationTokenPtr cancellationToken;

  // Node tracking for progress calculation
  std::atomic<size_t>* nodesCompleted{nullptr};
  std::atomic<size_t>* nodesFailed{nullptr};
  std::atomic<size_t>* nodesSkipped{nullptr};
  size_t totalNodes{0};

  // Execution order tracking - increments when each node starts/completes
  // Useful for understanding TBB's parallel execution order
  std::atomic<size_t>* executionSequence{nullptr};   // Order nodes started
  std::atomic<size_t>* completionSequence{nullptr};  // Order nodes completed

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

  // Helper to emit lifecycle events to external emitter (for ConsoleEventViewer)
  // IMPORTANT: Each method creates a ChildScope with the nodeId to ensure the event path
  // matches between Started/Completed/Failed events. The viewer uses the path as a key.
  //
  // Returns the execution sequence number (order this node started in TBB's parallel execution)
  size_t EmitNodeStarted(const std::string& nodeId, const std::string& transformName,
                         bool isCrossSectional, size_t assetCount) const {
    // Get execution sequence number (atomically increment and get previous value)
    size_t execSeq = executionSequence ? executionSequence->fetch_add(1) : 0;

    if (externalEmitter) {
      using namespace data_sdk::events;
      auto nodeEmitter = externalEmitter->ChildScope(ScopeType::Node, nodeId);
      nodeEmitter.EmitStarted(std::string(OperationType::Node), transformName);
      nodeEmitter.SetContext(std::string(ContextKey::IsCrossSectional), isCrossSectional);
      nodeEmitter.SetContext(std::string(ContextKey::AssetCount), static_cast<int64_t>(assetCount));
      nodeEmitter.SetContext("exec_seq", static_cast<int64_t>(execSeq));  // Execution order
    }
    return execSeq;
  }

  void EmitNodeCompleted(const std::string& nodeId, size_t assetsProcessed,
                         size_t assetsFailed, int64_t durationMs) const {
    // Get completion sequence number
    size_t completeSeq = completionSequence ? completionSequence->fetch_add(1) : 0;

    if (externalEmitter) {
      using namespace data_sdk::events;
      // Must use same ChildScope path as EmitNodeStarted for the viewer to match events
      auto nodeEmitter = externalEmitter->ChildScope(ScopeType::Node, nodeId);
      nodeEmitter.EmitCompleted(std::string(OperationType::Node), nodeId);
      nodeEmitter.SetContext(std::string(ContextKey::AssetsProcessed), static_cast<int64_t>(assetsProcessed));
      nodeEmitter.SetContext(std::string(ContextKey::AssetsFailed), static_cast<int64_t>(assetsFailed));
      nodeEmitter.SetContext(std::string(ContextKey::DurationMs), durationMs);
      nodeEmitter.SetContext("complete_seq", static_cast<int64_t>(completeSeq));  // Completion order
    }
  }

  void EmitNodeFailed(const std::string& nodeId, const std::string& errorMessage) const {
    if (externalEmitter) {
      using namespace data_sdk::events;
      // Must use same ChildScope path as EmitNodeStarted for the viewer to match events
      auto nodeEmitter = externalEmitter->ChildScope(ScopeType::Node, nodeId);
      nodeEmitter.EmitFailed(std::string(OperationType::Node), nodeId, errorMessage);
    }
  }

  void EmitNodeSkipped(const std::string& nodeId, const std::string& reason) const {
    if (externalEmitter) {
      using namespace data_sdk::events;
      // Must use same ChildScope path as EmitNodeStarted for the viewer to match events
      auto nodeEmitter = externalEmitter->ChildScope(ScopeType::Node, nodeId);
      nodeEmitter.EmitSkipped(std::string(OperationType::Node), nodeId, reason);
    }
  }

  void EmitNodeProgress(const std::string& nodeId, size_t current, size_t total, const std::string& message) const {
    if (externalEmitter) {
      using namespace data_sdk::events;
      // Must use same ChildScope path as EmitNodeStarted for the viewer to match events
      auto nodeEmitter = externalEmitter->ChildScope(ScopeType::Node, nodeId);
      nodeEmitter.EmitProgress(current, total, message);
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

  // ==========================================================================
  // Logging helpers - emit logs via the event system for client visibility
  // These replace SPDLOG_* calls for logs that should be visible to clients.
  // Keep SPDLOG_* only for internal debug/critical logs not meant for clients.
  // ==========================================================================

  void EmitDebug(const std::string& message) const {
    if (externalEmitter) {
      externalEmitter->EmitDebug(message);
    }
  }

  void EmitInfo(const std::string& message) const {
    if (externalEmitter) {
      externalEmitter->EmitInfo(message);
    }
  }

  void EmitWarning(const std::string& message) const {
    if (externalEmitter) {
      externalEmitter->EmitWarning(message);
    }
  }

  void EmitError(const std::string& message) const {
    if (externalEmitter) {
      externalEmitter->EmitError(message);
    }
  }

  // Emit a node-scoped log (appears under the node's path in viewer)
  void EmitNodeLog(const std::string& nodeId, data_sdk::events::LogEvent::Level level,
                   const std::string& message) const {
    if (externalEmitter) {
      auto nodeEmitter = externalEmitter->ChildScope(data_sdk::events::ScopeType::Node, nodeId);
      nodeEmitter.EmitLog(level, message);
    }
  }

  void EmitNodeDebug(const std::string& nodeId, const std::string& message) const {
    EmitNodeLog(nodeId, data_sdk::events::LogEvent::Level::Debug, message);
  }

  void EmitNodeInfo(const std::string& nodeId, const std::string& message) const {
    EmitNodeLog(nodeId, data_sdk::events::LogEvent::Level::Info, message);
  }

  void EmitNodeWarning(const std::string& nodeId, const std::string& message) const {
    EmitNodeLog(nodeId, data_sdk::events::LogEvent::Level::Warning, message);
  }

  void EmitNodeError(const std::string& nodeId, const std::string& message) const {
    EmitNodeLog(nodeId, data_sdk::events::LogEvent::Level::Error, message);
  }
};

} // namespace epoch_script::runtime
