#pragma once
//
// Event Dispatcher Interface and Implementation
// Thread-safe event emission using boost::signals2
//

#include "orchestrator_events.h"
#include <memory>
#include <mutex>
#include <set>
#include <functional>

namespace epoch_script::runtime::events {

// ============================================================================
// Event Filter - Determines which events a subscriber receives
// ============================================================================

class EventFilter {
public:
    // Factory methods for common filters
    static EventFilter All();
    static EventFilter None();
    static EventFilter Only(std::initializer_list<EventType> types);
    static EventFilter Except(std::initializer_list<EventType> types);

    // Preset filters
    static EventFilter PipelineOnly();
    static EventFilter NodesOnly();
    static EventFilter ProgressOnly();
    static EventFilter TransformProgressOnly();

    // Check if filter accepts an event
    [[nodiscard]] bool Accepts(EventType type) const;
    [[nodiscard]] bool Accepts(const OrchestratorEvent& event) const;

    // Combine filters
    EventFilter operator|(const EventFilter& other) const;
    EventFilter operator&(const EventFilter& other) const;

private:
    std::set<EventType> m_types;
    bool m_isWhitelist{true};  // true = only accept listed, false = accept all except listed

    EventFilter(std::set<EventType> types, bool whitelist);
};

// ============================================================================
// Event Dispatcher Interface
// ============================================================================

class IEventDispatcher {
public:
    virtual ~IEventDispatcher() = default;

    // Emit an event to all subscribers (thread-safe)
    virtual void Emit(OrchestratorEvent event) = 0;

    // Subscribe to events with optional filter
    // Returns connection that can be used to disconnect
    virtual boost::signals2::connection Subscribe(
        OrchestratorEventSlot handler,
        EventFilter filter = EventFilter::All()) = 0;

    // Typed subscription helper - only receives events of type T
    template<typename T>
    boost::signals2::connection SubscribeTo(std::function<void(const T&)> handler) {
        return Subscribe(
            [handler = std::move(handler)](const OrchestratorEvent& event) {
                if (const auto* typed = std::get_if<T>(&event)) {
                    handler(*typed);
                }
            },
            EventFilter::Only({EventTypeFor<T>::value})
        );
    }
};

using IEventDispatcherPtr = std::shared_ptr<IEventDispatcher>;

// ============================================================================
// Thread-Safe Event Dispatcher Implementation
// ============================================================================

class EventDispatcher : public IEventDispatcher {
public:
    EventDispatcher() = default;
    ~EventDispatcher() override = default;

    // Non-copyable
    EventDispatcher(const EventDispatcher&) = delete;
    EventDispatcher& operator=(const EventDispatcher&) = delete;

    void Emit(OrchestratorEvent event) override;

    boost::signals2::connection Subscribe(
        OrchestratorEventSlot handler,
        EventFilter filter = EventFilter::All()) override;

    // Get number of connected subscribers
    [[nodiscard]] size_t SubscriberCount() const;

private:
    OrchestratorEventSignal m_signal;
};

// ============================================================================
// Null Event Dispatcher (No-op when events are disabled)
// ============================================================================

class NullEventDispatcher : public IEventDispatcher {
public:
    void Emit(OrchestratorEvent) override {
        // No-op
    }

    boost::signals2::connection Subscribe(
        OrchestratorEventSlot,
        EventFilter) override {
        return {};  // Return empty connection
    }
};

// ============================================================================
// Factory Functions
// ============================================================================

inline IEventDispatcherPtr MakeEventDispatcher() {
    return std::make_shared<EventDispatcher>();
}

inline IEventDispatcherPtr MakeNullEventDispatcher() {
    return std::make_shared<NullEventDispatcher>();
}

} // namespace epoch_script::runtime::events
