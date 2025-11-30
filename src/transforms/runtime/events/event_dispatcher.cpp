//
// Event Dispatcher Implementation
//

#include "event_dispatcher.h"

namespace epoch_script::runtime::events {

// ============================================================================
// EventFilter Implementation
// ============================================================================

EventFilter::EventFilter(std::set<EventType> types, bool whitelist)
    : m_types(std::move(types))
    , m_isWhitelist(whitelist) {}

EventFilter EventFilter::All() {
    return EventFilter({}, false);  // Empty blacklist = accept all
}

EventFilter EventFilter::None() {
    return EventFilter({}, true);  // Empty whitelist = accept none
}

EventFilter EventFilter::Only(std::initializer_list<EventType> types) {
    return EventFilter(std::set<EventType>(types), true);
}

EventFilter EventFilter::Except(std::initializer_list<EventType> types) {
    return EventFilter(std::set<EventType>(types), false);
}

EventFilter EventFilter::PipelineOnly() {
    return Only({
        EventType::PipelineStarted,
        EventType::PipelineCompleted,
        EventType::PipelineFailed,
        EventType::PipelineCancelled
    });
}

EventFilter EventFilter::NodesOnly() {
    return Only({
        EventType::NodeStarted,
        EventType::NodeCompleted,
        EventType::NodeFailed,
        EventType::NodeSkipped
    });
}

EventFilter EventFilter::ProgressOnly() {
    return Only({
        EventType::TransformProgress,
        EventType::ProgressSummary
    });
}

EventFilter EventFilter::TransformProgressOnly() {
    return Only({EventType::TransformProgress});
}

bool EventFilter::Accepts(EventType type) const {
    bool found = m_types.contains(type);
    return m_isWhitelist ? found : !found;
}

bool EventFilter::Accepts(const OrchestratorEvent& event) const {
    return Accepts(GetEventType(event));
}

EventFilter EventFilter::operator|(const EventFilter& other) const {
    // Union of two filters
    if (!m_isWhitelist && !other.m_isWhitelist) {
        // Both blacklists: intersection of blacklists
        std::set<EventType> intersection;
        std::set_intersection(
            m_types.begin(), m_types.end(),
            other.m_types.begin(), other.m_types.end(),
            std::inserter(intersection, intersection.begin())
        );
        return EventFilter(intersection, false);
    } else if (m_isWhitelist && other.m_isWhitelist) {
        // Both whitelists: union of whitelists
        std::set<EventType> united;
        std::set_union(
            m_types.begin(), m_types.end(),
            other.m_types.begin(), other.m_types.end(),
            std::inserter(united, united.begin())
        );
        return EventFilter(united, true);
    } else {
        // Mixed: whitelist takes precedence, subtract blacklist
        const auto& whitelist = m_isWhitelist ? m_types : other.m_types;
        const auto& blacklist = m_isWhitelist ? other.m_types : m_types;
        std::set<EventType> result;
        std::set_difference(
            whitelist.begin(), whitelist.end(),
            blacklist.begin(), blacklist.end(),
            std::inserter(result, result.begin())
        );
        return EventFilter(result, true);
    }
}

EventFilter EventFilter::operator&(const EventFilter& other) const {
    // Intersection of two filters
    if (m_isWhitelist && other.m_isWhitelist) {
        // Both whitelists: intersection
        std::set<EventType> intersection;
        std::set_intersection(
            m_types.begin(), m_types.end(),
            other.m_types.begin(), other.m_types.end(),
            std::inserter(intersection, intersection.begin())
        );
        return EventFilter(intersection, true);
    } else if (!m_isWhitelist && !other.m_isWhitelist) {
        // Both blacklists: union of blacklists
        std::set<EventType> united;
        std::set_union(
            m_types.begin(), m_types.end(),
            other.m_types.begin(), other.m_types.end(),
            std::inserter(united, united.begin())
        );
        return EventFilter(united, false);
    } else {
        // Mixed: subtract blacklist from whitelist
        const auto& whitelist = m_isWhitelist ? m_types : other.m_types;
        const auto& blacklist = m_isWhitelist ? other.m_types : m_types;
        std::set<EventType> result;
        std::set_difference(
            whitelist.begin(), whitelist.end(),
            blacklist.begin(), blacklist.end(),
            std::inserter(result, result.begin())
        );
        return EventFilter(result, true);
    }
}

// ============================================================================
// EventDispatcher Implementation
// ============================================================================

void EventDispatcher::Emit(OrchestratorEvent event) {
    // boost::signals2 is thread-safe for emission
    m_signal(event);
}

boost::signals2::connection EventDispatcher::Subscribe(
    OrchestratorEventSlot handler,
    EventFilter filter) {

    // Wrap the handler with filter check
    auto filteredHandler = [filter = std::move(filter),
                            handler = std::move(handler)]
                           (const OrchestratorEvent& event) {
        if (filter.Accepts(event)) {
            handler(event);
        }
    };

    // boost::signals2 is thread-safe for connection
    return m_signal.connect(std::move(filteredHandler));
}

size_t EventDispatcher::SubscriberCount() const {
    return m_signal.num_slots();
}

} // namespace epoch_script::runtime::events
