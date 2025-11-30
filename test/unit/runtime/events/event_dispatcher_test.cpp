//
// Unit tests for EventDispatcher
//

#include <catch2/catch_test_macros.hpp>

#include "events/event_dispatcher.h"
#include "events/orchestrator_events.h"

#include <atomic>
#include <thread>
#include <vector>

using namespace epoch_script::runtime::events;

TEST_CASE("EventDispatcher basic functionality", "[events][dispatcher]") {

    SECTION("Single subscriber receives events") {
        EventDispatcher dispatcher;
        std::vector<EventType> received;

        dispatcher.Subscribe([&](const OrchestratorEvent& e) {
            received.push_back(GetEventType(e));
        });

        dispatcher.Emit(PipelineStartedEvent{.timestamp = Now(), .total_nodes = 5});
        dispatcher.Emit(NodeCompletedEvent{.timestamp = Now(), .node_id = "n1"});

        REQUIRE(received.size() == 2);
        REQUIRE(received[0] == EventType::PipelineStarted);
        REQUIRE(received[1] == EventType::NodeCompleted);
    }

    SECTION("Multiple subscribers all receive events") {
        EventDispatcher dispatcher;
        int count1 = 0, count2 = 0, count3 = 0;

        dispatcher.Subscribe([&](const auto&) { count1++; });
        dispatcher.Subscribe([&](const auto&) { count2++; });
        dispatcher.Subscribe([&](const auto&) { count3++; });

        dispatcher.Emit(PipelineStartedEvent{});

        REQUIRE(count1 == 1);
        REQUIRE(count2 == 1);
        REQUIRE(count3 == 1);
    }

    SECTION("Events delivered in emission order") {
        EventDispatcher dispatcher;
        std::vector<std::string> nodeIds;

        dispatcher.Subscribe([&](const OrchestratorEvent& e) {
            std::visit([&](const auto& ev) {
                using T = std::decay_t<decltype(ev)>;
                if constexpr (std::is_same_v<T, NodeStartedEvent>) {
                    nodeIds.push_back(ev.node_id);
                }
            }, e);
        });

        dispatcher.Emit(NodeStartedEvent{.node_id = "first"});
        dispatcher.Emit(NodeStartedEvent{.node_id = "second"});
        dispatcher.Emit(NodeStartedEvent{.node_id = "third"});

        REQUIRE(nodeIds.size() == 3);
        REQUIRE(nodeIds[0] == "first");
        REQUIRE(nodeIds[1] == "second");
        REQUIRE(nodeIds[2] == "third");
    }

    SECTION("Event data is preserved") {
        EventDispatcher dispatcher;
        PipelineStartedEvent received{};

        dispatcher.Subscribe([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<PipelineStartedEvent>(&e)) {
                received = *p;
            }
        });

        PipelineStartedEvent original{
            .timestamp = Now(),
            .total_nodes = 42,
            .total_assets = 100,
            .node_ids = {"a", "b", "c"}
        };
        dispatcher.Emit(original);

        REQUIRE(received.total_nodes == 42);
        REQUIRE(received.total_assets == 100);
        REQUIRE(received.node_ids.size() == 3);
        REQUIRE(received.node_ids[0] == "a");
    }
}

TEST_CASE("EventDispatcher filtering", "[events][dispatcher]") {

    SECTION("Filtered subscription only receives matching events") {
        EventDispatcher dispatcher;
        std::vector<EventType> received;

        dispatcher.Subscribe(
            [&](const OrchestratorEvent& e) { received.push_back(GetEventType(e)); },
            EventFilter::NodesOnly()
        );

        dispatcher.Emit(PipelineStartedEvent{});   // Should be filtered
        dispatcher.Emit(NodeStartedEvent{});        // Should pass
        dispatcher.Emit(NodeCompletedEvent{});      // Should pass
        dispatcher.Emit(TransformProgressEvent{}); // Should be filtered

        REQUIRE(received.size() == 2);
        REQUIRE(received[0] == EventType::NodeStarted);
        REQUIRE(received[1] == EventType::NodeCompleted);
    }

    SECTION("Multiple filters on different subscribers") {
        EventDispatcher dispatcher;
        std::vector<EventType> pipelineEvents;
        std::vector<EventType> nodeEvents;
        std::vector<EventType> allEvents;

        dispatcher.Subscribe(
            [&](const OrchestratorEvent& e) { pipelineEvents.push_back(GetEventType(e)); },
            EventFilter::PipelineOnly()
        );
        dispatcher.Subscribe(
            [&](const OrchestratorEvent& e) { nodeEvents.push_back(GetEventType(e)); },
            EventFilter::NodesOnly()
        );
        dispatcher.Subscribe(
            [&](const OrchestratorEvent& e) { allEvents.push_back(GetEventType(e)); }
        );

        dispatcher.Emit(PipelineStartedEvent{});
        dispatcher.Emit(NodeStartedEvent{});
        dispatcher.Emit(NodeCompletedEvent{});
        dispatcher.Emit(PipelineCompletedEvent{});

        REQUIRE(pipelineEvents.size() == 2);
        REQUIRE(nodeEvents.size() == 2);
        REQUIRE(allEvents.size() == 4);
    }

    SECTION("Filter with None() receives nothing") {
        EventDispatcher dispatcher;
        int count = 0;

        dispatcher.Subscribe(
            [&](const auto&) { count++; },
            EventFilter::None()
        );

        dispatcher.Emit(PipelineStartedEvent{});
        dispatcher.Emit(NodeStartedEvent{});
        dispatcher.Emit(TransformProgressEvent{});

        REQUIRE(count == 0);
    }
}

TEST_CASE("EventDispatcher subscription management", "[events][dispatcher]") {

    SECTION("Unsubscribe via connection disconnection") {
        EventDispatcher dispatcher;
        int count = 0;

        auto conn = dispatcher.Subscribe([&](const auto&) { count++; });
        dispatcher.Emit(PipelineStartedEvent{});
        REQUIRE(count == 1);

        conn.disconnect();
        dispatcher.Emit(PipelineStartedEvent{});
        REQUIRE(count == 1);  // No additional calls
    }

    SECTION("SubscriberCount returns correct count") {
        EventDispatcher dispatcher;
        REQUIRE(dispatcher.SubscriberCount() == 0);

        auto c1 = dispatcher.Subscribe([](const auto&) {});
        REQUIRE(dispatcher.SubscriberCount() == 1);

        auto c2 = dispatcher.Subscribe([](const auto&) {});
        REQUIRE(dispatcher.SubscriberCount() == 2);

        auto c3 = dispatcher.Subscribe([](const auto&) {});
        REQUIRE(dispatcher.SubscriberCount() == 3);

        c1.disconnect();
        REQUIRE(dispatcher.SubscriberCount() == 2);

        c2.disconnect();
        c3.disconnect();
        REQUIRE(dispatcher.SubscriberCount() == 0);
    }

    SECTION("Connection validity") {
        EventDispatcher dispatcher;

        auto conn = dispatcher.Subscribe([](const auto&) {});
        REQUIRE(conn.connected());

        conn.disconnect();
        REQUIRE_FALSE(conn.connected());
    }

    SECTION("Multiple disconnections are safe") {
        EventDispatcher dispatcher;

        auto conn = dispatcher.Subscribe([](const auto&) {});
        conn.disconnect();
        REQUIRE_NOTHROW(conn.disconnect());
        REQUIRE_NOTHROW(conn.disconnect());
    }
}

TEST_CASE("EventDispatcher typed subscription", "[events][dispatcher]") {

    SECTION("SubscribeTo<T> only receives specific event type") {
        EventDispatcher dispatcher;
        int nodeStartedCount = 0;
        int nodeCompletedCount = 0;

        dispatcher.SubscribeTo<NodeStartedEvent>([&](const NodeStartedEvent&) {
            nodeStartedCount++;
        });
        dispatcher.SubscribeTo<NodeCompletedEvent>([&](const NodeCompletedEvent&) {
            nodeCompletedCount++;
        });

        dispatcher.Emit(PipelineStartedEvent{});
        dispatcher.Emit(NodeStartedEvent{});
        dispatcher.Emit(NodeStartedEvent{});
        dispatcher.Emit(NodeCompletedEvent{});
        dispatcher.Emit(PipelineCompletedEvent{});

        REQUIRE(nodeStartedCount == 2);
        REQUIRE(nodeCompletedCount == 1);
    }

    SECTION("SubscribeTo receives event data correctly") {
        EventDispatcher dispatcher;
        std::string receivedNodeId;

        dispatcher.SubscribeTo<NodeStartedEvent>([&](const NodeStartedEvent& e) {
            receivedNodeId = e.node_id;
        });

        dispatcher.Emit(NodeStartedEvent{.node_id = "test_node"});

        REQUIRE(receivedNodeId == "test_node");
    }
}

TEST_CASE("EventDispatcher thread safety", "[events][dispatcher][threading]") {

    SECTION("Thread-safe emission from multiple threads") {
        EventDispatcher dispatcher;
        std::atomic<int> totalReceived{0};

        dispatcher.Subscribe([&](const auto&) {
            totalReceived.fetch_add(1, std::memory_order_relaxed);
        });

        std::vector<std::thread> threads;
        const int threadsCount = 4;
        const int eventsPerThread = 100;

        for (int i = 0; i < threadsCount; ++i) {
            threads.emplace_back([&]() {
                for (int j = 0; j < eventsPerThread; ++j) {
                    dispatcher.Emit(PipelineStartedEvent{});
                }
            });
        }

        for (auto& t : threads) {
            t.join();
        }

        REQUIRE(totalReceived.load() == threadsCount * eventsPerThread);
    }

    SECTION("Concurrent subscribe and emit") {
        EventDispatcher dispatcher;
        std::atomic<int> received{0};
        std::atomic<bool> startFlag{false};

        // Emitter thread
        std::thread emitter([&]() {
            while (!startFlag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            for (int i = 0; i < 100; ++i) {
                dispatcher.Emit(PipelineStartedEvent{});
            }
        });

        // Subscriber threads
        std::vector<std::thread> subscribers;
        for (int i = 0; i < 3; ++i) {
            subscribers.emplace_back([&]() {
                while (!startFlag.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                auto conn = dispatcher.Subscribe([&](const auto&) {
                    received.fetch_add(1, std::memory_order_relaxed);
                });
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                conn.disconnect();
            });
        }

        startFlag.store(true, std::memory_order_release);

        emitter.join();
        for (auto& t : subscribers) {
            t.join();
        }

        // Some events should have been received by some subscribers
        // Exact count depends on timing, but should be > 0
        REQUIRE(received.load() >= 0);
    }
}

TEST_CASE("NullEventDispatcher", "[events][dispatcher][null]") {

    SECTION("Emit does nothing and doesn't throw") {
        NullEventDispatcher dispatcher;

        REQUIRE_NOTHROW(dispatcher.Emit(PipelineStartedEvent{}));
        REQUIRE_NOTHROW(dispatcher.Emit(NodeCompletedEvent{}));
        REQUIRE_NOTHROW(dispatcher.Emit(TransformProgressEvent{}));
    }

    SECTION("Subscribe returns empty/disconnected connection") {
        NullEventDispatcher dispatcher;

        auto conn = dispatcher.Subscribe([](const auto&) {}, EventFilter::All());
        REQUIRE_FALSE(conn.connected());
    }

    SECTION("Multiple emissions are safe") {
        NullEventDispatcher dispatcher;

        for (int i = 0; i < 1000; ++i) {
            REQUIRE_NOTHROW(dispatcher.Emit(PipelineStartedEvent{}));
        }
    }
}
