//
// Unit tests for TransformProgressEmitter
//

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>

#include "events/transform_progress_emitter.h"
#include "events/event_dispatcher.h"
#include "events/cancellation_token.h"

using namespace epoch_script::runtime::events;

TEST_CASE("TransformProgressEmitter construction", "[events][emitter]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();

    SECTION("Constructor sets node_id and transform_name") {
        TransformProgressEmitter emitter(dispatcher, token, "node_123", "SMATransform");

        REQUIRE(emitter.GetNodeId() == "node_123");
        REQUIRE(emitter.GetTransformName() == "SMATransform");
    }

    SECTION("Initial state has no asset ID") {
        TransformProgressEmitter emitter(dispatcher, token, "n1", "t1");

        REQUIRE_FALSE(emitter.GetAssetId().has_value());
    }

    SECTION("Works with null dispatcher") {
        REQUIRE_NOTHROW(TransformProgressEmitter(nullptr, token, "n1", "t1"));
    }

    SECTION("Works with null cancellation token") {
        REQUIRE_NOTHROW(TransformProgressEmitter(dispatcher, nullptr, "n1", "t1"));
    }

    SECTION("Works with both null") {
        REQUIRE_NOTHROW(TransformProgressEmitter(nullptr, nullptr, "n1", "t1"));
    }
}

TEST_CASE("TransformProgressEmitter asset context", "[events][emitter]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();
    TransformProgressEmitter emitter(dispatcher, token, "n1", "t1");

    SECTION("SetAssetId sets the asset ID") {
        emitter.SetAssetId("AAPL");
        REQUIRE(emitter.GetAssetId().has_value());
        REQUIRE(*emitter.GetAssetId() == "AAPL");
    }

    SECTION("ClearAssetId clears the asset ID") {
        emitter.SetAssetId("MSFT");
        REQUIRE(emitter.GetAssetId().has_value());

        emitter.ClearAssetId();
        REQUIRE_FALSE(emitter.GetAssetId().has_value());
    }

    SECTION("Asset ID can be changed") {
        emitter.SetAssetId("AAPL");
        REQUIRE(*emitter.GetAssetId() == "AAPL");

        emitter.SetAssetId("GOOG");
        REQUIRE(*emitter.GetAssetId() == "GOOG");
    }

    SECTION("Asset ID included in emitted events") {
        TransformProgressEvent received{};

        dispatcher->Subscribe([&](const OrchestratorEvent& e) {
            if (auto* p = std::get_if<TransformProgressEvent>(&e)) {
                received = *p;
            }
        });

        emitter.SetAssetId("MSFT");
        emitter.EmitProgress(1, 10, "");

        REQUIRE(received.asset_id.has_value());
        REQUIRE(*received.asset_id == "MSFT");
    }
}

TEST_CASE("TransformProgressEmitter EmitProgress", "[events][emitter]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();
    TransformProgressEmitter emitter(dispatcher, token, "n1", "TestTransform");
    TransformProgressEvent received{};

    dispatcher->Subscribe([&](const OrchestratorEvent& e) {
        if (auto* p = std::get_if<TransformProgressEvent>(&e)) {
            received = *p;
        }
    });

    SECTION("Sets current_step and total_steps") {
        emitter.EmitProgress(25, 100, "Processing...");

        REQUIRE(received.current_step.has_value());
        REQUIRE(*received.current_step == 25);
        REQUIRE(received.total_steps.has_value());
        REQUIRE(*received.total_steps == 100);
    }

    SECTION("Calculates progress percentage") {
        emitter.EmitProgress(25, 100, "");

        REQUIRE(received.progress_percent.has_value());
        REQUIRE_THAT(*received.progress_percent, Catch::Matchers::WithinAbs(25.0, 0.01));
    }

    SECTION("Sets message") {
        emitter.EmitProgress(1, 10, "Processing item 1 of 10");

        REQUIRE(received.message == "Processing item 1 of 10");
    }

    SECTION("Zero total avoids division by zero") {
        REQUIRE_NOTHROW(emitter.EmitProgress(5, 0, ""));
        REQUIRE_FALSE(received.progress_percent.has_value());
    }

    SECTION("Progress at 100%") {
        emitter.EmitProgress(100, 100, "Complete");

        REQUIRE_THAT(*received.progress_percent, Catch::Matchers::WithinAbs(100.0, 0.01));
    }

    SECTION("Progress at 0%") {
        emitter.EmitProgress(0, 100, "Starting");

        REQUIRE_THAT(*received.progress_percent, Catch::Matchers::WithinAbs(0.0, 0.01));
    }

    SECTION("Sets node_id and transform_name") {
        emitter.EmitProgress(1, 10, "");

        REQUIRE(received.node_id == "n1");
        REQUIRE(received.transform_name == "TestTransform");
    }

    SECTION("Sets timestamp") {
        auto before = Now();
        emitter.EmitProgress(1, 10, "");
        auto after = Now();

        REQUIRE(received.timestamp >= before);
        REQUIRE(received.timestamp <= after);
    }
}

TEST_CASE("TransformProgressEmitter EmitEpoch", "[events][emitter]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();
    TransformProgressEmitter emitter(dispatcher, token, "n1", "HMM");
    TransformProgressEvent received{};

    dispatcher->Subscribe([&](const OrchestratorEvent& e) {
        if (auto* p = std::get_if<TransformProgressEvent>(&e)) {
            received = *p;
        }
    });

    SECTION("Sets epoch as current_step") {
        emitter.EmitEpoch(47, 100);

        REQUIRE(received.current_step.has_value());
        REQUIRE(*received.current_step == 47);
        REQUIRE(received.total_steps.has_value());
        REQUIRE(*received.total_steps == 100);
    }

    SECTION("Sets loss") {
        emitter.EmitEpoch(1, 10, 0.0234);

        REQUIRE(received.loss.has_value());
        REQUIRE_THAT(*received.loss, Catch::Matchers::WithinAbs(0.0234, 0.0001));
    }

    SECTION("Sets accuracy") {
        emitter.EmitEpoch(1, 10, std::nullopt, 0.89);

        REQUIRE(received.accuracy.has_value());
        REQUIRE_THAT(*received.accuracy, Catch::Matchers::WithinAbs(0.89, 0.001));
    }

    SECTION("Sets learning_rate") {
        emitter.EmitEpoch(1, 10, std::nullopt, std::nullopt, 0.001);

        REQUIRE(received.learning_rate.has_value());
        REQUIRE_THAT(*received.learning_rate, Catch::Matchers::WithinAbs(0.001, 0.0001));
    }

    SECTION("All ML parameters") {
        emitter.EmitEpoch(50, 100, 0.0234, 0.89, 0.001);

        REQUIRE(*received.current_step == 50);
        REQUIRE(*received.total_steps == 100);
        REQUIRE_THAT(*received.loss, Catch::Matchers::WithinAbs(0.0234, 0.0001));
        REQUIRE_THAT(*received.accuracy, Catch::Matchers::WithinAbs(0.89, 0.001));
        REQUIRE_THAT(*received.learning_rate, Catch::Matchers::WithinAbs(0.001, 0.0001));
    }

    SECTION("Builds message with epoch info") {
        emitter.EmitEpoch(47, 100);

        REQUIRE_THAT(received.message, Catch::Matchers::ContainsSubstring("Epoch"));
        REQUIRE_THAT(received.message, Catch::Matchers::ContainsSubstring("47"));
        REQUIRE_THAT(received.message, Catch::Matchers::ContainsSubstring("100"));
    }

    SECTION("Message includes loss when provided") {
        emitter.EmitEpoch(1, 10, 0.0234);

        REQUIRE_THAT(received.message, Catch::Matchers::ContainsSubstring("loss"));
    }
}

TEST_CASE("TransformProgressEmitter EmitIteration", "[events][emitter]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();
    TransformProgressEmitter emitter(dispatcher, token, "n1", "t1");
    TransformProgressEvent received{};

    dispatcher->Subscribe([&](const OrchestratorEvent& e) {
        if (auto* p = std::get_if<TransformProgressEvent>(&e)) {
            received = *p;
        }
    });

    SECTION("Sets iteration field") {
        emitter.EmitIteration(42);

        REQUIRE(received.iteration.has_value());
        REQUIRE(*received.iteration == 42);
    }

    SECTION("Stores metric in metadata") {
        emitter.EmitIteration(1, 3.14159);

        REQUIRE(received.metadata.contains("metric"));
    }

    SECTION("Sets custom message") {
        emitter.EmitIteration(10, std::nullopt, "Converging");

        REQUIRE(received.message == "Converging");
    }

    SECTION("Default message includes iteration") {
        emitter.EmitIteration(42);

        REQUIRE_THAT(received.message, Catch::Matchers::ContainsSubstring("Iteration"));
        REQUIRE_THAT(received.message, Catch::Matchers::ContainsSubstring("42"));
    }
}

TEST_CASE("TransformProgressEmitter Emit (raw)", "[events][emitter]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();
    TransformProgressEmitter emitter(dispatcher, token, "n1", "t1");
    TransformProgressEvent received{};

    dispatcher->Subscribe([&](const OrchestratorEvent& e) {
        if (auto* p = std::get_if<TransformProgressEvent>(&e)) {
            received = *p;
        }
    });

    SECTION("Auto-fills missing node_id") {
        TransformProgressEvent partial;
        partial.message = "Custom event";
        emitter.Emit(partial);

        REQUIRE(received.node_id == "n1");
    }

    SECTION("Auto-fills missing transform_name") {
        TransformProgressEvent partial;
        partial.message = "Custom event";
        emitter.Emit(partial);

        REQUIRE(received.transform_name == "t1");
    }

    SECTION("Auto-fills missing timestamp") {
        TransformProgressEvent partial;
        partial.message = "Custom event";
        emitter.Emit(partial);

        REQUIRE(received.timestamp != Timestamp{});
    }

    SECTION("Preserves provided fields") {
        TransformProgressEvent custom;
        custom.node_id = "custom_node";
        custom.transform_name = "custom_transform";
        custom.current_step = 99;
        custom.message = "Custom";
        emitter.Emit(custom);

        REQUIRE(received.node_id == "custom_node");
        REQUIRE(received.transform_name == "custom_transform");
        REQUIRE(*received.current_step == 99);
    }

    SECTION("Includes asset_id from emitter context") {
        emitter.SetAssetId("AAPL");

        TransformProgressEvent partial;
        partial.message = "Test";
        emitter.Emit(partial);

        REQUIRE(received.asset_id.has_value());
        REQUIRE(*received.asset_id == "AAPL");
    }
}

TEST_CASE("TransformProgressEmitter null dispatcher", "[events][emitter]") {

    auto token = std::make_shared<CancellationToken>();
    TransformProgressEmitter emitter(nullptr, token, "n1", "t1");

    SECTION("EmitProgress is no-op") {
        REQUIRE_NOTHROW(emitter.EmitProgress(1, 10, "test"));
    }

    SECTION("EmitEpoch is no-op") {
        REQUIRE_NOTHROW(emitter.EmitEpoch(1, 10, 0.1, 0.9));
    }

    SECTION("EmitIteration is no-op") {
        REQUIRE_NOTHROW(emitter.EmitIteration(1, 0.5, "test"));
    }

    SECTION("EmitCustomProgress is no-op") {
        REQUIRE_NOTHROW(emitter.EmitCustomProgress({}, "test"));
    }

    SECTION("Emit is no-op") {
        REQUIRE_NOTHROW(emitter.Emit(TransformProgressEvent{}));
    }
}

TEST_CASE("TransformProgressEmitter cancellation", "[events][emitter]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();
    TransformProgressEmitter emitter(dispatcher, token, "n1", "t1");

    SECTION("IsCancelled returns false when not cancelled") {
        REQUIRE_FALSE(emitter.IsCancelled());
    }

    SECTION("IsCancelled returns true after cancellation") {
        token->Cancel();
        REQUIRE(emitter.IsCancelled());
    }

    SECTION("ThrowIfCancelled does not throw when not cancelled") {
        REQUIRE_NOTHROW(emitter.ThrowIfCancelled());
    }

    SECTION("ThrowIfCancelled throws after cancellation") {
        token->Cancel();
        REQUIRE_THROWS_AS(emitter.ThrowIfCancelled(), OperationCancelledException);
    }

    SECTION("EmitEpochOrCancel emits when not cancelled") {
        int emitCount = 0;
        dispatcher->Subscribe([&](const auto&) { emitCount++; });

        REQUIRE_NOTHROW(emitter.EmitEpochOrCancel(1, 10));
        REQUIRE(emitCount == 1);
    }

    SECTION("EmitEpochOrCancel throws when cancelled") {
        token->Cancel();
        REQUIRE_THROWS_AS(emitter.EmitEpochOrCancel(1, 10), OperationCancelledException);
    }

    SECTION("EmitIterationOrCancel emits when not cancelled") {
        int emitCount = 0;
        dispatcher->Subscribe([&](const auto&) { emitCount++; });

        REQUIRE_NOTHROW(emitter.EmitIterationOrCancel(1));
        REQUIRE(emitCount == 1);
    }

    SECTION("EmitIterationOrCancel throws when cancelled") {
        token->Cancel();
        REQUIRE_THROWS_AS(emitter.EmitIterationOrCancel(1), OperationCancelledException);
    }

    SECTION("Null token means no cancellation support") {
        TransformProgressEmitter emitterNoToken(dispatcher, nullptr, "n1", "t1");

        REQUIRE_FALSE(emitterNoToken.IsCancelled());
        REQUIRE_NOTHROW(emitterNoToken.ThrowIfCancelled());
    }
}

TEST_CASE("AssetContextGuard RAII", "[events][emitter][raii]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();
    TransformProgressEmitter emitter(dispatcher, token, "n1", "t1");

    SECTION("Guard sets asset context on construction") {
        REQUIRE_FALSE(emitter.GetAssetId().has_value());

        {
            AssetContextGuard guard(emitter, "AAPL");
            REQUIRE(emitter.GetAssetId().has_value());
            REQUIRE(*emitter.GetAssetId() == "AAPL");
        }
    }

    SECTION("Guard clears asset context on destruction") {
        {
            AssetContextGuard guard(emitter, "AAPL");
        }

        REQUIRE_FALSE(emitter.GetAssetId().has_value());
    }

    SECTION("Guard clears context even on exception") {
        try {
            AssetContextGuard guard(emitter, "GOOG");
            throw std::runtime_error("test exception");
        } catch (const std::runtime_error&) {
            // Expected
        }

        REQUIRE_FALSE(emitter.GetAssetId().has_value());
    }

    SECTION("Nested guards work correctly") {
        {
            AssetContextGuard outerGuard(emitter, "AAPL");
            REQUIRE(*emitter.GetAssetId() == "AAPL");

            {
                AssetContextGuard innerGuard(emitter, "GOOG");
                REQUIRE(*emitter.GetAssetId() == "GOOG");
            }

            // Note: After inner guard destruction, context is cleared
            // This is expected behavior - guards don't stack
            REQUIRE_FALSE(emitter.GetAssetId().has_value());
        }
    }
}

TEST_CASE("MakeProgressEmitter factory", "[events][emitter]") {

    auto dispatcher = std::make_shared<EventDispatcher>();
    auto token = std::make_shared<CancellationToken>();

    SECTION("Creates valid emitter") {
        auto emitter = MakeProgressEmitter(dispatcher, token, "node_id", "transform_name");

        REQUIRE(emitter != nullptr);
        REQUIRE(emitter->GetNodeId() == "node_id");
        REQUIRE(emitter->GetTransformName() == "transform_name");
    }

    SECTION("Emitter can emit events") {
        auto emitter = MakeProgressEmitter(dispatcher, token, "n1", "t1");
        int count = 0;

        dispatcher->Subscribe([&](const auto&) { count++; });

        emitter->EmitProgress(1, 10, "test");
        REQUIRE(count == 1);
    }
}
