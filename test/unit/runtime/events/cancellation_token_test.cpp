//
// Unit tests for CancellationToken
//

#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_string.hpp>

#include "events/cancellation_token.h"

#include <atomic>
#include <thread>
#include <vector>

using namespace epoch_script::runtime::events;

TEST_CASE("CancellationToken", "[events][cancellation]") {

    SECTION("Initial state is not cancelled") {
        CancellationToken token;
        REQUIRE_FALSE(token.IsCancelled());
    }

    SECTION("Cancel sets cancelled state") {
        CancellationToken token;
        token.Cancel();
        REQUIRE(token.IsCancelled());
    }

    SECTION("Multiple Cancel calls are idempotent") {
        CancellationToken token;
        token.Cancel();
        token.Cancel();
        token.Cancel();
        REQUIRE(token.IsCancelled());
    }

    SECTION("Reset clears cancelled state") {
        CancellationToken token;
        token.Cancel();
        REQUIRE(token.IsCancelled());
        token.Reset();
        REQUIRE_FALSE(token.IsCancelled());
    }

    SECTION("ThrowIfCancelled does not throw when not cancelled") {
        CancellationToken token;
        REQUIRE_NOTHROW(token.ThrowIfCancelled());
    }

    SECTION("ThrowIfCancelled throws OperationCancelledException when cancelled") {
        CancellationToken token;
        token.Cancel();
        REQUIRE_THROWS_AS(token.ThrowIfCancelled(), OperationCancelledException);
    }

    SECTION("ThrowIfCancelled with context includes message") {
        CancellationToken token;
        token.Cancel();
        REQUIRE_THROWS_WITH(
            token.ThrowIfCancelled("during epoch 5"),
            Catch::Matchers::ContainsSubstring("during epoch 5")
        );
    }

    SECTION("OperationCancelledException default message") {
        OperationCancelledException ex;
        REQUIRE_THAT(ex.what(), Catch::Matchers::ContainsSubstring("cancelled"));
    }

    SECTION("OperationCancelledException custom message") {
        OperationCancelledException ex("Custom cancellation reason");
        REQUIRE_THAT(ex.what(), Catch::Matchers::ContainsSubstring("Custom cancellation reason"));
    }
}

TEST_CASE("CancellationToken thread safety", "[events][cancellation][threading]") {

    SECTION("Concurrent reads and single write") {
        auto token = std::make_shared<CancellationToken>();
        std::atomic<int> readAfterCancel{0};
        std::atomic<bool> startFlag{false};
        std::atomic<bool> stopReading{false};

        // Multiple reader threads - read continuously until stopped
        std::vector<std::thread> readers;
        for (int i = 0; i < 4; ++i) {
            readers.emplace_back([&]() {
                while (!startFlag.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                while (!stopReading.load(std::memory_order_acquire)) {
                    if (token->IsCancelled()) {
                        readAfterCancel.fetch_add(1, std::memory_order_relaxed);
                    }
                    std::this_thread::yield();
                }
            });
        }

        // Writer thread
        std::thread writer([&]() {
            while (!startFlag.load(std::memory_order_acquire)) {
                std::this_thread::yield();
            }
            std::this_thread::sleep_for(std::chrono::microseconds(50));
            token->Cancel();
            // Let readers observe the cancellation for a bit
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
            stopReading.store(true, std::memory_order_release);
        });

        startFlag.store(true, std::memory_order_release);

        writer.join();
        for (auto& t : readers) {
            t.join();
        }

        REQUIRE(token->IsCancelled());
        // Some reads should have observed cancellation
        REQUIRE(readAfterCancel.load() > 0);
    }

    SECTION("Multiple concurrent cancellations") {
        auto token = std::make_shared<CancellationToken>();
        std::atomic<bool> startFlag{false};

        std::vector<std::thread> cancellers;
        for (int i = 0; i < 8; ++i) {
            cancellers.emplace_back([&]() {
                while (!startFlag.load(std::memory_order_acquire)) {
                    std::this_thread::yield();
                }
                token->Cancel();
            });
        }

        startFlag.store(true, std::memory_order_release);

        for (auto& t : cancellers) {
            t.join();
        }

        REQUIRE(token->IsCancelled());
    }
}

TEST_CASE("CancellationGuard", "[events][cancellation][raii]") {

    SECTION("Guard checks cancellation on construction") {
        auto token = std::make_shared<CancellationToken>();

        REQUIRE_NOTHROW(CancellationGuard(token));
    }

    SECTION("Guard throws on construction if already cancelled") {
        auto token = std::make_shared<CancellationToken>();
        token->Cancel();

        REQUIRE_THROWS_AS(CancellationGuard(token), OperationCancelledException);
    }

    SECTION("Guard CheckCancellation method") {
        auto token = std::make_shared<CancellationToken>();

        // Test non-cancelled state - guard can go out of scope safely
        {
            CancellationGuard guard(token);
            REQUIRE_NOTHROW(guard.CheckCancellation());
        }

        // After cancel, CheckCancellation throws
        token->Cancel();

        // Guard destructor also throws when cancelled, so wrap in try-catch
        try {
            CancellationGuard guard(token);  // This throws on construction
            FAIL("Should have thrown on construction");
        } catch (const OperationCancelledException&) {
            // Expected - guard checks on construction
        }
    }

    SECTION("Guard IsCancelled method") {
        auto token = std::make_shared<CancellationToken>();

        // Test non-cancelled state
        {
            CancellationGuard guard(token);
            REQUIRE_FALSE(guard.IsCancelled());
        }

        // After cancel
        token->Cancel();
        // Can't construct guard anymore - it throws on construction
        REQUIRE(token->IsCancelled());
    }

    SECTION("Guard with null token") {
        CancellationGuard guard(nullptr);

        // Should not throw - null token means no cancellation support
        REQUIRE_NOTHROW(guard.CheckCancellation());
        REQUIRE_FALSE(guard.IsCancelled());
    }
}
