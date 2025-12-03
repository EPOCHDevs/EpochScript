/**
 * @file rolling_window_iterator_test.cpp
 * @brief Isolated unit tests for RollingWindowIterator
 *
 * Tests the rolling window infrastructure without any ML dependencies.
 */

#include <catch2/catch_test_macros.hpp>
#include "transforms/components/ml/rolling_window_iterator.h"
#include <vector>
#include <iostream>

using namespace epoch_script::transform::ml_utils;

TEST_CASE("RollingWindowIterator - Basic Rolling", "[rolling_window][isolated]") {
    // 100 rows, window_size=60, step_size=1
    const size_t total_rows = 100;
    const size_t window_size = 60;
    const size_t step_size = 1;

    SECTION("Construction succeeds") {
        REQUIRE_NOTHROW(RollingWindowIterator(total_rows, window_size, step_size, WindowType::Rolling));
    }

    SECTION("Window count is correct") {
        RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Rolling);

        // With 100 rows and window_size=60, we should have windows for rows 60-99
        // That's 40 prediction rows, with step_size=1, that's 40 windows
        INFO("Total windows: " << iter.TotalWindows());
        REQUIRE(iter.TotalWindows() == 40);
    }

    SECTION("First window is correct") {
        RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Rolling);

        auto first = iter.Next();

        INFO("First window: train[" << first.train_start << ", " << first.train_end
             << "), predict[" << first.predict_start << ", " << first.predict_end << ")");

        // First window: train on [0, 60), predict on [60, 61)
        REQUIRE(first.train_start == 0);
        REQUIRE(first.train_end == 60);
        REQUIRE(first.predict_start == 60);
        REQUIRE(first.predict_end == 61);
        REQUIRE(first.iteration_index == 0);
    }

    SECTION("Last window is correct") {
        RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Rolling);

        WindowSpec last;
        while (iter.HasNext()) {
            last = iter.Next();
        }

        INFO("Last window: train[" << last.train_start << ", " << last.train_end
             << "), predict[" << last.predict_start << ", " << last.predict_end << ")");

        // Last window should predict on the last row
        REQUIRE(last.predict_end == total_rows);
        REQUIRE(last.is_final == true);
    }

    SECTION("ForEach iterates all windows") {
        RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Rolling);

        size_t count = 0;
        iter.ForEach([&count](const WindowSpec& spec) {
            count++;
            // Verify basic invariants
            REQUIRE(spec.train_end > spec.train_start);
            REQUIRE(spec.predict_end > spec.predict_start);
            REQUIRE(spec.predict_start == spec.train_end);
        });

        REQUIRE(count == iter.TotalWindows());
    }
}

TEST_CASE("RollingWindowIterator - Step Size > 1", "[rolling_window][isolated]") {
    const size_t total_rows = 100;
    const size_t window_size = 60;
    const size_t step_size = 5;

    SECTION("Window count with step_size=5") {
        RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Rolling);

        // With 40 prediction rows and step_size=5, should have ~8 windows
        INFO("Total windows: " << iter.TotalWindows());
        REQUIRE(iter.TotalWindows() > 0);
        REQUIRE(iter.TotalWindows() <= 40 / step_size + 1);
    }

    SECTION("Prediction windows span step_size rows") {
        RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Rolling);

        auto first = iter.Next();

        INFO("First window predict size: " << first.PredictSize());

        // First prediction window should be [60, 65) = 5 rows
        REQUIRE(first.predict_start == 60);
        REQUIRE(first.PredictSize() == step_size);
    }
}

TEST_CASE("RollingWindowIterator - Expanding Window", "[rolling_window][isolated]") {
    const size_t total_rows = 100;
    const size_t window_size = 60;  // min_window for expanding
    const size_t step_size = 1;

    SECTION("Expanding window train_start is always 0") {
        RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Expanding);

        iter.ForEach([](const WindowSpec& spec) {
            REQUIRE(spec.train_start == 0);
        });
    }

    SECTION("Expanding window train_end grows") {
        RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Expanding);

        size_t prev_train_end = 0;
        iter.ForEach([&prev_train_end](const WindowSpec& spec) {
            REQUIRE(spec.train_end >= prev_train_end);
            prev_train_end = spec.train_end;
        });
    }
}

TEST_CASE("RollingWindowIterator - Edge Cases", "[rolling_window][isolated]") {
    SECTION("window_size == total_rows") {
        // Edge case: exactly enough data for one window
        const size_t total_rows = 60;
        const size_t window_size = 60;

        // This should throw because there's no room for prediction
        // window_size > total_rows is checked, but window_size == total_rows
        // leaves no prediction rows
        RollingWindowIterator iter(total_rows, window_size, 1, WindowType::Rolling);

        // Should have 0 windows since predict_start >= total_rows
        REQUIRE(iter.TotalWindows() == 0);
    }

    SECTION("window_size + 1 == total_rows") {
        // Just barely enough for 1 prediction row
        const size_t total_rows = 61;
        const size_t window_size = 60;

        RollingWindowIterator iter(total_rows, window_size, 1, WindowType::Rolling);

        REQUIRE(iter.TotalWindows() == 1);

        auto window = iter.Next();
        REQUIRE(window.predict_start == 60);
        REQUIRE(window.predict_end == 61);
    }

    SECTION("Invalid construction - window_size > total_rows") {
        REQUIRE_THROWS(RollingWindowIterator(50, 60, 1, WindowType::Rolling));
    }

    SECTION("Invalid construction - window_size = 0") {
        REQUIRE_THROWS(RollingWindowIterator(100, 0, 1, WindowType::Rolling));
    }

    SECTION("Invalid construction - step_size = 0") {
        REQUIRE_THROWS(RollingWindowIterator(100, 60, 0, WindowType::Rolling));
    }
}

TEST_CASE("RollingWindowIterator - Print Windows Debug", "[rolling_window][isolated][.debug]") {
    // This test is for debugging - manually inspect output
    const size_t total_rows = 150;
    const size_t window_size = 60;
    const size_t step_size = 1;

    RollingWindowIterator iter(total_rows, window_size, step_size, WindowType::Rolling);

    std::cout << "\n=== Rolling Windows (total_rows=" << total_rows
              << ", window_size=" << window_size
              << ", step_size=" << step_size << ") ===" << std::endl;
    std::cout << "Total windows: " << iter.TotalWindows() << std::endl;

    size_t count = 0;
    iter.ForEach([&count](const WindowSpec& spec) {
        if (count < 5 || spec.is_final) {
            std::cout << "Window " << spec.iteration_index
                      << ": train[" << spec.train_start << ", " << spec.train_end << ")"
                      << " predict[" << spec.predict_start << ", " << spec.predict_end << ")"
                      << (spec.is_final ? " [FINAL]" : "") << std::endl;
        } else if (count == 5) {
            std::cout << "..." << std::endl;
        }
        count++;
    });
}
