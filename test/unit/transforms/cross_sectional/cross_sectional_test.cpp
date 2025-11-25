//
// Created by adesola on 12/3/24.
//
#include "epoch_frame/array.h"
#include <epoch_script/core/bar_attribute.h>
#include "epoch_script/strategy/registration.h"
#include <epoch_script/transforms/core/config_helper.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include "transforms/components/cross_sectional/rank.h"
#include "transforms/components/cross_sectional/returns.h"
#include <catch2/catch_test_macros.hpp>
#include <epoch_core/catch_defs.h>
#include <epoch_frame/factory/index_factory.h>

using namespace epoch_core;
using namespace epoch_script;
using namespace epoch_script::transform;
using namespace std::chrono_literals;
using namespace epoch_frame;

TEST_CASE("Cross-sectional Transforms") {

  SECTION("CrossSectionalReturnsOperation") {
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
         epoch_frame::DateTime{2020y, std::chrono::January, 2d},
         epoch_frame::DateTime{2020y, std::chrono::January, 3d},
         epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

    // Shared Setup: Define multiple input DataFrames representing different
    // stocks
    epoch_frame::DataFrame input_data =
        make_dataframe<double>(index,
                               {{100.0, 102.0, 101.0, 105.0},
                                {200.0, 198.0, 202.0, 205.0},
                                {300.0, 303.0, 299.0, 310.0}},
                               {"aapl", "msft", "tsla"});
    // Define metadata
    TransformConfiguration config = cs_momentum(
        19, strategy::InputValue(strategy::NodeReference("", "returns")),
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY);

    // Use registry to create the transform
    auto transform = MAKE_TRANSFORM(config);

    auto output_index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 2d},
         epoch_frame::DateTime{2020y, std::chrono::January, 3d},
         epoch_frame::DateTime{2020y, std::chrono::January, 4d}});

    epoch_frame::DataFrame expected = make_dataframe<double>(
        output_index, {{1.006667, 1.005726, 1.036315}}, {config.GetOutputId().GetColumnName()});

    // Apply transform
    epoch_frame::DataFrame output = transform->TransformData(
        input_data
            .apply([](Array const &s) { return s.pct_change(); },
                   AxisType::Column)
            .iloc({1, std::nullopt}));

    // Verify output
    INFO("Comparing output with expected values\n"
         << output << "\n!=\n"
         << expected);
    REQUIRE(output.equals(expected, arrow::EqualOptions{}.atol(1e-2)));
  }

  SECTION("CrossSectionalRankOperations") {
    auto index = epoch_frame::factory::index::make_datetime_index(
        {epoch_frame::DateTime{2020y, std::chrono::January, 1d},
         epoch_frame::DateTime{2020y, std::chrono::January, 2d},
         epoch_frame::DateTime{2020y, std::chrono::January, 3d}});

    const auto daily_tf =
        epoch_script::EpochStratifyXConstants::instance().DAILY_FREQUENCY;
    const std::string scores_input = "scores";

    SECTION("Regular case with multiple assets") {
      // Create scores DataFrame (5 assets x 3 days)
      // This dataframe is stored with each asset as a column and dates as rows
      epoch_frame::DataFrame scores_data = make_dataframe<double>(
          index,
          {{10.0, 30.0, 20.0},  // Asset1 (3 days)
           {50.0, 40.0, 60.0},  // Asset2 (3 days)
           {80.0, 90.0, 70.0},  // Asset3 (3 days)
           {15.0, 25.0, 35.0},  // Asset4 (3 days)
           {45.0, 55.0, 65.0}}, // Asset5 (3 days)
          {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

      SECTION("TopK with k=2") {
        // Create TopK transform with k=2
        TransformConfiguration config = cs_topk(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 2, daily_tf);

        auto transform = MAKE_TRANSFORM(config);

        epoch_frame::DataFrame output = transform->TransformData(scores_data);

        // Expected: For each day, the top 2 highest scores should be marked as
        // true Day 1: Asset3 (80), Asset2 (50) are top 2 Day 2: Asset3 (90),
        // Asset5 (55) are top 2 Day 3: Asset3 (70), Asset5 (65) are top 2

        std::vector<std::vector<bool>> expected_data = {
            {false, false, false}, // Asset1
            {true, false, false},  // Asset2
            {true, true, true},    // Asset3
            {false, false, false}, // Asset4
            {false, true, true}    // Asset5
        };

        epoch_frame::DataFrame expected = make_dataframe<bool>(
            index, expected_data,
            {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

        INFO("Comparing TopK (k=2) output with expected values\n"
             << output << "\n!=\n"
             << expected);
        REQUIRE(output.equals(expected));
      }

      SECTION("BottomK with k=2") {
        // Create BottomK transform with k=2
        TransformConfiguration config =
            cs_bottomk(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 2, daily_tf);

        auto transform = MAKE_TRANSFORM(config);

        epoch_frame::DataFrame output = transform->TransformData(scores_data);

        // Expected: For each day, the 2 lowest scores should be marked as true
        // Day 1: Asset1 (10), Asset4 (15) are bottom 2
        // Day 2: Asset1 (30), Asset4 (25) are bottom 2
        // Day 3: Asset1 (20), Asset4 (35) are bottom 2

        std::vector<std::vector<bool>> expected_data = {
            {true, true, true},    // Asset1
            {false, false, false}, // Asset2
            {false, false, false}, // Asset3
            {true, true, true},    // Asset4
            {false, false, false}  // Asset5
        };

        epoch_frame::DataFrame expected = make_dataframe<bool>(
            index, expected_data,
            {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

        INFO("Comparing BottomK (k=2) output with expected values\n"
             << output << "\n!=\n"
             << expected);
        REQUIRE(output.equals(expected));
      }

      SECTION("TopKPercentile with k=40") {
        // Create TopKPercentile transform with k=40%
        TransformConfiguration config =
            cs_topk_percentile(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 40, daily_tf);

        auto transform = MAKE_TRANSFORM(config);

        epoch_frame::DataFrame output = transform->TransformData(scores_data);

        // With 5 assets, 40% = 2 assets (ceil(0.4 * 5) = 2)
        // Expected: Same as TopK with k=2

        std::vector<std::vector<bool>> expected_data = {
            {false, false, false}, // Asset1
            {true, false, false},  // Asset2
            {true, true, true},    // Asset3
            {false, false, false}, // Asset4
            {false, true, true}    // Asset5
        };

        epoch_frame::DataFrame expected = make_dataframe<bool>(
            index, expected_data,
            {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

        INFO("Comparing TopKPercentile (k=40) output with expected values\n"
             << output << "\n!=\n"
             << expected);
        REQUIRE(output.equals(expected));
      }

      SECTION("BottomKPercentile with k=40") {
        // Create BottomKPercentile transform with k=40%
        TransformConfiguration config =
            cs_bottomk_percentile(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 40, daily_tf);

        auto transform = MAKE_TRANSFORM(config);

        epoch_frame::DataFrame output = transform->TransformData(scores_data);

        // With 5 assets, 40% = 2 assets (ceil(0.4 * 5) = 2)
        // Expected: Same as BottomK with k=2

        std::vector<std::vector<bool>> expected_data = {
            {true, true, true},    // Asset1
            {false, false, false}, // Asset2
            {false, false, false}, // Asset3
            {true, true, true},    // Asset4
            {false, false, false}  // Asset5
        };

        epoch_frame::DataFrame expected = make_dataframe<bool>(
            index, expected_data,
            {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

        INFO("Comparing BottomKPercentile (k=40) output with expected values\n"
             << output << "\n!=\n"
             << expected);
        REQUIRE(output.equals(expected));
      }
    }

    SECTION("Edge Cases") {
      SECTION("k=1 (select only top/bottom 1)") {
        // Create scores DataFrame
        epoch_frame::DataFrame scores_data = make_dataframe<double>(
            index,
            {{10.0, 30.0, 20.0},  // Asset1
             {50.0, 40.0, 60.0},  // Asset2
             {80.0, 90.0, 70.0},  // Asset3
             {15.0, 25.0, 35.0},  // Asset4
             {45.0, 55.0, 65.0}}, // Asset5
            {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

        // TopK with k=1
        {
          TransformConfiguration config = cs_topk(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 1, daily_tf);
          auto transform =
              std::make_unique<CrossSectionalTopKOperation>(config);
          epoch_frame::DataFrame output = transform->TransformData(scores_data);

          // Only Asset3 should be selected as the highest each day
          std::vector<std::vector<bool>> expected_data = {
              {false, false, false}, // Asset1
              {false, false, false}, // Asset2
              {true, true, true},    // Asset3
              {false, false, false}, // Asset4
              {false, false, false}  // Asset5
          };

          epoch_frame::DataFrame expected = make_dataframe<bool>(
              index, expected_data,
              {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

          INFO("Comparing TopK (k=1) output with expected values\n"
               << output << "\n!=\n"
               << expected);
          REQUIRE(output.equals(expected));
        }

        // BottomK with k=1
        {
          TransformConfiguration config =
              cs_bottomk(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 1, daily_tf);
          auto transform =
              std::make_unique<CrossSectionalBottomKOperation>(config);
          epoch_frame::DataFrame output = transform->TransformData(scores_data);

          // Only Asset1 should be selected as the lowest each day
          std::vector<std::vector<bool>> expected_data = {
              {true, false, true},   // Asset1
              {false, false, false}, // Asset2
              {false, false, false}, // Asset3
              {false, true, false},  // Asset4
              {false, false, false}  // Asset5
          };

          epoch_frame::DataFrame expected = make_dataframe<bool>(
              index, expected_data,
              {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

          INFO("Comparing BottomK (k=1) output with expected values\n"
               << output << "\n!=\n"
               << expected);
          REQUIRE(output.equals(expected));
        }
      }

      SECTION("k=all assets (select all assets)") {
        // Create scores DataFrame
        epoch_frame::DataFrame scores_data = make_dataframe<double>(
            index,
            {{10.0, 30.0, 20.0},  // Asset1
             {50.0, 40.0, 60.0},  // Asset2
             {80.0, 90.0, 70.0},  // Asset3
             {15.0, 25.0, 35.0},  // Asset4
             {45.0, 55.0, 65.0}}, // Asset5
            {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

        // TopK with k=5 (all assets)
        {
          TransformConfiguration config = cs_topk(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 5, daily_tf);
          auto transform = MAKE_TRANSFORM(config);
          epoch_frame::DataFrame output = transform->TransformData(scores_data);

          // All assets should be selected
          std::vector<std::vector<bool>> expected_data = {{true, true, true},
                                                          {true, true, true},
                                                          {true, true, true},
                                                          {true, true, true},
                                                          {true, true, true}};

          epoch_frame::DataFrame expected = make_dataframe<bool>(
              index, expected_data,
              {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

          INFO("Comparing TopK (k=5) output with expected values\n"
               << output << "\n!=\n"
               << expected);
          REQUIRE(output.equals(expected));
        }

        // BottomK with k=5 (all assets)
        {
          TransformConfiguration config =
              cs_bottomk(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 5, daily_tf);
          auto transform = MAKE_TRANSFORM(config);
          epoch_frame::DataFrame output = transform->TransformData(scores_data);

          // All assets should be selected
          std::vector<std::vector<bool>> expected_data = {{true, true, true},
                                                          {true, true, true},
                                                          {true, true, true},
                                                          {true, true, true},
                                                          {true, true, true}};

          epoch_frame::DataFrame expected = make_dataframe<bool>(
              index, expected_data,
              {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

          INFO("Comparing BottomK (k=5) output with expected values\n"
               << output << "\n!=\n"
               << expected);
          REQUIRE(output.equals(expected));
        }
      }

      SECTION("Assets with equal values") {
        // Create scores DataFrame with ties
        auto tie_index = epoch_frame::factory::index::make_datetime_index(
            {epoch_frame::DateTime{2020y, std::chrono::January, 1d}});

        epoch_frame::DataFrame scores_data = make_dataframe<double>(
            tie_index,
            {{50.0},
             {50.0},
             {80.0},
             {50.0},
             {45.0}}, // Several assets have same value
            {"Asset1", "Asset2", "Asset3", "Asset4", "Asset5"});

        // TopK with k=2, but three assets are tied
        {
          TransformConfiguration config = cs_topk(1, strategy::InputValue(strategy::NodeReference("", scores_input)), 2, daily_tf);
          auto transform = MAKE_TRANSFORM(config);

          // In case of ties, we just verify we get a result without error
          // The actual selection behavior with ties is implementation-dependent
          REQUIRE_NOTHROW(transform->TransformData(scores_data));
        }
      }
    }
  }
}