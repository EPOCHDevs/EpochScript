//
// Created by Claude Code
// CSE Optimizer Unit Tests
//

#include <catch2/catch_all.hpp>
#include "transforms/compiler/cse_optimizer.h"
#include "transforms/compiler/compilation_context.h"
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/core/time_frame.h>

using namespace epoch_script;
using namespace epoch_script::strategy;

TEST_CASE("CSE Optimizer - Basic Deduplication", "[cse_optimizer]")
{
    SECTION("Deduplicates identical transforms")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create two identical ema(period=20) nodes
        AlgorithmNode ema1, ema2, input;

        input.id = "src";
        input.type = "market_data_source";

        ema1.id = "ema_0";
        ema1.type = "ema";
        ema1.options["period"] = MetaDataOptionDefinition(20.0);
        ema1.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        ema2.id = "ema_1";
        ema2.type = "ema";
        ema2.options["period"] = MetaDataOptionDefinition(20.0);
        ema2.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        std::vector<AlgorithmNode> algorithms = {input, ema1, ema2};
        context.used_node_ids = {"src", "ema_0", "ema_1"};

        optimizer.Optimize(algorithms, context);

        // Should have only 2 nodes left (src + 1 ema)
        REQUIRE(algorithms.size() == 2);

        // ema_1 should be removed from used_node_ids
        REQUIRE(context.used_node_ids.count("ema_0") == 1);
        REQUIRE(context.used_node_ids.count("ema_1") == 0);
    }

    SECTION("Preserves transforms with different parameters")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create ema(20) and ema(50) - should NOT be deduplicated
        AlgorithmNode ema20, ema50, input;

        input.id = "src";
        input.type = "market_data_source";

        ema20.id = "ema_0";
        ema20.type = "ema";
        ema20.options["period"] = MetaDataOptionDefinition(20.0);
        ema20.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        ema50.id = "ema_1";
        ema50.type = "ema";
        ema50.options["period"] = MetaDataOptionDefinition(50.0);
        ema50.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        std::vector<AlgorithmNode> algorithms = {input, ema20, ema50};
        context.used_node_ids = {"src", "ema_0", "ema_1"};

        optimizer.Optimize(algorithms, context);

        // Should still have 3 nodes (different parameters)
        REQUIRE(algorithms.size() == 3);
        REQUIRE(context.used_node_ids.size() == 3);
    }

    SECTION("Preserves transforms with different inputs")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create ema(20) on src.c and ema(20) on src.h - should NOT be deduplicated
        AlgorithmNode ema_close, ema_high, input;

        input.id = "src";
        input.type = "market_data_source";

        ema_close.id = "ema_0";
        ema_close.type = "ema";
        ema_close.options["period"] = MetaDataOptionDefinition(20.0);
        ema_close.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        ema_high.id = "ema_1";
        ema_high.type = "ema";
        ema_high.options["period"] = MetaDataOptionDefinition(20.0);
        ema_high.inputs["src"].push_back(strategy::NodeReference{"src", "h"});

        std::vector<AlgorithmNode> algorithms = {input, ema_close, ema_high};
        context.used_node_ids = {"src", "ema_0", "ema_1"};

        optimizer.Optimize(algorithms, context);

        // Should still have 3 nodes (different inputs)
        REQUIRE(algorithms.size() == 3);
        REQUIRE(context.used_node_ids.size() == 3);
    }
}

TEST_CASE("CSE Optimizer - Reference Rewriting", "[cse_optimizer]")
{
    SECTION("Rewrites references to point to canonical node")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create: ema_0, ema_1 (duplicate), add node that uses ema_1
        AlgorithmNode src, ema0, ema1, add_node;

        src.id = "src";
        src.type = "market_data_source";

        ema0.id = "ema_0";
        ema0.type = "ema";
        ema0.options["period"] = MetaDataOptionDefinition(20.0);
        ema0.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        ema1.id = "ema_1";
        ema1.type = "ema";
        ema1.options["period"] = MetaDataOptionDefinition(20.0);
        ema1.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        add_node.id = "add_0";
        add_node.type = "add";
        add_node.inputs["lhs"].push_back(strategy::NodeReference{"ema_1", "result"});  // References ema_1
        add_node.inputs["rhs"].push_back(strategy::NodeReference{"ema_0", "result"});

        std::vector<AlgorithmNode> algorithms = {src, ema0, ema1, add_node};
        context.used_node_ids = {"src", "ema_0", "ema_1", "add_0"};

        optimizer.Optimize(algorithms, context);

        // Find the add node in the result
        auto add_it = std::find_if(algorithms.begin(), algorithms.end(),
            [](const AlgorithmNode& n) { return n.type == "add"; });

        REQUIRE(add_it != algorithms.end());

        // The reference to ema_1 should be rewritten to ema_0
        REQUIRE(add_it->inputs["lhs"][0].GetColumnIdentifier() == "ema_0#result");
        REQUIRE(add_it->inputs["rhs"][0].GetColumnIdentifier() == "ema_0#result");
    }
}

TEST_CASE("CSE Optimizer - Executor Exclusion", "[cse_optimizer]")
{
    SECTION("Never deduplicates executor nodes")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create two identical executor nodes
        AlgorithmNode exec1, exec2, signal;

        signal.id = "signal_0";
        signal.type = "gt";

        exec1.id = "executor_0";
        exec1.type = "trade_signal_executor";
        exec1.options["name"] = MetaDataOptionDefinition(std::string("Signal1"));
        exec1.inputs["signal"].push_back(strategy::NodeReference{"signal_0", "result"});

        exec2.id = "executor_1";
        exec2.type = "trade_signal_executor";
        exec2.options["name"] = MetaDataOptionDefinition(std::string("Signal1"));
        exec2.inputs["signal"].push_back(strategy::NodeReference{"signal_0", "result"});

        std::vector<AlgorithmNode> algorithms = {signal, exec1, exec2};
        context.used_node_ids = {"signal_0", "executor_0", "executor_1"};

        optimizer.Optimize(algorithms, context);

        // Should keep both executors (they have side effects)
        REQUIRE(algorithms.size() == 3);
        REQUIRE(context.used_node_ids.size() == 3);
    }
}

TEST_CASE("CSE Optimizer - Multi-Output Deduplication", "[cse_optimizer]")
{
    SECTION("Deduplicates multi-output transforms correctly")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create two identical bbands calls - both return (upper, middle, lower)
        AlgorithmNode src, bbands1, bbands2, use_upper1, use_middle2;

        src.id = "src";
        src.type = "market_data_source";

        bbands1.id = "bbands_0";
        bbands1.type = "bbands";
        bbands1.options["period"] = MetaDataOptionDefinition(20.0);
        bbands1.options["stddev"] = MetaDataOptionDefinition(2.0);
        bbands1.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        bbands2.id = "bbands_1";
        bbands2.type = "bbands";
        bbands2.options["period"] = MetaDataOptionDefinition(20.0);
        bbands2.options["stddev"] = MetaDataOptionDefinition(2.0);
        bbands2.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        // One node uses upper from bbands_0
        use_upper1.id = "gt_0";
        use_upper1.type = "gt";
        use_upper1.inputs["lhs"].push_back(strategy::NodeReference{"src", "c"});
        use_upper1.inputs["rhs"].push_back(strategy::NodeReference{"bbands_0", "upper"});

        // Another node uses middle from bbands_1
        use_middle2.id = "lt_0";
        use_middle2.type = "lt";
        use_middle2.inputs["lhs"].push_back(strategy::NodeReference{"src", "c"});
        use_middle2.inputs["rhs"].push_back(strategy::NodeReference{"bbands_1", "middle"});

        std::vector<AlgorithmNode> algorithms = {src, bbands1, bbands2, use_upper1, use_middle2};
        context.used_node_ids = {"src", "bbands_0", "bbands_1", "gt_0", "lt_0"};

        optimizer.Optimize(algorithms, context);

        // Should have 4 nodes (src, bbands_0, gt_0, lt_0)
        REQUIRE(algorithms.size() == 4);

        // Find lt_0 and verify it now references bbands_0#middle
        auto lt_it = std::find_if(algorithms.begin(), algorithms.end(),
            [](const AlgorithmNode& n) { return n.id == "lt_0"; });

        REQUIRE(lt_it != algorithms.end());
        REQUIRE(lt_it->inputs["rhs"][0].GetColumnIdentifier() == "bbands_0#middle");
    }
}

TEST_CASE("CSE Optimizer - Lag Operation Deduplication", "[cse_optimizer]")
{
    SECTION("Deduplicates identical lag operations")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create two identical lag(10) operations
        AlgorithmNode src, lag1, lag2;

        src.id = "src";
        src.type = "market_data_source";

        lag1.id = "lag_0";
        lag1.type = "lag";
        lag1.options["periods"] = MetaDataOptionDefinition(10.0);
        lag1.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        lag2.id = "lag_1";
        lag2.type = "lag";
        lag2.options["periods"] = MetaDataOptionDefinition(10.0);
        lag2.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        std::vector<AlgorithmNode> algorithms = {src, lag1, lag2};
        context.used_node_ids = {"src", "lag_0", "lag_1"};

        optimizer.Optimize(algorithms, context);

        // Should have 2 nodes (src + 1 lag)
        REQUIRE(algorithms.size() == 2);
        REQUIRE(context.used_node_ids.count("lag_0") == 1);
        REQUIRE(context.used_node_ids.count("lag_1") == 0);
    }
}

TEST_CASE("CSE Optimizer - Complex Scenario", "[cse_optimizer]")
{
    SECTION("Handles complex graph with multiple duplicates")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Simulate: signal1 = ema(20)(src.c) > 100
        //           signal2 = ema(20)(src.c) > ema(50)(src.c)
        //           signal3 = src.c > ema(20)(src.c)
        // Should create only 2 ema nodes: ema(20) and ema(50)

        AlgorithmNode src, num100, ema20_a, ema20_b, ema20_c, ema50;
        AlgorithmNode gt1, gt2, gt3;

        src.id = "src";
        src.type = "market_data_source";

        num100.id = "number_0";
        num100.type = "number";
        num100.options["value"] = MetaDataOptionDefinition(100.0);

        // Three ema(20) nodes that should be deduplicated
        ema20_a.id = "ema_0";
        ema20_a.type = "ema";
        ema20_a.options["period"] = MetaDataOptionDefinition(20.0);
        ema20_a.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        ema20_b.id = "ema_1";
        ema20_b.type = "ema";
        ema20_b.options["period"] = MetaDataOptionDefinition(20.0);
        ema20_b.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        ema20_c.id = "ema_2";
        ema20_c.type = "ema";
        ema20_c.options["period"] = MetaDataOptionDefinition(20.0);
        ema20_c.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        // One ema(50) node
        ema50.id = "ema_3";
        ema50.type = "ema";
        ema50.options["period"] = MetaDataOptionDefinition(50.0);
        ema50.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        // Three comparison nodes
        gt1.id = "gt_0";
        gt1.type = "gt";
        gt1.inputs["lhs"].push_back(strategy::NodeReference{"ema_0", "result"});
        gt1.inputs["rhs"].push_back(strategy::NodeReference{"number_0", "result"});

        gt2.id = "gt_1";
        gt2.type = "gt";
        gt2.inputs["lhs"].push_back(strategy::NodeReference{"ema_1", "result"});
        gt2.inputs["rhs"].push_back(strategy::NodeReference{"ema_3", "result"});

        gt3.id = "gt_2";
        gt3.type = "gt";
        gt3.inputs["lhs"].push_back(strategy::NodeReference{"src", "c"});
        gt3.inputs["rhs"].push_back(strategy::NodeReference{"ema_2", "result"});

        std::vector<AlgorithmNode> algorithms = {
            src, num100, ema20_a, ema20_b, ema20_c, ema50, gt1, gt2, gt3
        };

        context.used_node_ids = {
            "src", "number_0", "ema_0", "ema_1", "ema_2", "ema_3", "gt_0", "gt_1", "gt_2"
        };

        optimizer.Optimize(algorithms, context);

        // Should have 7 nodes: src, number_0, ema_0, ema_3, gt_0, gt_1, gt_2
        // (ema_1 and ema_2 are duplicates of ema_0)
        REQUIRE(algorithms.size() == 7);

        // Verify ema_1 and ema_2 are removed
        REQUIRE(context.used_node_ids.count("ema_0") == 1);
        REQUIRE(context.used_node_ids.count("ema_1") == 0);
        REQUIRE(context.used_node_ids.count("ema_2") == 0);
        REQUIRE(context.used_node_ids.count("ema_3") == 1);

        // Verify references were rewritten correctly
        auto gt1_it = std::find_if(algorithms.begin(), algorithms.end(),
            [](const AlgorithmNode& n) { return n.id == "gt_1"; });
        auto gt2_it = std::find_if(algorithms.begin(), algorithms.end(),
            [](const AlgorithmNode& n) { return n.id == "gt_2"; });

        REQUIRE(gt1_it != algorithms.end());
        REQUIRE(gt2_it != algorithms.end());

        REQUIRE(gt1_it->inputs["lhs"][0].GetColumnIdentifier() == "ema_0#result");  // Rewritten from ema_1
        REQUIRE(gt2_it->inputs["rhs"][0].GetColumnIdentifier() == "ema_0#result");  // Rewritten from ema_2
    }
}

TEST_CASE("CSE Optimizer - Hash Collisions", "[cse_optimizer]")
{
    SECTION("Correctly handles hash collisions with full equality check")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create nodes that might hash to same value but are different
        // (This is a defense test - even if hashes collide, full equality check prevents wrong dedup)
        AlgorithmNode src, ema20, sma20;

        src.id = "src";
        src.type = "market_data_source";

        ema20.id = "ema_0";
        ema20.type = "ema";
        ema20.options["period"] = MetaDataOptionDefinition(20.0);
        ema20.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        sma20.id = "sma_0";
        sma20.type = "sma";  // Different type
        sma20.options["period"] = MetaDataOptionDefinition(20.0);
        sma20.inputs["src"].push_back(strategy::NodeReference{"src", "c"});

        std::vector<AlgorithmNode> algorithms = {src, ema20, sma20};
        context.used_node_ids = {"src", "ema_0", "sma_0"};

        optimizer.Optimize(algorithms, context);

        // Should keep both (different types)
        REQUIRE(algorithms.size() == 3);
    }
}

TEST_CASE("CSE Optimizer - Text Node Deduplication", "[cse_optimizer]")
{
    SECTION("Deduplicates identical text/string literal nodes")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create multiple identical empty string nodes
        AlgorithmNode text1, text2, text3;

        text1.id = "text_0";
        text1.type = "text";
        text1.options["value"] = MetaDataOptionDefinition(std::string(""));

        text2.id = "text_1";
        text2.type = "text";
        text2.options["value"] = MetaDataOptionDefinition(std::string(""));

        text3.id = "text_2";
        text3.type = "text";
        text3.options["value"] = MetaDataOptionDefinition(std::string(""));

        std::vector<AlgorithmNode> algorithms = {text1, text2, text3};
        context.used_node_ids = {"text_0", "text_1", "text_2"};

        optimizer.Optimize(algorithms, context);

        // Should have only 1 text node left
        REQUIRE(algorithms.size() == 1);
        REQUIRE(context.used_node_ids.count("text_0") == 1);
        REQUIRE(context.used_node_ids.count("text_1") == 0);
        REQUIRE(context.used_node_ids.count("text_2") == 0);
    }

    SECTION("Preserves text nodes with different values")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create text nodes with different values
        AlgorithmNode text_a, text_b, text_empty;

        text_a.id = "text_0";
        text_a.type = "text";
        text_a.options["value"] = MetaDataOptionDefinition(std::string("Technical"));

        text_b.id = "text_1";
        text_b.type = "text";
        text_b.options["value"] = MetaDataOptionDefinition(std::string("Macro Confirmed"));

        text_empty.id = "text_2";
        text_empty.type = "text";
        text_empty.options["value"] = MetaDataOptionDefinition(std::string(""));

        std::vector<AlgorithmNode> algorithms = {text_a, text_b, text_empty};
        context.used_node_ids = {"text_0", "text_1", "text_2"};

        optimizer.Optimize(algorithms, context);

        // Should keep all 3 (different values)
        REQUIRE(algorithms.size() == 3);
        REQUIRE(context.used_node_ids.size() == 3);
    }

    SECTION("Deduplicates multiple empty strings from conditional_select_string pattern")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Simulate pattern from your script:
        // conditional_select_string(technical_only, "Technical", "")
        // conditional_select_string(macro_confirmed, "Macro Confirmed", "")

        AlgorithmNode bool_true, bool_false;
        AlgorithmNode text_technical, text_macro, text_empty1, text_empty2;
        AlgorithmNode cond1, cond2;

        bool_true.id = "bool_true_0";
        bool_true.type = "bool_true";

        bool_false.id = "bool_false_0";
        bool_false.type = "bool_false";

        text_technical.id = "text_0";
        text_technical.type = "text";
        text_technical.options["value"] = MetaDataOptionDefinition(std::string("Technical"));

        text_macro.id = "text_1";
        text_macro.type = "text";
        text_macro.options["value"] = MetaDataOptionDefinition(std::string("Macro Confirmed"));

        text_empty1.id = "text_2";
        text_empty1.type = "text";
        text_empty1.options["value"] = MetaDataOptionDefinition(std::string(""));

        text_empty2.id = "text_3";
        text_empty2.type = "text";
        text_empty2.options["value"] = MetaDataOptionDefinition(std::string(""));

        cond1.id = "conditional_select_string_0";
        cond1.type = "conditional_select_string";
        cond1.inputs["condition"].push_back(strategy::NodeReference{"bool_true_0", "result"});
        cond1.inputs["true_value"].push_back(strategy::NodeReference{"text_0", "result"});
        cond1.inputs["false_value"].push_back(strategy::NodeReference{"text_2", "result"});

        cond2.id = "conditional_select_string_1";
        cond2.type = "conditional_select_string";
        cond2.inputs["condition"].push_back(strategy::NodeReference{"bool_false_0", "result"});
        cond2.inputs["true_value"].push_back(strategy::NodeReference{"text_1", "result"});
        cond2.inputs["false_value"].push_back(strategy::NodeReference{"text_3", "result"});

        std::vector<AlgorithmNode> algorithms = {
            bool_true, bool_false,
            text_technical, text_macro, text_empty1, text_empty2,
            cond1, cond2
        };

        context.used_node_ids = {
            "bool_true_0", "bool_false_0",
            "text_0", "text_1", "text_2", "text_3",
            "conditional_select_string_0", "conditional_select_string_1"
        };

        optimizer.Optimize(algorithms, context);

        // Should have 7 nodes (text_3 should be deduplicated to text_2)
        REQUIRE(algorithms.size() == 7);

        // text_2 should remain, text_3 should be removed
        REQUIRE(context.used_node_ids.count("text_2") == 1);
        REQUIRE(context.used_node_ids.count("text_3") == 0);

        // Verify that cond2's reference to text_3 was rewritten to text_2
        auto cond2_it = std::find_if(algorithms.begin(), algorithms.end(),
            [](const AlgorithmNode& n) { return n.id == "conditional_select_string_1"; });

        REQUIRE(cond2_it != algorithms.end());
        REQUIRE(cond2_it->inputs["false_value"][0].GetColumnIdentifier() == "text_2#result");
    }

    SECTION("Deduplicates text nodes even with different timeframes")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Create two identical text("") nodes but with different timeframes
        // This simulates the bug where literals get assigned different timeframes
        // based on their usage context
        AlgorithmNode text1, text2, text3;

        text1.id = "text_0";
        text1.type = "text";
        text1.options["value"] = MetaDataOptionDefinition(std::string(""));
        text1.timeframe = TimeFrame("1h");  // Different timeframe

        text2.id = "text_1";
        text2.type = "text";
        text2.options["value"] = MetaDataOptionDefinition(std::string(""));
        text2.timeframe = TimeFrame("1d");  // Different timeframe

        text3.id = "text_2";
        text3.type = "text";
        text3.options["value"] = MetaDataOptionDefinition(std::string(""));
        // No timeframe

        std::vector<AlgorithmNode> algorithms = {text1, text2, text3};
        context.used_node_ids = {"text_0", "text_1", "text_2"};

        optimizer.Optimize(algorithms, context);

        // Should have only 1 text node left despite different timeframes
        // Scalars are timeframe-agnostic
        REQUIRE(algorithms.size() == 1);
        REQUIRE(context.used_node_ids.count("text_0") == 1);
        REQUIRE(context.used_node_ids.count("text_1") == 0);
        REQUIRE(context.used_node_ids.count("text_2") == 0);
    }

    SECTION("Deduplicates number nodes with different timeframes")
    {
        CompilationContext context;
        CSEOptimizer optimizer;

        // Same test for number literals
        AlgorithmNode num1, num2;

        num1.id = "number_0";
        num1.type = "number";
        num1.options["value"] = MetaDataOptionDefinition(100.0);
        num1.timeframe = TimeFrame("1h");

        num2.id = "number_1";
        num2.type = "number";
        num2.options["value"] = MetaDataOptionDefinition(100.0);
        num2.timeframe = TimeFrame("1d");

        std::vector<AlgorithmNode> algorithms = {num1, num2};
        context.used_node_ids = {"number_0", "number_1"};

        optimizer.Optimize(algorithms, context);

        // Should deduplicate despite different timeframes
        REQUIRE(algorithms.size() == 1);
        REQUIRE(context.used_node_ids.count("number_0") == 1);
        REQUIRE(context.used_node_ids.count("number_1") == 0);
    }
}
