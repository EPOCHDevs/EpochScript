#include <catch2/catch_test_macros.hpp>
#include <catch2/catch_approx.hpp>
#include "../../../src/transforms/compiler/scalar_inlining_pass.h"
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/transforms/core/constant_value.h>
#include <epoch_script/core/metadata_options.h>

using namespace epoch_script;
using namespace epoch_script::compiler;
using namespace epoch_script::strategy;
using namespace epoch_script::transform;

// ============================================================================
// Helper Functions
// ============================================================================

/**
 * @brief Create a scalar AlgorithmNode
 */
AlgorithmNode MakeScalarNode(const std::string& id, const std::string& type,
                             const MetaDataArgDefinitionMapping& options = {}) {
    AlgorithmNode node;
    node.id = id;
    node.type = type;
    node.options = options;
    return node;
}

/**
 * @brief Create a non-scalar AlgorithmNode with inputs
 */
AlgorithmNode MakeRegularNode(const std::string& id, const std::string& type,
                              const InputMapping& inputs) {
    AlgorithmNode node;
    node.id = id;
    node.type = type;
    node.inputs = inputs;
    return node;
}

// ============================================================================
// TEST SUITE: ScalarInliningPass::Run - End-to-end optimization tests
// ============================================================================

TEST_CASE("ScalarInliningPass handles empty and no-scalar graphs", "[scalar_inlining]") {
    SECTION("Empty graph returns empty") {
        std::vector<AlgorithmNode> algorithms;
        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.empty());
    }

    SECTION("Graph with no scalars unchanged") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeRegularNode("add_0", "add", {
            {"SLOT0",  std::vector<InputValue>{NodeReference{"price", "result"}}},
            {"SLOT1",  std::vector<InputValue>{NodeReference{"volume", "result"}}}
        }));
        algorithms.push_back(MakeRegularNode("gt_0", "gt", {
            {"SLOT0",  std::vector<InputValue>{NodeReference{"add_0", "result"}}},
            {"SLOT1",  std::vector<InputValue>{NodeReference{"threshold", "result"}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 2);
        REQUIRE(result[0].id == "add_0");
        REQUIRE(result[1].id == "gt_0");
    }
}

TEST_CASE("ScalarInliningPass inlines number scalars", "[scalar_inlining]") {
    SECTION("Single number scalar inlined and removed") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{42.0});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("num_0", "number", options));
        algorithms.push_back(MakeRegularNode("gt_0", "gt", {
            {"SLOT0", {InputValue{NodeReference{"price", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"num_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        // Scalar node removed, only gt_0 remains
        REQUIRE(result.size() == 1);
        REQUIRE(result[0].id == "gt_0");

        // Scalar inlined into gt_0
        REQUIRE(result[0].inputs.size() == 2);  // SLOT0 and SLOT1
        REQUIRE(result[0].inputs.count("SLOT0") == 1);
        REQUIRE(result[0].inputs.count("SLOT1") == 1);
        REQUIRE((result[0].inputs["SLOT1"][0].IsLiteral() && result[0].inputs["SLOT1"][0].GetLiteral().IsDecimal()));
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetDecimal() == 42.0);
    }

    SECTION("Negative number inlined") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{-99.5});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("num_0", "number", options));
        algorithms.push_back(MakeRegularNode("add_0", "add", {
            {"SLOT0", {InputValue{NodeReference{"price", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"num_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetDecimal() == -99.5);
    }
}

TEST_CASE("ScalarInliningPass inlines text scalars", "[scalar_inlining]") {
    SECTION("String scalar inlined") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{std::string("AAPL")});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("text_0", "text", options));
        algorithms.push_back(MakeRegularNode("filter_0", "filter", {
            {"SLOT0", {InputValue{NodeReference{"symbols", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"text_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE((result[0].inputs["SLOT1"][0].IsLiteral() && result[0].inputs["SLOT1"][0].GetLiteral().IsString()));
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetString() == "AAPL");
    }

    SECTION("Empty string inlined") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{std::string("")});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("text_0", "text", options));
        algorithms.push_back(MakeRegularNode("func_0", "some_func", {
            {"SLOT0", {InputValue{NodeReference{"text_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT0"][0].GetLiteral().GetString() == "");
    }
}

TEST_CASE("ScalarInliningPass inlines boolean scalars", "[scalar_inlining]") {
    SECTION("bool_true inlined") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("bool_0", "bool_true"));
        algorithms.push_back(MakeRegularNode("select_0", "select", {
            {"SLOT0", {InputValue{NodeReference{"data", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"bool_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(((result[0].inputs["SLOT1"][0].IsLiteral()) &&
            (result[0].inputs["SLOT1"][0].GetLiteral().IsBoolean())));
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetBoolean() == true);
    }

    SECTION("bool_false inlined") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("bool_0", "bool_false"));
        algorithms.push_back(MakeRegularNode("select_0", "select", {
            {"SLOT0", {InputValue{NodeReference{"data", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"bool_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetBoolean() == false);
    }
}

TEST_CASE("ScalarInliningPass inlines numeric constants", "[scalar_inlining]") {
    SECTION("zero, one, negative_one") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("zero_0", "zero"));
        algorithms.push_back(MakeScalarNode("one_0", "one"));
        algorithms.push_back(MakeScalarNode("neg_0", "negative_one"));
        algorithms.push_back(MakeRegularNode("func_0", "calc", {
            {"SLOT0", {InputValue{NodeReference{"zero_0", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"one_0", "result"}}}},
            {"SLOT2", {InputValue{NodeReference{"neg_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT0"][0].GetLiteral().GetDecimal() == 0.0);
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetDecimal() == 1.0);
        REQUIRE(result[0].inputs["SLOT2"][0].GetLiteral().GetDecimal() == -1.0);
    }
}

TEST_CASE("ScalarInliningPass inlines mathematical constants", "[scalar_inlining]") {
    SECTION("pi, e, phi") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("pi_0", "pi"));
        algorithms.push_back(MakeScalarNode("e_0", "e"));
        algorithms.push_back(MakeScalarNode("phi_0", "phi"));
        algorithms.push_back(MakeRegularNode("func_0", "calc", {
            {"SLOT0", {InputValue{NodeReference{"pi_0", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"e_0", "result"}}}},
            {"SLOT2", {InputValue{NodeReference{"phi_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT0"][0].GetLiteral().GetDecimal() == Catch::Approx(3.141592653589793));
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetDecimal() == Catch::Approx(2.718281828459045));
        REQUIRE(result[0].inputs["SLOT2"][0].GetLiteral().GetDecimal() == Catch::Approx(1.618033988749895));
    }

    SECTION("sqrt constants") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("sqrt2_0", "sqrt2"));
        algorithms.push_back(MakeScalarNode("sqrt3_0", "sqrt3"));
        algorithms.push_back(MakeScalarNode("sqrt5_0", "sqrt5"));
        algorithms.push_back(MakeRegularNode("func_0", "calc", {
            {"SLOT0", {InputValue{NodeReference{"sqrt2_0", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"sqrt3_0", "result"}}}},
            {"SLOT2", {InputValue{NodeReference{"sqrt5_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT0"][0].GetLiteral().GetDecimal() == Catch::Approx(1.414213562373095));
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetDecimal() == Catch::Approx(1.732050807568877));
        REQUIRE(result[0].inputs["SLOT2"][0].GetLiteral().GetDecimal() == Catch::Approx(2.236067977499790));
    }

    SECTION("logarithm constants") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("ln2_0", "ln2"));
        algorithms.push_back(MakeScalarNode("ln10_0", "ln10"));
        algorithms.push_back(MakeScalarNode("log2e_0", "log2e"));
        algorithms.push_back(MakeScalarNode("log10e_0", "log10e"));
        algorithms.push_back(MakeRegularNode("func_0", "calc", {
            {"SLOT0", {InputValue{NodeReference{"ln2_0", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"ln10_0", "result"}}}},
            {"SLOT2", {InputValue{NodeReference{"log2e_0", "result"}}}},
            {"SLOT3", {InputValue{NodeReference{"log10e_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT0"][0].GetLiteral().GetDecimal() == Catch::Approx(0.693147180559945));
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetDecimal() == Catch::Approx(2.302585092994046));
        REQUIRE(result[0].inputs["SLOT2"][0].GetLiteral().GetDecimal() == Catch::Approx(1.442695040888963));
        REQUIRE(result[0].inputs["SLOT3"][0].GetLiteral().GetDecimal() == Catch::Approx(0.434294481903252));
    }
}

TEST_CASE("ScalarInliningPass inlines typed nulls", "[scalar_inlining]") {
    SECTION("null_number") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("null_0", "null_number"));
        algorithms.push_back(MakeRegularNode("func_0", "coalesce", {
            {"SLOT0", {InputValue{NodeReference{"data", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"null_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(((result[0].inputs["SLOT1"][0].IsLiteral()) && (result[0].inputs["SLOT1"][0].GetLiteral().IsNull())));
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetNull().type == epoch_core::IODataType::Decimal);
    }

    SECTION("All typed nulls") {
        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("null_num", "null_number"));
        algorithms.push_back(MakeScalarNode("null_str", "null_string"));
        algorithms.push_back(MakeScalarNode("null_bool", "null_boolean"));
        algorithms.push_back(MakeScalarNode("null_ts", "null_timestamp"));
        algorithms.push_back(MakeRegularNode("func_0", "func", {
            {"SLOT0", {InputValue{NodeReference{"null_num", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"null_str", "result"}}}},
            {"SLOT2", {InputValue{NodeReference{"null_bool", "result"}}}},
            {"SLOT3", {InputValue{NodeReference{"null_ts", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT0"][0].GetLiteral().GetNull().type == epoch_core::IODataType::Decimal);
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetNull().type == epoch_core::IODataType::String);
        REQUIRE(result[0].inputs["SLOT2"][0].GetLiteral().GetNull().type == epoch_core::IODataType::Boolean);
        REQUIRE(result[0].inputs["SLOT3"][0].GetLiteral().GetNull().type == epoch_core::IODataType::Timestamp);
    }
}

TEST_CASE("ScalarInliningPass handles multiple consumers", "[scalar_inlining]") {
    SECTION("Scalar used by multiple nodes") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{5.0});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("num_0", "number", options));
        algorithms.push_back(MakeRegularNode("add_0", "add", {
            {"SLOT0", {InputValue{NodeReference{"price", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"num_0", "result"}}}}
        }));
        algorithms.push_back(MakeRegularNode("mul_0", "mul", {
            {"SLOT0", {InputValue{NodeReference{"volume", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"num_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        // Scalar node removed, add_0 and mul_0 remain
        REQUIRE(result.size() == 2);

        // Both consumers have scalar inlined
        auto& add_node = result[0].id == "add_0" ? result[0] : result[1];
        auto& mul_node = result[0].id == "mul_0" ? result[0] : result[1];

        REQUIRE(add_node.inputs["SLOT1"][0].GetLiteral().GetDecimal() == 5.0);
        REQUIRE(mul_node.inputs["SLOT1"][0].GetLiteral().GetDecimal() == 5.0);
    }
}

TEST_CASE("ScalarInliningPass handles mixed inputs", "[scalar_inlining]") {
    SECTION("Node with both scalar and regular inputs") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{100.0});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("threshold", "number", options));
        algorithms.push_back(MakeRegularNode("gt_0", "gt", {
            {"SLOT0", {InputValue{NodeReference{"price", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"threshold", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].id == "gt_0");
        REQUIRE(result[0].inputs.size() == 2);  // price and threshold
        REQUIRE((result[0].inputs["SLOT0"][0].GetNodeReference().GetNodeId() == "price" && result[0].inputs["SLOT0"][0].GetNodeReference().GetHandle() == "result"));
        REQUIRE(result[0].inputs.count("SLOT1") == 1);  // threshold inlined
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetDecimal() == 100.0);
    }

    SECTION("Multiple mixed types") {
        MetaDataArgDefinitionMapping num_options;
        num_options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{42.0});

        MetaDataArgDefinitionMapping text_options;
        text_options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{std::string("test")});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("num_0", "number", num_options));
        algorithms.push_back(MakeScalarNode("text_0", "text", text_options));
        algorithms.push_back(MakeScalarNode("bool_0", "bool_true"));
        algorithms.push_back(MakeRegularNode("func_0", "some_func", {
            {"SLOT0", {InputValue{NodeReference{"data", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"num_0", "result"}}}},
            {"SLOT2", {InputValue{NodeReference{"text_0", "result"}}}},
            {"SLOT3", {InputValue{NodeReference{"bool_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs.size() == 4);  // All 4 slots with scalars inlined
        REQUIRE(result[0].inputs["SLOT1"][0].GetLiteral().GetDecimal() == 42.0);
        REQUIRE(result[0].inputs["SLOT2"][0].GetLiteral().GetString() == "test");
        REQUIRE(result[0].inputs["SLOT3"][0].GetLiteral().GetBoolean() == true);
    }
}

TEST_CASE("ScalarInliningPass handles complex graphs", "[scalar_inlining]") {
    SECTION("Chained operations with scalars") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{10.0});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeRegularNode("price", "get_price", {}));
        algorithms.push_back(MakeScalarNode("threshold", "number", options));
        algorithms.push_back(MakeRegularNode("gt_0", "gt", {
            {"SLOT0", {InputValue{NodeReference{"price", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"threshold", "result"}}}}
        }));
        algorithms.push_back(MakeRegularNode("sink_0", "sink", {
            {"SLOT0", {InputValue{NodeReference{"gt_0", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        // threshold scalar removed, 3 nodes remain
        REQUIRE(result.size() == 3);

        // Find gt_0 node
        AlgorithmNode* gt_node = nullptr;
        for (auto& node : result) {
            if (node.id == "gt_0") {
                gt_node = &node;
                break;
            }
        }

        REQUIRE(gt_node != nullptr);
        REQUIRE(gt_node->inputs.size() == 2);
        REQUIRE(gt_node->inputs.count("SLOT1") == 1);
        REQUIRE(gt_node->inputs["SLOT1"].front().GetLiteral().GetDecimal() == 10.0);
    }

    SECTION("Multiple scalars in complex graph") {
        MetaDataArgDefinitionMapping opt1;
        opt1["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{5.0});

        MetaDataArgDefinitionMapping opt2;
        opt2["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{10.0});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeRegularNode("price", "get_price", {}));
        algorithms.push_back(MakeScalarNode("min_val", "number", opt1));
        algorithms.push_back(MakeScalarNode("max_val", "number", opt2));
        algorithms.push_back(MakeRegularNode("clip_0", "clip", {
            {"SLOT0", {InputValue{NodeReference{"price", "result"}}}},
            {"SLOT1", {InputValue{NodeReference{"min_val", "result"}}}},
            {"SLOT2", {InputValue{NodeReference{"max_val", "result"}}}}
        }));

        auto result = ScalarInliningPass::Run(algorithms);

        REQUIRE(result.size() == 2);  // price and clip_0 remain

        auto& clip_node = result[0].id == "clip_0" ? result[0] : result[1];
        REQUIRE(clip_node.inputs.size() == 3);  // price, min_val, max_val
        REQUIRE(clip_node.inputs.count("SLOT1") == 1);  // min_val inlined
        REQUIRE(clip_node.inputs.count("SLOT2") == 1);  // max_val inlined
        REQUIRE(clip_node.inputs["SLOT1"][0].GetLiteral().GetDecimal() == 5.0);
        REQUIRE(clip_node.inputs["SLOT2"][0].GetLiteral().GetDecimal() == 10.0);
    }
}

TEST_CASE("ScalarInliningPass handles edge cases", "[scalar_inlining]") {
    SECTION("Scalar with no consumers still removed") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{99.0});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("num_0", "number", options));
        algorithms.push_back(MakeRegularNode("price", "get_price", {}));

        auto result = ScalarInliningPass::Run(algorithms);

        // Unused scalar removed
        REQUIRE(result.size() == 1);
        REQUIRE(result[0].id == "price");
    }

    SECTION("Only scalar nodes in graph") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{42.0});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("num_0", "number", options));
        algorithms.push_back(MakeScalarNode("pi_0", "pi"));
        algorithms.push_back(MakeScalarNode("bool_0", "bool_true"));

        auto result = ScalarInliningPass::Run(algorithms);

        // All scalars removed
        REQUIRE(result.empty());
    }

    SECTION("Input slot with multiple references not inlined") {
        MetaDataArgDefinitionMapping options;
        options["value"] = MetaDataOptionDefinition(MetaDataOptionDefinition::T{42.0});

        std::vector<AlgorithmNode> algorithms;
        algorithms.push_back(MakeScalarNode("num_0", "number", options));

        // Create node with multi-reference input (edge case)
        AlgorithmNode multi_input_node;
        multi_input_node.id = "func_0";
        multi_input_node.type = "some_func";
        multi_input_node.inputs["SLOT0"] = {InputValue{NodeReference{"num_0", "result"}}, InputValue{NodeReference{"other", "result"}}};

        algorithms.push_back(multi_input_node);

        auto result = ScalarInliningPass::Run(algorithms);

        // Scalar still removed, but multi-ref input not inlined
        REQUIRE(result.size() == 1);
        REQUIRE(result[0].inputs["SLOT0"].size() == 2);
        REQUIRE(!result[0].inputs.empty());
    }
}
