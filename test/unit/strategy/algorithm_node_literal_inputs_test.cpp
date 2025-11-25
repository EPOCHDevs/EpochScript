#include <catch2/catch_test_macros.hpp>
#include <epoch_script/strategy/metadata.h>
#include <epoch_script/transforms/core/constant_value.h>
#include <yaml-cpp/yaml.h>
#include <glaze/glaze.hpp>

using namespace epoch_script;
using namespace epoch_script::strategy;
using namespace epoch_script::transform;

// ============================================================================
// TEST SUITE: AlgorithmNode with InputValue - Structure and Equality
// ============================================================================

TEST_CASE("AlgorithmNode InputValue field exists and works", "[algorithm_node][input_value]") {
    SECTION("Create node with empty inputs") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "add";

        REQUIRE(node.inputs.empty());
    }

    SECTION("Create node with literal inputs") {
        AlgorithmNode node;
        node.id = "add_0";
        node.type = "add";
        node.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};
        node.inputs["SLOT1"] = {InputValue(ConstantValue(10.0))};

        REQUIRE(node.inputs.size() == 2);
        REQUIRE(node.inputs["SLOT0"].size() == 1);
        REQUIRE(node.inputs["SLOT1"].size() == 1);

        REQUIRE(node.inputs["SLOT0"].front().IsLiteral());
        REQUIRE(node.inputs["SLOT0"].front().GetLiteral().GetDecimal() == 42.0);

        REQUIRE(node.inputs["SLOT1"].front().IsLiteral());
        REQUIRE(node.inputs["SLOT1"].front().GetLiteral().GetDecimal() == 10.0);
    }

    SECTION("Node with both reference and literal inputs") {
        AlgorithmNode node;
        node.id = "add_0";
        node.type = "add";
        node.inputs["SLOT0"] = {InputValue(NodeReference("price", "result"))};
        node.inputs["SLOT1"] = {InputValue(ConstantValue(100.0))};

        REQUIRE(node.inputs.size() == 2);
        REQUIRE(node.inputs["SLOT0"].front().IsNodeReference());
        REQUIRE(node.inputs["SLOT0"].front().GetNodeReference().GetNodeId() == "price");
        REQUIRE(node.inputs["SLOT0"].front().GetNodeReference().GetHandle() == "result");
        REQUIRE(node.inputs["SLOT1"].front().IsLiteral());
        REQUIRE(node.inputs["SLOT1"].front().GetLiteral().GetDecimal() == 100.0);
    }
}

TEST_CASE("AlgorithmNode equality with InputValue", "[algorithm_node][input_value]") {
    SECTION("Equal nodes with same literal inputs") {
        AlgorithmNode node1;
        node1.id = "add_0";
        node1.type = "add";
        node1.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};

        AlgorithmNode node2;
        node2.id = "add_0";
        node2.type = "add";
        node2.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};

        REQUIRE(node1 == node2);
    }

    SECTION("Not equal - different literal values") {
        AlgorithmNode node1;
        node1.id = "add_0";
        node1.type = "add";
        node1.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};

        AlgorithmNode node2;
        node2.id = "add_0";
        node2.type = "add";
        node2.inputs["SLOT0"] = {InputValue(ConstantValue(99.0))};

        REQUIRE_FALSE(node1 == node2);
    }

    SECTION("Not equal - missing inputs") {
        AlgorithmNode node1;
        node1.id = "add_0";
        node1.type = "add";
        node1.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};

        AlgorithmNode node2;
        node2.id = "add_0";
        node2.type = "add";

        REQUIRE_FALSE(node1 == node2);
    }

    SECTION("Not equal - different input keys") {
        AlgorithmNode node1;
        node1.id = "add_0";
        node1.type = "add";
        node1.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};

        AlgorithmNode node2;
        node2.id = "add_0";
        node2.type = "add";
        node2.inputs["SLOT1"] = {InputValue(ConstantValue(42.0))};

        REQUIRE_FALSE(node1 == node2);
    }

    SECTION("Not equal - reference vs literal") {
        AlgorithmNode node1;
        node1.id = "add_0";
        node1.type = "add";
        node1.inputs["SLOT0"] = {InputValue(NodeReference("some", "ref"))};

        AlgorithmNode node2;
        node2.id = "add_0";
        node2.type = "add";
        node2.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};

        REQUIRE_FALSE(node1 == node2);
    }

    SECTION("Equal with mixed reference and literal inputs") {
        AlgorithmNode node1;
        node1.id = "add_0";
        node1.type = "add";
        node1.inputs["SLOT0"] = {InputValue(NodeReference("price", "result"))};
        node1.inputs["SLOT1"] = {InputValue(ConstantValue(100.0))};

        AlgorithmNode node2;
        node2.id = "add_0";
        node2.type = "add";
        node2.inputs["SLOT0"] = {InputValue(NodeReference("price", "result"))};
        node2.inputs["SLOT1"] = {InputValue(ConstantValue(100.0))};

        REQUIRE(node1 == node2);
    }
}

TEST_CASE("AlgorithmNode InputValue with different ConstantValue types", "[algorithm_node][input_value]") {
    SECTION("Decimal literal") {
        AlgorithmNode node;
        node.id = "node_0";
        node.type = "test";
        node.inputs["value"] = {InputValue(ConstantValue(3.14))};

        REQUIRE(node.inputs["value"].front().IsLiteral());
        REQUIRE(node.inputs["value"].front().GetLiteral().IsDecimal());
        REQUIRE(node.inputs["value"].front().GetLiteral().GetDecimal() == 3.14);
    }

    SECTION("String literal") {
        AlgorithmNode node;
        node.id = "node_0";
        node.type = "test";
        node.inputs["symbol"] = {InputValue(ConstantValue(std::string("AAPL")))};

        REQUIRE(node.inputs["symbol"].front().IsLiteral());
        REQUIRE(node.inputs["symbol"].front().GetLiteral().IsString());
        REQUIRE(node.inputs["symbol"].front().GetLiteral().GetString() == "AAPL");
    }

    SECTION("Boolean literal") {
        AlgorithmNode node;
        node.id = "node_0";
        node.type = "test";
        node.inputs["flag"] = {InputValue(ConstantValue(true))};

        REQUIRE(node.inputs["flag"].front().IsLiteral());
        REQUIRE(node.inputs["flag"].front().GetLiteral().IsBoolean());
        REQUIRE(node.inputs["flag"].front().GetLiteral().GetBoolean() == true);
    }

    SECTION("Integer literal") {
        AlgorithmNode node;
        node.id = "node_0";
        node.type = "test";
        node.inputs["count"] = {InputValue(ConstantValue(42))};

        REQUIRE(node.inputs["count"].front().IsLiteral());
        REQUIRE(node.inputs["count"].front().GetLiteral().IsDecimal());
        REQUIRE(node.inputs["count"].front().GetLiteral().GetDecimal() == 42);
    }

    SECTION("Timestamp literal") {
        auto dt = epoch_frame::DateTime::from_str("2024-01-01 10:30:00", "UTC");
        AlgorithmNode node;
        node.id = "node_0";
        node.type = "test";
        node.inputs["timestamp"] = {InputValue(ConstantValue(dt))};

        REQUIRE(node.inputs["timestamp"].front().IsLiteral());
        REQUIRE(node.inputs["timestamp"].front().GetLiteral().IsTimestamp());
        REQUIRE(node.inputs["timestamp"].front().GetLiteral().GetTimestamp() == dt);
    }

    SECTION("Null literal") {
        AlgorithmNode node;
        node.id = "node_0";
        node.type = "test";
        node.inputs["null_val"] = {InputValue(ConstantValue::MakeNull(epoch_core::IODataType::Decimal))};

        REQUIRE(node.inputs["null_val"].front().IsLiteral());
        REQUIRE(node.inputs["null_val"].front().GetLiteral().IsNull());
        REQUIRE(node.inputs["null_val"].front().GetLiteral().GetNull().type == epoch_core::IODataType::Decimal);
    }

    SECTION("Multiple types in same node") {
        AlgorithmNode node;
        node.id = "node_0";
        node.type = "test";
        node.inputs["num"] = {InputValue(ConstantValue(42.0))};
        node.inputs["str"] = {InputValue(ConstantValue(std::string("test")))};
        node.inputs["bool"] = {InputValue(ConstantValue(false))};

        REQUIRE(node.inputs.size() == 3);
        REQUIRE(node.inputs["num"].front().GetLiteral().IsDecimal());
        REQUIRE(node.inputs["str"].front().GetLiteral().IsString());
        REQUIRE(node.inputs["bool"].front().GetLiteral().IsBoolean());
    }
}

// ============================================================================
// TEST SUITE: Copy and Move Semantics
// ============================================================================

TEST_CASE("AlgorithmNode InputValue copy and move", "[algorithm_node][input_value]") {
    SECTION("Copy construction") {
        AlgorithmNode original;
        original.id = "add_0";
        original.type = "add";
        original.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};
        original.inputs["SLOT1"] = {InputValue(ConstantValue(std::string("test")))};

        AlgorithmNode copy = original;

        REQUIRE(copy.id == original.id);
        REQUIRE(copy.type == original.type);
        REQUIRE(copy.inputs.size() == 2);
        REQUIRE(copy.inputs["SLOT0"].front().GetLiteral().GetDecimal() == 42.0);
        REQUIRE(copy.inputs["SLOT1"].front().GetLiteral().GetString() == "test");
        REQUIRE(copy == original);
    }

    SECTION("Copy assignment") {
        AlgorithmNode original;
        original.id = "add_0";
        original.type = "add";
        original.inputs["SLOT0"] = {InputValue(ConstantValue(99.0))};

        AlgorithmNode copy;
        copy = original;

        REQUIRE(copy.inputs.size() == 1);
        REQUIRE(copy.inputs["SLOT0"].front().GetLiteral().GetDecimal() == 99.0);
        REQUIRE(copy == original);
    }

    SECTION("Move construction") {
        AlgorithmNode original;
        original.id = "add_0";
        original.type = "add";
        original.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};

        AlgorithmNode moved = std::move(original);

        REQUIRE(moved.id == "add_0");
        REQUIRE(moved.inputs.size() == 1);
        REQUIRE(moved.inputs["SLOT0"].front().GetLiteral().GetDecimal() == 42.0);
    }
}

// ============================================================================
// TEST SUITE: Edge Cases and Complex Scenarios
// ============================================================================

TEST_CASE("AlgorithmNode InputValue edge cases", "[algorithm_node][input_value]") {
    SECTION("Large number of inputs") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";

        for (int i = 0; i < 100; ++i) {
            node.inputs["SLOT" + std::to_string(i)] = {InputValue(ConstantValue(static_cast<double>(i)))};
        }

        REQUIRE(node.inputs.size() == 100);
        REQUIRE(node.inputs["SLOT50"].front().GetLiteral().GetDecimal() == 50.0);
    }

    SECTION("Overwriting input value") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";
        node.inputs["SLOT0"] = {InputValue(ConstantValue(10.0))};

        REQUIRE(node.inputs["SLOT0"].front().GetLiteral().GetDecimal() == 10.0);

        node.inputs["SLOT0"] = {InputValue(ConstantValue(20.0))};

        REQUIRE(node.inputs["SLOT0"].front().GetLiteral().GetDecimal() == 20.0);
    }

    SECTION("Removing input") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";
        node.inputs["SLOT0"] = {InputValue(ConstantValue(10.0))};
        node.inputs["SLOT1"] = {InputValue(ConstantValue(20.0))};

        REQUIRE(node.inputs.size() == 2);

        node.inputs.erase("SLOT0");

        REQUIRE(node.inputs.size() == 1);
        REQUIRE(node.inputs.count("SLOT0") == 0);
        REQUIRE(node.inputs.count("SLOT1") == 1);
    }

    SECTION("Clear all inputs") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";
        node.inputs["SLOT0"] = {InputValue(ConstantValue(10.0))};
        node.inputs["SLOT1"] = {InputValue(ConstantValue(20.0))};

        node.inputs.clear();

        REQUIRE(node.inputs.empty());
    }

    SECTION("Node with only literal inputs") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";
        node.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};
        node.inputs["SLOT1"] = {InputValue(ConstantValue(std::string("value")))};

        REQUIRE(node.inputs.size() == 2);
        REQUIRE(node.inputs["SLOT0"].front().IsLiteral());
        REQUIRE(node.inputs["SLOT1"].front().IsLiteral());
    }

    SECTION("Multiple InputValues in same slot") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";
        node.inputs["SLOT0"] = {
            InputValue(NodeReference("ref1", "out")),
            InputValue(ConstantValue(42.0)),
            InputValue(NodeReference("ref2", "out"))
        };

        REQUIRE(node.inputs["SLOT0"].size() == 3);
        REQUIRE(node.inputs["SLOT0"][0].IsNodeReference());
        REQUIRE(node.inputs["SLOT0"][0].GetNodeReference().GetNodeId() == "ref1");
        REQUIRE(node.inputs["SLOT0"][0].GetNodeReference().GetHandle() == "out");
        REQUIRE(node.inputs["SLOT0"][1].IsLiteral());
        REQUIRE(node.inputs["SLOT0"][1].GetLiteral().GetDecimal() == 42.0);
        REQUIRE(node.inputs["SLOT0"][2].IsNodeReference());
        REQUIRE(node.inputs["SLOT0"][2].GetNodeReference().GetNodeId() == "ref2");
    }
}

// ============================================================================
// TEST SUITE: GetColumnIdentifier
// ============================================================================

TEST_CASE("InputValue GetColumnIdentifier", "[input_value][column_identifier]") {
    SECTION("Node reference returns the combined reference string") {
        InputValue input(NodeReference("price", "result"));

        // GetColumnIdentifier returns "node_id#handle" format for DataFrame columns
        REQUIRE(input.GetColumnIdentifier() == "price#result");
    }

    SECTION("Literal returns the slot name") {
        InputValue input(ConstantValue(42.0));
        REQUIRE(input.GetColumnIdentifier() == "num_42");
    }

    SECTION("Different literal types all return slot name") {
        InputValue decimal_input(ConstantValue(3.14));
        InputValue string_input(ConstantValue(std::string("AAPL")));
        InputValue bool_input(ConstantValue(true));
        InputValue bool_input_false(ConstantValue(false));

        InputValue int_input(ConstantValue(100));

        REQUIRE(decimal_input.GetColumnIdentifier() == "dec_3_14");
        REQUIRE(string_input.GetColumnIdentifier() == "text_AAPL");
        REQUIRE(bool_input.GetColumnIdentifier() == "bool_true");
        REQUIRE(bool_input_false.GetColumnIdentifier() == "bool_false");

        REQUIRE(int_input.GetColumnIdentifier() == "num_100");
    }



    SECTION("Mixed references and literals in variadic context") {
        std::vector<InputValue> inputs = {
            InputValue(NodeReference("series_a", "result")),
            InputValue(ConstantValue(100.0)),
            InputValue(NodeReference("series_b", "result"))
        };

        std::vector<std::string> column_ids;
        for (const auto& input : inputs) {
            column_ids.push_back(input.GetColumnIdentifier());
        }

        REQUIRE(column_ids.size() == 3);
        REQUIRE(column_ids[0] == "series_a#result");
        REQUIRE(column_ids[1] == "num_100");  // Literal uses slot name
        REQUIRE(column_ids[2] == "series_b#result");
    }
}

// ============================================================================
// TEST SUITE: NodeReference Tests
// ============================================================================

TEST_CASE("NodeReference structure", "[node_reference]") {
    SECTION("Create NodeReference with node_id and handle") {
        NodeReference ref("my_node", "output");

        REQUIRE(ref.GetNodeId() == "my_node");
        REQUIRE(ref.GetHandle() == "output");
        REQUIRE(ref.GetRef() == "my_node#output");
    }

    SECTION("NodeReference equality") {
        NodeReference ref1("node_a", "result");
        NodeReference ref2("node_a", "result");
        NodeReference ref3("node_b", "result");
        NodeReference ref4("node_a", "other");

        REQUIRE(ref1 == ref2);
        REQUIRE_FALSE(ref1 == ref3);
        REQUIRE_FALSE(ref1 == ref4);
    }

    SECTION("InputValue with NodeReference") {
        InputValue input(NodeReference("price_node", "close"));

        REQUIRE(input.IsNodeReference());
        REQUIRE_FALSE(input.IsLiteral());
        REQUIRE(input.GetNodeReference().GetNodeId() == "price_node");
        REQUIRE(input.GetNodeReference().GetHandle() == "close");
    }
}

// ============================================================================
// TEST SUITE: Variadic Inputs (Multiple InputValues per Slot)
// ============================================================================

TEST_CASE("AlgorithmNode variadic inputs", "[algorithm_node][input_value][variadic]") {
    SECTION("Multiple references in one slot") {
        AlgorithmNode node;
        node.id = "concat_0";
        node.type = "concat";
        node.inputs["SLOT0"] = {
            InputValue(NodeReference("series_a", "result")),
            InputValue(NodeReference("series_b", "result")),
            InputValue(NodeReference("series_c", "result"))
        };

        REQUIRE(node.inputs["SLOT0"].size() == 3);
        REQUIRE(node.inputs["SLOT0"][0].IsNodeReference());
        REQUIRE(node.inputs["SLOT0"][0].GetNodeReference().GetNodeId() == "series_a");
        REQUIRE(node.inputs["SLOT0"][1].GetNodeReference().GetNodeId() == "series_b");
        REQUIRE(node.inputs["SLOT0"][2].GetNodeReference().GetNodeId() == "series_c");
    }

    SECTION("Multiple literals in one slot") {
        AlgorithmNode node;
        node.id = "sum_0";
        node.type = "sum";
        node.inputs["values"] = {
            InputValue(ConstantValue(1.0)),
            InputValue(ConstantValue(2.0)),
            InputValue(ConstantValue(3.0)),
            InputValue(ConstantValue(4.0))
        };

        REQUIRE(node.inputs["values"].size() == 4);
        REQUIRE(node.inputs["values"][0].GetLiteral().GetDecimal() == 1.0);
        REQUIRE(node.inputs["values"][1].GetLiteral().GetDecimal() == 2.0);
        REQUIRE(node.inputs["values"][2].GetLiteral().GetDecimal() == 3.0);
        REQUIRE(node.inputs["values"][3].GetLiteral().GetDecimal() == 4.0);
    }

    SECTION("Mixed references and literals in one slot") {
        AlgorithmNode node;
        node.id = "custom_0";
        node.type = "custom";
        node.inputs["SLOT0"] = {
            InputValue(NodeReference("price", "result")),
            InputValue(ConstantValue(100.0)),
            InputValue(NodeReference("volume", "result")),
            InputValue(ConstantValue(std::string("marker")))
        };

        REQUIRE(node.inputs["SLOT0"].size() == 4);
        REQUIRE(node.inputs["SLOT0"][0].IsNodeReference());
        REQUIRE(node.inputs["SLOT0"][0].GetNodeReference().GetNodeId() == "price");
        REQUIRE(node.inputs["SLOT0"][1].IsLiteral());
        REQUIRE(node.inputs["SLOT0"][1].GetLiteral().GetDecimal() == 100.0);
        REQUIRE(node.inputs["SLOT0"][2].IsNodeReference());
        REQUIRE(node.inputs["SLOT0"][2].GetNodeReference().GetNodeId() == "volume");
        REQUIRE(node.inputs["SLOT0"][3].IsLiteral());
        REQUIRE(node.inputs["SLOT0"][3].GetLiteral().GetString() == "marker");
    }

    SECTION("Multiple variadic slots") {
        AlgorithmNode node;
        node.id = "multi_0";
        node.type = "multi_input";
        node.inputs["SLOT0"] = {
            InputValue(NodeReference("a", "result")),
            InputValue(NodeReference("b", "result"))
        };
        node.inputs["SLOT1"] = {
            InputValue(ConstantValue(10.0)),
            InputValue(ConstantValue(20.0)),
            InputValue(ConstantValue(30.0))
        };

        REQUIRE(node.inputs.size() == 2);
        REQUIRE(node.inputs["SLOT0"].size() == 2);
        REQUIRE(node.inputs["SLOT1"].size() == 3);
    }

    SECTION("Equality with variadic inputs - same order") {
        AlgorithmNode node1;
        node1.id = "test_0";
        node1.type = "test";
        node1.inputs["SLOT0"] = {
            InputValue(NodeReference("ref1", "out")),
            InputValue(ConstantValue(42.0)),
            InputValue(NodeReference("ref2", "out"))
        };

        AlgorithmNode node2;
        node2.id = "test_0";
        node2.type = "test";
        node2.inputs["SLOT0"] = {
            InputValue(NodeReference("ref1", "out")),
            InputValue(ConstantValue(42.0)),
            InputValue(NodeReference("ref2", "out"))
        };

        REQUIRE(node1 == node2);
    }

    SECTION("Not equal - different order in variadic slot") {
        AlgorithmNode node1;
        node1.id = "test_0";
        node1.type = "test";
        node1.inputs["SLOT0"] = {
            InputValue(NodeReference("ref1", "out")),
            InputValue(NodeReference("ref2", "out"))
        };

        AlgorithmNode node2;
        node2.id = "test_0";
        node2.type = "test";
        node2.inputs["SLOT0"] = {
            InputValue(NodeReference("ref2", "out")),
            InputValue(NodeReference("ref1", "out"))
        };

        REQUIRE_FALSE(node1 == node2);
    }

    SECTION("Not equal - different size variadic slot") {
        AlgorithmNode node1;
        node1.id = "test_0";
        node1.type = "test";
        node1.inputs["SLOT0"] = {
            InputValue(NodeReference("ref1", "out")),
            InputValue(NodeReference("ref2", "out"))
        };

        AlgorithmNode node2;
        node2.id = "test_0";
        node2.type = "test";
        node2.inputs["SLOT0"] = {
            InputValue(NodeReference("ref1", "out")),
            InputValue(NodeReference("ref2", "out")),
            InputValue(NodeReference("ref3", "out"))
        };

        REQUIRE_FALSE(node1 == node2);
    }

    SECTION("Append to variadic slot") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";
        node.inputs["SLOT0"] = {InputValue(NodeReference("ref1", "out"))};

        REQUIRE(node.inputs["SLOT0"].size() == 1);

        node.inputs["SLOT0"].push_back(InputValue(NodeReference("ref2", "out")));
        node.inputs["SLOT0"].push_back(InputValue(ConstantValue(99.0)));

        REQUIRE(node.inputs["SLOT0"].size() == 3);
        REQUIRE(node.inputs["SLOT0"][0].GetNodeReference().GetNodeId() == "ref1");
        REQUIRE(node.inputs["SLOT0"][1].GetNodeReference().GetNodeId() == "ref2");
        REQUIRE(node.inputs["SLOT0"][2].GetLiteral().GetDecimal() == 99.0);
    }

    SECTION("Empty variadic slot") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";
        node.inputs["SLOT0"] = {};

        REQUIRE(node.inputs.size() == 1);
        REQUIRE(node.inputs["SLOT0"].empty());
    }

    SECTION("Copy with variadic inputs") {
        AlgorithmNode original;
        original.id = "test_0";
        original.type = "test";
        original.inputs["SLOT0"] = {
            InputValue(NodeReference("ref1", "out")),
            InputValue(ConstantValue(1.0)),
            InputValue(NodeReference("ref2", "out")),
            InputValue(ConstantValue(2.0))
        };

        AlgorithmNode copy = original;

        REQUIRE(copy.inputs["SLOT0"].size() == 4);
        REQUIRE(copy.inputs["SLOT0"][0].GetNodeReference().GetNodeId() == "ref1");
        REQUIRE(copy.inputs["SLOT0"][1].GetLiteral().GetDecimal() == 1.0);
        REQUIRE(copy.inputs["SLOT0"][2].GetNodeReference().GetNodeId() == "ref2");
        REQUIRE(copy.inputs["SLOT0"][3].GetLiteral().GetDecimal() == 2.0);
        REQUIRE(copy == original);
    }

    SECTION("Multiple literal types in variadic slot") {
        AlgorithmNode node;
        node.id = "test_0";
        node.type = "test";
        node.inputs["SLOT0"] = {
            InputValue(ConstantValue(42.0)),
            InputValue(ConstantValue(std::string("hello"))),
            InputValue(ConstantValue(true)),
            InputValue(ConstantValue(int64_t(123)))
        };

        REQUIRE(node.inputs["SLOT0"].size() == 4);
        REQUIRE(node.inputs["SLOT0"][0].GetLiteral().IsDecimal());
        REQUIRE(node.inputs["SLOT0"][1].GetLiteral().IsString());
        REQUIRE(node.inputs["SLOT0"][2].GetLiteral().IsBoolean());
        REQUIRE(node.inputs["SLOT0"][3].GetLiteral().IsDecimal());
    }
}

// ============================================================================
// TEST SUITE: YAML Serialization/Deserialization
// ============================================================================

TEST_CASE("AlgorithmNode YAML deserialization with InputValue", "[algorithm_node][input_value][yaml]") {
    SECTION("Deserialize node with only literal inputs") {
        std::string yaml_str = R"(
type: add
id: add_0
inputs:
  SLOT0:
      type: literal
      value:
        type: decimal
        value: 42.0
  SLOT1:
      type: literal
      value:
        type: decimal
        value: 10.0
)";
        YAML::Node yaml = YAML::Load(yaml_str);
        AlgorithmNode node = yaml.as<AlgorithmNode>();

        REQUIRE(node.id == "add_0");
        REQUIRE(node.type == "add");
        REQUIRE(node.inputs.size() == 2);
        REQUIRE(node.inputs["SLOT0"].front().IsLiteral());
        REQUIRE(node.inputs["SLOT0"].front().GetLiteral().GetDecimal() == 42.0);
        REQUIRE(node.inputs["SLOT1"].front().GetLiteral().GetDecimal() == 10.0);
    }

    SECTION("Deserialize node with both reference and literal inputs") {
        std::string yaml_str = R"(
type: add
id: add_0
inputs:
  SLOT0:
      type: ref
      value:
          node_id: price
          handle: result
  SLOT1:
      type: literal
      value: { type: decimal, value: 100.0 }
)";
        YAML::Node yaml = YAML::Load(yaml_str);
        AlgorithmNode node = yaml.as<AlgorithmNode>();

        REQUIRE(node.id == "add_0");
        REQUIRE(node.type == "add");
        REQUIRE(node.inputs.size() == 2);
        REQUIRE(node.inputs["SLOT0"].front().IsNodeReference());
        REQUIRE(node.inputs["SLOT0"].front().GetNodeReference().GetNodeId() == "price");
        REQUIRE(node.inputs["SLOT0"].front().GetNodeReference().GetHandle() == "result");
        REQUIRE(node.inputs["SLOT1"].front().IsLiteral());
        REQUIRE(node.inputs["SLOT1"].front().GetLiteral().GetDecimal() == 100.0);
    }

    SECTION("Deserialize variadic references") {
        // Use first_non_null_number which has SLOT input with allowMultipleConnections=true
        // Note: ARG constant equals "SLOT"
        std::string yaml_str = R"(
type: first_non_null_number
id: coalesce_0
inputs:
  SLOT:
    - type: ref
      value:
          node_id: series_a
          handle: result
    - type: ref
      value:
          node_id: series_b
          handle: result
    - type: ref
      value:
          node_id: series_c
          handle: result
)";
        YAML::Node yaml = YAML::Load(yaml_str);
        AlgorithmNode node = yaml.as<AlgorithmNode>();

        REQUIRE(node.inputs[ARG].size() == 3);
        REQUIRE(node.inputs[ARG][0].IsNodeReference());
        REQUIRE(node.inputs[ARG][0].GetNodeReference().GetNodeId() == "series_a");
        REQUIRE(node.inputs[ARG][1].GetNodeReference().GetNodeId() == "series_b");
        REQUIRE(node.inputs[ARG][2].GetNodeReference().GetNodeId() == "series_c");
    }
}

// ============================================================================
// TEST SUITE: JSON Serialization/Deserialization
// ============================================================================

TEST_CASE("AlgorithmNode JSON serialization with InputValue", "[algorithm_node][input_value][json]") {
    SECTION("Round-trip: node with literal inputs") {
        AlgorithmNode original;
        original.id = "add_0";
        original.type = "add";
        original.inputs["SLOT0"] = {InputValue(ConstantValue(42.0))};
        original.inputs["SLOT1"] = {InputValue(ConstantValue(10.5))};

        auto json_result = glz::write_json(original);
        REQUIRE(json_result.has_value());
        std::string json = json_result.value();

        AlgorithmNode deserialized;
        auto parse_error = glz::read_json(deserialized, json);
        REQUIRE_FALSE(parse_error);

        REQUIRE(deserialized.id == original.id);
        REQUIRE(deserialized.type == original.type);
        REQUIRE(deserialized.inputs.size() == 2);
        REQUIRE(deserialized.inputs["SLOT0"].front().GetLiteral().GetDecimal() == 42.0);
        REQUIRE(deserialized.inputs["SLOT1"].front().GetLiteral().GetDecimal() == 10.5);
        REQUIRE(deserialized == original);
    }

    SECTION("Round-trip: node with both reference and literal inputs") {
        AlgorithmNode original;
        original.id = "add_0";
        original.type = "add";
        original.inputs["SLOT0"] = {InputValue(NodeReference("price", "result"))};
        original.inputs["SLOT1"] = {InputValue(ConstantValue(100.0))};

        auto json_result = glz::write_json(original);
        REQUIRE(json_result.has_value());
        std::string json = json_result.value();

        AlgorithmNode deserialized;
        auto parse_error = glz::read_json(deserialized, json);
        REQUIRE_FALSE(parse_error);

        REQUIRE(deserialized.inputs.size() == 2);
        REQUIRE(deserialized.inputs["SLOT0"].front().GetNodeReference().GetNodeId() == "price");
        REQUIRE(deserialized.inputs["SLOT0"].front().GetNodeReference().GetHandle() == "result");
        REQUIRE(deserialized.inputs["SLOT1"].front().GetLiteral().GetDecimal() == 100.0);
        REQUIRE(deserialized == original);
    }

    SECTION("Round-trip: different ConstantValue types") {
        AlgorithmNode original;
        original.id = "test_0";
        original.type = "add";
        original.inputs["decimal"] = {InputValue(ConstantValue(3.14))};
        original.inputs["string"] = {InputValue(ConstantValue(std::string("test")))};
        original.inputs["bool"] = {InputValue(ConstantValue(true))};
        original.inputs["int"] = {InputValue(ConstantValue(int64_t(42)))};

        auto json_result = glz::write_json(original);
        REQUIRE(json_result.has_value());
        std::string json = json_result.value();

        AlgorithmNode deserialized;
        auto parse_error = glz::read_json(deserialized, json);
        REQUIRE_FALSE(parse_error);

        REQUIRE(deserialized.inputs.size() == 4);
        REQUIRE(deserialized.inputs["decimal"].front().GetLiteral().GetDecimal() == 3.14);
        REQUIRE(deserialized.inputs["string"].front().GetLiteral().GetString() == "test");
        REQUIRE(deserialized.inputs["bool"].front().GetLiteral().GetBoolean() == true);
        REQUIRE(deserialized.inputs["int"].front().GetLiteral().GetDecimal() == 42);
        REQUIRE(deserialized == original);
    }

    SECTION("Round-trip: variadic references") {
        AlgorithmNode original;
        original.id = "concat_0";
        original.type = "concat";
        original.inputs["SLOT0"] = {
            InputValue(NodeReference("series_a", "result")),
            InputValue(NodeReference("series_b", "result")),
            InputValue(NodeReference("series_c", "result"))
        };

        auto json_result = glz::write_json(original);
        REQUIRE(json_result.has_value());
        std::string json = json_result.value();

        AlgorithmNode deserialized;
        auto parse_error = glz::read_json(deserialized, json);
        REQUIRE_FALSE(parse_error);

        REQUIRE(deserialized.inputs["SLOT0"].size() == 3);
        REQUIRE(deserialized.inputs["SLOT0"][0].GetNodeReference().GetNodeId() == "series_a");
        REQUIRE(deserialized.inputs["SLOT0"][1].GetNodeReference().GetNodeId() == "series_b");
        REQUIRE(deserialized.inputs["SLOT0"][2].GetNodeReference().GetNodeId() == "series_c");
        REQUIRE(deserialized == original);
    }

    SECTION("Round-trip: variadic mixed references and literals") {
        AlgorithmNode original;
        original.id = "custom_0";
        original.type = "custom";
        original.inputs["SLOT0"] = {
            InputValue(NodeReference("price", "result")),
            InputValue(ConstantValue(100.0)),
            InputValue(NodeReference("volume", "result")),
            InputValue(ConstantValue(std::string("marker")))
        };

        auto json_result = glz::write_json(original);
        REQUIRE(json_result.has_value());
        std::string json = json_result.value();

        AlgorithmNode deserialized;
        auto parse_error = glz::read_json(deserialized, json);
        REQUIRE_FALSE(parse_error);

        REQUIRE(deserialized.inputs["SLOT0"].size() == 4);
        REQUIRE(deserialized.inputs["SLOT0"][0].IsNodeReference());
        REQUIRE(deserialized.inputs["SLOT0"][0].GetNodeReference().GetNodeId() == "price");
        REQUIRE(deserialized.inputs["SLOT0"][1].IsLiteral());
        REQUIRE(deserialized.inputs["SLOT0"][1].GetLiteral().GetDecimal() == 100.0);
        REQUIRE(deserialized.inputs["SLOT0"][2].IsNodeReference());
        REQUIRE(deserialized.inputs["SLOT0"][2].GetNodeReference().GetNodeId() == "volume");
        REQUIRE(deserialized.inputs["SLOT0"][3].IsLiteral());
        REQUIRE(deserialized.inputs["SLOT0"][3].GetLiteral().GetString() == "marker");
        REQUIRE(deserialized == original);
    }

    SECTION("Round-trip: multiple variadic slots") {
        AlgorithmNode original;
        original.id = "multi_0";
        original.type = "multi_input";
        original.inputs["SLOT0"] = {
            InputValue(NodeReference("a", "result")),
            InputValue(NodeReference("b", "result"))
        };
        original.inputs["SLOT1"] = {
            InputValue(ConstantValue(10.0)),
            InputValue(ConstantValue(20.0)),
            InputValue(ConstantValue(30.0))
        };

        auto json_result = glz::write_json(original);
        REQUIRE(json_result.has_value());
        std::string json = json_result.value();

        AlgorithmNode deserialized;
        auto parse_error = glz::read_json(deserialized, json);
        REQUIRE_FALSE(parse_error);

        REQUIRE(deserialized.inputs.size() == 2);
        REQUIRE(deserialized.inputs["SLOT0"].size() == 2);
        REQUIRE(deserialized.inputs["SLOT1"].size() == 3);
        REQUIRE(deserialized == original);
    }
}
