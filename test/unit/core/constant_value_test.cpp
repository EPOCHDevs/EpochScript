//
// Unit tests for ConstantValue class
//

#include <catch2/catch_test_macros.hpp>
#include <epoch_script/transforms/core/constant_value.h>
#include <epoch_frame/datetime.h>
#include <glaze/glaze.hpp>
#include <yaml-cpp/yaml.h>

using namespace epoch_script::transform;
using namespace epoch_frame;
using namespace epoch_core;

TEST_CASE("ConstantValue - Construction and Type Checking", "[constant_value]") {
    SECTION("Construct decimal constant") {
        ConstantValue val(42.5);
        REQUIRE(val.IsDecimal());
        REQUIRE(val.GetDecimal() == 42.5);
        REQUIRE(val.GetType() == IODataType::Decimal);
        REQUIRE_FALSE(val.IsString());
        REQUIRE_FALSE(val.IsBoolean());
        REQUIRE_FALSE(val.IsTimestamp());
        REQUIRE_FALSE(val.IsNull());
    }

    SECTION("Construct integer constant") {
        ConstantValue val(static_cast<int64_t>(42));
        REQUIRE(val.IsDecimal());
        REQUIRE(val.GetDecimal() == 42);
        REQUIRE(val.GetType() == IODataType::Decimal);
        REQUIRE(val.IsDecimal());
        REQUIRE_FALSE(val.IsString());
        REQUIRE_FALSE(val.IsBoolean());
        REQUIRE_FALSE(val.IsTimestamp());
        REQUIRE_FALSE(val.IsNull());
    }

    SECTION("Construct string constant") {
        ConstantValue val(std::string("hello"));
        REQUIRE(val.IsString());
        REQUIRE(val.GetString() == "hello");
        REQUIRE(val.GetType() == IODataType::String);
        REQUIRE_FALSE(val.IsDecimal());
        REQUIRE_FALSE(val.IsDecimal());
        REQUIRE_FALSE(val.IsBoolean());
        REQUIRE_FALSE(val.IsTimestamp());
        REQUIRE_FALSE(val.IsNull());
    }

    SECTION("Construct boolean constant - true") {
        ConstantValue val(true);
        REQUIRE(val.IsBoolean());
        REQUIRE(val.GetBoolean() == true);
        REQUIRE(val.GetType() == IODataType::Boolean);
        REQUIRE_FALSE(val.IsDecimal());
        REQUIRE_FALSE(val.IsDecimal());
        REQUIRE_FALSE(val.IsString());
        REQUIRE_FALSE(val.IsTimestamp());
        REQUIRE_FALSE(val.IsNull());
    }

    SECTION("Construct boolean constant - false") {
        ConstantValue val(false);
        REQUIRE(val.IsBoolean());
        REQUIRE(val.GetBoolean() == false);
        REQUIRE(val.GetType() == IODataType::Boolean);
    }

    SECTION("Construct timestamp constant") {
        auto dt = DateTime::from_str("2024-01-01 10:30:00", "UTC");
        ConstantValue val(dt);
        REQUIRE(val.IsTimestamp());
        REQUIRE(val.GetTimestamp() == dt);
        REQUIRE(val.GetType() == IODataType::Timestamp);
        REQUIRE_FALSE(val.IsDecimal());
        REQUIRE_FALSE(val.IsDecimal());
        REQUIRE_FALSE(val.IsString());
        REQUIRE_FALSE(val.IsBoolean());
        REQUIRE_FALSE(val.IsNull());
    }

    SECTION("Construct typed null - Decimal") {
        ConstantValue val = ConstantValue::MakeNull(IODataType::Decimal);
        REQUIRE(val.IsNull());
        REQUIRE(val.GetNull().type == IODataType::Decimal);
        REQUIRE(val.GetType() == IODataType::Decimal);
        REQUIRE_FALSE(val.IsDecimal());
        REQUIRE_FALSE(val.IsDecimal());
        REQUIRE_FALSE(val.IsString());
        REQUIRE_FALSE(val.IsBoolean());
        REQUIRE_FALSE(val.IsTimestamp());
    }

    SECTION("Construct typed null - String") {
        ConstantValue val = ConstantValue::MakeNull(IODataType::String);
        REQUIRE(val.IsNull());
        REQUIRE(val.GetNull().type == IODataType::String);
        REQUIRE(val.GetType() == IODataType::String);
    }

    SECTION("Construct typed null - Boolean") {
        ConstantValue val = ConstantValue::MakeNull(IODataType::Boolean);
        REQUIRE(val.IsNull());
        REQUIRE(val.GetNull().type == IODataType::Boolean);
    }

    SECTION("Construct typed null - Integer") {
        ConstantValue val = ConstantValue::MakeNull(IODataType::Integer);
        REQUIRE(val.IsNull());
        REQUIRE(val.GetNull().type == IODataType::Integer);
    }

    SECTION("Construct typed null - Timestamp") {
        ConstantValue val = ConstantValue::MakeNull(IODataType::Timestamp);
        REQUIRE(val.IsNull());
        REQUIRE(val.GetNull().type == IODataType::Timestamp);
    }

    SECTION("Default constructor creates null decimal") {
        ConstantValue val;
        REQUIRE(val.IsNull());
        REQUIRE(val.GetNull().type == IODataType::Decimal);
    }
}

TEST_CASE("ConstantValue - Type Safety", "[constant_value][safety]") {
    SECTION("Throws on decimal type mismatch") {
        ConstantValue val(42.5);
        REQUIRE_THROWS_AS(val.GetString(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetBoolean(), std::runtime_error);
        REQUIRE_NOTHROW(val.GetDecimal());
        REQUIRE_THROWS_AS(val.GetTimestamp(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetNull(), std::runtime_error);
    }

    SECTION("Throws on string type mismatch") {
        ConstantValue val(std::string("test"));
        REQUIRE_THROWS_AS(val.GetDecimal(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetBoolean(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetDecimal(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetTimestamp(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetNull(), std::runtime_error);
    }

    SECTION("Throws on boolean type mismatch") {
        ConstantValue val(true);
        REQUIRE_THROWS_AS(val.GetDecimal(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetString(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetDecimal(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetTimestamp(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetNull(), std::runtime_error);
    }

    SECTION("Throws on integer type mismatch") {
        ConstantValue val(static_cast<int64_t>(42));
        REQUIRE_NOTHROW(val.GetDecimal());
        REQUIRE_THROWS_AS(val.GetString(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetBoolean(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetTimestamp(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetNull(), std::runtime_error);
    }

    SECTION("Throws on null type mismatch") {
        ConstantValue val = ConstantValue::MakeNull(IODataType::Decimal);
        REQUIRE_THROWS_AS(val.GetDecimal(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetString(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetBoolean(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetDecimal(), std::runtime_error);
        REQUIRE_THROWS_AS(val.GetTimestamp(), std::runtime_error);
        // GetNull() should work
        REQUIRE_NOTHROW(val.GetNull());
    }

    SECTION("Cannot create null with Any type") {
        REQUIRE_THROWS_AS(
            ConstantValue::MakeNull(IODataType::Any),
            std::runtime_error
        );
    }

    SECTION("Cannot create null with Number type") {
        REQUIRE_THROWS_AS(
            ConstantValue::MakeNull(IODataType::Number),
            std::runtime_error
        );
    }
}

TEST_CASE("ConstantValue - YAML Serialization", "[constant_value][yaml]") {
    SECTION("Round-trip decimal value") {
        ConstantValue original(3.14);
        YAML::Node yaml = original.ToYAML();

        REQUIRE(yaml["type"].as<std::string>() == "decimal");
        REQUIRE(yaml["value"].as<double>() == 3.14);

        ConstantValue deserialized = ConstantValue::FromYAML(yaml);
        REQUIRE(deserialized == original);
        REQUIRE(deserialized.IsDecimal());
        REQUIRE(deserialized.GetDecimal() == 3.14);
    }

    SECTION("Round-trip string value") {
        ConstantValue original(std::string("test string"));
        YAML::Node yaml = original.ToYAML();

        REQUIRE(yaml["type"].as<std::string>() == "string");
        REQUIRE(yaml["value"].as<std::string>() == "test string");

        ConstantValue deserialized = ConstantValue::FromYAML(yaml);
        REQUIRE(deserialized == original);
        REQUIRE(deserialized.IsString());
        REQUIRE(deserialized.GetString() == "test string");
    }

    SECTION("Round-trip boolean value - true") {
        ConstantValue original(true);
        YAML::Node yaml = original.ToYAML();

        REQUIRE(yaml["type"].as<std::string>() == "boolean");
        REQUIRE(yaml["value"].as<bool>() == true);

        ConstantValue deserialized = ConstantValue::FromYAML(yaml);
        REQUIRE(deserialized == original);
        REQUIRE(deserialized.IsBoolean());
        REQUIRE(deserialized.GetBoolean() == true);
    }

    SECTION("Round-trip boolean value - false") {
        ConstantValue original(false);
        YAML::Node yaml = original.ToYAML();

        REQUIRE(yaml["value"].as<bool>() == false);

        ConstantValue deserialized = ConstantValue::FromYAML(yaml);
        REQUIRE(deserialized.GetBoolean() == false);
    }

    SECTION("Round-trip integer value") {
        ConstantValue original(static_cast<int64_t>(42));
        YAML::Node yaml = original.ToYAML();

        REQUIRE(yaml["type"].as<std::string>() == "decimal");
        REQUIRE(yaml["value"].as<int64_t>() == 42);

        ConstantValue deserialized = ConstantValue::FromYAML(yaml);
        REQUIRE(deserialized == original);
        REQUIRE(deserialized.IsDecimal());
        REQUIRE(deserialized.GetDecimal() == 42);
    }

    SECTION("Round-trip timestamp value") {
        auto dt = DateTime::from_str("2024-01-01 10:30:00", "UTC");
        ConstantValue original(dt);
        YAML::Node yaml = original.ToYAML();

        REQUIRE(yaml["type"].as<std::string>() == "timestamp");
        REQUIRE(yaml["value"].as<std::string>() == dt.repr());

        ConstantValue deserialized = ConstantValue::FromYAML(yaml);
        REQUIRE(deserialized.IsTimestamp());
        REQUIRE(deserialized.GetTimestamp() == dt);
    }

    SECTION("Round-trip null value - Decimal") {
        ConstantValue original = ConstantValue::MakeNull(IODataType::Decimal);
        YAML::Node yaml = original.ToYAML();

        REQUIRE(yaml["type"].as<std::string>() == "null");
        REQUIRE(yaml["null_type"].as<std::string>() == "Decimal");

        ConstantValue deserialized = ConstantValue::FromYAML(yaml);
        REQUIRE(deserialized.IsNull());
        REQUIRE(deserialized.GetNull().type == IODataType::Decimal);
    }

    SECTION("Round-trip null value - String") {
        ConstantValue original = ConstantValue::MakeNull(IODataType::String);
        YAML::Node yaml = original.ToYAML();

        REQUIRE(yaml["null_type"].as<std::string>() == "String");

        ConstantValue deserialized = ConstantValue::FromYAML(yaml);
        REQUIRE(deserialized.IsNull());
        REQUIRE(deserialized.GetNull().type == IODataType::String);
    }

    SECTION("FromYAML rejects missing type field") {
        YAML::Node yaml;
        yaml["value"] = 42.0;

        REQUIRE_THROWS_AS(
            ConstantValue::FromYAML(yaml),
            std::runtime_error
        );
    }

    SECTION("FromYAML rejects unknown type") {
        YAML::Node yaml;
        yaml["type"] = "unknown_type";
        yaml["value"] = 42.0;

        REQUIRE_THROWS_AS(
            ConstantValue::FromYAML(yaml),
            std::runtime_error
        );
    }
}

TEST_CASE("ConstantValue - JSON Serialization (Glaze)", "[constant_value][json]") {
    SECTION("Write and read decimal") {
        ConstantValue original(42.5);
        std::string json = glz::write_json(original).value();

        REQUIRE_FALSE(json.empty());

        ConstantValue deserialized(0.0);
        auto err = glz::read_json(deserialized, json);
        REQUIRE_FALSE(err);
        REQUIRE(deserialized == original);
        REQUIRE(deserialized.GetDecimal() == 42.5);
    }

    SECTION("Write and read integer") {
        ConstantValue original(static_cast<int64_t>(42));
        std::string json = glz::write_json(original).value();

        ConstantValue deserialized(0.0);
        auto err = glz::read_json(deserialized, json);
        REQUIRE_FALSE(err);
        REQUIRE(deserialized.IsDecimal());
        REQUIRE(deserialized.GetDecimal() == 42);
    }

    SECTION("Write and read string") {
        ConstantValue original(std::string("hello"));
        std::string json = glz::write_json(original).value();

        ConstantValue deserialized(0.0);
        auto err = glz::read_json(deserialized, json);
        REQUIRE_FALSE(err);
        REQUIRE(deserialized.IsString());
        REQUIRE(deserialized.GetString() == "hello");
    }

    SECTION("Write and read boolean - true") {
        ConstantValue original(true);
        std::string json = glz::write_json(original).value();

        ConstantValue deserialized(0.0);
        auto err = glz::read_json(deserialized, json);
        REQUIRE_FALSE(err);
        REQUIRE(deserialized.IsBoolean());
        REQUIRE(deserialized.GetBoolean() == true);
    }

    SECTION("Write and read boolean - false") {
        ConstantValue original(false);
        std::string json = glz::write_json(original).value();

        ConstantValue deserialized(0.0);
        auto err = glz::read_json(deserialized, json);
        REQUIRE_FALSE(err);
        REQUIRE(deserialized.IsBoolean());
        REQUIRE(deserialized.GetBoolean() == false);
    }

    SECTION("Write and read null") {
        ConstantValue original = ConstantValue::MakeNull(IODataType::String);
        std::string json = glz::write_json(original).value();

        ConstantValue deserialized(0.0);
        auto err = glz::read_json(deserialized, json);
        REQUIRE_FALSE(err);
        REQUIRE(deserialized.IsNull());
        REQUIRE(deserialized.GetNull().type == IODataType::String);
    }
}

TEST_CASE("ConstantValue - ToString", "[constant_value]") {
    SECTION("Decimal toString") {
        ConstantValue val(42.5);
        std::string str = val.ToString();
        REQUIRE(str.find("42.5") != std::string::npos);
    }

    SECTION("String toString") {
        ConstantValue val(std::string("test"));
        std::string str = val.ToString();
        REQUIRE(str == "\"test\"");
    }

    SECTION("Boolean toString - true") {
        ConstantValue val(true);
        REQUIRE(val.ToString() == "true");
    }

    SECTION("Boolean toString - false") {
        ConstantValue val(false);
        REQUIRE(val.ToString() == "false");
    }

    SECTION("Integer toString") {
        ConstantValue val(static_cast<int64_t>(42));
        std::string str = val.ToString();
        REQUIRE(str.find("42") != std::string::npos);
    }

    SECTION("Timestamp toString") {
        auto dt = DateTime::from_str("2024-01-01 10:30:00", "UTC");
        ConstantValue val(dt);
        std::string str = val.ToString();
        REQUIRE(str == dt.repr());
    }

    SECTION("Null toString") {
        ConstantValue val = ConstantValue::MakeNull(IODataType::Decimal);
        REQUIRE(val.ToString() == "null(Decimal)");
    }
}

TEST_CASE("ConstantValue - Equality Comparison", "[constant_value]") {
    SECTION("Equal decimal values") {
        ConstantValue val1(42.5);
        ConstantValue val2(42.5);
        REQUIRE(val1 == val2);
    }

    SECTION("Different decimal values") {
        ConstantValue val1(42.5);
        ConstantValue val2(43.5);
        REQUIRE_FALSE(val1 == val2);
    }

    SECTION("Equal string values") {
        ConstantValue val1(std::string("test"));
        ConstantValue val2(std::string("test"));
        REQUIRE(val1 == val2);
    }

    SECTION("Different string values") {
        ConstantValue val1(std::string("test1"));
        ConstantValue val2(std::string("test2"));
        REQUIRE_FALSE(val1 == val2);
    }

    SECTION("Equal boolean values") {
        ConstantValue val1(true);
        ConstantValue val2(true);
        REQUIRE(val1 == val2);
    }

    SECTION("Different boolean values") {
        ConstantValue val1(true);
        ConstantValue val2(false);
        REQUIRE_FALSE(val1 == val2);
    }

    SECTION("Equal null values with same type") {
        ConstantValue val1 = ConstantValue::MakeNull(IODataType::Decimal);
        ConstantValue val2 = ConstantValue::MakeNull(IODataType::Decimal);
        REQUIRE(val1 == val2);
    }

    SECTION("Different null types") {
        ConstantValue val1 = ConstantValue::MakeNull(IODataType::Decimal);
        ConstantValue val2 = ConstantValue::MakeNull(IODataType::String);
        REQUIRE_FALSE(val1 == val2);
    }

    SECTION("Different types are not equal") {
        ConstantValue val1(42.0);
        ConstantValue val2(std::string("42"));
        REQUIRE_FALSE(val1 == val2);
    }
}

TEST_CASE("ConstantValue - Edge Cases", "[constant_value][edge_cases]") {
    SECTION("Integer vs decimal IS same") {
        ConstantValue int_val(static_cast<int64_t>(42));
        ConstantValue dec_val(42.0);

        REQUIRE(int_val.IsDecimal());
        REQUIRE(dec_val.IsDecimal());
        REQUIRE(int_val == dec_val);
    }

    SECTION("Empty string") {
        ConstantValue val(std::string(""));
        REQUIRE(val.IsString());
        REQUIRE(val.GetString() == "");
        REQUIRE(val.ToString() == "\"\"");
    }

    SECTION("Negative numbers") {
        ConstantValue dec(-42.5);
        ConstantValue int_val(static_cast<int64_t>(-42));

        REQUIRE(dec.GetDecimal() == -42.5);
        REQUIRE(int_val.GetDecimal() == -42);
    }

    SECTION("Very large numbers") {
        ConstantValue val(1e100);
        REQUIRE(val.IsDecimal());
        REQUIRE(val.GetDecimal() == 1e100);
    }

    SECTION("Special string characters") {
        ConstantValue val(std::string("test\nwith\nnewlines"));
        REQUIRE(val.GetString() == "test\nwith\nnewlines");
    }
}
