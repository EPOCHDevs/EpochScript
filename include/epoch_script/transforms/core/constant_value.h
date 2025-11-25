#pragma once

#include <variant>
#include <string>
#include <optional>
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <cmath>
#include <yaml-cpp/yaml.h>
#include <epoch_frame/datetime.h>

// Try including metadata.h directly - if this causes circular dependency,
// we'll need to extract IODataType to a separate header
#include <epoch_script/transforms/core/metadata.h>
#include <google/protobuf/stubs/port.h>

namespace epoch_script::transform {

/**
 * @brief Represents a compile-time constant value in the transform graph
 *
 * Replaces scalar transforms by storing constant values directly in the graph.
 * Constants are embedded in AlgorithmNode.literal_inputs instead of creating
 * transform dependencies.
 *
 * Design:
 * - Variant holds the actual value (double, string, bool, int64_t, or null)
 * - Type-safe accessors throw on type mismatch
 * - Serializable to/from YAML for graph persistence
 * - Can be materialized to DataFrame columns on-demand during execution
 */
class ConstantValue {
public:
    /// Null sentinel type for typed null values
    struct Null {
        epoch_core::IODataType type;

        Null(epoch_core::IODataType t) : type(t) {
            // Prevent creating untyped nulls - null must have a specific type
            if (type == epoch_core::IODataType::Any || type == epoch_core::IODataType::Number) {
                throw std::runtime_error(
                    "Cannot create Null with type 'Any' or 'Number'. "
                    "Use a specific type: Decimal, Integer, String, Boolean, or Timestamp");
            }
        }

        // Explicit copy/move to ensure proper copying
        Null(const Null&) = default;
        Null(Null&&) = default;
        Null& operator=(const Null&) = default;
        Null& operator=(Null&&) = default;

        bool operator==(const Null& other) const {
            return type == other.type;
        }
    };

    using ValueVariant = std::variant<
        double,                    // Decimal constants (3.14, 42.0)
        std::string,               // String constants ("hello", "symbol")
        bool,                      // Boolean constants (true, false)
        epoch_frame::DateTime,     // Timestamp constants (DateTime objects)
        Null                       // Typed null (null_number, null_string, etc.)
    >;

    /// Default constructor - creates null decimal
    ConstantValue() : m_value(Null{epoch_core::IODataType::Decimal}) {}

    /// Type-specific constructors
    template<typename T> requires std::is_arithmetic_v<T>
    explicit ConstantValue(T val) : m_value(static_cast<double>(val)) {}

    explicit ConstantValue(std::string val) : m_value(std::move(val)) {}
    explicit ConstantValue(bool val) : m_value(val) {}
    explicit ConstantValue(epoch_frame::DateTime val) : m_value(std::move(val)) {}
    explicit ConstantValue(Null val) : m_value(val) {}

    // Explicit copy/move to ensure variant is properly copied
    ConstantValue(const ConstantValue&) = default;
    ConstantValue(ConstantValue&&) = default;
    ConstantValue& operator=(const ConstantValue&) = default;
    ConstantValue& operator=(ConstantValue&&) = default;

    /// Create typed null
    static ConstantValue MakeNull(epoch_core::IODataType type) {
        return ConstantValue(Null{type});
    }

    /// Type checking
    [[nodiscard]] bool IsDecimal() const { return std::holds_alternative<double>(m_value); }
    [[nodiscard]] bool IsString() const { return std::holds_alternative<std::string>(m_value); }
    [[nodiscard]] bool IsBoolean() const { return std::holds_alternative<bool>(m_value); }
    [[nodiscard]] bool IsTimestamp() const { return std::holds_alternative<epoch_frame::DateTime>(m_value); }
    [[nodiscard]] bool IsNull() const { return std::holds_alternative<Null>(m_value); }

    /// Get the IODataType of this constant
    [[nodiscard]] epoch_core::IODataType GetType() const {
        if (IsDecimal()) return epoch_core::IODataType::Decimal;
        if (IsString()) return epoch_core::IODataType::String;
        if (IsBoolean()) return epoch_core::IODataType::Boolean;
        if (IsTimestamp()) return epoch_core::IODataType::Timestamp;
        if (IsNull()) return std::get<Null>(m_value).type;
        throw std::runtime_error("Unknown ConstantValue type");
    }

    /// Type-safe accessors (throw on mismatch)
    [[nodiscard]] double GetDecimal() const {
        if (!IsDecimal()) {
            throw std::runtime_error("ConstantValue is not a decimal");
        }
        return std::get<double>(m_value);
    }

    [[nodiscard]] const std::string& GetString() const {
        if (!IsString()) {
            throw std::runtime_error("ConstantValue is not a string");
        }
        return std::get<std::string>(m_value);
    }

    [[nodiscard]] bool GetBoolean() const {
        if (!IsBoolean()) {
            throw std::runtime_error("ConstantValue is not a boolean");
        }
        return std::get<bool>(m_value);
    }


    [[nodiscard]] const epoch_frame::DateTime& GetTimestamp() const {
        if (!IsTimestamp()) {
            throw std::runtime_error("ConstantValue is not a timestamp");
        }
        return std::get<epoch_frame::DateTime>(m_value);
    }

    [[nodiscard]] Null GetNull() const {
        if (!IsNull()) {
            throw std::runtime_error("ConstantValue is not null");
        }
        return std::get<Null>(m_value);
    }



    /// Get the underlying variant (for generic handling)
    [[nodiscard]] const ValueVariant& GetVariant() const { return m_value; }

    /// Generate unique column name from constant value (for DataFrame materialization)
    [[nodiscard]] std::string GetColumnName() const {
        if (IsDecimal()) {
            double val = GetDecimal();
            // Check if it's a whole number (no fractional part)
            if (val == std::trunc(val)) {
                // Integer value - use cleaner format without decimal
                std::string val_str = std::to_string(static_cast<int64_t>(val));
                if (val < 0) {
                    val_str[0] = 'n'; // Replace minus with 'n' for negative
                }
                return "num_" + val_str;
            }
            // Has fractional part - format with minimal digits
            std::ostringstream oss;
            oss << std::fixed << std::setprecision(6) << val;
            std::string val_str = oss.str();
            // Remove trailing zeros after decimal point
            size_t dot_pos = val_str.find('.');
            if (dot_pos != std::string::npos) {
                size_t last_nonzero = val_str.find_last_not_of('0');
                if (last_nonzero > dot_pos) {
                    val_str = val_str.substr(0, last_nonzero + 1);
                } else {
                    // All zeros after decimal, make it clean
                    val_str = val_str.substr(0, dot_pos);
                    return "num_" + val_str;
                }
            }
            std::replace(val_str.begin(), val_str.end(), '.', '_');
            std::replace(val_str.begin(), val_str.end(), '-', 'n'); // negative sign
            return "dec_" + val_str;
        } else if (IsBoolean()) {
            return GetBoolean() ? "bool_true" : "bool_false";
        } else if (IsString()) {
            const std::string& str_val = GetString();
            // For short strings, sanitize and use directly
            if (str_val.length() <= 30) {
                std::string sanitized = str_val;
                // Replace non-alphanumeric with underscore
                for (char& c : sanitized) {
                    if (!std::isalnum(c)) c = '_';
                }
                return "text_" + sanitized;
            }
            // For long strings, use hash to avoid collisions
            std::hash<std::string> hasher;
            return "text_hash_" + std::to_string(hasher(str_val));
        } else if (IsTimestamp()) {
            return "time_" + GetTimestamp().repr();
        } else if (IsNull()) {
            return "null_" + epoch_core::IODataTypeWrapper::ToString(GetNull().type);
        }
        throw std::runtime_error("Unknown ConstantValue type in GetColumnName()");
    }

    /// Equality comparison
    bool operator==(const ConstantValue& other) const {
        return m_value == other.m_value;
    }

    friend std::ostream& operator<<(std::ostream& os, const ConstantValue& cv) {
        os << cv.ToString();
        return os;
    }

    /// Serialization to YAML
    [[nodiscard]] YAML::Node ToYAML() const {
        YAML::Node node;

        if (IsDecimal()) {
            node["type"] = "decimal";
            node["value"] = GetDecimal();
        } else if (IsString()) {
            node["type"] = "string";
            node["value"] = GetString();
        } else if (IsBoolean()) {
            node["type"] = "boolean";
            node["value"] = GetBoolean();
        } else if (IsTimestamp()) {
            node["type"] = "timestamp";
            // Serialize DateTime as ISO8601 string
            node["value"] = GetTimestamp().repr();
        } else if (IsNull()) {
            node["type"] = "null";
            node["null_type"] = epoch_core::IODataTypeWrapper::ToString(GetNull().type);
        }

        return node;
    }

    /// Deserialization from YAML
    static ConstantValue FromYAML(const YAML::Node& node) {
        if (!node["type"]) {
            throw std::runtime_error("ConstantValue YAML missing 'type' field");
        }

        std::string type = node["type"].as<std::string>();

        if (type == "decimal") {
            return ConstantValue(node["value"].as<double>());
        } else if (type == "string") {
            return ConstantValue(node["value"].as<std::string>());
        } else if (type == "boolean") {
            return ConstantValue(node["value"].as<bool>());
        } else if (type == "timestamp") {
            // Deserialize DateTime from ISO8601 string (default to UTC timezone)
            std::string timestamp_str = node["value"].as<std::string>();
            return ConstantValue(epoch_frame::DateTime::from_str(timestamp_str, "UTC", "%Y-%m-%dT%H:%M:%SZ"));
        } else if (type == "null") {
            std::string null_type = node["null_type"].as<std::string>();
            return ConstantValue::MakeNull(
                epoch_core::IODataTypeWrapper::FromString(null_type)
            );
        } else {
            throw std::runtime_error("Unknown ConstantValue type in YAML: " + type);
        }
    }

    /// Convert to string for debugging/logging
    [[nodiscard]] std::string ToString() const {
        if (IsDecimal()) {
            return std::to_string(GetDecimal());
        } else if (IsString()) {
            return "\"" + GetString() + "\"";
        } else if (IsBoolean()) {
            return GetBoolean() ? "true" : "false";
        } else if (IsTimestamp()) {
            return GetTimestamp().repr();
        } else if (IsNull()) {
            return "null(" + std::string(epoch_core::IODataTypeWrapper::ToString(GetNull().type)) + ")";
        }
        return "<unknown>";
    }

private:
    ValueVariant m_value;
};

} // namespace epoch_script::transform

/// Glaze JSON conversion for ConstantValue
namespace glz {
template <>
struct meta<epoch_script::transform::ConstantValue::Null> {
    using T = epoch_script::transform::ConstantValue::Null;
    static constexpr auto value = object("type", &T::type);
};

template <>
struct meta<epoch_script::transform::ConstantValue> {
    static constexpr auto read = [](epoch_script::transform::ConstantValue &x, const glz::generic &in) {
        // Handle each variant type that Glaze can produce
        if (in.is_number()) {
            // Could be double or int64_t - check if it's an integer value
            double val = in.get<double>();
            x = epoch_script::transform::ConstantValue(val);
        } else if (in.is_boolean()) {
            x = epoch_script::transform::ConstantValue(in.get<bool>());
        } else if (in.is_string()) {
            x = epoch_script::transform::ConstantValue(in.get<std::string>());
        } else if (in.is_object()) {
            // Check if it's a Null type
            if (in.contains("type")) {
                auto type_val = in["type"];
                if (type_val.is_string()) {
                    std::string type_str = type_val.get<std::string>();
                    auto io_type = epoch_core::IODataTypeWrapper::FromString(type_str);
                    x = epoch_script::transform::ConstantValue::MakeNull(io_type);
                    return;
                }
            }
            // Check if it's a DateTime (has date/time fields)
            // For now, serialize DateTime as string in repr() format
            throw std::runtime_error("Unsupported ConstantValue object type in JSON");
        } else {
            throw std::runtime_error("Unsupported ConstantValue type in JSON");
        }
    };

    static constexpr auto write = [](const epoch_script::transform::ConstantValue &x) -> auto {
        return x.GetVariant();
    };

    static constexpr auto value = glz::custom<read, write>;
};
} // namespace glz

/// YAML conversion operators for ConstantValue
namespace YAML {
template<>
struct convert<epoch_script::transform::ConstantValue> {
    static Node encode(const epoch_script::transform::ConstantValue& rhs) {
        return rhs.ToYAML();
    }

    static bool decode(const Node& node, epoch_script::transform::ConstantValue& rhs) {
        try {
            rhs = epoch_script::transform::ConstantValue::FromYAML(node);
            return true;
        } catch (...) {
            return false;
        }
    }
};
} // namespace YAML
