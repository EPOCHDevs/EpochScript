//
// Created by dewe on 9/12/24.
//

#include <epoch_script/core/metadata_options.h>
#include "doc_deserialization_helper.h"
#include <epoch_core/ranges_to.h>
#include <unordered_set>
#include <sstream>

namespace epoch_script {

// Helper function to convert YAML node to JSON string
static std::string YamlNodeToJsonString(const YAML::Node& node) {
  std::ostringstream json;

  if (node.IsNull()) {
    json << "null";
  } else if (node.IsScalar()) {
    std::string val = node.as<std::string>();
    // Check if it's a boolean or null literal
    if (val == "true" || val == "false" || val == "null") {
      json << val;
    } else {
      // Try to parse as number
      char* endptr = nullptr;
      std::strtod(val.c_str(), &endptr);
      if (endptr != val.c_str() && *endptr == '\0') {
        // Valid number
        json << val;
      } else {
        // It's a string (or enum), quote it
        json << "\"" << val << "\"";
      }
    }
  } else if (node.IsSequence()) {
    json << "[";
    for (size_t i = 0; i < node.size(); ++i) {
      if (i > 0) json << ",";
      json << YamlNodeToJsonString(node[i]);
    }
    json << "]";
  } else if (node.IsMap()) {
    json << "{";
    bool first = true;
    for (auto it = node.begin(); it != node.end(); ++it) {
      if (!first) json << ",";
      first = false;
      json << "\"" << it->first.as<std::string>() << "\":";
      json << YamlNodeToJsonString(it->second);
    }
    json << "}";
  }

  return json.str();
}

// Implementation of GetIconEnumeration moved from header to avoid static initialization issues
const std::vector<std::string_view>& EventMarkerSchema::glaze_json_schema::GetIconEnumeration() {
  static const std::vector<std::string_view> views = []() {
    // Build icon vector on first access (not during static init)
    static const std::vector<std::string> icons = []() {
      std::vector<std::string> result;
      for (const auto& iconStr : epoch_core::IconWrapper::GetAllAsStrings()) {
        if (iconStr != "Null") {  // Exclude the Null sentinel value
          result.emplace_back(iconStr);
        }
      }
      return result;
    }();

    // Create views to the static strings
    std::vector<std::string_view> icon_views;
    icon_views.reserve(icons.size());
    for (const auto& icon : icons) {
      icon_views.emplace_back(icon);
    }
    return icon_views;
  }();
  return views;
}

using SequenceItem = std::variant<double, std::string>;
using Sequence = std::vector<SequenceItem>;
void MetaDataOptionDefinition::AssertType(
    epoch_core::MetaDataOptionType const &argType,
    std::unordered_set<std::string> const &selections) const {
  switch (argType) {
  case epoch_core::MetaDataOptionType::Integer:
  case epoch_core::MetaDataOptionType::Decimal:
    AssertType<double>();
    break;
  case epoch_core::MetaDataOptionType::Boolean:
    AssertType<bool>();
    break;
  case epoch_core::MetaDataOptionType::Select: {
    auto option = GetValueByType<std::string>();
    AssertFromStream(selections.contains(option),
                     "Invalid select member: "
                         << option << ", Expected one of "
                         << epoch_core::toString(selections));
    break;
  }
  case epoch_core::MetaDataOptionType::Time: {
    // Support both epoch_frame::Time and string
    if (std::holds_alternative<epoch_frame::Time>(m_optionsVariant)) {
      // If it's already a Time object, it's valid
      break;
    }
    AssertType<std::string>();
    auto const &val = GetValueByType<std::string>();
    auto count_colon =
        static_cast<int>(std::count(val.begin(), val.end(), ':'));
    AssertFromStream(count_colon == 1 || count_colon == 2,
                     "Time must be HH:MM or HH:MM:SS, got: " << val);
    auto parse_component = [](std::string const &s, size_t &pos) -> int {
      size_t next = s.find(':', pos);
      std::string token = s.substr(
          pos, next == std::string::npos ? std::string::npos : next - pos);
      AssertFromStream(!token.empty() && std::ranges::all_of(token, ::isdigit),
                       "Invalid time component: " << token);
      int v = std::stoi(token);
      pos = (next == std::string::npos) ? std::string::npos : next + 1;
      return v;
    };
    size_t pos = 0;
    int hh = parse_component(val, pos);
    int mm = parse_component(val, pos);
    int ss = 0;
    if (pos != std::string::npos) {
      ss = parse_component(val, pos);
    }
    AssertFromStream(0 <= hh && hh < 24, "Hour out of range: " << hh);
    AssertFromStream(0 <= mm && mm < 60, "Minute out of range: " << mm);
    AssertFromStream(0 <= ss && ss < 60, "Second out of range: " << ss);
    break;
  }
  case epoch_core::MetaDataOptionType::NumericList: {
    AssertType<Sequence>();
    // Additional validation: ensure all items are numeric
    const auto &seq = GetValueByType<Sequence>();
    for (const auto &item : seq) {
      if (!std::holds_alternative<double>(item)) {
        throw std::runtime_error("NumericList contains non-numeric values");
      }
    }
    break;
  }
  case epoch_core::MetaDataOptionType::StringList: {
    AssertType<Sequence>();
    // Additional validation: ensure all items are strings
    const auto &seq = GetValueByType<Sequence>();
    for (const auto &item : seq) {
      if (!std::holds_alternative<std::string>(item)) {
        throw std::runtime_error("StringList contains non-string values");
      }
    }
    break;
  }
  case epoch_core::MetaDataOptionType::String:
    AssertType<std::string>();
    break;
  case epoch_core::MetaDataOptionType::EventMarkerSchema:
    AssertType<EventMarkerSchema>();
    break;
  case epoch_core::MetaDataOptionType::SqlStatement:
    AssertType<SqlStatement>();
    break;
  case epoch_core::MetaDataOptionType::TableReportSchema:
    AssertType<TableReportSchema>();
    break;
  case epoch_core::MetaDataOptionType::Null:
    throw std::runtime_error("Null value not allowed.");
  }
}

bool MetaDataOptionDefinition::IsType(
    epoch_core::MetaDataOptionType const &argType) const {
  switch (argType) {
  case epoch_core::MetaDataOptionType::Integer:
  case epoch_core::MetaDataOptionType::Decimal:
    return std::holds_alternative<double>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::Boolean:
    return std::holds_alternative<bool>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::Select:
    return std::holds_alternative<std::string>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::Time:
    return std::holds_alternative<std::string>(m_optionsVariant) ||
           std::holds_alternative<epoch_frame::Time>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::NumericList:
  case epoch_core::MetaDataOptionType::StringList:
    return std::holds_alternative<Sequence>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::String:
    return std::holds_alternative<std::string>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::EventMarkerSchema:
    return std::holds_alternative<EventMarkerSchema>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::SqlStatement:
    return std::holds_alternative<SqlStatement>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::TableReportSchema:
    return std::holds_alternative<TableReportSchema>(m_optionsVariant);
  case epoch_core::MetaDataOptionType::Null:
    return false;
  }
  std::unreachable();
}

double MetaDataOptionDefinition::GetNumericValue() const {
  if (std::holds_alternative<double>(m_optionsVariant)) {
    return GetDecimal();
  }
  if (std::holds_alternative<bool>(m_optionsVariant)) {
    return static_cast<double>(GetBoolean());
  }
  std::stringstream ss;
  ss << "Invalid Numeric MetaDataOptionType Type\n";
  ss << "Got: " << typeid(m_optionsVariant).name() << "\n";
  throw std::runtime_error(ss.str());
}

epoch_frame::Time TimeFromString(std::string const &val) {
  auto parse_component = [](std::string const &s, size_t &pos) -> int {
    size_t next = s.find(':', pos);
    std::string token = s.substr(
        pos, next == std::string::npos ? std::string::npos : next - pos);
    AssertFromStream(!token.empty() && std::ranges::all_of(token, ::isdigit),
                     "Invalid time component: " << token);
    int v = std::stoi(token);
    pos = (next == std::string::npos) ? std::string::npos : next + 1;
    return v;
  };

  size_t pos = 0;
  int hh = parse_component(val, pos);
  int mm = parse_component(val, pos);
  int ss = 0;
  if (pos != std::string::npos) {
    ss = parse_component(val, pos);
  }
  AssertFromStream(0 <= hh && hh < 24, "Hour out of range: " << hh);
  AssertFromStream(0 <= mm && mm < 60, "Minute out of range: " << mm);
  AssertFromStream(0 <= ss && ss < 60, "Second out of range: " << ss);
  return epoch_frame::Time{chrono_hour(hh), chrono_minute(mm),
                           chrono_second(ss), chrono_millisecond(0), "UTC"};
}

epoch_frame::Time MetaDataOptionDefinition::GetTime() const {
  // Support both epoch_frame::Time directly and string conversion for backward compatibility
  if (std::holds_alternative<epoch_frame::Time>(m_optionsVariant)) {
    return GetValueByType<epoch_frame::Time>();
  }

  AssertFromStream(std::holds_alternative<std::string>(m_optionsVariant),
                   "GetTime expects either an epoch_frame::Time or a string Time option");
  const auto &val = GetValueByType<std::string>();
  return TimeFromString(val);
}

size_t MetaDataOptionDefinition::GetHash() const {
  return std::visit(
      [this](auto &&arg) {
        using K = std::decay_t<decltype(arg)>;
        if constexpr (std::same_as<K, MetaDataArgRef>) {
          return std::hash<std::string>{}(GetRef());
        } else if constexpr (std::same_as<K, Sequence>) {
          size_t seed = 0;
          for (const auto &item : arg) {
            size_t h = std::visit(
                [](const auto &v) {
                  return std::hash<std::decay_t<decltype(v)>>{}(v);
                },
                item);
            seed ^= h + 0x9e3779b97f4a7c15ULL + (seed << 6) + (seed >> 2);
          }
          return seed;
        } else if constexpr (std::same_as<K, epoch_frame::Time>) {
          // Hash Time by hashing its string representation
          size_t seed = 0;
          seed ^= std::hash<int>{}(arg.hour.count()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          seed ^= std::hash<int>{}(arg.minute.count()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          seed ^= std::hash<int>{}(arg.second.count()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          seed ^= std::hash<int>{}(arg.microsecond.count()) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          seed ^= std::hash<std::string>{}(arg.tz) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          return seed;
        } else if constexpr (std::same_as<K, EventMarkerSchema>) {
          // Hash EventMarkerSchema by hashing its fields
          size_t seed = 0;
          seed ^= std::hash<std::string>{}(arg.title) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          seed ^= std::hash<std::string>{}(arg.select_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          for (const auto &schema : arg.schemas) {
            seed ^= std::hash<std::string>{}(schema.column_id) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          }
          return seed;
        } else if constexpr (std::same_as<K, SqlStatement>) {
          // Hash SqlStatement by hashing its SQL string
          return std::hash<std::string>{}(arg.GetSql());
        } else if constexpr (std::same_as<K, TableReportSchema>) {
          // Hash TableReportSchema by hashing its fields
          size_t seed = 0;
          seed ^= std::hash<std::string>{}(arg.title) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          seed ^= std::hash<std::string>{}(arg.select_key) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          for (const auto &col : arg.columns) {
            seed ^= std::hash<std::string>{}(col.column_id) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
            seed ^= std::hash<std::string>{}(col.title) + 0x9e3779b9 + (seed << 6) + (seed >> 2);
          }
          return seed;
        } else {
          return std::hash<K>{}(arg);
        }
      },
      m_optionsVariant);
}

std::string MetaDataOptionDefinition::ToString() const {
  return std::visit(
      [this](auto &&arg) {
        using K = std::decay_t<decltype(arg)>;
        if constexpr (std::same_as<K, MetaDataArgRef>) {
          return std::string("$ref:") + GetRef();
        } else if constexpr (std::same_as<K, std::string>) {
          return arg;
        } else if constexpr (std::same_as<K, Sequence>) {
          std::string out = "[";
          for (size_t i = 0; i < arg.size(); ++i) {
            std::visit(
                [&out](const auto &v) {
                  if constexpr (std::same_as<std::decay_t<decltype(v)>,
                                             double>) {
                    out += std::to_string(v);
                  } else {
                    out += v;
                  }
                },
                arg[i]);
            if (i + 1 < arg.size())
              out += ",";
          }
          out += "]";
          return out;
        } else if constexpr (std::same_as<K, bool>) {
          return arg ? std::string("true") : std::string("false");
        } else if constexpr (std::same_as<K, epoch_frame::Time>) {
          // Use the built-in repr() method for simple string representation
          return arg.repr();
        } else if constexpr (std::same_as<K, EventMarkerSchema>) {
          // Use glaze to pretty print the full EventMarkerSchema structure
          return glz::write_json(arg).value_or("{}");
        } else if constexpr (std::same_as<K, SqlStatement>) {
          // Return the SQL string
          return arg.GetSql();
        } else if constexpr (std::same_as<K, TableReportSchema>) {
          // Use glaze to pretty print the full TableReportSchema structure
          return glz::write_json(arg).value_or("{}");
        } else {
          return std::to_string(arg);
        }
      },
      m_optionsVariant);
}

MetaDataOptionDefinition
CreateMetaDataArgDefinition(YAML::Node const &node, MetaDataOption const &arg) {
  if (arg.type == epoch_core::MetaDataOptionType::NumericList ||
      arg.type == epoch_core::MetaDataOptionType::StringList) {
    AssertFromStream(node.IsSequence() || node.IsScalar(),
                     "invalid transform option type: "
                         << node
                         << ", expected a sequence or bracketed string for "
                         << arg.id << ".");
  } else {
    // Skip processing if node is null or undefined
    if (node.IsNull() || !node.IsDefined()) {
      // Return default value if available
      if (arg.defaultValue) {
        return *arg.defaultValue;
      }
      // Otherwise, return an empty value of the appropriate type
      switch (arg.type) {
        case epoch_core::MetaDataOptionType::String:
          return MetaDataOptionDefinition{std::string{}};
        case epoch_core::MetaDataOptionType::Integer:
        case epoch_core::MetaDataOptionType::Decimal:
          return MetaDataOptionDefinition{double{0.0}};
        case epoch_core::MetaDataOptionType::Boolean:
          return MetaDataOptionDefinition{false};
        default:
          return MetaDataOptionDefinition{};
      }
    }
    // EventMarkerSchema, SqlStatement, and TableReportSchema can be Maps/Objects, others must be Scalars
    if (arg.type != epoch_core::MetaDataOptionType::EventMarkerSchema &&
        arg.type != epoch_core::MetaDataOptionType::SqlStatement &&
        arg.type != epoch_core::MetaDataOptionType::TableReportSchema) {
      AssertFromStream(node.IsScalar(), "invalid transform option type: "
                                            << node << ", expected a scalar for "
                                            << arg.id << ".");
    }
  }
  switch (arg.type) {
  case epoch_core::MetaDataOptionType::Integer:
  case epoch_core::MetaDataOptionType::Decimal:
    return MetaDataOptionDefinition{node.as<double>()};
  case epoch_core::MetaDataOptionType::Boolean: {
    return MetaDataOptionDefinition{node.as<bool>()};
  }
  case epoch_core::MetaDataOptionType::Select: {
    return MetaDataOptionDefinition{node.as<std::string>()};
  }
  case epoch_core::MetaDataOptionType::Time: {
    return MetaDataOptionDefinition{node.as<std::string>()};
  }
  case epoch_core::MetaDataOptionType::NumericList: {
    if (node.IsSequence()) {
      return MetaDataOptionDefinition{node.as<std::vector<double>>()};
    }
    return MetaDataOptionDefinition{node.as<std::string>()};
  }
  case epoch_core::MetaDataOptionType::StringList: {
    if (node.IsSequence()) {
      return MetaDataOptionDefinition{node.as<std::vector<std::string>>()};
    }
    return MetaDataOptionDefinition{node.as<std::string>()};
  }
  case epoch_core::MetaDataOptionType::String:
    return MetaDataOptionDefinition{node.as<std::string>()};
  case epoch_core::MetaDataOptionType::EventMarkerSchema: {
    // EventMarkerSchema only comes from EpochScript DSL as a YAML Map - convert to JSON
    // Config helpers now use TransformDefinitionData and bypass YAML entirely
    if (node.IsMap()) {
      std::string jsonStr = YamlNodeToJsonString(node);
      return MetaDataOptionDefinition{jsonStr};
    } else {
      throw std::runtime_error("EventMarkerSchema must be a Map/Object, not a scalar");
    }
  }
  case epoch_core::MetaDataOptionType::SqlStatement: {
    // SqlStatement can be a scalar string or a map with "sql" key
    if (node.IsScalar()) {
      return MetaDataOptionDefinition{MetaDataOptionDefinition::T{SqlStatement{node.as<std::string>()}}};
    } else if (node.IsMap() && node["sql"]) {
      return MetaDataOptionDefinition{MetaDataOptionDefinition::T{SqlStatement{node["sql"].as<std::string>()}}};
    } else {
      throw std::runtime_error("SqlStatement must be a scalar string or a map with 'sql' key");
    }
  }
  case epoch_core::MetaDataOptionType::TableReportSchema: {
    // TableReportSchema only comes from EpochScript DSL as a YAML Map - convert to JSON
    // Config helpers now use TransformDefinitionData and bypass YAML entirely
    if (node.IsMap()) {
      std::string jsonStr = YamlNodeToJsonString(node);
      return MetaDataOptionDefinition{jsonStr};
    } else {
      throw std::runtime_error("TableReportSchema must be a Map/Object, not a scalar");
    }
  }
  case epoch_core::MetaDataOptionType::Null:
    break;
  }
  throw std::runtime_error("Invalid MetaDataOptionType Type");
}

void MetaDataOption::decode(const YAML::Node &element) {
  static std::unordered_map<std::string, MetaDataOption> PLACEHOLDER_MAP{
      {"PERIOD",
       {.id = "period",
        .name = "Period",
        .type = epoch_core::MetaDataOptionType::Integer,
        .min = 1, // Period must be at least 1
        .max = 1000,
        .desc = "Number of bars to look back for calculation",
        .tuningGuidance = "Shorter periods (5-20) are more responsive but noisier. Longer periods (50-200) are smoother but lag more. Common values: 14 (swing trading), 20 (daily), 50/200 (long-term trends)"}}};

  if (element.IsScalar()) {
    *this = epoch_core::lookup(PLACEHOLDER_MAP, element.as<std::string>());
    return;
  }

  id = element["id"].as<std::string>();
  name = element["name"].as<std::string>();
  {
    auto rawType = element["type"].as<std::string>();
    std::string lowered = rawType;
    std::transform(
        lowered.begin(), lowered.end(), lowered.begin(),
        [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
    if (lowered == "numeric_list") {
      type = epoch_core::MetaDataOptionType::NumericList;
    } else if (lowered == "string_list") {
      type = epoch_core::MetaDataOptionType::StringList;
    } else if (lowered == "string") {
      type = epoch_core::MetaDataOptionType::String;
    } else {
      type = epoch_core::MetaDataOptionTypeWrapper::FromString(rawType);
    }
  }

  selectOption = element["selectOption"].as<std::vector<SelectOption>>(
      std::vector<SelectOption>{});

  if (element["default"]) {
    defaultValue = CreateMetaDataArgDefinition(element["default"], *this);
  }
  min = element["min"].as<double>(0);
  max = element["max"].as<double>(10000);
  step_size = element["step_size"].as<double>(0.000001);

  isRequired = element["required"].as<bool>(true);
  desc = element["desc"].as<std::string>("");
  tuningGuidance = element["tuningGuidance"].as<std::string>("");
}
} // namespace epoch_script