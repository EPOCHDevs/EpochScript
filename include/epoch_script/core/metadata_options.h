//
// Created by dewe on 9/12/24.
//

#pragma once
#include <algorithm>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <epoch_core/enum_wrapper.h>
#include <epoch_frame/datetime.h>
#include <epoch_script/core/constants.h>
#include <epoch_script/core/glaze_custom_types.h>
#include <glaze/glaze.hpp>
#include <limits>
#include <string_view>
#include <unordered_set>
#include <variant>
#include <vector>
#include <yaml-cpp/yaml.h>
#include <epoch_script/core/sql_statement.h>

CREATE_ENUM(MetaDataOptionType, Integer, Decimal, Boolean, Select, NumericList,
            StringList, Time, String, EventMarkerSchema, SqlStatement, TableReportSchema);

namespace epoch_script
{

  epoch_frame::Time TimeFromString(std::string const &str);

  struct MetaDataArgRef
  {
    std::string refName{};
    bool operator==(const MetaDataArgRef &) const = default;
  };

  using SequenceItem = std::variant<double, std::string>;
  using Sequence = std::vector<SequenceItem>;

  // Card selector schema structures
  struct CardColumnSchema {
    std::string column_id;
    epoch_core::CardSlot slot;
    epoch_core::CardRenderType render_type;
    std::map<epoch_core::Color, std::vector<glz::generic>> color_map;  // Ordered map for deterministic JSON serialization
    std::optional<std::string> label;  // Optional display label - if set, UI shows this instead of column_id

    // Note: Custom equality operator needed because glz::generic doesn't have operator==
    // We compare color_map by serializing to JSON string
    bool operator==(const CardColumnSchema& other) const {
      if (column_id != other.column_id || slot != other.slot ||
          render_type != other.render_type || label != other.label) {
        return false;
      }

      // Compare color_map by serializing to JSON
      return glz::write_json(color_map) == glz::write_json(other.color_map);
    }

    struct glaze_json_schema {
      glz::schema column_id{
        .description = "ID of the DataFrame column to display in this card slot",
        .minLength = 1
      };
      glz::schema slot{
        .description = "Card slot position where this column will be rendered",
        .enumeration = std::vector<std::string_view>{"PrimaryBadge", "SecondaryBadge", "Hero", "Subtitle", "Footer", "Details"}
      };
      glz::schema render_type{
        .description = "How to render this column's value",
        .enumeration = std::vector<std::string_view>{"Text", "Integer", "Decimal", "Percent", "Monetary", "Duration", "Badge", "Timestamp", "Boolean"}
      };
      glz::schema color_map{
        .description = "Maps card colors to lists of column values that trigger that color. Keys: Success, Error, Warning, Info, Primary, Default. Values can be any type (string, number, boolean) and are matched by type and value"
      };
      glz::schema label{
        .description = "Optional display label shown to users. If not set, UI uses column_id. Pattern: only show labels for Details slot (better visual style)"
      };
    };
  };

  // Card selector schema using boolean column filter
  struct EventMarkerSchema {
    std::string title;
    epoch_core::Icon icon = epoch_core::Icon::Info;
    std::string select_key;  // Boolean column to filter rows
    std::vector<CardColumnSchema> schemas;

    bool operator==(const EventMarkerSchema &) const = default;

    struct glaze_json_schema {
      // Helper to build icon enumeration from IconWrapper (always up-to-date)
      // Implementation moved to .cpp file to avoid static initialization order fiasco
      static const std::vector<std::string_view>& GetIconEnumeration();

      glz::schema title{
        .description = "Title displayed above the card selector widget",
        .minLength = 1
      };
      glz::schema icon{
        .description = "Icon displayed in collapsed sidebar view to identify card type (see: https://lucide.dev/icons)",
        .enumeration = GetIconEnumeration()  // Now safely returns cached result
      };
      glz::schema select_key{
        .description = "Name of boolean DataFrame column used to filter rows (only rows where this column is true will be shown as cards)",
        .minLength = 1
      };
      glz::schema schemas{
        .description = "Array of column definitions specifying how each DataFrame column should be rendered in the cards",
        .minItems = 1
      };
    };
  };


  // Legacy type alias for backwards compatibility - kept for existing code
  using CardSchemaList = EventMarkerSchema;

  // Simple table column schema for table_report
  struct TableColumnSchema {
    std::string column_id;  // SLOT reference (e.g., "SLOT0", "SLOT1")
    std::string title;      // Display name for the column

    bool operator==(const TableColumnSchema &) const = default;

    struct glaze_json_schema {
      glz::schema column_id{
        .description = "ID of the DataFrame column to display (e.g., SLOT0, SLOT1)",
        .minLength = 1
      };
      glz::schema title{
        .description = "Display name for this column in the table",
        .minLength = 1
      };
    };
  };

  // Table report schema with select_key filtering
  struct TableReportSchema {
    std::string title;                      // Report title
    std::string select_key;                 // Boolean column for filtering
    std::vector<TableColumnSchema> columns; // Column configurations

    bool operator==(const TableReportSchema &) const = default;

    struct glaze_json_schema {
      glz::schema title{
        .description = "Title displayed above the table",
        .minLength = 1
      };
      glz::schema select_key{
        .description = "Name of boolean DataFrame column used to filter rows (only rows where this column is true will be shown)",
        .minLength = 1
      };
      glz::schema columns{
        .description = "Array of column definitions specifying which columns to display and their titles",
        .minItems = 1
      };
    };
  };

  class MetaDataOptionDefinition
  {
  public:
    using T = std::variant<Sequence, MetaDataArgRef, std::string, bool, double, epoch_frame::Time, EventMarkerSchema, SqlStatement, TableReportSchema>;

    explicit MetaDataOptionDefinition() = default;

    // Overloads for directly passing our variant type T
    explicit MetaDataOptionDefinition(const T &value)
    {
      if (std::holds_alternative<std::string>(value))
      {
        m_optionsVariant = ParseStringOverride(std::get<std::string>(value));
      }
      else
      {
        m_optionsVariant = value;
      }
    }

    explicit MetaDataOptionDefinition(T &&value)
    {
      if (std::holds_alternative<std::string>(value))
      {
        m_optionsVariant =
            ParseStringOverride(std::move(std::get<std::string>(value)));
      }
      else
      {
        m_optionsVariant = std::move(value);
      }
    }

    // Convenience constructors for vector types
    explicit MetaDataOptionDefinition(const std::vector<double> &values)
    {
      Sequence seq;
      seq.reserve(values.size());
      for (const auto &val : values)
      {
        seq.emplace_back(val);
      }
      m_optionsVariant = std::move(seq);
    }

    explicit MetaDataOptionDefinition(std::vector<double> &&values)
    {
      Sequence seq;
      seq.reserve(values.size());
      for (auto &&val : values)
      {
        seq.emplace_back(std::move(val));
      }
      m_optionsVariant = std::move(seq);
    }

    explicit MetaDataOptionDefinition(const std::vector<std::string> &values)
    {
      Sequence seq;
      seq.reserve(values.size());
      for (const auto &val : values)
      {
        seq.emplace_back(val);
      }
      m_optionsVariant = std::move(seq);
    }

    explicit MetaDataOptionDefinition(std::vector<std::string> &&values)
    {
      Sequence seq;
      seq.reserve(values.size());
      for (auto &&val : values)
      {
        seq.emplace_back(std::move(val));
      }
      m_optionsVariant = std::move(seq);
    }

    // Overload for string-like types
    template <typename StringLike>
      requires(std::is_convertible_v<std::decay_t<StringLike>, std::string_view>)
    explicit MetaDataOptionDefinition(StringLike &&value)
    {
      std::string materialized{std::forward<StringLike>(value)};
      m_optionsVariant = ParseStringOverride(std::move(materialized));
    }

    // Overload for types directly constructible into the variant
    template <typename K>
      requires(!std::is_convertible_v<std::decay_t<K>, std::string_view> &&
               std::is_constructible_v<T, K> &&
               !std::is_same_v<std::decay_t<K>, T>)
    explicit MetaDataOptionDefinition(K &&value)
        : m_optionsVariant(std::forward<K>(value))
    {
    }

    [[nodiscard]] auto GetVariant() const { return m_optionsVariant; }

    template <class K>
    [[nodiscard]] bool IsType() const
    {
      return std::holds_alternative<K>(m_optionsVariant);
    }

    [[nodiscard]] auto GetDecimal() const { return GetValueByType<double>(); }

    [[nodiscard]] double GetNumericValue() const;

    [[nodiscard]] auto GetInteger() const
    {
      return static_cast<int64_t>(GetValueByType<double>());
    }

    [[nodiscard]] auto GetBoolean() const { return GetValueByType<bool>(); }

    [[nodiscard]] epoch_frame::Time GetTime() const;

    [[nodiscard]] EventMarkerSchema GetCardSchemaList() const
    {
      return GetValueByType<EventMarkerSchema>();
    }

    [[nodiscard]] SqlStatement GetSqlStatement() const
    {
      return GetValueByType<SqlStatement>();
    }

    [[nodiscard]] TableReportSchema GetTableReportSchema() const
    {
      return GetValueByType<TableReportSchema>();
    }

    std::string GetRef() const
    {
      return GetValueByType<MetaDataArgRef>().refName;
    }

    template <class T>
    T GetSelectOption() const
    {
      return epoch_core::EnumWrapper<T>::type::FromString(
          GetValueByType<std::string>());
    }

    std::string GetSelectOption() const { return GetValueByType<std::string>(); }

    std::string GetString() const { return GetValueByType<std::string>(); }

    size_t GetHash() const;

    void AssertType(epoch_core::MetaDataOptionType const &argType,
                    std::unordered_set<std::string> const &selections = {}) const;

    bool IsType(epoch_core::MetaDataOptionType const &argType) const;

    template <class T>
    void AssertType() const
    {
      AssertFromStream(std::holds_alternative<T>(m_optionsVariant),
                       "Wrong type! Expected: "
                           << typeid(T).name()
                           << ", but got: " << typeid(m_optionsVariant).name());
    }

    bool operator==(const MetaDataOptionDefinition &other) const = default;

    std::string ToString() const;

    T m_optionsVariant{0.0};

  private:
    template <class T>
    T GetValueByType() const
    {
      std::stringstream errorStreamer;
      try
      {
        return std::get<T>(m_optionsVariant);
      }
      catch (std::bad_variant_access const &)
      {
        errorStreamer << "Error: Bad variant access.\n"
                      << "Expected type: " << typeid(T).name() << std::endl;
      }
      catch (...)
      {
        errorStreamer << "Error: An unknown error occurred." << std::endl;
      }
      throw std::runtime_error(errorStreamer.str());
    }

    static T ParseStringOverride(std::string input)
    {
      // trim leading/trailing whitespace
      auto is_space = [](unsigned char ch)
      { return std::isspace(ch) != 0; };
      input.erase(input.begin(),
                  std::find_if(input.begin(), input.end(),
                               [&](unsigned char ch)
                               { return !is_space(ch); }));
      input.erase(std::find_if(input.rbegin(), input.rend(),
                               [&](unsigned char ch)
                               { return !is_space(ch); })
                      .base(),
                  input.end());

      // Disallow empty string after trimming
      if (input.empty())
      {
        return input;
      }

      // MetaDataArgRef encoded as $ref:<name>
      if (input.rfind("$ref:", 0) == 0)
      {
        return T{MetaDataArgRef{input.substr(5)}};
      }

      // list parsing: [a,b,c] or [1,2,3]
      if (!input.empty() && input.front() == '[' && input.back() == ']')
      {
        std::string content = input.substr(1, input.size() - 2);

        auto trim = [&](std::string &s)
        {
          s.erase(s.begin(),
                  std::find_if(s.begin(), s.end(),
                               [&](unsigned char ch)
                               { return !is_space(ch); }));
          s.erase(std::find_if(s.rbegin(), s.rend(),
                               [&](unsigned char ch)
                               { return !is_space(ch); })
                      .base(),
                  s.end());
        };

        std::vector<std::string> tokens;
        {
          std::string current;
          for (char c : content)
          {
            if (c == ',')
            {
              trim(current);
              if (!current.empty())
              {
                // strip surrounding quotes if present
                if ((current.front() == '"' && current.back() == '"') ||
                    (current.front() == '\'' && current.back() == '\''))
                {
                  if (current.size() >= 2)
                  {
                    current = current.substr(1, current.size() - 2);
                  }
                }
                tokens.push_back(current);
              }
              else
              {
                tokens.emplace_back("");
              }
              current.clear();
            }
            else
            {
              current.push_back(c);
            }
          }
          trim(current);
          if (!current.empty())
          {
            if ((current.front() == '"' && current.back() == '"') ||
                (current.front() == '\'' && current.back() == '\''))
            {
              if (current.size() >= 2)
              {
                current = current.substr(1, current.size() - 2);
              }
            }
            tokens.push_back(current);
          }
        }

        if (tokens.empty())
        {
          return T{Sequence{}};
        }

        bool any_numeric = false;
        bool any_non_numeric = false;
        Sequence sequence;
        sequence.reserve(tokens.size());

        for (auto const &tok : tokens)
        {
          if (tok.empty())
          {
            sequence.emplace_back(std::string{});
            any_non_numeric = true;
            continue;
          }
          char *end_ptr = nullptr;
          errno = 0;
          double parsed = std::strtod(tok.c_str(), &end_ptr);
          if (end_ptr != nullptr && *end_ptr == '\0' && errno == 0 &&
              std::isfinite(parsed))
          {
            sequence.emplace_back(parsed);
            any_numeric = true;
          }
          else
          {
            sequence.emplace_back(tok);
            any_non_numeric = true;
          }
        }

        if (any_numeric && any_non_numeric)
        {
          throw std::runtime_error("Mixed types in list literal are not allowed");
        }

        return T{std::move(sequence)};
      }

      // lowercase copy for boolean check
      std::string lowered = input;
      std::transform(
          lowered.begin(), lowered.end(), lowered.begin(),
          [](unsigned char c)
          { return static_cast<char>(std::tolower(c)); });

      if (lowered == "true")
      {
        return T{true};
      }
      if (lowered == "false")
      {
        return T{false};
      }

      // Check for special numeric values
      if (lowered == "nan")
      {
        return T{std::nan("")};
      }
      if (lowered == "inf" || lowered == "infinity")
      {
        return T{std::numeric_limits<double>::infinity()};
      }
      if (lowered == "-inf" || lowered == "-infinity")
      {
        return T{-std::numeric_limits<double>::infinity()};
      }

      // Check for explicit invalid numeric strings that should remain as strings
      if (lowered == "not_a_number")
      {
        // This should remain as a string, not be parsed as a number
        return T{std::move(input)};
      }

      // numeric check using strtod; accepts +/-, decimals, and scientific
      // notation
      if (!input.empty())
      {
        char *end_ptr = nullptr;
        errno = 0;
        double parsed = std::strtod(input.c_str(), &end_ptr);
        if (end_ptr != nullptr && *end_ptr == '\0' && errno == 0 &&
            std::isfinite(parsed))
        {
          return T{parsed};
        }
      }

      // fallback to original string
      return T{std::move(input)};
    }
  };

  using MetaDataArgDefinitionMapping =
      std::unordered_map<std::string, MetaDataOptionDefinition>;

  struct SelectOption
  {
    std::string name{};
    std::string value{};

    void decode(YAML::Node const &node)
    {
      name = node["name"].as<std::string>();
      value = node["value"].as<std::string>();
    }
  };

  struct MetaDataOption
  {
    std::string id;
    std::string name;
    epoch_core::MetaDataOptionType type;
    std::optional<MetaDataOptionDefinition> defaultValue{std::nullopt};
    bool isRequired{false};
    std::vector<SelectOption> selectOption{};
    double min{-std::numeric_limits<double>::max()};
    double max{std::numeric_limits<double>::max()};
    double step_size{0.000001};
    std::string desc{};
    std::string tuningGuidance{}; // How to adjust this parameter for different strategies

    void decode(YAML::Node const &);

    YAML::Node encode() const { return {}; }
  };

  using MetaDataOptionList = std::vector<MetaDataOption>;

  MetaDataOptionDefinition CreateMetaDataArgDefinition(YAML::Node const &,
                                                       MetaDataOption const &);
} // namespace epoch_script

namespace YAML
{
  template <>
  struct convert<epoch_script::MetaDataOption>
  {
    static bool decode(const Node &node, epoch_script::MetaDataOption &t)
    {
      t.decode(node);
      return true;
    }
  };

  template <>
  struct convert<epoch_script::SelectOption>
  {
    static bool decode(const Node &node, epoch_script::SelectOption &t)
    {
      t.decode(node);
      return true;
    }
  };

} // namespace YAML

namespace glz
{
  template <>
  struct meta<epoch_script::MetaDataArgRef>
  {
    using T = epoch_script::MetaDataArgRef;
    static constexpr auto value = object("refName", &T::refName);
  };

  // SqlStatement serialization - serialize/deserialize as plain string
  template <>
  struct meta<epoch_script::SqlStatement>
  {
    static constexpr auto read =
        [](epoch_script::SqlStatement &value, const std::string &in)
    {
      value = epoch_script::SqlStatement{in};
    };

    static constexpr auto write =
        [](const epoch_script::SqlStatement &x) -> std::string
    {
      return x.GetSql();
    };

    static constexpr auto value = glz::custom<read, write>;
  };

  template <>
  struct meta<epoch_script::MetaDataOptionDefinition>
  {
    static constexpr auto read =
        [](epoch_script::MetaDataOptionDefinition &value, const generic &in)
    {
      if (in.is_number())
      {
        value = epoch_script::MetaDataOptionDefinition{in.get<double>()};
      }
      else if (in.is_boolean())
      {
        value = epoch_script::MetaDataOptionDefinition{in.get<bool>()};
      }
      else if (in.is_string())
      {
        value =
            epoch_script::MetaDataOptionDefinition{in.get<std::string>()};
      }
      else if (in.is_object() && in.contains("refName"))
      {
        auto refName = in["refName"].get<std::string>();
        value = epoch_script::MetaDataOptionDefinition{
            epoch_script::MetaDataArgRef{refName}};
      }
      else if (in.is_object() && in.contains("hour") && in.contains("minute"))
      {
        // Time object - manually construct from generic fields
        epoch_frame::Time time;
        time.hour = chrono_hour(static_cast<int>(in["hour"].get<double>()));
        time.minute = chrono_minute(static_cast<int>(in["minute"].get<double>()));
        time.second = in.contains("second") ? chrono_second(static_cast<int>(in["second"].get<double>())) : chrono_second(0);
        time.microsecond = in.contains("microsecond") ? chrono_microsecond(static_cast<int>(in["microsecond"].get<double>())) : chrono_microsecond(0);
        time.tz = in.contains("tz") ? in["tz"].get<std::string>() : "";
        value = epoch_script::MetaDataOptionDefinition{
            epoch_script::MetaDataOptionDefinition::T{time}};
      }
      else if (in.is_object() && in.contains("schemas"))
      {
        // EventMarkerSchema object
        if (!in.contains("select_key"))
        {
          throw std::runtime_error("EventMarkerSchema object must contain 'select_key' field");
        }
        epoch_script::EventMarkerSchema schema;
        auto error = glz::read_json(schema, in.dump().value_or("{}"));
        if (error)
        {
          throw std::runtime_error("Failed to parse EventMarkerSchema JSON: " +
                                   glz::format_error(error));
        }
        value = epoch_script::MetaDataOptionDefinition{
            epoch_script::MetaDataOptionDefinition::T{std::move(schema)}};
      }
      else if (in.is_object() && in.contains("columns"))
      {
        // TableReportSchema object
        if (!in.contains("select_key"))
        {
          throw std::runtime_error("TableReportSchema object must contain 'select_key' field");
        }
        epoch_script::TableReportSchema schema;
        auto error = glz::read_json(schema, in.dump().value_or("{}"));
        if (error)
        {
          throw std::runtime_error("Failed to parse TableReportSchema JSON: " +
                                   glz::format_error(error));
        }
        value = epoch_script::MetaDataOptionDefinition{
            epoch_script::MetaDataOptionDefinition::T{std::move(schema)}};
      }
      else
      {
        auto dumped = in.dump();
        if (!dumped.has_value())
        {
          throw std::runtime_error("Failed to dump JSON: " +
                                   glz::format_error(dumped.error()));
        }
        value = epoch_script::MetaDataOptionDefinition{dumped.value()};
      }
    };

    static constexpr auto write =
        [](const epoch_script::MetaDataOptionDefinition &x) -> auto
    {
      return x.GetVariant();
    };

    static constexpr auto value = glz::custom<read, write>;
  };
} // namespace glz