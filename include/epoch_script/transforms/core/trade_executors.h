//
// Created by adesola on 5/12/25.
//

#pragma once

#include "epoch_frame/factory/dataframe_factory.h"
#include "itransform.h"

#include <map>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace epoch_script::transform {

// Canonical keys for the four-node trade signal interface
static const std::string TE_ENTER_LONG_KEY = "enter_long";
static const std::string TE_ENTER_SHORT_KEY = "enter_short";
static const std::string TE_EXIT_LONG_KEY = "exit_long";
static const std::string TE_EXIT_SHORT_KEY = "exit_short";

// Adapter that converts a numeric input (+/-) into boolean
// enter_long/enter_short
struct TradeExecutorAdapter final : ITransform {
  explicit TradeExecutorAdapter(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    auto input = bars[GetInputId()];
    const epoch_frame::Scalar zero{0};
    auto is_long = input > zero;
    auto is_short = input < zero;
    return epoch_frame::make_dataframe(
        bars.index(), {is_long.array(), is_short.array()},
        {GetOutputId("long"), GetOutputId("short")});
  }
};

// Only 4 types as requested by user
enum class TradeExecutorType {
  SingleExecutor,
  SingleExecutorWithExit,
  MultipleExecutor,
  MultipleExecutorWithExit
};

struct TradeExecutorTransform final : ITransform {
  static TradeExecutorType GetType(bool hasLong, bool hasShort, bool hasExit) {
    if (hasLong && !hasShort && !hasExit)
      return TradeExecutorType::SingleExecutor;
    if ((hasLong || hasShort) && hasExit && !(hasLong && hasShort))
      return TradeExecutorType::SingleExecutorWithExit;
    if (hasLong && hasShort && !hasExit)
      return TradeExecutorType::MultipleExecutor;
    if (hasLong && hasShort && hasExit)
      return TradeExecutorType::MultipleExecutorWithExit;
    return TradeExecutorType::SingleExecutor;
  }

  explicit TradeExecutorTransform(const TransformConfiguration &config)
      : ITransform(config) {
    const auto inputs = config.GetInputs();

    // Map of output-key priority to pair<outputKey, inputColumn>
    std::map<int, std::pair<std::string, std::string>> priorityMap;

    for (const auto &[inputId, inputColumns] : inputs) {
      if (inputColumns.empty())
        continue;
      const auto &inputColumn = inputColumns.front();

      if (inputId == TE_ENTER_LONG_KEY) {
        priorityMap[4] = std::make_pair(TE_ENTER_LONG_KEY, inputColumn.GetColumnIdentifier());
        m_hasLong = true;
      } else if (inputId == TE_ENTER_SHORT_KEY) {
        priorityMap[3] = std::make_pair(TE_ENTER_SHORT_KEY, inputColumn.GetColumnIdentifier());
        m_hasShort = true;
      } else if (inputId == TE_EXIT_LONG_KEY) {
        priorityMap[2] = std::make_pair(TE_EXIT_LONG_KEY, inputColumn.GetColumnIdentifier());
        m_hasExit = true;
      } else if (inputId == TE_EXIT_SHORT_KEY) {
        priorityMap[1] = std::make_pair(TE_EXIT_SHORT_KEY, inputColumn.GetColumnIdentifier());
        m_hasExit = true;
      }
    }

    for (const auto &kv : priorityMap) {
      const auto &outKey = kv.second.first;
      const auto &inCol = kv.second.second;
      m_outputs.insert(outKey);
      m_replacements[inCol] = outKey;
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    std::vector<std::string> columns;
    columns.reserve(m_replacements.size());
    for (const auto &kv : m_replacements)
      columns.push_back(kv.first);

    if (columns.empty()) {
      // No valid mappings; return empty DataFrame with same index
      return epoch_frame::DataFrame(
          bars.index(),
          arrow::Table::MakeEmpty(arrow::schema(arrow::FieldVector{}))
              .MoveValueUnsafe());
    }

    auto result = bars[columns].rename(m_replacements);
    return result;
  }

  std::vector<epoch_script::transforms::IOMetaData>
  GetOutputMetaData() const override {
    std::vector<epoch_script::transforms::IOMetaData> output;
    if (m_hasLong)
      output.emplace_back(epoch_core::IODataType::Boolean, TE_ENTER_LONG_KEY,
                          TE_ENTER_LONG_KEY, false, false);
    if (m_hasShort)
      output.emplace_back(epoch_core::IODataType::Boolean, TE_ENTER_SHORT_KEY,
                          TE_ENTER_SHORT_KEY, false, false);
    if (m_hasExit) {
      // Expose both exit keys if they are connected
      if (m_outputs.contains(TE_EXIT_LONG_KEY))
        output.emplace_back(epoch_core::IODataType::Boolean, TE_EXIT_LONG_KEY,
                            TE_EXIT_LONG_KEY, false, false);
      if (m_outputs.contains(TE_EXIT_SHORT_KEY))
        output.emplace_back(epoch_core::IODataType::Boolean, TE_EXIT_SHORT_KEY,
                            TE_EXIT_SHORT_KEY, false, false);
    }
    return output;
  }

  std::string GetOutputId(const std::string &output) const override {
    AssertFromStream(m_outputs.contains(output),
                     "Invalid TradeExecutor output: " << output);
    return output;
  }

private:
  std::unordered_map<std::string, std::string>
      m_replacements; // inCol -> outKey
  std::unordered_set<std::string> m_outputs;
  bool m_hasLong{false};
  bool m_hasShort{false};
  bool m_hasExit{false};
};

} // namespace epoch_script::transform