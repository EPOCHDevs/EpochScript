#pragma once
//
// Created by dewe on 1/8/23.
//
#include "transform_configuration.h"
#include "memory"
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series.h>
#include <epoch_protos/tearsheet.pb.h>
#include <optional>
#include <epoch_script/transforms/runtime/types.h>

// Forward declaration
namespace epoch_tearsheet {
    class DashboardBuilder;
}

namespace epoch_script::transform {
struct ITransformBase {

  virtual std::string GetId() const = 0;

  virtual std::string GetName() const = 0;

  virtual epoch_script::MetaDataOptionDefinition
  GetOption(std::string const &param) const = 0;

  virtual epoch_script::MetaDataOptionList GetOptionsMetaData() const = 0;

  virtual std::string GetOutputId(const std::string &output) const = 0;

  virtual std::string GetOutputId() const = 0;

  virtual std::string GetInputId(const std::string &inputId) const = 0;

  virtual std::string GetInputId() const = 0;

  virtual std::vector<std::string> GetInputIds() const = 0;

  virtual std::vector<epoch_script::transforms::IOMetaData>
  GetOutputMetaData() const = 0;

  virtual epoch_script::TimeFrame GetTimeframe() const = 0;

  virtual TransformConfiguration GetConfiguration() const = 0;

  virtual epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &) const = 0;

  // New stateless interfaces for reporters/event markers
  // Child classes override these to provide dashboards/events based on transform output
  virtual std::optional<epoch_tearsheet::DashboardBuilder>
  GetDashboard(const epoch_frame::DataFrame &df) const = 0;

  virtual std::optional<EventMarkerData>
  GetEventMarkers(const epoch_frame::DataFrame &df) const = 0;

  // Higher-order method that combines all interfaces
  // Calls TransformData, then GetDashboard and GetEventMarkers with the result
  virtual epoch_script::runtime::TransformResult
  TransformDataWithMetadata(const epoch_frame::DataFrame &df) const = 0;

  // Virtual method for getting required data sources
  // Default returns metadata requiredDataSources, but transforms can override
  // to do template expansion (e.g., FRED replaces {category} with actual option value)
  virtual std::vector<std::string> GetRequiredDataSources() const = 0;

  virtual ~ITransformBase() = default;
};

class ITransform : public ITransformBase {

public:
  explicit ITransform(TransformConfiguration config)
      : m_config(std::move(config)) {}

  std::string GetId() const final { return m_config.GetId(); }

  inline std::string GetName() const final {
    return m_config.GetTransformName();
  }

  epoch_script::MetaDataOptionDefinition
  GetOption(std::string const &param) const final {
    return m_config.GetOptionValue(param);
  }

  epoch_script::MetaDataOptionList GetOptionsMetaData() const final {
    return m_config.GetTransformDefinition().GetMetadata().options;
  }

  std::string GetOutputId(const std::string &output) const override {
    return m_config.GetOutputId(output);
  }

  std::string GetOutputId() const final { return m_config.GetOutputId(); }

  std::string GetInputId(const std::string &inputId) const override {
    return m_config.GetInput(inputId);
  }

  std::string GetInputId() const override { return m_config.GetInput(); }

  std::vector<std::string> GetInputIds() const override {
    std::vector<std::string> result;

    const auto inputs = m_config.GetTransformDefinition().GetMetadata().inputs;
    std::ranges::for_each(
        inputs, [&](epoch_script::transforms::IOMetaData const &io) {
          auto out = m_config.GetInputs(io.id);
          if (out.empty()) {
            AssertFromStream(
                m_config.GetTransformName() ==
                    epoch_script::transforms::TRADE_SIGNAL_EXECUTOR_ID,
                "Only trade signal executor can have unconnected inputs.");
            return;
          }
          result.insert(result.end(), out.begin(), out.end());
        });
    return result;
  }

  std::vector<epoch_script::transforms::IOMetaData>
  GetOutputMetaData() const override {
    return m_config.GetOutputs();
  }

  epoch_script::TimeFrame GetTimeframe() const final {
    return m_config.GetTimeframe();
  }

  TransformConfiguration GetConfiguration() const final { return m_config; }

  friend std::ostream &operator<<(std::ostream &os, ITransform const &model) {
    os << model.m_config.ToString();
    return os;
  }

  // New stateless interface implementations - provide defaults
  std::optional<epoch_tearsheet::DashboardBuilder>
  GetDashboard(const epoch_frame::DataFrame &) const override {
    return std::nullopt;  // Default: no dashboard
  }

  std::optional<EventMarkerData>
  GetEventMarkers(const epoch_frame::DataFrame &) const override {
    return std::nullopt;  // Default: no event markers
  }

  epoch_script::runtime::TransformResult
  TransformDataWithMetadata(const epoch_frame::DataFrame &df) const override {
    auto data = TransformData(df);
    return {data, GetDashboard(data), GetEventMarkers(data)};
  }

  // Default implementation: return metadata's requiredDataSources as-is
  std::vector<std::string> GetRequiredDataSources() const override {
    return m_config.GetTransformDefinition().GetMetadata().requiredDataSources;
  }

  ~ITransform() override = default;
  using Ptr = std::shared_ptr<ITransform>;

protected:
  TransformConfiguration m_config;

  static auto GetValidSeries(epoch_frame::Series const &input) {
    const auto output = input.loc(input.is_valid());
    return std::pair{output.contiguous_array(), output};
  }

  epoch_frame::DataFrame MakeResult(epoch_frame::Series const &series) const {
    return series.to_frame(GetOutputId());
  }
};

using ITransformBasePtr = std::unique_ptr<ITransformBase>;
} // namespace epoch_script::transform