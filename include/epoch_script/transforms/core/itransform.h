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

// Forward declaration for progress emitter
namespace epoch_script::runtime::events {
    class TransformProgressEmitter;
    using TransformProgressEmitterPtr = std::shared_ptr<TransformProgressEmitter>;
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

  virtual const TransformConfiguration& GetConfiguration() const = 0;

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

  // Progress emitter for internal transform progress reporting
  // Set by orchestrator before execution, used by transforms to emit progress events
  virtual void SetProgressEmitter(
      runtime::events::TransformProgressEmitterPtr emitter) = 0;
  virtual runtime::events::TransformProgressEmitterPtr GetProgressEmitter() const = 0;

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
    return m_config.GetOutputId(output).GetColumnName();
  }

  std::string GetOutputId() const final { return m_config.GetOutputId().GetColumnName(); }

  std::string GetInputId(const std::string & slot) const override {
    return m_config.GetInput(slot).GetColumnIdentifier();
  }

  std::string GetInputId() const override {
    return  m_config.GetInput().GetColumnIdentifier();
  }

  std::vector<std::string> GetInputIds() const override {
    std::vector<std::string> result;

    const auto slots = m_config.GetTransformDefinition().GetMetadata().inputs;
    std::ranges::for_each(
        slots, [&](epoch_script::transforms::IOMetaData const &slot) {
          auto input_values = m_config.GetInputs(slot.id);
          if (input_values.empty()) {
            AssertFromStream(
                m_config.GetTransformName() ==
                    epoch_script::transforms::TRADE_SIGNAL_EXECUTOR_ID,
                "Only trade signal executor can have unconnected inputs.");
            return;
          }
          // Convert InputValue objects to column identifiers
          for (const auto& input_value : input_values) {
            result.push_back(input_value.GetColumnIdentifier());
          }
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

  const TransformConfiguration& GetConfiguration() const final { return m_config; }

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

  // Progress emitter implementation
  void SetProgressEmitter(
      runtime::events::TransformProgressEmitterPtr emitter) override {
    m_progressEmitter = std::move(emitter);
  }

  runtime::events::TransformProgressEmitterPtr GetProgressEmitter() const override {
    return m_progressEmitter;
  }

  ~ITransform() override = default;
  using Ptr = std::shared_ptr<ITransform>;

protected:
  static auto GetValidSeries(epoch_frame::Series const &input) {
    const auto output = input.loc(input.is_valid());
    return std::pair{output.contiguous_array(), output};
  }

  epoch_frame::DataFrame MakeResult(epoch_frame::Series const &series) const {
    return series.to_frame(GetOutputId());
  }

  // Helper methods for derived classes to emit progress
  // These are no-ops if no emitter is set
  // Implementations are in itransform_progress.cpp to avoid header dependency
  void EmitProgress(size_t current, size_t total,
                    const std::string& message = "") const;

  void EmitEpoch(size_t epoch, size_t total_epochs,
                 std::optional<double> loss = std::nullopt,
                 std::optional<double> accuracy = std::nullopt) const;

  void EmitIteration(size_t iteration,
                     std::optional<double> metric = std::nullopt,
                     const std::string& message = "") const;

  // Check cancellation and throw if cancelled
  void ThrowIfCancelled() const;

  // Check if pipeline has been cancelled
  [[nodiscard]] bool IsCancelled() const;

  TransformConfiguration m_config;
  mutable runtime::events::TransformProgressEmitterPtr m_progressEmitter;
};

using ITransformBasePtr = std::unique_ptr<ITransformBase>;
} // namespace epoch_script::transform