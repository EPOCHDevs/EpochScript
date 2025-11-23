#pragma once
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <epoch_frame/frame_or_series.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_script/transforms/core/registry.h>
#include <epoch_script/transforms/core/transform_configuration.h>
#include <epoch_script/transforms/core/transform_registry.h>
#include <yaml-cpp/yaml.h>

#include <epoch_dashboard/tearsheet/tearsheet_builder.h>

namespace epoch_script::reports {

// IReporter extends ITransform to add TearSheet generation capability
// Following the TradeExecutorTransform pattern for column mapping
class IReporter : public epoch_script::transform::ITransform {
public:
  explicit IReporter(epoch_script::transform::TransformConfiguration config, bool skipRename=false)
      : ITransform(std::move(config)) {
    if (!skipRename)
    // Build column mapping from inputs like TradeExecutor does
    // Map {transform_id}#result columns to expected names
    BuildColumnMappings();
  }

  // TransformData normalizes column names (stateless - no dashboard generation)
  epoch_frame::DataFrame TransformData(const epoch_frame::DataFrame &df) const override {
    // 1. Get expected columns from configuration inputs
    std::vector<std::string> inputColumns;
    for (const auto& [inputId, columns] : m_config.GetInputs()) {
      inputColumns.insert(inputColumns.end(), columns.begin(), columns.end());
    }
    for (const auto& column : m_config.GetTransformDefinition().GetMetadata().requiredDataSources) {
      inputColumns.emplace_back(column);
    }

    if (inputColumns.empty()) {
      // No inputs configured, return empty DataFrame
      return epoch_frame::DataFrame(
          df.index(),
          arrow::Table::MakeEmpty(arrow::schema(arrow::FieldVector{}))
              .MoveValueUnsafe());
    }

    // 2. Rename columns to canonical names (e.g., "gap_classifier#result" -> "gap")
    // This follows TradeExecutorTransform pattern
    auto normalizedDf = m_columnMappings.empty() ? df[inputColumns] : df[inputColumns].rename(m_columnMappings);

    // 3. Return normalized DataFrame (computation graph expects DataFrame output)
    // Dashboard generation happens in GetDashboard() for stateless execution
    return normalizedDf;
  }

  // New stateless interface: build dashboard locally and return
  std::optional<epoch_tearsheet::DashboardBuilder>
  GetDashboard(const epoch_frame::DataFrame &df) const override {
    epoch_tearsheet::DashboardBuilder dashboard;
    generateTearsheet(df, dashboard);  // Child classes fill dashboard
    return dashboard;
  }

  virtual ~IReporter() = default;

protected:
  // Child classes implement this to populate the dashboard
  // Updated signature: now receives dashboard reference (stateless pattern)
  virtual void generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                  epoch_tearsheet::DashboardBuilder &dashboard) const = 0;

  void BuildColumnMappings() {
    // Similar to TradeExecutorTransform constructor
    // Map input columns like "gap_classifier#result" to expected names like "gap"
    const auto inputs = m_config.GetInputs();
    for (const auto& [inputId, inputColumns] : inputs) {
      for (auto const& column: inputColumns) {
      m_columnMappings.emplace(column, inputId);
      }
    }

    for (const auto& column : m_config.GetTransformDefinition().GetMetadata().requiredDataSources) {
      m_columnMappings.emplace(column, column);
    }
  }

  // Removed: mutable epoch_tearsheet::DashboardBuilder m_dashboard; (stateless now!)
  std::unordered_map<std::string, std::string> m_columnMappings;
};

// Template specialization pattern for report metadata
// Each report implementation should specialize this
template<typename ReportClass>
struct ReportMetadata {
  static epoch_script::transforms::TransformsMetaData Get() {
    // Child classes specialize this to provide their metadata
    static_assert(sizeof(ReportClass) == 0,
                  "Report class must specialize ReportMetadata<T>::Get()");
    return {};
  }
};

// Unified registration function that handles both metadata and transform
// No need to pass id - it comes from metadata
template<typename ReportClass>
void RegisterReport() {
  // 1. Get and register the metadata
  auto metadata = ReportMetadata<ReportClass>::Get();
  epoch_script::transforms::ITransformRegistry::GetInstance().Register(metadata);

  // 2. Register the transform factory using metadata.id
  epoch_script::transform::Register<ReportClass>(metadata.id);
}

} // namespace epoch_script::reports
