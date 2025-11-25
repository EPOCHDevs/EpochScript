#include "data_column_resolver.h"
#include <unordered_map>
#include <unordered_set>

namespace epoch_script::chart_metadata {

std::vector<std::string> DataColumnResolver::ResolveColumns(
    const epoch_script::transform::TransformConfiguration &cfg) {
  std::vector<std::string> columns;
  columns.push_back(INDEX_COLUMN);

  for (auto const &outputId : cfg.GetOutputs()) {
    columns.emplace_back(cfg.GetOutputId(outputId.id).GetColumnName());
  }

  return columns;
}

} // namespace epoch_script::chart_metadata