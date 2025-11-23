#pragma once

#include <epoch_script/transforms/components/reports/ireport.h>
#include <epoch_dashboard/tearsheet/card_builder.h>
#include <epoch_dashboard/tearsheet/scalar_converter.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/series_or_scalar.h>
#include <epoch_frame/scalar.h>

namespace epoch_script::reports {

class BaseCardReport : public IReporter {
public:
  explicit BaseCardReport(epoch_script::transform::TransformConfiguration config)
      : IReporter(std::move(config), true) {}

protected:
  void generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                          epoch_tearsheet::DashboardBuilder &dashboard) const override;

  // Shared helper methods
  std::string GetCategory() const;
  std::string GetTitle() const;
  epoch_proto::EpochFolioDashboardWidget GetWidgetType() const;

  // Virtual method for aggregation - subclasses provide specific defaults
  virtual std::string GetAggregation() const = 0;
};


} // namespace epoch_script::reports