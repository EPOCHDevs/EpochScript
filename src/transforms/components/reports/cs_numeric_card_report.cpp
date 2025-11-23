#include "cs_numeric_card_report.h"
#include <epoch_dashboard/tearsheet/card_builder.h>
#include <epoch_dashboard/tearsheet/scalar_converter.h>
#include <epoch_frame/dataframe.h>
#include <arrow/compute/api_aggregate.h>
#include <sstream>

namespace epoch_script::reports {

std::string CSNumericCardReport::GetAggregation() const {
  return epoch_core::CSNumericArrowAggregateFunctionWrapper::ToString(m_agg);
}

void CSNumericCardReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                     epoch_tearsheet::DashboardBuilder &dashboard) const {
  using namespace epoch_frame;

  if (normalizedDf.empty() || normalizedDf.num_cols() == 0) {
    std::cerr << "Error: CSNumericCardReport received empty DataFrame" << std::endl;
    return;
  }

  // Get aggregation function
  auto aggregation = GetAggregation();

  // Create a card builder for the entire group
  epoch_tearsheet::CardBuilder cardBuilder;
  cardBuilder.setType(epoch_proto::EpochFolioDashboardWidget::WidgetCard)
             .setCategory(m_category);

  // Iterate through all asset columns
  for (const auto& assetColumn : normalizedDf.column_names()) {
    try {
      auto series = normalizedDf[assetColumn];

      // Apply aggregation - handle special cases like in BaseCardReport
      epoch_frame::Scalar result;

      if (aggregation == "stddev") {
        arrow::compute::VarianceOptions options(1);  // ddof=1
        result = series.stddev(options, epoch_frame::AxisType::Column);
      } else if (aggregation == "variance") {
        arrow::compute::VarianceOptions options(1);  // ddof=1
        result = series.variance(options, epoch_frame::AxisType::Column);
      } else if (aggregation == "skew") {
        arrow::compute::SkewOptions options = arrow::compute::SkewOptions::Defaults();
        result = series.agg(epoch_frame::AxisType::Column, "skew", true, options);
      } else if (aggregation == "kurtosis") {
        arrow::compute::SkewOptions options = arrow::compute::SkewOptions::Defaults();
        result = series.agg(epoch_frame::AxisType::Column, "kurtosis", true, options);
      } else if (aggregation == "count_distinct") {
        arrow::compute::CountOptions options = arrow::compute::CountOptions::Defaults();
        result = series.agg(epoch_frame::AxisType::Column, "count_distinct", true, options);
      } else if (aggregation == "tdigest") {
        arrow::compute::TDigestOptions options = arrow::compute::TDigestOptions::Defaults();
        result = series.agg(epoch_frame::AxisType::Column, "tdigest", true, options);
      } else if (aggregation == "product") {
        arrow::compute::ScalarAggregateOptions options = arrow::compute::ScalarAggregateOptions::Defaults();
        result = series.agg(epoch_frame::AxisType::Column, "product", true, options);
      } else if (aggregation == "count_all") {
        arrow::compute::CountOptions options = arrow::compute::CountOptions::Defaults();
        options.mode = arrow::compute::CountOptions::ALL;
        result = series.agg(epoch_frame::AxisType::Column, "count", true, options);
      } else if (aggregation == "first") {
        result = series.iloc(0);
      } else if (aggregation == "last") {
        result = series.iloc(series.size() - 1);
      } else {
        // Use generic agg method
        result = series.agg(epoch_frame::AxisType::Column, aggregation);
      }

      // Skip if result is null
      if (result.is_null()) {
        std::cerr << "Warning: Aggregation '" << aggregation << "' returned null for asset '"
                  << assetColumn << "'" << std::endl;
        continue;
      }

      // Build card data
      epoch_tearsheet::CardDataBuilder dataBuilder;

      // Set title - use asset name or custom title pattern
      std::string cardTitle = m_title.empty() ? assetColumn : m_title + " - " + assetColumn;
      dataBuilder.setTitle(cardTitle);

      // Convert scalar to protobuf
      epoch_proto::Scalar scalarValue;
      try {
        scalarValue = epoch_tearsheet::ScalarFactory::create(result);
      } catch (const std::exception& e) {
        std::cerr << "Error: Failed to convert scalar for asset '" << assetColumn
                  << "': " << e.what() << std::endl;
        continue;
      }

      dataBuilder.setValue(scalarValue);

      // Set the type based on the protobuf scalar type
      if (scalarValue.has_boolean_value()) {
        dataBuilder.setType(epoch_proto::TypeBoolean);
      } else if (scalarValue.has_integer_value()) {
        dataBuilder.setType(epoch_proto::TypeInteger);
      } else if (scalarValue.has_decimal_value()) {
        dataBuilder.setType(epoch_proto::TypeDecimal);
      } else if (scalarValue.has_string_value()) {
        dataBuilder.setType(epoch_proto::TypeString);
      }

      // Add this card data
      cardBuilder.addCardData(dataBuilder.build());

    } catch (const std::exception& e) {
      std::cerr << "Warning: Failed to process asset column '" << assetColumn
                << "': " << e.what() << std::endl;
      continue;
    }
  }

  // Add the card group to dashboard
  dashboard.addCard(cardBuilder.build());
}

} // namespace epoch_script::reports
