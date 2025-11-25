#include "base_card_report.h"
#include <epoch_dashboard/tearsheet/scalar_converter.h>
#include <epoch_frame/dataframe.h>
#include <arrow/compute/api_aggregate.h>
#include <sstream>

namespace epoch_script::reports {

void BaseCardReport::generateTearsheet(const epoch_frame::DataFrame &normalizedDf,
                                        epoch_tearsheet::DashboardBuilder &dashboard) const {
  // For single input transforms, get the column identifier for DataFrame indexing
  auto inputCol = this->GetInputId();

  // Get the series for the input column
  auto series = normalizedDf[inputCol];

  // Get the single aggregation to apply
  auto aggregation = GetAggregation();

  try {
    // Apply aggregation - handle special cases that require specific options or return structs
    epoch_frame::Scalar result;

    if (aggregation == "stddev") {
      // stddev requires VarianceOptions - use ddof=1 for sample standard deviation (matches test expectations)
      arrow::compute::VarianceOptions options(1);  // ddof=1 for sample statistics
      result = series.stddev(options, epoch_frame::AxisType::Column);
    } else if (aggregation == "variance") {
      // variance requires VarianceOptions - use ddof=1 for sample variance (matches test expectations)
      arrow::compute::VarianceOptions options(1);  // ddof=1 for sample statistics
      result = series.variance(options, epoch_frame::AxisType::Column);
    } else if (aggregation == "skew") {
      // skew requires SkewOptions
      arrow::compute::SkewOptions options = arrow::compute::SkewOptions::Defaults();
      result = series.agg(epoch_frame::AxisType::Column, "skew", true, options);
    } else if (aggregation == "kurtosis") {
      // kurtosis requires SkewOptions (same as skew)
      arrow::compute::SkewOptions options = arrow::compute::SkewOptions::Defaults();
      result = series.agg(epoch_frame::AxisType::Column, "kurtosis", true, options);
    } else if (aggregation == "count_distinct") {
      // count_distinct requires CountOptions
      arrow::compute::CountOptions options = arrow::compute::CountOptions::Defaults();
      result = series.agg(epoch_frame::AxisType::Column, "count_distinct", true, options);
    } else if (aggregation == "quantile") {
      // quantile requires QuantileOptions - handle generically for QuantileCardReport
      auto options = m_config.GetOptions();

      // Get quantile value from options
      double quantileValue = 0.5;  // default median
      if (options.contains("quantile")) {
        if (options["quantile"].IsType(epoch_core::MetaDataOptionType::Decimal)) {
          quantileValue = options["quantile"].GetDecimal();
        } else if (options["quantile"].IsType(epoch_core::MetaDataOptionType::Integer)) {
          quantileValue = static_cast<double>(options["quantile"].GetInteger());
        }
        // Clamp to valid range [0.0, 1.0]
        if (quantileValue < 0.0) quantileValue = 0.0;
        if (quantileValue > 1.0) quantileValue = 1.0;
      }

      // Create QuantileOptions with custom quantile value and interpolation
      arrow::compute::QuantileOptions quantileOptions;
      quantileOptions.q = {quantileValue};  // Set the quantile value

      // Set default interpolation to LINEAR for consistent behavior
      quantileOptions.interpolation = arrow::compute::QuantileOptions::LINEAR;

      // Handle interpolation if specified
      if (options.contains("interpolation") && options["interpolation"].IsType(epoch_core::MetaDataOptionType::String)) {
        std::string interpMethod = options["interpolation"].GetString();
        if (interpMethod == "linear") {
          quantileOptions.interpolation = arrow::compute::QuantileOptions::LINEAR;
        } else if (interpMethod == "lower") {
          quantileOptions.interpolation = arrow::compute::QuantileOptions::LOWER;
        } else if (interpMethod == "higher") {
          quantileOptions.interpolation = arrow::compute::QuantileOptions::HIGHER;
        } else if (interpMethod == "midpoint") {
          quantileOptions.interpolation = arrow::compute::QuantileOptions::MIDPOINT;
        } else if (interpMethod == "nearest") {
          quantileOptions.interpolation = arrow::compute::QuantileOptions::NEAREST;
        }
      }

      result = series.agg(epoch_frame::AxisType::Column, "quantile", true, quantileOptions);
    } else if (aggregation == "tdigest") {
      // tdigest requires TDigestOptions
      arrow::compute::TDigestOptions options = arrow::compute::TDigestOptions::Defaults();
      result = series.agg(epoch_frame::AxisType::Column, "tdigest", true, options);
    } else if (aggregation == "index") {
      // index requires IndexOptions - handle generically for IndexCardReport
      auto options = m_config.GetOptions();
      std::shared_ptr<arrow::Scalar> arrowScalar;

      if (options.contains("target_value")) {
        // Get the data type of the series to determine the appropriate scalar type
        auto seriesType = series.dtype();

        if (options["target_value"].IsType(epoch_core::MetaDataOptionType::String)) {
          std::string valueStr = options["target_value"].GetString();

          // Convert string to the appropriate type based on series data type
          if (seriesType->id() == arrow::Type::DOUBLE || seriesType->id() == arrow::Type::FLOAT) {
            // Convert string to double for numeric columns
            try {
              double doubleValue = std::stod(valueStr);
              arrowScalar = arrow::MakeScalar(doubleValue);
            } catch (const std::exception& e) {
              std::cerr << "Warning: Could not convert target_value '" << valueStr << "' to double, using 0.0" << std::endl;
              arrowScalar = arrow::MakeScalar(0.0);
            }
          } else if (seriesType->id() == arrow::Type::INT64 || seriesType->id() == arrow::Type::INT32) {
            // Convert string to integer for integer columns
            try {
              int64_t intValue = std::stoll(valueStr);
              arrowScalar = arrow::MakeScalar(intValue);
            } catch (const std::exception& e) {
              std::cerr << "Warning: Could not convert target_value '" << valueStr << "' to integer, using 0" << std::endl;
              arrowScalar = arrow::MakeScalar(static_cast<int64_t>(0));
            }
          } else {
            // Keep as string for string columns
            arrowScalar = arrow::MakeScalar(valueStr);
          }
        } else if (options["target_value"].IsType(epoch_core::MetaDataOptionType::Integer)) {
          int64_t intValue = options["target_value"].GetInteger();

          // Convert integer to match series type
          auto seriesType = series.dtype();
          if (seriesType->id() == arrow::Type::DOUBLE || seriesType->id() == arrow::Type::FLOAT) {
            arrowScalar = arrow::MakeScalar(static_cast<double>(intValue));
          } else {
            arrowScalar = arrow::MakeScalar(intValue);
          }
        } else if (options["target_value"].IsType(epoch_core::MetaDataOptionType::Decimal)) {
          double doubleValue = options["target_value"].GetDecimal();
          arrowScalar = arrow::MakeScalar(doubleValue);
        }
      } else {
        // Default value based on series type
        auto seriesType = series.dtype();
        if (seriesType->id() == arrow::Type::DOUBLE || seriesType->id() == arrow::Type::FLOAT) {
          arrowScalar = arrow::MakeScalar(0.0);
        } else if (seriesType->id() == arrow::Type::INT64 || seriesType->id() == arrow::Type::INT32) {
          arrowScalar = arrow::MakeScalar(static_cast<int64_t>(0));
        } else {
          arrowScalar = arrow::MakeScalar(std::string(""));
        }
      }

      arrow::compute::IndexOptions indexOptions(arrowScalar);
      result = series.agg(epoch_frame::AxisType::Column, "index", true, indexOptions);
    } else if (aggregation == "product") {
      // product requires ScalarAggregateOptions
      arrow::compute::ScalarAggregateOptions options = arrow::compute::ScalarAggregateOptions::Defaults();
      result = series.agg(epoch_frame::AxisType::Column, "product", true, options);
    } else if (aggregation == "count_all") {
      // count_all requires no arguments - count all elements including nulls
      arrow::compute::CountOptions options = arrow::compute::CountOptions::Defaults();
      options.mode = arrow::compute::CountOptions::ALL;
      result = series.agg(epoch_frame::AxisType::Column, "count", true, options);
    } else {
      // Use generic agg method for other aggregations
      result = series.agg(epoch_frame::AxisType::Column, aggregation);
    }

    // Skip if result is null
    if (result.is_null()) {
      std::cerr << "Warning: Aggregation '" << aggregation << "' returned null for column '" << inputCol << "'" << std::endl;
      return;
    }

    // Create CardDef using builder
    epoch_tearsheet::CardBuilder cardBuilder;

    cardBuilder.setType(GetWidgetType())
               .setCategory(GetCategory());

    // Build card data
    epoch_tearsheet::CardDataBuilder DataBuilder;

    // Set title - use custom title or generate from aggregation
    std::string title = GetTitle();
    if (title.empty()) {
      std::ostringstream titleStream;
      titleStream << aggregation << "(" << inputCol << ")";
      title = titleStream.str();
    }
    DataBuilder.setTitle(title);

    // Use ScalarFactory to directly convert epoch_frame::Scalar to epoch_proto::Scalar
    epoch_proto::Scalar scalarValue;
    try {
      scalarValue = epoch_tearsheet::ScalarFactory::create(result);
    } catch (const std::exception& e) {
      SPDLOG_DEBUG("Error: Failed to convert scalar to protobuf: {}", e.what());
      // Try to handle integer case manually if ScalarFactory fails
      // For now, just return to avoid crash
      return;
    }
    DataBuilder.setValue(scalarValue);

    // Set the type based on the protobuf scalar type
    // NOTE: There's a known issue in ScalarFactory where false boolean values
    // don't set any value field in the protobuf, causing type detection to fail
    if (scalarValue.has_boolean_value()) {
      DataBuilder.setType(epoch_proto::TypeBoolean);
    } else if (scalarValue.has_integer_value()) {
      DataBuilder.setType(epoch_proto::TypeInteger);
    } else if (scalarValue.has_decimal_value()) {
      DataBuilder.setType(epoch_proto::TypeDecimal);
    } else if (scalarValue.has_string_value()) {
      DataBuilder.setType(epoch_proto::TypeString);
    }
    // If none of the above, leave type unset (will use default)

    // Add the single card
    cardBuilder.addCardData(DataBuilder.build());

    dashboard.addCard(cardBuilder.build());

  } catch (const std::exception& e) {
    // Aggregation failed, return empty
    std::cerr << "Error: Aggregation '" << aggregation << "' failed with exception: " << e.what() << std::endl;
    return;
  }
}

std::string BaseCardReport::GetCategory() const {
  auto options = m_config.GetOptions();
  if (options.contains("category") && options["category"].IsType(epoch_core::MetaDataOptionType::String)) {
    return options["category"].GetString();
  }
  return "";
}

std::string BaseCardReport::GetTitle() const {
  auto options = m_config.GetOptions();
  if (options.contains("title") && options["title"].IsType(epoch_core::MetaDataOptionType::String)) {
    return options["title"].GetString();
  }
  return "";  // Empty means auto-generate
}

epoch_proto::EpochFolioDashboardWidget BaseCardReport::GetWidgetType() const {
  auto options = m_config.GetOptions();
  if (options.contains("widget_type") && options["widget_type"].IsType(epoch_core::MetaDataOptionType::String)) {
    std::string type = options["widget_type"].GetString();
    // Map string to enum
    if (type == "CARD") {
      return epoch_proto::EpochFolioDashboardWidget::WidgetCard;
    } else if (type == "METRIC") {
      return epoch_proto::EpochFolioDashboardWidget::WidgetCard; // METRIC maps to WidgetCard
    }
    // Add more mappings as needed
  }
  return epoch_proto::EpochFolioDashboardWidget::WidgetCard;  // Default widget type
}

} // namespace epoch_script::reports
