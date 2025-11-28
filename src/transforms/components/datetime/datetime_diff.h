#pragma once

#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/array_factory.h>
#include <methods/temporal.h>
#include <epoch_core/enum_wrapper.h>

CREATE_ENUM(DatetimeDiffUnit, days, hours, minutes, seconds, milliseconds, microseconds, weeks, months, quarters, years);

namespace epoch_script::transform {

/**
 * DatetimeDiff - Calculate time difference between two timestamp columns
 *
 * Computes the time difference between SLOT and SLOT1 timestamp columns
 * in the selected unit (days, hours, minutes, etc.).
 *
 * Example usage:
 *   diff = datetime_diff(transaction_date, period_end, unit="days")
 *   recent = diff.value <= 30
 */
class DatetimeDiff : public ITransform {
public:
  explicit DatetimeDiff(const TransformConfiguration &config)
      : ITransform(config),
        m_unit(config.GetOptionValue("unit").GetSelectOption<epoch_core::DatetimeDiffUnit>()) {}

private:
  epoch_core::DatetimeDiffUnit m_unit;

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const override {
    return epoch_frame::DataFrame{df.index(), Call(df)};
  }

  arrow::TablePtr Call(epoch_frame::DataFrame const &bars) const {
    using namespace epoch_frame;

    // Get the two timestamp columns
    auto timestamp1_array = bars[GetInputId(ARG0)].contiguous_array();
    auto timestamp2_array = bars[GetInputId(ARG1)].contiguous_array();

    // DEBUG: Always log input types with column names
    auto ts1_type_str = timestamp1_array.value()->type()->ToString();
    auto ts2_type_str = timestamp2_array.value()->type()->ToString();
    SPDLOG_INFO("datetime_diff - ts1: {} ({}) ts2: {} ({})",
                ts1_type_str, GetInputId(ARG0), ts2_type_str, GetInputId(ARG1));

    TemporalOperation<true> temporal(timestamp1_array);

    // Calculate difference in selected unit
    Array result;
    switch (m_unit) {
      case epoch_core::DatetimeDiffUnit::Null:
        // Should never happen - enum has no Null value
        throw std::runtime_error("Invalid datetime diff unit: Null");
        break;
      case epoch_core::DatetimeDiffUnit::days:
        result = temporal.days_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::hours:
        result = temporal.hours_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::minutes:
        result = temporal.minutes_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::seconds:
        result = temporal.seconds_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::milliseconds:
        result = temporal.milliseconds_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::microseconds:
        result = temporal.microseconds_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::weeks:
        result = temporal.weeks_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::months:
        result = temporal.months_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::quarters:
        result = temporal.quarters_between(timestamp2_array);
        break;
      case epoch_core::DatetimeDiffUnit::years:
        result = temporal.years_between(timestamp2_array);
        break;
    }

    // Cast result to int64 if needed (month_interval returns MonthIntervalArray)
    std::shared_ptr<arrow::Array> final_result;
    if (result.value()->type()->id() == arrow::Type::INTERVAL_MONTHS) {
      // MonthIntervalArray stores int32 values - manually extract to Int64Array
      auto month_array = std::static_pointer_cast<arrow::MonthIntervalArray>(result.value());
      arrow::Int64Builder builder;
      auto status = builder.Reserve(month_array->length());
      if (!status.ok()) {
        throw std::runtime_error("Failed to reserve Int64Builder: " + status.ToString());
      }
      for (int64_t i = 0; i < month_array->length(); ++i) {
        if (month_array->IsNull(i)) {
          status = builder.AppendNull();
        } else {
          status = builder.Append(month_array->Value(i));
        }
        if (!status.ok()) {
          throw std::runtime_error("Failed to append to Int64Builder: " + status.ToString());
        }
      }
      auto build_result = builder.Finish();
      if (!build_result.ok()) {
        throw std::runtime_error("Failed to build Int64Array: " + build_result.status().ToString());
      }
      final_result = build_result.ValueOrDie();
    } else {
      final_result = result.value();
    }

    // Build output schema with single value
    auto schema = arrow::schema({
        {GetOutputId("value"), arrow::int64()}
    });

    std::vector<std::shared_ptr<arrow::Array>> arrays = {final_result};

    return AssertTableResultIsOk(arrow::Table::Make(schema, arrays));
  }
};

} // namespace epoch_script::transform
