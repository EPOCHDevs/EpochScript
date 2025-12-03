//
// Created by dewe on 9/21/24.
//

#pragma once
#include "epoch_frame/array.h"
#include "epoch_frame/factory/dataframe_factory.h"
#include <epoch_script/transforms/core/itransform.h>
#include "../type_tags.h"

namespace epoch_script::transform {
class BooleanSelectTransform : public ITransform {
public:
  explicit BooleanSelectTransform(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

// Typed BooleanSelect - DRY template pattern
template <typename TypeTag>
class TypedBooleanSelect : public ITransform {
public:
  explicit TypedBooleanSelect(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

// Type aliases using clear naming convention: boolean_select_{type}
using BooleanSelectString = TypedBooleanSelect<StringType>;
using BooleanSelectNumber = TypedBooleanSelect<NumberType>;
using BooleanSelectBoolean = TypedBooleanSelect<BooleanType>;
using BooleanSelectTimestamp = TypedBooleanSelect<TimestampType>;

// Extern template declarations to reduce compilation time
extern template class TypedBooleanSelect<StringType>;
extern template class TypedBooleanSelect<NumberType>;
extern template class TypedBooleanSelect<BooleanType>;
extern template class TypedBooleanSelect<TimestampType>;

template <size_t N> class ZeroIndexSelectTransform : public ITransform {
public:
  explicit ZeroIndexSelectTransform(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

extern template class ZeroIndexSelectTransform<2>;

extern template class ZeroIndexSelectTransform<3>;

extern template class ZeroIndexSelectTransform<4>;

extern template class ZeroIndexSelectTransform<5>;

// Typed Switch transforms - DRY template pattern (fixed N slots - deprecated, use TypedSwitch)
template <size_t N, typename TypeTag>
class TypedZeroIndexSelect : public ITransform {
public:
  explicit TypedZeroIndexSelect(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

// Type aliases using clear naming convention: switch{N}_{type}
using Switch2String = TypedZeroIndexSelect<2, StringType>;
using Switch2Number = TypedZeroIndexSelect<2, NumberType>;
using Switch2Boolean = TypedZeroIndexSelect<2, BooleanType>;
using Switch2Timestamp = TypedZeroIndexSelect<2, TimestampType>;

using Switch3String = TypedZeroIndexSelect<3, StringType>;
using Switch3Number = TypedZeroIndexSelect<3, NumberType>;
using Switch3Boolean = TypedZeroIndexSelect<3, BooleanType>;
using Switch3Timestamp = TypedZeroIndexSelect<3, TimestampType>;

using Switch4String = TypedZeroIndexSelect<4, StringType>;
using Switch4Number = TypedZeroIndexSelect<4, NumberType>;
using Switch4Boolean = TypedZeroIndexSelect<4, BooleanType>;
using Switch4Timestamp = TypedZeroIndexSelect<4, TimestampType>;

using Switch5String = TypedZeroIndexSelect<5, StringType>;
using Switch5Number = TypedZeroIndexSelect<5, NumberType>;
using Switch5Boolean = TypedZeroIndexSelect<5, BooleanType>;
using Switch5Timestamp = TypedZeroIndexSelect<5, TimestampType>;

// Extern template declarations to reduce compilation time
extern template class TypedZeroIndexSelect<2, StringType>;
extern template class TypedZeroIndexSelect<2, NumberType>;
extern template class TypedZeroIndexSelect<2, BooleanType>;
extern template class TypedZeroIndexSelect<2, TimestampType>;

extern template class TypedZeroIndexSelect<3, StringType>;
extern template class TypedZeroIndexSelect<3, NumberType>;
extern template class TypedZeroIndexSelect<3, BooleanType>;
extern template class TypedZeroIndexSelect<3, TimestampType>;

extern template class TypedZeroIndexSelect<4, StringType>;
extern template class TypedZeroIndexSelect<4, NumberType>;
extern template class TypedZeroIndexSelect<4, BooleanType>;
extern template class TypedZeroIndexSelect<4, TimestampType>;

extern template class TypedZeroIndexSelect<5, StringType>;
extern template class TypedZeroIndexSelect<5, NumberType>;
extern template class TypedZeroIndexSelect<5, BooleanType>;
extern template class TypedZeroIndexSelect<5, TimestampType>;

// Varargs Switch transforms - supports any number of inputs
template <typename TypeTag>
class TypedSwitch : public ITransform {
public:
  explicit TypedSwitch(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

// Type aliases for varargs switch: switch_{type}
using SwitchString = TypedSwitch<StringType>;
using SwitchNumber = TypedSwitch<NumberType>;
using SwitchBoolean = TypedSwitch<BooleanType>;
using SwitchTimestamp = TypedSwitch<TimestampType>;

// Extern template declarations
extern template class TypedSwitch<StringType>;
extern template class TypedSwitch<NumberType>;
extern template class TypedSwitch<BooleanType>;
extern template class TypedSwitch<TimestampType>;

// Advanced Selection classes
class PercentileSelect : public ITransform {
public:
  explicit PercentileSelect(const TransformConfiguration &config)
      : ITransform(config),
        m_lookback(config.GetOptionValue("lookback").GetInteger()),
        m_percentile(config.GetOptionValue("percentile").GetInteger()) {
    AssertFromStream(m_lookback > 0, "Lookback must be greater than 0");
    AssertFromStream(m_percentile >= 0 && m_percentile <= 100,
                     "Percentile must be between 0 and 100");
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    epoch_frame::Series value = bars[GetInputId("value")];
    epoch_frame::Array high_output =
        bars[GetInputId("high")].contiguous_array();
    epoch_frame::Array low_output = bars[GetInputId("low")].contiguous_array();

    // Calculate the rolling percentile
    epoch_frame::Array percentile_value =
        value.rolling_agg({.window_size = m_lookback})
            .quantile(m_percentile / 100.0)
            .contiguous_array();

    return epoch_frame::make_dataframe(
        bars.index(),
        {high_output
             .where(value.contiguous_array() >= percentile_value, low_output)
             .as_chunked_array()},
        {GetOutputId()});
  }

private:
  int m_lookback;
  double m_percentile;
};

// Typed PercentileSelect - DRY template pattern
template <typename TypeTag>
class TypedPercentileSelect : public ITransform {
public:
  explicit TypedPercentileSelect(const TransformConfiguration &config)
      : ITransform(config),
        m_lookback(config.GetOptionValue("lookback").GetInteger()),
        m_percentile(config.GetOptionValue("percentile").GetInteger()) {
    AssertFromStream(m_lookback > 0, "Lookback must be greater than 0");
    AssertFromStream(m_percentile >= 0 && m_percentile <= 100,
                     "Percentile must be between 0 and 100");
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;

private:
  int m_lookback;
  double m_percentile;
};

// Type aliases using clear naming convention: percentile_select_{type}
using PercentileSelectString = TypedPercentileSelect<StringType>;
using PercentileSelectNumber = TypedPercentileSelect<NumberType>;
using PercentileSelectBoolean = TypedPercentileSelect<BooleanType>;
using PercentileSelectTimestamp = TypedPercentileSelect<TimestampType>;

// Extern template declarations to reduce compilation time
extern template class TypedPercentileSelect<StringType>;
extern template class TypedPercentileSelect<NumberType>;
extern template class TypedPercentileSelect<BooleanType>;
extern template class TypedPercentileSelect<TimestampType>;

// BooleanBranch takes a single boolean input and splits data into two outputs
class BooleanBranch : public ITransform {
public:
  explicit BooleanBranch(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    using namespace epoch_frame;

    // Get the condition and value series
    Series truth = bars[GetInputId()];
    Series false_mask = !truth;

    return make_dataframe(bars.index(), {truth.array(), false_mask.array()},
                          {GetOutputId("true"), GetOutputId("false")});
  }
};
;

// RatioBranch outputs signals based on the ratio between two values
class RatioBranch : public ITransform {
public:
  explicit RatioBranch(const TransformConfiguration &config)
      : ITransform(config) {
    // Get thresholds for ratio comparison
    m_threshold_high =
        config.GetOptionValue("threshold_high").GetDecimal();
    m_threshold_low = config.GetOptionValue("threshold_low").GetDecimal();
    AssertFromStream(m_threshold_high > m_threshold_low,
                     "Threshold high must be greater than threshold low");
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    using namespace epoch_frame;

    // Get the numerator and denominator
    Series ratio = bars[GetInputId("ratio")];

    // Create output masks based on ratio thresholds
    Series high = ratio > Scalar(m_threshold_high);
    Series normal = (ratio >= Scalar(m_threshold_low)) &&
                    (ratio <= Scalar(m_threshold_high));
    Series low = ratio < Scalar(m_threshold_low);

    // Return DataFrame with all outputs
    return make_dataframe(
        bars.index(), {high.array(), normal.array(), low.array()},
        {GetOutputId("high"), GetOutputId("normal"), GetOutputId("low")});
  }

private:
  double m_threshold_high;
  double m_threshold_low;
};

// FirstNonNull (Coalesce) - returns first non-null value from varargs inputs
class FirstNonNullTransform : public ITransform {
public:
  explicit FirstNonNullTransform(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

// Typed FirstNonNull - DRY template pattern
template <typename TypeTag>
class TypedFirstNonNull : public ITransform {
public:
  explicit TypedFirstNonNull(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

// Type aliases using clear naming convention: first_non_null_{type}
using FirstNonNullString = TypedFirstNonNull<StringType>;
using FirstNonNullNumber = TypedFirstNonNull<NumberType>;
using FirstNonNullBoolean = TypedFirstNonNull<BooleanType>;
using FirstNonNullTimestamp = TypedFirstNonNull<TimestampType>;

// Extern template declarations to reduce compilation time
extern template class TypedFirstNonNull<StringType>;
extern template class TypedFirstNonNull<NumberType>;
extern template class TypedFirstNonNull<BooleanType>;
extern template class TypedFirstNonNull<TimestampType>;

// ConditionalSelect (Case When) - SQL-style multi-condition selector
class ConditionalSelectTransform : public ITransform {
public:
  explicit ConditionalSelectTransform(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

// Typed ConditionalSelect - DRY template pattern
template <typename TypeTag>
class TypedConditionalSelect : public ITransform {
public:
  explicit TypedConditionalSelect(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override;
};

// Type aliases using clear naming convention: conditional_select_{type}
using ConditionalSelectString = TypedConditionalSelect<StringType>;
using ConditionalSelectNumber = TypedConditionalSelect<NumberType>;
using ConditionalSelectBoolean = TypedConditionalSelect<BooleanType>;
using ConditionalSelectTimestamp = TypedConditionalSelect<TimestampType>;

// Extern template declarations to reduce compilation time
extern template class TypedConditionalSelect<StringType>;
extern template class TypedConditionalSelect<NumberType>;
extern template class TypedConditionalSelect<BooleanType>;
extern template class TypedConditionalSelect<TimestampType>;

} // namespace epoch_script::transform