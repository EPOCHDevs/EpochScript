//
// Created by Claude Code
// Alias - Compiler-inserted transform to create unique column identifiers
// for variable assignments that reference the same source column
//

#pragma once
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/series.h>
#include <epoch_frame/factory/dataframe_factory.h>

namespace epoch_script::transform {

// Typed Alias transforms - pass input through unchanged but with a new column identifier
// This allows multiple variables to reference the same source column while
// maintaining unique column identifiers (e.g., pe#result, ps#result, pb#result
// all referencing src#price_to_earnings)

// AliasDecimal - for Decimal/Number types
class AliasDecimal : public ITransform {
public:
  explicit AliasDecimal(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    epoch_frame::Series input = bars[GetInputId()];
    return MakeResult(input);
  }
};

// AliasBoolean - for Boolean types
class AliasBoolean : public ITransform {
public:
  explicit AliasBoolean(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    epoch_frame::Series input = bars[GetInputId()];
    return MakeResult(input);
  }
};

// AliasString - for String types
class AliasString : public ITransform {
public:
  explicit AliasString(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    epoch_frame::Series input = bars[GetInputId()];
    return MakeResult(input);
  }
};

// AliasInteger - for Integer types
class AliasInteger : public ITransform {
public:
  explicit AliasInteger(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    epoch_frame::Series input = bars[GetInputId()];
    return MakeResult(input);
  }
};

// AliasTimestamp - for Timestamp types
class AliasTimestamp : public ITransform {
public:
  explicit AliasTimestamp(const TransformConfiguration &config)
      : ITransform(config) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &bars) const override {
    epoch_frame::Series input = bars[GetInputId()];
    return MakeResult(input);
  }
};

} // namespace epoch_script::transform
