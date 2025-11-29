//
// Created by adesola on 4/16/25.
//

#pragma once

#include <epoch_script/core/bar_attribute.h>
#include <epoch_script/transforms/core/itransform.h>
#include <epoch_frame/dataframe.h>
#include <epoch_frame/index.h>
#include <epoch_frame/series.h>
#include <span>

namespace epoch_script::transform {
template <typename T = double> class SeriesSpan {

public:
  SeriesSpan(epoch_frame::Series const &s)
      : _arr(s.contiguous_array().to_view<T>()) {
    _span = std::span<const double>(_arr->raw_values(), s.size());
  }

  SeriesSpan(epoch_frame::DataFrame const &df, std::string const &col_name)
      : SeriesSpan(df[col_name]) {}

  auto begin() const { return _span.begin(); }

  auto end() const { return _span.end(); }

private:
  std::shared_ptr<typename arrow::CTypeTraits<T>::ArrayType> _arr;
  std::span<const double> _span;
};

struct HighSpan : SeriesSpan<> {
  explicit HighSpan(epoch_frame::DataFrame const &df)
      : SeriesSpan(df,
                   epoch_script::EpochStratifyXConstants::instance().HIGH()) {
  }
};

struct LowSpan : SeriesSpan<> {
  explicit LowSpan(epoch_frame::DataFrame const &df)
      : SeriesSpan(df,
                   epoch_script::EpochStratifyXConstants::instance().LOW()) {}
};

struct CloseSpan : SeriesSpan<> {
  explicit CloseSpan(epoch_frame::DataFrame const &df)
      : SeriesSpan(
            df, epoch_script::EpochStratifyXConstants::instance().CLOSE()) {}
};

struct OpenSpan : SeriesSpan<> {
  explicit OpenSpan(epoch_frame::DataFrame const &df)
      : SeriesSpan(df,
                   epoch_script::EpochStratifyXConstants::instance().OPEN()) {
  }
};

struct VolumeSpan : SeriesSpan<> {
  explicit VolumeSpan(epoch_frame::DataFrame const &df)
      : SeriesSpan(
            df, epoch_script::EpochStratifyXConstants::instance().VOLUME()) {}
};

class IndexSpan {
public:
  explicit IndexSpan(epoch_frame::DataFrame const &df)
      : _span(df.index()->array().to_timestamp_view()->raw_values(),
              df.size()) {}
  explicit IndexSpan(epoch_frame::Series const &df)
      : _span(df.index()->array().to_timestamp_view()->raw_values(),
              df.size()) {}

  auto begin() const { return _span.begin(); }

  auto end() const { return _span.end(); }

private:
  std::span<const int64_t> _span;
};

void run_visit(auto const &df, auto &visitor, auto const &arg0) {
  const IndexSpan indexSpan{df};

  visitor.pre();
  visitor(indexSpan.begin(), indexSpan.end(), arg0.begin(), arg0.end());
  visitor.post();
}

void run_visit(auto const &df, auto &visitor, auto const &arg0,
               auto const &arg1) {
  const IndexSpan indexSpan{df};

  visitor.pre();
  visitor(indexSpan.begin(), indexSpan.end(), arg0.begin(), arg0.end(),
          arg1.begin(), arg1.end());
  visitor.post();
}

void run_visit(auto const &df, auto &visitor, auto const &arg0,
               auto const &arg1, auto const &arg2) {
  const IndexSpan indexSpan{df};

  visitor.pre();
  visitor(indexSpan.begin(), indexSpan.end(), arg0.begin(), arg0.end(),
          arg1.begin(), arg1.end(), arg2.begin(), arg2.end());
  visitor.post();
}

void run_visit(auto const &df, auto &visitor, auto const &arg0,
               auto const &arg1, auto const &arg2, auto const &arg3) {
  const IndexSpan indexSpan{df};

  visitor.pre();
  visitor(indexSpan.begin(), indexSpan.end(), arg0.begin(), arg0.end(),
          arg1.begin(), arg1.end(), arg2.begin(), arg2.end(), arg3.begin(),
          arg3.end());
  visitor.post();
}

void run_visit(auto const &df, auto &visitor, auto const &arg0,
               auto const &arg1, auto const &arg2, auto const &arg3,
               auto const &arg4) {
  const IndexSpan indexSpan{df};

  visitor.pre();
  visitor(indexSpan.begin(), indexSpan.end(), arg0.begin(), arg0.end(),
          arg1.begin(), arg1.end(), arg2.begin(), arg2.end(), arg3.begin(),
          arg3.end(), arg4.begin(), arg4.end());
  visitor.post();
}

template <class Visitor, class... Spans>
class SingleResultHMDFTransform : public ITransform {
public:
  SingleResultHMDFTransform(TransformConfiguration const &config,
                            Visitor visitor)
      : ITransform(config), m_visitor_template(std::move(visitor)) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(epoch_frame::DataFrame const &df) const final {
    // Create local visitor copy to avoid state accumulation across assets
    Visitor visitor = m_visitor_template;

    run_visit(df, visitor, Spans{df}...);

    return make_dataframe(df.index(),
                          std::vector{epoch_frame::factory::array::make_array(
                              visitor.get_result())},
                          {GetOutputId("result")});
  }

private:
  Visitor m_visitor_template;  // Template visitor, copied for each transform call
};
} // namespace epoch_script::transform
