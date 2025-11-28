#pragma once
//
// Created by dewe on 4/14/23.
//
#include "epoch_frame/dataframe.h"
#include "epoch_frame/factory/table_factory.h"
#include "../../core/bar_attribute.h"
#include "itransform.h"

#include <glaze/json/read.hpp>

namespace epoch_script::transform
{
  // Generic resampler that handles all column types based on name and type
  inline epoch_frame::DataFrame
  resample_generic(epoch_frame::DataFrame const &df,
                   epoch_frame::DateOffsetHandlerPtr const &offset)
  {
    const auto C = epoch_script::EpochStratifyXConstants::instance();

    // Create lambda for generic resampling
    auto generic_resample_fn = [&](epoch_frame::DataFrame const& group_df) {
      arrow::ArrayVector array_list;
      std::vector<arrow::FieldPtr> fields;

      // Iterate through all columns in the dataframe
      auto schema = group_df.column_names();
      for (const auto& col_name : schema) {
        auto field = df.table()->schema()->GetFieldByName(col_name);
        arrow::ScalarPtr aggregated_value;

        // Priority 1: Check for OHLCV column names (using predefined constants)
        if (col_name == "o") { // open
          aggregated_value = group_df[col_name].iloc(0).value(); // first
        }
        else if (col_name == "h") { // high
          aggregated_value = group_df[col_name].max().value(); // max
        }
        else if (col_name == "l") { // low
          aggregated_value = group_df[col_name].min().value(); // min
        }
        else if (col_name == "c") { // close
          aggregated_value = group_df[col_name].iloc(-1).value(); // last
        }
        else if (col_name == "v") { // volume
          aggregated_value = group_df[col_name].sum().value(); // sum
        }
        // Priority 2: Check for special column names (only if they exist)
        else if (col_name == "vw") { // volume-weighted average
          aggregated_value = group_df[col_name].mean().value(); // average
        }
        else if (col_name == "n") { // count/number
          aggregated_value = group_df[col_name].sum().value(); // sum
        }
        else {
          // Default: take last non-null value (handles sparse data like economic indicators)
          auto non_null_series = group_df[col_name].drop_null();
          if (non_null_series.size() > 0) {
            aggregated_value = non_null_series.iloc(-1).value();
          } else {
            aggregated_value = group_df[col_name].iloc(-1).value();
          }
        }

        array_list.emplace_back(
            arrow::MakeArrayFromScalar(*aggregated_value, 1).MoveValueUnsafe());
        fields.emplace_back(field);
      }

      return arrow::Table::Make(arrow::schema(fields), array_list, 1);
    };

    return df.resample_by_apply({.freq = offset,
                                 .closed = epoch_core::GrouperClosedType::Right,
                                 .label = epoch_core::GrouperLabelType::Right})
             .apply(generic_resample_fn);
  }

  // Legacy function - now calls generic resampler
  inline epoch_frame::DataFrame
  resample_ohlcv(epoch_frame::DataFrame const &df,
                 epoch_frame::DateOffsetHandlerPtr const &offset)
  {
    return resample_generic(df, offset);
  }

  // TODO: Accept Option objects
  struct BarResampler
  {
    explicit BarResampler(
        const epoch_script::transform::TransformConfiguration &config)
    {
      auto interval = config.GetOptionValue("interval").GetInteger();
      glz::generic json;
      json["interval"] = interval;
      json["type"] = config.GetOptionValue("type").GetSelectOption();
      json["weekday"] = "Sunday";

      auto error = glz::read_json(m_timeframe, json.dump().value());
      if (error)
      {
        throw std::runtime_error(std::format("Failed to read timeframe: {}",
                                             glz::format_error(error)));
      }
    }

    [[nodiscard]] epoch_frame::DataFrame
    TransformData(epoch_frame::DataFrame const &bars) const
    {
      return resample_ohlcv(bars, m_timeframe->GetOffset());
    }

  private:
    std::optional<epoch_script::TimeFrame> m_timeframe;
  };
} // namespace epoch_script::transform
