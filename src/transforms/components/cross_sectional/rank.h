#pragma once
//
// Created by dewe on 4/14/23.
//
#include "epoch_frame/array.h"
#include <epoch_script/transforms/core/itransform.h>
#include <cstdint>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_script/core/constants.h>
#include <arrow/compute/api.h>

namespace epoch_script::transform {
/**
 * @brief Cross-sectional returns operation
 *
 * This transform calculates cross-sectional returns across multiple assets.
 * It computes the mean percentage change across all assets (columns) at each
 * time point, then calculates the cumulative product of these mean returns
 * plus 1.
 *
 * The operation is performed per time step across all assets, and the result
 * is broadcasted back to each asset in the output.
 *
 * Input: DataFrame containing percentage changes for multiple assets
 * Output: DataFrame containing cumulative cross-sectional returns
 */

template <bool ascending = true, bool is_percentile = false>
struct CrossSectionalRankOperation final : ITransform {

  static constexpr auto cmp =
      std::conditional_t<ascending, std::less<double>, std::greater<double>>{};

  explicit CrossSectionalRankOperation(const TransformConfiguration &config)
      : ITransform(config),
        k(static_cast<size_t>(config.GetOptionValue("k").GetInteger())) {
    if constexpr (is_percentile) {
      AssertFromFormat(k > 0 && k <= 100,
                       "k must be between 0 and 100(inclusive)");
    } else {
      AssertFromFormat(k > 0, "k must be greater than 0");
    }
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &scores) const override {
    using namespace epoch_frame;
    auto k_ = GetK(scores.num_cols());
    std::vector<std::size_t> idx(scores.num_cols());
    std::iota(idx.begin(), idx.end(), 0);

    return scores.apply(
        [&](Array const &array) {
          auto ranks = idx;
          auto data = array.to_view<double>();
          auto comp = [&](std::size_t a, std::size_t b) {
            auto lhs = data->Value(int64_t(a));
            auto rhs = data->Value(int64_t(b));
            return cmp(lhs, rhs);
          };

          std::nth_element(ranks.begin(), ranks.begin() + k_ - 1, ranks.end(),
                           comp);

          std::vector<bool> mask(data->length(), false);
          for (size_t i = 0; i < k_; ++i) {
            mask[ranks[i]] = true;
          }
          return Array::FromVector(mask);
        },
        AxisType::Row);
  }

private:
  size_t k;

  constexpr auto GetK(size_t n) const {
    size_t k_{k};
    if constexpr (is_percentile) {
      k_ = static_cast<size_t>(std::ceil((k / 100.0) * n));
    }
    return std::clamp(k_, 1UL, n);
  }
};

using CrossSectionalTopKOperation = CrossSectionalRankOperation<false, false>;
using CrossSectionalBottomKOperation = CrossSectionalRankOperation<true, false>;
using CrossSectionalTopKPercentileOperation =
    CrossSectionalRankOperation<false, true>;
using CrossSectionalBottomKPercentileOperation =
    CrossSectionalRankOperation<true, true>;

/**
 * Cross-Sectional Rank
 *
 * Assigns ordinal ranks (1, 2, 3, ...) to assets at each timestamp.
 * Rank 1 is assigned to the smallest value (ascending) or largest value (descending).
 *
 * Different from top_k/bottom_k which return boolean masks.
 * This returns actual rank positions useful for:
 *   - Factor portfolio construction
 *   - Percentile-based signals
 *   - Relative strength analysis
 *
 * Options:
 *   ascending: If true, lowest value gets rank 1 (default: true)
 */
class CSRank final : public ITransform {
public:
  explicit CSRank(const TransformConfiguration &config)
      : ITransform(config),
        m_ascending(config.GetOptionValue("ascending").GetBoolean()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &df) const override {
    using namespace epoch_frame;

    if (df.empty() || df.num_cols() == 0) {
      throw std::runtime_error("CSRank requires multi-column DataFrame");
    }

    // Use Arrow's built-in rank function (available in Arrow v21+)
    arrow::compute::RankOptions opts(
        m_ascending ? arrow::compute::SortOrder::Ascending
                    : arrow::compute::SortOrder::Descending,
        arrow::compute::NullPlacement::AtEnd,
        arrow::compute::RankOptions::Tiebreaker::First);

    return df.apply(
        [&opts](const Array &row_array) -> Array {
          const Series row_series(row_array.value());

          auto result = arrow::compute::CallFunction(
              "rank", {row_series.contiguous_array().value()}, &opts);

          if (!result.ok()) {
            // If rank fails, return original
            return Array(row_series.array());
          }

          // Convert uint64 ranks to double
          auto rank_array = result->make_array();
          auto cast_result = arrow::compute::Cast(rank_array, arrow::float64());
          if (!cast_result.ok()) {
            return Array(row_series.array());
          }

          return Array(cast_result->make_array());
        },
        AxisType::Row);
  }

private:
  bool m_ascending;
};

/**
 * Cross-Sectional Rank Quantile (Percentile Rank)
 *
 * Assigns percentile ranks (0.0 to 1.0) to assets at each timestamp.
 * Useful for:
 *   - Normalized factor scores
 *   - Quantile-based portfolio construction
 *   - Combining factors with different scales
 *
 * Options:
 *   ascending: If true, lowest value gets 0.0 (default: true)
 */
class CSRankQuantile final : public ITransform {
public:
  explicit CSRankQuantile(const TransformConfiguration &config)
      : ITransform(config),
        m_ascending(config.GetOptionValue("ascending").GetBoolean()) {}

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame &df) const override {
    using namespace epoch_frame;

    if (df.empty() || df.num_cols() == 0) {
      throw std::runtime_error("CSRankQuantile requires multi-column DataFrame");
    }

    // Use Arrow's built-in rank_quantile function (available in Arrow v21+)
    arrow::compute::RankOptions opts(
        m_ascending ? arrow::compute::SortOrder::Ascending
                    : arrow::compute::SortOrder::Descending,
        arrow::compute::NullPlacement::AtEnd,
        arrow::compute::RankOptions::Tiebreaker::First);

    return df.apply(
        [&opts](const Array &row_array) -> Array {
          const Series row_series(row_array.value());

          auto result = arrow::compute::CallFunction(
              "rank_quantile", {row_series.contiguous_array().value()}, &opts);

          if (!result.ok()) {
            // If rank_quantile fails, return original
            return Array(row_series.array());
          }

          return Array(result->make_array());
        },
        AxisType::Row);
  }

private:
  bool m_ascending;
};

// Metadata for cs_rank and cs_rank_quantile
inline std::vector<epoch_script::transforms::TransformsMetaData>
MakeCSRankMetaData() {
  using namespace epoch_script::transforms;
  std::vector<TransformsMetaData> metadataList;

  metadataList.emplace_back(TransformsMetaData{
      .id = "cs_rank",
      .category = epoch_core::TransformCategory::Statistical,
      .plotKind = epoch_core::TransformPlotKind::Null,
      .name = "Cross-Sectional Rank",
      .options =
          {MetaDataOption{
               .id = "ascending",
               .name = "Ascending",
               .type = epoch_core::MetaDataOptionType::Boolean,
               .defaultValue = MetaDataOptionDefinition(true),
               .desc = "If true, lowest value gets rank 1"}},
      .isCrossSectional = true,
      .desc = "Assigns ordinal ranks (1, 2, 3, ...) to assets at each timestamp. "
              "Useful for factor portfolio construction and relative strength analysis.",
      .inputs = {IOMetaDataConstants::DECIMAL_INPUT_METADATA},
      .outputs = {IOMetaDataConstants::DECIMAL_OUTPUT_METADATA},
      .tags = {"cross-sectional", "ranking", "ordinal", "factor"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .strategyTypes = {"research", "trading"},
      .relatedTransforms = {"cs_rank_quantile", "top_k", "bottom_k", "cs_zscore"},
      .assetRequirements = {"multi-asset"}});

  metadataList.emplace_back(TransformsMetaData{
      .id = "cs_rank_quantile",
      .category = epoch_core::TransformCategory::Statistical,
      .plotKind = epoch_core::TransformPlotKind::Null,
      .name = "Cross-Sectional Rank Quantile",
      .options =
          {MetaDataOption{
               .id = "ascending",
               .name = "Ascending",
               .type = epoch_core::MetaDataOptionType::Boolean,
               .defaultValue = MetaDataOptionDefinition(true),
               .desc = "If true, lowest value gets 0.0, highest gets 1.0"}},
      .isCrossSectional = true,
      .desc = "Assigns percentile ranks (0.0 to 1.0) to assets at each timestamp. "
              "Useful for normalized factor scores and quantile-based portfolios.",
      .inputs = {IOMetaDataConstants::DECIMAL_INPUT_METADATA},
      .outputs = {IOMetaDataConstants::DECIMAL_OUTPUT_METADATA},
      .tags = {"cross-sectional", "ranking", "percentile", "quantile", "factor"},
      .requiresTimeFrame = false,
      .allowNullInputs = true,
      .strategyTypes = {"research", "trading"},
      .relatedTransforms = {"cs_rank", "top_k_percent", "bottom_k_percent", "cs_zscore"},
      .assetRequirements = {"multi-asset"}});

  return metadataList;
}

} // namespace epoch_script::transform
