#pragma once
//
// ML Dataset Splitting Utilities
//
// Provides zero-copy train/test splitting using DataFrame iloc.
//
#include "epoch_frame/dataframe.h"
#include <cmath>
#include <stdexcept>

namespace epoch_script::transform::ml_utils {

/**
 * @brief Result of a train/test split operation
 *
 * Contains views into the original DataFrame (zero-copy via iloc).
 */
struct TrainTestSplit {
  epoch_frame::DataFrame train;
  epoch_frame::DataFrame test;
  size_t train_size;
  size_t test_size;
};

/**
 * @brief Split DataFrame by ratio (percentage-based)
 *
 * First split_ratio portion for training, remainder for testing.
 * Uses iloc for zero-copy views.
 *
 * @param df Source DataFrame
 * @param split_ratio Fraction for training (0.0 < ratio <= 1.0)
 * @return TrainTestSplit with train and test DataFrames
 *
 * @throws std::runtime_error if split_ratio is invalid
 *
 * Example:
 *   auto [train, test, _, _] = split_by_ratio(df, 0.7);
 *   // train = first 70% of rows
 *   // test = last 30% of rows
 */
inline TrainTestSplit split_by_ratio(const epoch_frame::DataFrame &df,
                                     double split_ratio) {
  if (split_ratio <= 0.0 || split_ratio > 1.0) {
    throw std::runtime_error("split_ratio must be in (0, 1]");
  }

  const size_t n_rows = df.num_rows();
  const size_t train_end = static_cast<size_t>(std::ceil(n_rows * split_ratio));

  if (train_end == 0) {
    throw std::runtime_error("Training set would be empty with given split_ratio");
  }

  // iloc uses UnResolvedIntegerSliceBound: {start, stop, step}
  auto train = df.iloc({0, static_cast<int64_t>(train_end), std::nullopt});

  // Test set: from train_end to end
  // If split_ratio == 1.0, test will be empty
  epoch_frame::DataFrame test;
  size_t test_size = 0;

  if (train_end < n_rows) {
    test = df.iloc({static_cast<int64_t>(train_end), std::nullopt, std::nullopt});
    test_size = n_rows - train_end;
  } else {
    // Empty test set - create empty DataFrame with same schema
    test = df.iloc({0, 0, std::nullopt});
    test_size = 0;
  }

  return TrainTestSplit{
      .train = train,
      .test = test,
      .train_size = train_end,
      .test_size = test_size
  };
}

/**
 * @brief Split DataFrame by fixed training window size
 *
 * First train_size rows for training, remainder for testing.
 * Uses iloc for zero-copy views.
 *
 * @param df Source DataFrame
 * @param train_size Number of rows for training
 * @return TrainTestSplit with train and test DataFrames
 *
 * @throws std::runtime_error if train_size > df.num_rows()
 *
 * Example:
 *   auto [train, test, _, _] = split_by_count(df, 1000);
 *   // train = first 1000 rows
 *   // test = remaining rows
 */
inline TrainTestSplit split_by_count(const epoch_frame::DataFrame &df,
                                     size_t train_size) {
  const size_t n_rows = df.num_rows();

  if (train_size > n_rows) {
    throw std::runtime_error("train_size (" + std::to_string(train_size) +
                             ") exceeds DataFrame rows (" + std::to_string(n_rows) + ")");
  }

  if (train_size == 0) {
    throw std::runtime_error("train_size must be > 0");
  }

  auto train = df.iloc({0, static_cast<int64_t>(train_size), std::nullopt});

  epoch_frame::DataFrame test;
  size_t test_size = 0;

  if (train_size < n_rows) {
    test = df.iloc({static_cast<int64_t>(train_size), std::nullopt, std::nullopt});
    test_size = n_rows - train_size;
  } else {
    test = df.iloc({0, 0, std::nullopt});
    test_size = 0;
  }

  return TrainTestSplit{
      .train = train,
      .test = test,
      .train_size = train_size,
      .test_size = test_size
  };
}

/**
 * @brief Split DataFrame with gap (purge) between train and test
 *
 * Useful for preventing data leakage when features have lookback.
 *
 * @param df Source DataFrame
 * @param split_ratio Fraction for training (before gap)
 * @param gap Number of rows to skip between train and test
 * @return TrainTestSplit (train and test with gap in between)
 *
 * Example:
 *   auto [train, test, _, _] = split_by_ratio_with_gap(df, 0.6, 20);
 *   // train = first 60% of rows
 *   // gap = 20 rows skipped
 *   // test = remaining rows after gap
 */
inline TrainTestSplit split_by_ratio_with_gap(const epoch_frame::DataFrame &df,
                                              double split_ratio,
                                              size_t gap) {
  if (split_ratio <= 0.0 || split_ratio >= 1.0) {
    throw std::runtime_error("split_ratio must be in (0, 1) for gap split");
  }

  const size_t n_rows = df.num_rows();
  const size_t train_end = static_cast<size_t>(std::ceil(n_rows * split_ratio));
  const size_t test_start = train_end + gap;

  if (test_start >= n_rows) {
    throw std::runtime_error("Gap too large: no test data remaining");
  }

  auto train = df.iloc({0, static_cast<int64_t>(train_end), std::nullopt});
  auto test = df.iloc({static_cast<int64_t>(test_start), std::nullopt, std::nullopt});

  return TrainTestSplit{
      .train = train,
      .test = test,
      .train_size = train_end,
      .test_size = n_rows - test_start
  };
}

/**
 * @brief Get train DataFrame only (for fit-only operations)
 *
 * Convenience function when you only need training data.
 *
 * @param df Source DataFrame
 * @param split_ratio Fraction for training
 * @return Training DataFrame view
 */
inline epoch_frame::DataFrame get_train(const epoch_frame::DataFrame &df,
                                        double split_ratio) {
  return split_by_ratio(df, split_ratio).train;
}

/**
 * @brief Get train DataFrame by count
 *
 * @param df Source DataFrame
 * @param train_size Number of training rows
 * @return Training DataFrame view
 */
inline epoch_frame::DataFrame get_train(const epoch_frame::DataFrame &df,
                                        size_t train_size) {
  return split_by_count(df, train_size).train;
}

} // namespace epoch_script::transform::ml_utils
