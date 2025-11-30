#pragma once
//
// Rolling Window Iterator for ML Transforms
//
// Provides shared window iteration logic for all rolling ML transforms.
// Supports both rolling (fixed window) and expanding (cumulative) modes.
//
#include "epoch_frame/dataframe.h"
#include <cstddef>
#include <vector>
#include <functional>
#include <stdexcept>

namespace epoch_script::transform::ml_utils {

/**
 * @brief Window type for rolling ML transforms
 */
enum class WindowType {
  Rolling,   // Fixed-size window that slides (train on [t-window, t])
  Expanding  // Cumulative window from start (train on [0, t])
};

/**
 * @brief Window specification for a single iteration
 */
struct WindowSpec {
  size_t train_start;      // Start index of training window (inclusive)
  size_t train_end;        // End index of training window (exclusive)
  size_t predict_start;    // Start index for prediction (inclusive)
  size_t predict_end;      // End index for prediction (exclusive)
  size_t iteration_index;  // Which iteration this is (0-indexed)
  bool is_final;           // Is this the last window?

  /**
   * @brief Get training window size
   */
  [[nodiscard]] size_t TrainSize() const { return train_end - train_start; }

  /**
   * @brief Get prediction window size
   */
  [[nodiscard]] size_t PredictSize() const { return predict_end - predict_start; }
};

/**
 * @brief Generates rolling/expanding window specifications
 *
 * This is the shared iteration logic used by all rolling ML transforms.
 * Avoids duplication of the window calculation loop.
 *
 * For Rolling mode:
 *   Window 0: train [0, window_size), predict [window_size, window_size + step_size)
 *   Window 1: train [step_size, window_size + step_size), predict [window_size + step_size, ...)
 *   ...
 *
 * For Expanding mode:
 *   Window 0: train [0, min_window), predict [min_window, min_window + step_size)
 *   Window 1: train [0, min_window + step_size), predict [min_window + step_size, ...)
 *   ...
 */
class RollingWindowIterator {
public:
  /**
   * @brief Construct a rolling window iterator
   *
   * @param total_rows Total number of rows in the dataset
   * @param window_size For Rolling: window size; For Expanding: minimum window
   * @param step_size How many rows to advance per iteration (default 1)
   * @param type WindowType::Rolling or WindowType::Expanding
   */
  RollingWindowIterator(
    size_t total_rows,
    size_t window_size,
    size_t step_size = 1,
    WindowType type = WindowType::Rolling
  ) : m_total_rows(total_rows),
      m_window_size(window_size),
      m_step_size(step_size),
      m_type(type),
      m_current_position(0) {

    if (window_size == 0) {
      throw std::runtime_error("RollingWindowIterator: window_size must be > 0");
    }
    if (step_size == 0) {
      throw std::runtime_error("RollingWindowIterator: step_size must be > 0");
    }
    if (window_size > total_rows) {
      throw std::runtime_error("RollingWindowIterator: window_size (" +
                               std::to_string(window_size) + ") exceeds total_rows (" +
                               std::to_string(total_rows) + ")");
    }

    // Calculate total number of windows
    size_t remaining = total_rows - window_size;
    m_total_windows = (remaining / step_size) + 1;
    if (remaining % step_size != 0) {
      m_total_windows++;  // Partial final window
    }
  }

  /**
   * @brief Check if there are more windows to process
   */
  [[nodiscard]] bool HasNext() const {
    return m_current_position < m_total_windows;
  }

  /**
   * @brief Get next window specification
   */
  WindowSpec Next() {
    if (!HasNext()) {
      throw std::runtime_error("RollingWindowIterator: No more windows");
    }

    WindowSpec spec;
    spec.iteration_index = m_current_position;

    size_t offset = m_current_position * m_step_size;

    if (m_type == WindowType::Rolling) {
      // Fixed-size sliding window
      spec.train_start = offset;
      spec.train_end = offset + m_window_size;
    } else {
      // Expanding window from start
      spec.train_start = 0;
      spec.train_end = m_window_size + offset;
    }

    // Prediction window starts after training ends
    spec.predict_start = spec.train_end;
    spec.predict_end = std::min(spec.predict_start + m_step_size, m_total_rows);

    // Handle edge case: last window may have fewer prediction points
    if (spec.predict_end > m_total_rows) {
      spec.predict_end = m_total_rows;
    }

    spec.is_final = (m_current_position == m_total_windows - 1);

    m_current_position++;
    return spec;
  }

  /**
   * @brief Get total number of windows
   */
  [[nodiscard]] size_t TotalWindows() const { return m_total_windows; }

  /**
   * @brief Get window size
   */
  [[nodiscard]] size_t GetWindowSize() const { return m_window_size; }

  /**
   * @brief Get step size
   */
  [[nodiscard]] size_t GetStepSize() const { return m_step_size; }

  /**
   * @brief Get window type
   */
  [[nodiscard]] WindowType GetWindowType() const { return m_type; }

  /**
   * @brief Reset iterator to beginning
   */
  void Reset() { m_current_position = 0; }

  /**
   * @brief Iterate over all windows, calling callback for each
   *
   * @param callback Function(const WindowSpec&) -> void
   */
  template<typename Callback>
  void ForEach(Callback&& callback) {
    Reset();
    while (HasNext()) {
      callback(Next());
    }
  }

private:
  size_t m_total_rows;
  size_t m_window_size;
  size_t m_step_size;
  WindowType m_type;
  size_t m_current_position;
  size_t m_total_windows;
};

/**
 * @brief Result accumulator for rolling predictions
 *
 * Handles the complexity of collecting predictions from multiple windows
 * and combining them into a single output. Supports step_size > 1 where
 * predictions may cover multiple rows per window.
 */
class RollingOutputBuilder {
public:
  /**
   * @brief Initialize with expected output size
   *
   * @param total_rows Total number of rows in output
   * @param first_predict_idx Index where predictions start (after initial training window)
   */
  explicit RollingOutputBuilder(size_t total_rows, size_t first_predict_idx)
    : m_total_rows(total_rows),
      m_first_predict_idx(first_predict_idx) {}

  /**
   * @brief Get the starting index for predictions
   */
  [[nodiscard]] size_t GetFirstPredictIndex() const { return m_first_predict_idx; }

  /**
   * @brief Get total output rows (excludes initial training window)
   */
  [[nodiscard]] size_t GetOutputRows() const {
    return m_total_rows - m_first_predict_idx;
  }

  /**
   * @brief Slice index for output DataFrame
   *
   * Returns the index slice from first_predict_idx to end
   */
  [[nodiscard]] static epoch_frame::IndexPtr SliceOutputIndex(
    const epoch_frame::IndexPtr& full_index,
    size_t first_predict_idx) {
    return full_index->iloc({
      static_cast<int64_t>(first_predict_idx),
      std::nullopt,
      std::nullopt
    });
  }

private:
  size_t m_total_rows;
  size_t m_first_predict_idx;
};

/**
 * @brief Convert WindowType string to enum
 */
inline WindowType ParseWindowType(const std::string& type_str) {
  if (type_str == "expanding") {
    return WindowType::Expanding;
  }
  return WindowType::Rolling;  // Default
}

/**
 * @brief Convert WindowType enum to string
 */
inline std::string WindowTypeToString(WindowType type) {
  switch (type) {
    case WindowType::Expanding: return "expanding";
    case WindowType::Rolling: return "rolling";
  }
  return "rolling";
}

} // namespace epoch_script::transform::ml_utils
