#pragma once
//
// Rolling Window Iterator for ML Transforms
//
// Provides walk-forward validation window generation for ML transforms.
// Generates train/predict window pairs for rolling or expanding windows.
//
#include <epoch_frame/index.h>
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
 * @brief Window specification for a single ML iteration
 *
 * Extends epoch_frame's WindowBound with prediction semantics:
 * - train_start/train_end: The training window (from epoch_frame)
 * - predict_start/predict_end: The prediction window (rows after training)
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
 * @brief ML Rolling Window Iterator using epoch_frame infrastructure
 *
 * Wraps epoch_frame::window::RollingWindow or ExpandingWindow
 * and adds train/predict semantics for ML walk-forward validation.
 *
 * For Rolling mode:
 *   Window 0: train [0, window_size), predict [window_size, window_size + step_size)
 *   Window 1: train [step_size, window_size + step_size), predict [window_size + step_size, ...)
 *
 * For Expanding mode:
 *   Window 0: train [0, min_window), predict [min_window, min_window + step_size)
 *   Window 1: train [0, min_window + step_size), predict [min_window + step_size, ...)
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

    // Use epoch_frame's window generators to get bounds
    GenerateMLWindowBounds();
  }

  /**
   * @brief Check if there are more windows to process
   */
  [[nodiscard]] bool HasNext() const {
    return m_current_position < m_window_specs.size();
  }

  /**
   * @brief Get next window specification
   */
  WindowSpec Next() {
    if (!HasNext()) {
      throw std::runtime_error("RollingWindowIterator: No more windows");
    }
    return m_window_specs[m_current_position++];
  }

  /**
   * @brief Get total number of windows
   */
  [[nodiscard]] size_t TotalWindows() const { return m_window_specs.size(); }

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
  std::vector<WindowSpec> m_window_specs;

  /**
   * @brief Generate ML window bounds for walk-forward validation
   *
   * For Rolling mode:
   *   Window 0: train [0, window_size), predict [window_size, window_size + step_size)
   *   Window 1: train [step_size, window_size + step_size), predict [window_size + step_size, ...]
   *   ...
   *
   * For Expanding mode:
   *   Window 0: train [0, window_size), predict [window_size, window_size + step_size)
   *   Window 1: train [0, window_size + step_size), predict [window_size + step_size, ...]
   *   ...
   */
  void GenerateMLWindowBounds() {
    // Calculate how many prediction windows we can generate
    // First prediction starts at index window_size
    if (m_window_size >= m_total_rows) {
      return;  // No room for prediction
    }

    const size_t first_predict_idx = m_window_size;
    const size_t num_predict_rows = m_total_rows - first_predict_idx;

    // Number of windows = ceil(num_predict_rows / step_size)
    const size_t num_windows = (num_predict_rows + m_step_size - 1) / m_step_size;

    m_window_specs.reserve(num_windows);

    for (size_t w = 0; w < num_windows; ++w) {
      size_t train_start, train_end, predict_start, predict_end;

      if (m_type == WindowType::Rolling) {
        // Rolling: fixed-size window that slides
        // Window w starts training at w * step_size
        train_start = w * m_step_size;
        train_end = train_start + m_window_size;
      } else {
        // Expanding: train from beginning, window grows
        train_start = 0;
        train_end = m_window_size + w * m_step_size;
      }

      // Prediction starts right after training window
      predict_start = train_end;
      predict_end = std::min(predict_start + m_step_size, m_total_rows);

      // Validate bounds
      if (train_end > m_total_rows || predict_start >= m_total_rows) {
        break;
      }

      WindowSpec spec{
        .train_start = train_start,
        .train_end = train_end,
        .predict_start = predict_start,
        .predict_end = predict_end,
        .iteration_index = m_window_specs.size(),
        .is_final = false
      };

      m_window_specs.push_back(spec);
    }

    // Mark the last window as final
    if (!m_window_specs.empty()) {
      m_window_specs.back().is_final = true;
    }
  }
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
