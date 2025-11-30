#pragma once
//
// CRTP Base Class for Rolling ML Transforms
//
// Provides common rolling infrastructure that all rolling ML transforms inherit.
// Uses CRTP (Curiously Recurring Template Pattern) for static polymorphism,
// avoiding virtual dispatch overhead in the hot loop.
//
#include <epoch_script/transforms/core/itransform.h>
#include "rolling_window_iterator.h"
#include "../statistics/dataframe_armadillo_utils.h"
#include <epoch_frame/factory/array_factory.h>
#include <epoch_frame/factory/dataframe_factory.h>
#include <armadillo>
#include <vector>
#include <string>

namespace epoch_script::transform {

/**
 * @brief Common configuration for all rolling ML transforms
 */
struct RollingMLConfig {
  size_t window_size{252};          // Training window size (rolling) or min window (expanding)
  size_t step_size{1};              // How many rows to advance per window
  ml_utils::WindowType window_type{ml_utils::WindowType::Rolling};
  size_t min_training_samples{100}; // Minimum samples required
};

/**
 * @brief CRTP Base class for Rolling ML Transforms (Unsupervised)
 *
 * Provides:
 * - Common rolling window configuration parsing
 * - The main rolling loop infrastructure
 * - Output accumulation pattern
 *
 * Derived classes must implement:
 * - ModelType TrainModel(const arma::mat& X) const
 * - void Predict(const ModelType& model, const arma::mat& X,
 *                const ml_utils::WindowSpec& window, OutputVectors& outputs) const
 * - std::vector<std::string> GetOutputColumnNames() const
 * - void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const
 *
 * Template Parameters:
 *   Derived - The derived class (CRTP pattern)
 *   ModelType - The type of trained model (e.g., arma::mat for centroids, mlpack::GMM)
 */
template<typename Derived, typename ModelType>
class RollingMLBaseUnsupervised : public ITransform {
public:
  explicit RollingMLBaseUnsupervised(const TransformConfiguration& cfg)
    : ITransform(cfg) {
    // Parse common rolling options
    m_config.window_size = static_cast<size_t>(
      cfg.GetOptionValue("window_size",
                         MetaDataOptionDefinition{252.0}).GetInteger());

    m_config.step_size = static_cast<size_t>(
      cfg.GetOptionValue("step_size",
                         MetaDataOptionDefinition{1.0}).GetInteger());

    auto window_type_str = cfg.GetOptionValue(
      "window_type", MetaDataOptionDefinition{std::string("rolling")}).GetString();
    m_config.window_type = ml_utils::ParseWindowType(window_type_str);

    m_config.min_training_samples = static_cast<size_t>(
      cfg.GetOptionValue("min_training_samples",
                         MetaDataOptionDefinition{100.0}).GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame& bars) const override {
    const auto& derived = static_cast<const Derived&>(*this);

    // Get input columns
    const auto cols = GetInputIds();
    if (cols.empty()) {
      throw std::runtime_error("Rolling ML requires at least one input column");
    }

    // Convert to armadillo matrix
    arma::mat X = utils::MatFromDataFrame(bars, cols);
    const size_t n_rows = X.n_rows;

    // Validate minimum data
    if (n_rows < m_config.window_size) {
      throw std::runtime_error("Insufficient data for rolling ML. Required window_size: " +
                               std::to_string(m_config.window_size) +
                               ", Got: " + std::to_string(n_rows));
    }

    if (m_config.window_size < m_config.min_training_samples) {
      throw std::runtime_error("window_size (" + std::to_string(m_config.window_size) +
                               ") must be >= min_training_samples (" +
                               std::to_string(m_config.min_training_samples) + ")");
    }

    // Create rolling window iterator
    ml_utils::RollingWindowIterator iterator(
      n_rows,
      m_config.window_size,
      m_config.step_size,
      m_config.window_type
    );

    // Get output column names from derived class
    auto output_names = derived.GetOutputColumnNames();

    // Calculate output size (rows after initial training window)
    const size_t output_rows = n_rows - m_config.window_size;

    // Initialize output vectors from derived class
    typename Derived::OutputVectors outputs;
    derived.InitializeOutputVectors(outputs, output_rows);

    // Track which output rows have been filled
    size_t output_offset = 0;

    // Main rolling loop
    iterator.ForEach([&](const ml_utils::WindowSpec& window) {
      // Extract training data
      arma::mat train_X = X.rows(window.train_start, window.train_end - 1);

      // Train model (calls derived class)
      ModelType model = derived.TrainModel(train_X);

      // Extract prediction data
      if (window.predict_start < n_rows && window.predict_start < window.predict_end) {
        arma::mat predict_X = X.rows(window.predict_start,
                                      std::min(window.predict_end, n_rows) - 1);

        // Predict and fill outputs (calls derived class)
        derived.Predict(model, predict_X, window, outputs, output_offset);

        output_offset += predict_X.n_rows;
      }
    });

    // Build output DataFrame
    epoch_frame::IndexPtr output_index = ml_utils::RollingOutputBuilder::SliceOutputIndex(
      bars.index(), m_config.window_size);

    return derived.BuildOutputDataFrame(output_index, outputs, output_names);
  }

protected:
  RollingMLConfig m_config;

  /**
   * @brief Get rolling configuration
   */
  [[nodiscard]] const RollingMLConfig& GetRollingConfig() const { return m_config; }
};

/**
 * @brief CRTP Base class for Rolling ML Transforms (Supervised)
 *
 * Similar to unsupervised but handles target column (y).
 *
 * Derived classes must implement:
 * - ModelType TrainModel(const arma::mat& X, const arma::vec& y) const
 * - void Predict(const ModelType& model, const arma::mat& X,
 *                const ml_utils::WindowSpec& window, OutputVectors& outputs) const
 * - std::vector<std::string> GetOutputColumnNames() const
 * - void InitializeOutputVectors(OutputVectors& outputs, size_t n_rows) const
 */
template<typename Derived, typename ModelType>
class RollingMLBaseSupervised : public ITransform {
public:
  explicit RollingMLBaseSupervised(const TransformConfiguration& cfg)
    : ITransform(cfg) {
    // Parse common rolling options
    m_config.window_size = static_cast<size_t>(
      cfg.GetOptionValue("window_size",
                         MetaDataOptionDefinition{252.0}).GetInteger());

    m_config.step_size = static_cast<size_t>(
      cfg.GetOptionValue("step_size",
                         MetaDataOptionDefinition{1.0}).GetInteger());

    auto window_type_str = cfg.GetOptionValue(
      "window_type", MetaDataOptionDefinition{std::string("rolling")}).GetString();
    m_config.window_type = ml_utils::ParseWindowType(window_type_str);

    m_config.min_training_samples = static_cast<size_t>(
      cfg.GetOptionValue("min_training_samples",
                         MetaDataOptionDefinition{100.0}).GetInteger());
  }

  [[nodiscard]] epoch_frame::DataFrame
  TransformData(const epoch_frame::DataFrame& bars) const override {
    const auto& derived = static_cast<const Derived&>(*this);

    // Get feature columns
    const auto feature_cols = GetInputIds();
    if (feature_cols.empty()) {
      throw std::runtime_error("Rolling supervised ML requires at least one feature input");
    }

    // Get target column
    const auto target_col = GetInputId("target");

    // Convert to armadillo
    arma::mat X = utils::MatFromDataFrame(bars, feature_cols);
    arma::vec y = utils::VecFromDataFrame(bars, target_col);
    const size_t n_rows = X.n_rows;

    // Validate minimum data
    if (n_rows < m_config.window_size) {
      throw std::runtime_error("Insufficient data for rolling ML. Required window_size: " +
                               std::to_string(m_config.window_size) +
                               ", Got: " + std::to_string(n_rows));
    }

    // Create rolling window iterator
    ml_utils::RollingWindowIterator iterator(
      n_rows,
      m_config.window_size,
      m_config.step_size,
      m_config.window_type
    );

    // Get output column names from derived class
    auto output_names = derived.GetOutputColumnNames();

    // Calculate output size
    const size_t output_rows = n_rows - m_config.window_size;

    // Initialize output vectors from derived class
    typename Derived::OutputVectors outputs;
    derived.InitializeOutputVectors(outputs, output_rows);

    // Track which output rows have been filled
    size_t output_offset = 0;

    // Main rolling loop
    iterator.ForEach([&](const ml_utils::WindowSpec& window) {
      // Extract training data
      arma::mat train_X = X.rows(window.train_start, window.train_end - 1);
      arma::vec train_y = y.subvec(window.train_start, window.train_end - 1);

      // Train model (calls derived class)
      ModelType model = derived.TrainModel(train_X, train_y);

      // Extract prediction data
      if (window.predict_start < n_rows && window.predict_start < window.predict_end) {
        arma::mat predict_X = X.rows(window.predict_start,
                                      std::min(window.predict_end, n_rows) - 1);

        // Predict and fill outputs (calls derived class)
        derived.Predict(model, predict_X, window, outputs, output_offset);

        output_offset += predict_X.n_rows;
      }
    });

    // Build output DataFrame
    epoch_frame::IndexPtr output_index = ml_utils::RollingOutputBuilder::SliceOutputIndex(
      bars.index(), m_config.window_size);

    return derived.BuildOutputDataFrame(output_index, outputs, output_names);
  }

protected:
  RollingMLConfig m_config;

  [[nodiscard]] const RollingMLConfig& GetRollingConfig() const { return m_config; }
};

} // namespace epoch_script::transform
