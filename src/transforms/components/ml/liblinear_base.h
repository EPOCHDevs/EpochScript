#pragma once
//
// LIBLINEAR Base Utilities and RAII Wrappers for Financial ML Transforms
//
#include <liblinear/linear.h>
#include <spdlog/spdlog.h>
#include <memory>
#include <vector>
#include <stdexcept>
#include <cstring>

namespace epoch_script::transform::liblinear_utils {

/**
 * @brief RAII wrapper for LIBLINEAR model
 */
struct ModelDeleter {
  void operator()(model* m) const {
    if (m) {
      free_and_destroy_model(&m);
    }
  }
};
using ModelPtr = std::unique_ptr<model, ModelDeleter>;

/**
 * @brief RAII wrapper for LIBLINEAR problem data
 * Manages feature_node arrays and y labels
 */
class ProblemData {
public:
  ProblemData() = default;
  ~ProblemData() = default;

  // Non-copyable
  ProblemData(const ProblemData&) = delete;
  ProblemData& operator=(const ProblemData&) = delete;

  // Movable
  ProblemData(ProblemData&&) = default;
  ProblemData& operator=(ProblemData&&) = default;

  /**
   * @brief Initialize problem from matrix data
   * @param X Feature matrix (n_samples x n_features)
   * @param y Target vector (n_samples)
   * @param bias Bias term (-1 to disable)
   */
  void Initialize(const std::vector<std::vector<double>>& X,
                  const std::vector<double>& y,
                  double bias = 1.0) {
    if (X.empty() || y.empty()) {
      throw std::runtime_error("Empty training data");
    }
    if (X.size() != y.size()) {
      throw std::runtime_error("X and y size mismatch");
    }

    n_samples_ = static_cast<int>(X.size());
    n_features_ = static_cast<int>(X[0].size());
    bias_ = bias;

    // Allocate y array
    y_.resize(n_samples_);
    std::copy(y.begin(), y.end(), y_.begin());

    // Allocate feature nodes
    // Each row needs n_features + 1 for bias (if enabled) + 1 for terminator
    size_t nodes_per_row = n_features_ + (bias >= 0 ? 1 : 0) + 1;
    nodes_.resize(n_samples_ * nodes_per_row);
    x_ptrs_.resize(n_samples_);

    for (int i = 0; i < n_samples_; ++i) {
      feature_node* row = &nodes_[i * nodes_per_row];
      x_ptrs_[i] = row;

      int j = 0;
      for (int f = 0; f < n_features_; ++f) {
        row[j].index = f + 1; // LIBLINEAR uses 1-based indexing
        row[j].value = X[i][f];
        ++j;
      }

      // Add bias term if enabled
      if (bias >= 0) {
        row[j].index = n_features_ + 1;
        row[j].value = bias;
        ++j;
      }

      // Terminator
      row[j].index = -1;
      row[j].value = 0;
    }

    // Setup problem struct
    problem_.l = n_samples_;
    problem_.n = n_features_ + (bias >= 0 ? 1 : 0);
    problem_.y = y_.data();
    problem_.x = x_ptrs_.data();
    problem_.bias = bias;
  }

  /**
   * @brief Get pointer to problem struct for training
   */
  const problem* GetProblem() const { return &problem_; }

  int NumSamples() const { return n_samples_; }
  int NumFeatures() const { return n_features_; }

private:
  int n_samples_{0};
  int n_features_{0};
  double bias_{1.0};

  std::vector<double> y_;
  std::vector<feature_node> nodes_;
  std::vector<feature_node*> x_ptrs_;
  problem problem_{};
};

/**
 * @brief RAII wrapper for a single prediction sample
 */
class PredictionSample {
public:
  PredictionSample(const std::vector<double>& features, double bias = 1.0) {
    size_t n = features.size();
    size_t nodes_count = n + (bias >= 0 ? 1 : 0) + 1;
    nodes_.resize(nodes_count);

    int j = 0;
    for (size_t f = 0; f < n; ++f) {
      nodes_[j].index = static_cast<int>(f + 1);
      nodes_[j].value = features[f];
      ++j;
    }

    if (bias >= 0) {
      nodes_[j].index = static_cast<int>(n + 1);
      nodes_[j].value = bias;
      ++j;
    }

    nodes_[j].index = -1;
    nodes_[j].value = 0;
  }

  const feature_node* Get() const { return nodes_.data(); }

private:
  std::vector<feature_node> nodes_;
};

/**
 * @brief Setup LIBLINEAR logging to use spdlog
 *
 * Routes LIBLINEAR's internal output through spdlog at debug level.
 * Call this before training to capture optimization progress.
 */
inline void SetupLogging() {
  set_print_string_function([](const char* msg) {
    // Strip trailing newline if present
    std::string message(msg);
    while (!message.empty() && (message.back() == '\n' || message.back() == '\r')) {
      message.pop_back();
    }
    if (!message.empty()) {
      spdlog::debug("[LIBLINEAR] {}", message);
    }
  });
}

/**
 * @brief Check if solver type is a classifier
 */
inline bool IsClassifier(int solver_type) {
  return solver_type == L2R_LR ||
         solver_type == L2R_L2LOSS_SVC_DUAL ||
         solver_type == L2R_L2LOSS_SVC ||
         solver_type == L2R_L1LOSS_SVC_DUAL ||
         solver_type == MCSVM_CS ||
         solver_type == L1R_L2LOSS_SVC ||
         solver_type == L1R_LR ||
         solver_type == L2R_LR_DUAL;
}

/**
 * @brief Check if solver type is a regressor
 */
inline bool IsRegressor(int solver_type) {
  return solver_type == L2R_L2LOSS_SVR ||
         solver_type == L2R_L2LOSS_SVR_DUAL ||
         solver_type == L2R_L1LOSS_SVR_DUAL;
}

/**
 * @brief Check if solver type supports probability estimates
 */
inline bool SupportsProbability(int solver_type) {
  // Only logistic regression supports probability
  return solver_type == L2R_LR ||
         solver_type == L1R_LR ||
         solver_type == L2R_LR_DUAL;
}

} // namespace epoch_script::transform::liblinear_utils
