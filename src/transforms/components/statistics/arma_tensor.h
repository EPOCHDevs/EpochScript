#pragma once
//
// ArmaTensor - Zero-copy bridge between Arrow NumericTensor and Armadillo matrix
//
// Owns the Arrow NumericTensor buffer and provides an Armadillo view into it.
// The arma::mat is a view (no copy) that remains valid for the lifetime
// of this object.
//
#include <arrow/tensor.h>
#include <arrow/type.h>
#include <arrow/buffer.h>
#include <arrow/record_batch.h>
#include <armadillo>
#include <stdexcept>
#include <vector>
#include <cstring>

namespace epoch_script::transform {

/**
 * @brief Zero-copy bridge between Arrow NumericTensor<DoubleType> and Armadillo matrix
 *
 * This class solves the memory layout mismatch problem:
 * - Arrow stores DataFrame columns as separate arrays
 * - Armadillo expects a single contiguous column-major block
 *
 * By using Arrow's NumericTensor (which IS a contiguous block), we can:
 * 1. Convert DataFrame -> RecordBatch -> Tensor (one optimized copy)
 * 2. Create Armadillo view of Tensor buffer (zero-copy)
 *
 * The result is a single copy instead of N column copies, plus
 * Arrow's optimized memory operations.
 *
 * @note Uses NumericTensor<DoubleType> for type safety - no runtime type checks needed
 */
class ArmaTensor {
public:
  using TensorType = arrow::NumericTensor<arrow::DoubleType>;

  /**
   * @brief Construct from Arrow NumericTensor (takes ownership of shared_ptr)
   *
   * @param tensor Arrow NumericTensor<DoubleType>, column-major layout
   * @throws std::runtime_error if tensor is null or non-contiguous
   */
  explicit ArmaTensor(std::shared_ptr<TensorType> tensor)
      : m_tensor(std::move(tensor)) {
    if (!m_tensor) {
      throw std::runtime_error("ArmaTensor: null tensor");
    }
    if (!m_tensor->is_contiguous()) {
      throw std::runtime_error("ArmaTensor: tensor must be contiguous");
    }
    InitializeView();
  }

  /**
   * @brief Construct from raw buffer with dimensions
   *
   * Creates a NumericTensor from an existing buffer. Useful when you already
   * have data in the right format.
   *
   * @param buffer Arrow buffer containing the double data
   * @param n_rows Number of rows
   * @param n_cols Number of columns
   * @param column_major If true, data is column-major (Fortran order) - default
   */
  ArmaTensor(std::shared_ptr<arrow::Buffer> buffer,
             int64_t n_rows, int64_t n_cols,
             bool column_major = true)
      : m_buffer(std::move(buffer)) {
    std::vector<int64_t> shape = {n_rows, n_cols};
    std::vector<int64_t> strides;

    if (column_major) {
      // Column-major: stride between rows is sizeof(double),
      // stride between columns is n_rows * sizeof(double)
      strides = {static_cast<int64_t>(sizeof(double)),
                 n_rows * static_cast<int64_t>(sizeof(double))};
    } else {
      // Row-major: stride between columns is sizeof(double),
      // stride between rows is n_cols * sizeof(double)
      strides = {n_cols * static_cast<int64_t>(sizeof(double)),
                 static_cast<int64_t>(sizeof(double))};
    }

    auto result = TensorType::Make(m_buffer, shape, strides);
    if (!result.ok()) {
      throw std::runtime_error("ArmaTensor: failed to create tensor: " +
                                result.status().ToString());
    }
    m_tensor = result.MoveValueUnsafe();
    InitializeView();
  }

  /**
   * @brief Construct by allocating new buffer and copying data
   *
   * @param data Pointer to source double array (column-major)
   * @param n_rows Number of rows
   * @param n_cols Number of columns
   */
  ArmaTensor(const double* data, int64_t n_rows, int64_t n_cols) {
    const int64_t byte_size = n_rows * n_cols * sizeof(double);
    auto buffer_result = arrow::AllocateBuffer(byte_size);
    if (!buffer_result.ok()) {
      throw std::runtime_error("ArmaTensor: failed to allocate buffer");
    }
    m_buffer = std::move(buffer_result).ValueUnsafe();

    // Copy data into buffer
    std::memcpy(m_buffer->mutable_data(), data, byte_size);

    // Create tensor with column-major strides
    std::vector<int64_t> shape = {n_rows, n_cols};
    std::vector<int64_t> strides = {
        static_cast<int64_t>(sizeof(double)),
        n_rows * static_cast<int64_t>(sizeof(double))
    };

    auto result = TensorType::Make(m_buffer, shape, strides);
    if (!result.ok()) {
      throw std::runtime_error("ArmaTensor: failed to create tensor");
    }
    m_tensor = result.MoveValueUnsafe();
    InitializeView();
  }

  // =========================================================================
  // Factory Methods
  // =========================================================================

  /**
   * @brief Create ArmaTensor from Arrow RecordBatch
   *
   * Uses RecordBatch::ToTensor() for optimized conversion to column-major tensor.
   * This is the preferred way to convert tabular data to matrix format.
   *
   * @param batch Arrow RecordBatch (all columns must be numeric, same type)
   * @param null_to_nan If true, convert null values to NaN (default: true)
   * @return ArmaTensor with zero-copy Armadillo view
   * @throws std::runtime_error if conversion fails
   */
  [[nodiscard]] static ArmaTensor FromRecordBatch(
      const std::shared_ptr<arrow::RecordBatch>& batch,
      bool null_to_nan = true) {
    if (!batch) {
      throw std::runtime_error("ArmaTensor::FromRecordBatch: null batch");
    }

    // ToTensor default is row_major=true, we need column-major for Armadillo
    auto tensor_result = batch->ToTensor(null_to_nan, /*row_major=*/false);
    if (!tensor_result.ok()) {
      throw std::runtime_error("ArmaTensor::FromRecordBatch: " +
                                tensor_result.status().ToString());
    }

    auto tensor = tensor_result.MoveValueUnsafe();

    // Cast to NumericTensor<DoubleType> if needed
    if (tensor->type()->id() != arrow::Type::DOUBLE) {
      // Need to cast - allocate new buffer and convert
      const int64_t n_rows = tensor->shape()[0];
      const int64_t n_cols = tensor->ndim() > 1 ? tensor->shape()[1] : 1;
      const int64_t byte_size = n_rows * n_cols * sizeof(double);

      auto buffer_result = arrow::AllocateBuffer(byte_size);
      if (!buffer_result.ok()) {
        throw std::runtime_error("ArmaTensor::FromRecordBatch: buffer allocation failed");
      }
      auto new_buffer = std::move(buffer_result).ValueUnsafe();
      double* dest = reinterpret_cast<double*>(new_buffer->mutable_data());

      // Convert based on source type
      const uint8_t* src = tensor->data()->data();
      const int64_t n_elem = n_rows * n_cols;

      switch (tensor->type()->id()) {
        case arrow::Type::FLOAT:
          for (int64_t i = 0; i < n_elem; ++i) {
            dest[i] = static_cast<double>(reinterpret_cast<const float*>(src)[i]);
          }
          break;
        case arrow::Type::INT64:
          for (int64_t i = 0; i < n_elem; ++i) {
            dest[i] = static_cast<double>(reinterpret_cast<const int64_t*>(src)[i]);
          }
          break;
        case arrow::Type::INT32:
          for (int64_t i = 0; i < n_elem; ++i) {
            dest[i] = static_cast<double>(reinterpret_cast<const int32_t*>(src)[i]);
          }
          break;
        default:
          throw std::runtime_error("ArmaTensor::FromRecordBatch: unsupported type " +
                                    tensor->type()->ToString());
      }

      return ArmaTensor(std::move(new_buffer), n_rows, n_cols, /*column_major=*/true);
    }

    // Already double - create NumericTensor wrapper
    std::vector<int64_t> shape(tensor->shape().begin(), tensor->shape().end());
    std::vector<int64_t> strides(tensor->strides().begin(), tensor->strides().end());

    auto numeric_result = TensorType::Make(tensor->data(), shape, strides);
    if (!numeric_result.ok()) {
      throw std::runtime_error("ArmaTensor::FromRecordBatch: NumericTensor creation failed");
    }

    return ArmaTensor(numeric_result.MoveValueUnsafe());
  }

  // =========================================================================
  // Accessors
  // =========================================================================

  /// Get the Armadillo matrix view (const)
  [[nodiscard]] const arma::mat& mat() const { return m_mat; }

  /// Get the Armadillo matrix view (mutable for in-place operations)
  [[nodiscard]] arma::mat& mat() { return m_mat; }

  /// Get the underlying Arrow NumericTensor
  [[nodiscard]] const std::shared_ptr<TensorType>& tensor() const { return m_tensor; }

  /// Get raw data pointer
  [[nodiscard]] const double* data() const {
    return reinterpret_cast<const double*>(m_tensor->raw_data());
  }

  /// Get mutable raw data pointer
  [[nodiscard]] double* mutable_data() {
    return const_cast<double*>(data());
  }

  /// Implicit conversion to const arma::mat& for convenience
  operator const arma::mat&() const { return m_mat; }

  /// Number of rows
  [[nodiscard]] arma::uword n_rows() const { return m_mat.n_rows; }

  /// Number of columns
  [[nodiscard]] arma::uword n_cols() const { return m_mat.n_cols; }

  /// Total number of elements
  [[nodiscard]] arma::uword n_elem() const { return m_mat.n_elem; }

  /// Check if tensor is column-major (Fortran order)
  [[nodiscard]] bool is_column_major() const { return m_tensor->is_column_major(); }

  /// Check if tensor is row-major (C order)
  [[nodiscard]] bool is_row_major() const { return m_tensor->is_row_major(); }

  // =========================================================================
  // Rule of Five - Move-only semantics
  // =========================================================================

  // Prevent copying (would create dangling view)
  ArmaTensor(const ArmaTensor&) = delete;
  ArmaTensor& operator=(const ArmaTensor&) = delete;

  // Allow moving
  ArmaTensor(ArmaTensor&& other) noexcept
      : m_tensor(std::move(other.m_tensor)),
        m_buffer(std::move(other.m_buffer)),
        m_mat() {
    // Re-initialize view to point to our buffer
    if (m_tensor) {
      InitializeView();
    }
  }

  ArmaTensor& operator=(ArmaTensor&& other) noexcept {
    if (this != &other) {
      m_tensor = std::move(other.m_tensor);
      m_buffer = std::move(other.m_buffer);
      if (m_tensor) {
        InitializeView();
      }
    }
    return *this;
  }

  ~ArmaTensor() = default;

private:
  std::shared_ptr<TensorType> m_tensor;
  std::shared_ptr<arrow::Buffer> m_buffer;  // Optional, only if we own the buffer
  arma::mat m_mat;

  void InitializeView() {
    const auto n_rows = static_cast<arma::uword>(m_tensor->shape()[0]);
    const auto n_cols = m_tensor->ndim() > 1
        ? static_cast<arma::uword>(m_tensor->shape()[1])
        : 1;

    // Get raw pointer to tensor data
    double* raw_data = const_cast<double*>(
        reinterpret_cast<const double*>(m_tensor->raw_data()));

    // Create Armadillo view - copy_aux_mem=false means ZERO COPY
    // strict=false allows the mat to be reassigned (for move semantics)
    // Memory lifetime is managed by Arrow's shared_ptr (m_tensor/m_buffer)
    m_mat = arma::mat(raw_data, n_rows, n_cols,
                      /*copy_aux_mem=*/false,
                      /*strict=*/false);
  }
};

/**
 * @brief Column vector variant of ArmaTensor
 */
class ArmaVecTensor {
public:
  using TensorType = arrow::NumericTensor<arrow::DoubleType>;

  explicit ArmaVecTensor(std::shared_ptr<TensorType> tensor)
      : m_tensor(std::move(tensor)) {
    if (!m_tensor || !m_tensor->is_contiguous()) {
      throw std::runtime_error("ArmaVecTensor: invalid tensor");
    }

    const auto n_elem = static_cast<arma::uword>(m_tensor->shape()[0]);
    double* raw_data = const_cast<double*>(
        reinterpret_cast<const double*>(m_tensor->raw_data()));

    m_vec = arma::vec(raw_data, n_elem, /*copy_aux_mem=*/false, /*strict=*/true);
  }

  ArmaVecTensor(std::shared_ptr<arrow::Buffer> buffer, int64_t n_elem)
      : m_buffer(std::move(buffer)) {
    std::vector<int64_t> shape = {n_elem};
    auto result = TensorType::Make(m_buffer, shape);
    if (!result.ok()) {
      throw std::runtime_error("ArmaVecTensor: failed to create tensor");
    }
    m_tensor = result.MoveValueUnsafe();

    double* raw_data = const_cast<double*>(
        reinterpret_cast<const double*>(m_tensor->raw_data()));
    m_vec = arma::vec(raw_data, n_elem, /*copy_aux_mem=*/false, /*strict=*/true);
  }

  [[nodiscard]] const arma::vec& vec() const { return m_vec; }
  [[nodiscard]] arma::vec& vec() { return m_vec; }
  [[nodiscard]] const std::shared_ptr<TensorType>& tensor() const { return m_tensor; }
  operator const arma::vec&() const { return m_vec; }

  [[nodiscard]] arma::uword n_elem() const { return m_vec.n_elem; }

  ArmaVecTensor(const ArmaVecTensor&) = delete;
  ArmaVecTensor& operator=(const ArmaVecTensor&) = delete;
  ArmaVecTensor(ArmaVecTensor&&) = default;
  ArmaVecTensor& operator=(ArmaVecTensor&&) = default;

private:
  std::shared_ptr<TensorType> m_tensor;
  std::shared_ptr<arrow::Buffer> m_buffer;
  arma::vec m_vec;
};

} // namespace epoch_script::transform
