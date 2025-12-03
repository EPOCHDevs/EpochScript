#pragma once
//
// DataFrame to Armadillo Matrix Conversion Utilities
//
// Provides both legacy copy-based API and new zero-copy ArmaTensor API.
//
#include "arma_tensor.h"
#include "epoch_frame/dataframe.h"
#include "epoch_frame/series.h"
#include <armadillo>
#include <arrow/table.h>
#include <arrow/compute/api.h>
#include <cstring>
#include <stdexcept>
#include <vector>

namespace epoch_script::transform::utils {

// =============================================================================
// New Zero-Copy API using ArmaTensor
// =============================================================================

/**
 * @brief Creates an ArmaTensor from DataFrame columns (zero-copy view)
 *
 * This is the preferred API for performance-critical code. The returned
 * ArmaTensor owns the data buffer, and its mat() method returns a zero-copy
 * Armadillo view.
 *
 * @param df The source DataFrame
 * @param column_names Vector of column names to extract
 * @return ArmaTensor with zero-copy Armadillo view
 *
 * @throws std::runtime_error if conversion fails
 *
 * @note The ArmaTensor must outlive any references to its mat() view
 */
inline ArmaTensor
ArmaTensorFromDataFrame(const epoch_frame::DataFrame &df,
                        const std::vector<std::string> &column_names) {
  if (column_names.empty()) {
    throw std::runtime_error("No columns specified for tensor conversion");
  }

  const size_t T = df.num_rows();
  const size_t D = column_names.size();

  if (T == 0) {
    // Return empty tensor
    auto buffer_result = arrow::AllocateBuffer(0);
    if (!buffer_result.ok()) {
      throw std::runtime_error("Failed to allocate empty buffer");
    }
    return ArmaTensor(std::move(buffer_result).ValueUnsafe(), 0, D);
  }

  // Select only the requested columns and convert to RecordBatch
  auto selected_df = df[column_names];
  auto batch_result = selected_df.table()->CombineChunksToBatch();
  if (!batch_result.ok()) {
    throw std::runtime_error("Failed to convert DataFrame to RecordBatch: " +
                              batch_result.status().ToString());
  }

  return ArmaTensor::FromRecordBatch(batch_result.MoveValueUnsafe());
}

/**
 * @brief Creates an ArmaVecTensor from a single DataFrame column (zero-copy view)
 *
 * @param df The source DataFrame
 * @param column_name Name of the column to extract
 * @return ArmaVecTensor with zero-copy Armadillo view
 */
inline ArmaVecTensor
ArmaVecTensorFromDataFrame(const epoch_frame::DataFrame &df,
                           const std::string &column_name) {
  const size_t T = df.num_rows();

  if (T == 0) {
    auto buffer_result = arrow::AllocateBuffer(0);
    if (!buffer_result.ok()) {
      throw std::runtime_error("Failed to allocate empty buffer");
    }
    return ArmaVecTensor(std::move(buffer_result).ValueUnsafe(), 0);
  }

  // Get column and convert to contiguous double array
  auto column_array = df[column_name].contiguous_array();
  if (column_array.type()->id() != arrow::Type::DOUBLE) {
    column_array = column_array.cast(arrow::float64());
  }

  // Get the underlying buffer
  auto chunks = column_array.as_chunked_array();
  if (chunks->num_chunks() != 1) {
    throw std::runtime_error("Expected single chunk array");
  }

  auto double_array = std::static_pointer_cast<arrow::DoubleArray>(chunks->chunk(0));
  auto buffer = double_array->values();

  // Create 1D tensor
  std::vector<int64_t> shape = {static_cast<int64_t>(T)};
  auto tensor_result = ArmaVecTensor::TensorType::Make(buffer, shape);
  if (!tensor_result.ok()) {
    throw std::runtime_error("Failed to create vector tensor");
  }

  return ArmaVecTensor(tensor_result.MoveValueUnsafe());
}

/**
 * @brief Creates an ArmaTensor from all numeric columns (zero-copy view)
 *
 * @param df The source DataFrame
 * @return ArmaTensor with zero-copy Armadillo view
 */
inline ArmaTensor
ArmaTensorFromDataFrameAllNumeric(const epoch_frame::DataFrame &df) {
  std::vector<std::string> numeric_columns;

  auto table = df.table();
  auto schema = table->schema();

  for (int i = 0; i < schema->num_fields(); ++i) {
    auto field = schema->field(i);
    auto type_id = field->type()->id();

    if (type_id == arrow::Type::DOUBLE || type_id == arrow::Type::FLOAT ||
        type_id == arrow::Type::INT64 || type_id == arrow::Type::INT32 ||
        type_id == arrow::Type::INT16 || type_id == arrow::Type::INT8 ||
        type_id == arrow::Type::UINT64 || type_id == arrow::Type::UINT32 ||
        type_id == arrow::Type::UINT16 || type_id == arrow::Type::UINT8) {
      numeric_columns.push_back(field->name());
    }
  }

  if (numeric_columns.empty()) {
    throw std::runtime_error("No numeric columns found in DataFrame");
  }

  return ArmaTensorFromDataFrame(df, numeric_columns);
}

// =============================================================================
// Legacy Copy-Based API (kept for backwards compatibility)
// =============================================================================

/**
 * @brief Converts specified columns from an epoch_frame::DataFrame to an
 * Armadillo matrix (COPIES data)
 *
 * @deprecated Prefer ArmaTensorFromDataFrame() for zero-copy performance
 *
 * @param df The source DataFrame
 * @param column_names Vector of column names to extract
 * @return arma::mat Matrix with dimensions [num_rows x num_columns]
 */
inline arma::mat
MatFromDataFrame(const epoch_frame::DataFrame &df,
                 const std::vector<std::string> &column_names) {
  if (column_names.empty()) {
    throw std::runtime_error("No columns specified for matrix conversion");
  }

  const size_t T = df.num_rows();
  const size_t D = column_names.size();

  if (T == 0) {
    return arma::mat(0, D);
  }

  // Special case: single column - can use advanced constructor
  if (D == 1) {
    const auto &col_name = column_names[0];
    auto column_array = df[col_name].contiguous_array();

    if (column_array.type()->id() != arrow::Type::DOUBLE) {
      column_array = column_array.cast(arrow::float64());
    }

    const auto column_view = column_array.template to_view<double>();
    const double *raw_data = column_view->raw_values();

    // copy_aux_mem=true means this creates a copy
    return arma::mat(const_cast<double *>(raw_data), T, 1, true, false);
  }

  // Multi-column case: use memcpy approach
  arma::mat X(T, D);

  for (size_t j = 0; j < D; ++j) {
    const auto &col_name = column_names[j];
    auto column_array = df[col_name].contiguous_array();

    if (column_array.type()->id() != arrow::Type::DOUBLE) {
      column_array = column_array.cast(arrow::float64());
    }

    const auto column_view = column_array.template to_view<double>();
    const double *raw_data = column_view->raw_values();

    std::memcpy(X.colptr(j), raw_data, T * sizeof(double));
  }

  return X;
}

/**
 * @brief Converts a single column from DataFrame to Armadillo column vector (COPIES data)
 *
 * @deprecated Prefer ArmaVecTensorFromDataFrame() for zero-copy performance
 */
inline arma::vec VecFromDataFrame(const epoch_frame::DataFrame &df,
                                  const std::string &column_name) {
  auto matrix = MatFromDataFrame(df, {column_name});
  return matrix.col(0);
}

/**
 * @brief Converts all numeric columns from DataFrame to Armadillo matrix (COPIES data)
 *
 * @deprecated Prefer ArmaTensorFromDataFrameAllNumeric() for zero-copy performance
 */
inline arma::mat MatFromDataFrameAllNumeric(const epoch_frame::DataFrame &df) {
  std::vector<std::string> numeric_columns;

  auto table = df.table();
  auto schema = table->schema();

  for (int i = 0; i < schema->num_fields(); ++i) {
    auto field = schema->field(i);
    auto type_id = field->type()->id();

    if (type_id == arrow::Type::DOUBLE || type_id == arrow::Type::FLOAT ||
        type_id == arrow::Type::INT64 || type_id == arrow::Type::INT32 ||
        type_id == arrow::Type::INT16 || type_id == arrow::Type::INT8 ||
        type_id == arrow::Type::UINT64 || type_id == arrow::Type::UINT32 ||
        type_id == arrow::Type::UINT16 || type_id == arrow::Type::UINT8) {
      numeric_columns.push_back(field->name());
    }
  }

  if (numeric_columns.empty()) {
    throw std::runtime_error("No numeric columns found in DataFrame");
  }

  return MatFromDataFrame(df, numeric_columns);
}

// =============================================================================
// Armadillo to Arrow Conversion (Output Helpers)
// =============================================================================

/**
 * @brief Converts arma::vec directly to Arrow ChunkedArray
 *
 * This avoids the intermediate std::vector copy by using memptr() directly.
 * Handles NaN values correctly for Arrow null semantics.
 *
 * @param v The armadillo vector to convert
 * @return Arrow ChunkedArrayPtr
 */
inline arrow::ChunkedArrayPtr
ArrayFromVec(const arma::vec& v) {
  const double* data = v.memptr();
  const size_t n = v.n_elem;
  return epoch_frame::factory::array::make_array<double>(data, data + n);
}

/**
 * @brief Converts arma::vec to Arrow ChunkedArray with sqrt transformation
 *
 * Common pattern in volatility modeling: sqrt(variance) = volatility.
 * Avoids intermediate vector by applying transform during array construction.
 *
 * @param v The variance vector
 * @return Arrow ChunkedArrayPtr of volatility (sqrt values)
 */
inline arrow::ChunkedArrayPtr
ArrayFromVecSqrt(const arma::vec& v) {
  arrow::DoubleBuilder builder;
  if (!builder.Reserve(static_cast<int64_t>(v.n_elem)).ok()) {
    throw std::runtime_error("Failed to reserve memory for array");
  }

  for (size_t i = 0; i < v.n_elem; ++i) {
    double val = std::sqrt(v(i));
    if (std::isnan(val)) {
      if (!builder.AppendNull().ok()) {
        throw std::runtime_error("Failed to append null");
      }
    } else {
      if (!builder.Append(val).ok()) {
        throw std::runtime_error("Failed to append value");
      }
    }
  }

  auto result = builder.Finish();
  if (!result.ok()) {
    throw std::runtime_error("Failed to finish array: " + result.status().ToString());
  }
  return std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{result.MoveValueUnsafe()});
}

/**
 * @brief Creates NaN-filled array with single value at last position
 *
 * Common pattern for model diagnostics that should only appear at end.
 *
 * @param n Total array length
 * @param last_value Value to place at position n-1
 * @return Arrow ChunkedArrayPtr
 */
inline arrow::ChunkedArrayPtr
ArrayWithLastValue(size_t n, double last_value) {
  arrow::DoubleBuilder builder;
  if (!builder.Reserve(static_cast<int64_t>(n)).ok()) {
    throw std::runtime_error("Failed to reserve memory");
  }

  for (size_t i = 0; i + 1 < n; ++i) {
    if (!builder.AppendNull().ok()) {
      throw std::runtime_error("Failed to append null");
    }
  }
  if (n > 0) {
    if (std::isnan(last_value)) {
      if (!builder.AppendNull().ok()) {
        throw std::runtime_error("Failed to append null");
      }
    } else {
      if (!builder.Append(last_value).ok()) {
        throw std::runtime_error("Failed to append last value");
      }
    }
  }

  auto result = builder.Finish();
  if (!result.ok()) {
    throw std::runtime_error("Failed to finish array");
  }
  return std::make_shared<arrow::ChunkedArray>(arrow::ArrayVector{result.MoveValueUnsafe()});
}

} // namespace epoch_script::transform::utils
