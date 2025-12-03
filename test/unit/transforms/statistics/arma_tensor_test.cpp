//
// Unit tests for ArmaTensor - Zero-copy Arrow/Armadillo bridge
//
#include "transforms/components/statistics/arma_tensor.h"
#include "transforms/components/statistics/dataframe_armadillo_utils.h"
#include <catch2/catch_test_macros.hpp>
#include <catch2/matchers/catch_matchers_floating_point.hpp>
#include <epoch_frame/factory/dataframe_factory.h>
#include <epoch_frame/factory/index_factory.h>
#include <epoch_frame/factory/array_factory.h>
#include <arrow/record_batch.h>
#include <arrow/builder.h>
#include <armadillo>

using namespace epoch_script::transform;
using Catch::Matchers::WithinAbs;

namespace {

// Helper to create a simple Arrow RecordBatch for testing
std::shared_ptr<arrow::RecordBatch> CreateTestRecordBatch(
    size_t n_rows, size_t n_cols, double start_val = 1.0) {
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  std::vector<std::shared_ptr<arrow::Field>> fields;

  for (size_t c = 0; c < n_cols; ++c) {
    arrow::DoubleBuilder builder;
    for (size_t r = 0; r < n_rows; ++r) {
      auto status = builder.Append(start_val + r * n_cols + c);
      REQUIRE(status.ok());
    }
    std::shared_ptr<arrow::Array> array;
    auto status = builder.Finish(&array);
    REQUIRE(status.ok());
    arrays.push_back(array);
    fields.push_back(arrow::field("col" + std::to_string(c), arrow::float64()));
  }

  auto schema = arrow::schema(fields);
  return arrow::RecordBatch::Make(schema, static_cast<int64_t>(n_rows), arrays);
}

// Helper to create a test DataFrame
epoch_frame::DataFrame CreateTestDataFrame(size_t n_rows, size_t n_cols) {
  std::vector<arrow::ChunkedArrayPtr> arrays;
  std::vector<std::string> col_names;

  for (size_t c = 0; c < n_cols; ++c) {
    std::vector<double> data(n_rows);
    for (size_t r = 0; r < n_rows; ++r) {
      data[r] = static_cast<double>(r * n_cols + c + 1);
    }
    arrays.push_back(epoch_frame::factory::array::make_array(data));
    col_names.push_back("col" + std::to_string(c));
  }

  auto index = epoch_frame::factory::index::from_range(n_rows);
  return epoch_frame::make_dataframe(index, arrays, col_names);
}

} // namespace

TEST_CASE("ArmaTensor construction from RecordBatch", "[arma_tensor]") {
  SECTION("Basic construction") {
    auto batch = CreateTestRecordBatch(100, 3);
    REQUIRE(batch != nullptr);

    auto tensor = ArmaTensor::FromRecordBatch(batch);

    REQUIRE(tensor.n_rows() == 100);
    REQUIRE(tensor.n_cols() == 3);
    REQUIRE(tensor.n_elem() == 300);
  }

  SECTION("Data integrity check") {
    auto batch = CreateTestRecordBatch(10, 2, 1.0);
    auto tensor = ArmaTensor::FromRecordBatch(batch);

    const arma::mat& mat = tensor.mat();

    // Verify first column values
    for (size_t r = 0; r < 10; ++r) {
      double expected_col0 = 1.0 + r * 2;  // start_val + r * n_cols + 0
      double expected_col1 = 2.0 + r * 2;  // start_val + r * n_cols + 1
      REQUIRE_THAT(mat(r, 0), WithinAbs(expected_col0, 1e-10));
      REQUIRE_THAT(mat(r, 1), WithinAbs(expected_col1, 1e-10));
    }
  }

  SECTION("Single column") {
    auto batch = CreateTestRecordBatch(50, 1);
    auto tensor = ArmaTensor::FromRecordBatch(batch);

    REQUIRE(tensor.n_rows() == 50);
    REQUIRE(tensor.n_cols() == 1);
  }

  SECTION("Null batch throws") {
    REQUIRE_THROWS_AS(
        ArmaTensor::FromRecordBatch(nullptr),
        std::runtime_error);
  }
}

TEST_CASE("ArmaTensor construction from buffer", "[arma_tensor]") {
  SECTION("Column-major layout") {
    const int64_t n_rows = 5;
    const int64_t n_cols = 3;
    const int64_t byte_size = n_rows * n_cols * sizeof(double);

    auto buffer_result = arrow::AllocateBuffer(byte_size);
    REQUIRE(buffer_result.ok());
    auto buffer = std::move(buffer_result).ValueUnsafe();

    // Fill with column-major data
    double* data = reinterpret_cast<double*>(buffer->mutable_data());
    for (int64_t c = 0; c < n_cols; ++c) {
      for (int64_t r = 0; r < n_rows; ++r) {
        data[c * n_rows + r] = r * 10 + c;
      }
    }

    ArmaTensor tensor(std::move(buffer), n_rows, n_cols, /*column_major=*/true);

    REQUIRE(tensor.n_rows() == 5);
    REQUIRE(tensor.n_cols() == 3);

    const arma::mat& mat = tensor.mat();
    REQUIRE_THAT(mat(0, 0), WithinAbs(0.0, 1e-10));   // r=0, c=0
    REQUIRE_THAT(mat(1, 0), WithinAbs(10.0, 1e-10)); // r=1, c=0
    REQUIRE_THAT(mat(0, 1), WithinAbs(1.0, 1e-10));   // r=0, c=1
    REQUIRE_THAT(mat(2, 2), WithinAbs(22.0, 1e-10)); // r=2, c=2
  }
}

TEST_CASE("ArmaTensor construction from raw data", "[arma_tensor]") {
  SECTION("Basic copy construction") {
    const size_t n_rows = 4;
    const size_t n_cols = 2;

    // Column-major data: [col0: 1,2,3,4] [col1: 5,6,7,8]
    std::vector<double> source_data = {1, 2, 3, 4, 5, 6, 7, 8};

    ArmaTensor tensor(source_data.data(),
                      static_cast<int64_t>(n_rows),
                      static_cast<int64_t>(n_cols));

    REQUIRE(tensor.n_rows() == 4);
    REQUIRE(tensor.n_cols() == 2);

    const arma::mat& mat = tensor.mat();
    REQUIRE_THAT(mat(0, 0), WithinAbs(1.0, 1e-10));
    REQUIRE_THAT(mat(3, 0), WithinAbs(4.0, 1e-10));
    REQUIRE_THAT(mat(0, 1), WithinAbs(5.0, 1e-10));
    REQUIRE_THAT(mat(3, 1), WithinAbs(8.0, 1e-10));
  }
}

TEST_CASE("ArmaTensor zero-copy verification", "[arma_tensor]") {
  SECTION("mat() returns view, not copy") {
    auto batch = CreateTestRecordBatch(100, 3);
    auto tensor = ArmaTensor::FromRecordBatch(batch);

    // Get mat reference
    const arma::mat& mat1 = tensor.mat();
    const arma::mat& mat2 = tensor.mat();

    // Both should point to same memory
    REQUIRE(mat1.memptr() == mat2.memptr());

    // Should match tensor's raw data pointer
    REQUIRE(mat1.memptr() == tensor.data());
  }

  SECTION("Mutable mat allows in-place modifications") {
    const int64_t n_rows = 3;
    const int64_t n_cols = 2;

    std::vector<double> source_data = {1, 2, 3, 4, 5, 6};
    ArmaTensor tensor(source_data.data(), n_rows, n_cols);

    // Modify through mutable mat
    arma::mat& mat = tensor.mat();
    mat(0, 0) = 99.0;

    // Verify modification persists
    REQUIRE_THAT(tensor.mat()(0, 0), WithinAbs(99.0, 1e-10));
    REQUIRE_THAT(tensor.data()[0], WithinAbs(99.0, 1e-10));
  }
}

TEST_CASE("ArmaTensor move semantics", "[arma_tensor]") {
  SECTION("Move construction") {
    auto batch = CreateTestRecordBatch(50, 2);
    auto tensor1 = ArmaTensor::FromRecordBatch(batch);

    const double* original_data = tensor1.data();
    size_t original_rows = tensor1.n_rows();

    ArmaTensor tensor2(std::move(tensor1));

    // tensor2 should have the data
    REQUIRE(tensor2.n_rows() == original_rows);
    REQUIRE(tensor2.data() == original_data);
  }

  SECTION("Move assignment") {
    auto batch1 = CreateTestRecordBatch(30, 2);
    auto batch2 = CreateTestRecordBatch(40, 3);

    auto tensor1 = ArmaTensor::FromRecordBatch(batch1);
    auto tensor2 = ArmaTensor::FromRecordBatch(batch2);

    const double* batch2_data = tensor2.data();

    tensor1 = std::move(tensor2);

    REQUIRE(tensor1.n_rows() == 40);
    REQUIRE(tensor1.n_cols() == 3);
    REQUIRE(tensor1.data() == batch2_data);
  }
}

TEST_CASE("ArmaTensor implicit conversion", "[arma_tensor]") {
  SECTION("Implicit conversion to const arma::mat&") {
    auto batch = CreateTestRecordBatch(20, 2);
    auto tensor = ArmaTensor::FromRecordBatch(batch);

    // Should work with functions expecting const arma::mat&
    auto compute_sum = [](const arma::mat& m) {
      return arma::accu(m);
    };

    double sum = compute_sum(tensor);
    REQUIRE(sum > 0);  // Just verify it compiles and runs
  }
}

TEST_CASE("ArmaVecTensor basic functionality", "[arma_tensor]") {
  SECTION("Construction from buffer") {
    const int64_t n_elem = 10;
    const int64_t byte_size = n_elem * sizeof(double);

    auto buffer_result = arrow::AllocateBuffer(byte_size);
    REQUIRE(buffer_result.ok());
    auto buffer = std::move(buffer_result).ValueUnsafe();

    double* data = reinterpret_cast<double*>(buffer->mutable_data());
    for (int64_t i = 0; i < n_elem; ++i) {
      data[i] = static_cast<double>(i + 1);
    }

    ArmaVecTensor vec_tensor(std::move(buffer), n_elem);

    REQUIRE(vec_tensor.n_elem() == 10);

    const arma::vec& vec = vec_tensor.vec();
    REQUIRE_THAT(vec(0), WithinAbs(1.0, 1e-10));
    REQUIRE_THAT(vec(9), WithinAbs(10.0, 1e-10));
  }

  SECTION("Zero-copy verification") {
    const int64_t n_elem = 5;
    const int64_t byte_size = n_elem * sizeof(double);

    auto buffer_result = arrow::AllocateBuffer(byte_size);
    REQUIRE(buffer_result.ok());
    auto buffer = std::move(buffer_result).ValueUnsafe();

    double* data = reinterpret_cast<double*>(buffer->mutable_data());
    for (int64_t i = 0; i < n_elem; ++i) {
      data[i] = static_cast<double>(i);
    }

    ArmaVecTensor vec_tensor(std::move(buffer), n_elem);

    const arma::vec& vec1 = vec_tensor.vec();
    const arma::vec& vec2 = vec_tensor.vec();

    REQUIRE(vec1.memptr() == vec2.memptr());
  }
}

TEST_CASE("ArmaTensorFromDataFrame utility", "[arma_tensor][utils]") {
  SECTION("Basic DataFrame conversion") {
    auto df = CreateTestDataFrame(50, 3);

    std::vector<std::string> cols = {"col0", "col1", "col2"};
    auto tensor = utils::ArmaTensorFromDataFrame(df, cols);

    REQUIRE(tensor.n_rows() == 50);
    REQUIRE(tensor.n_cols() == 3);
  }

  SECTION("Subset of columns") {
    auto df = CreateTestDataFrame(30, 5);

    std::vector<std::string> cols = {"col1", "col3"};
    auto tensor = utils::ArmaTensorFromDataFrame(df, cols);

    REQUIRE(tensor.n_rows() == 30);
    REQUIRE(tensor.n_cols() == 2);
  }

  SECTION("Empty columns throws") {
    auto df = CreateTestDataFrame(10, 2);

    std::vector<std::string> empty_cols;
    REQUIRE_THROWS_AS(
        utils::ArmaTensorFromDataFrame(df, empty_cols),
        std::runtime_error);
  }

  SECTION("Empty DataFrame returns empty tensor") {
    auto df = CreateTestDataFrame(0, 2);

    std::vector<std::string> cols = {"col0", "col1"};
    auto tensor = utils::ArmaTensorFromDataFrame(df, cols);

    REQUIRE(tensor.n_rows() == 0);
    REQUIRE(tensor.n_cols() == 2);
  }
}

TEST_CASE("ArmaTensorFromDataFrameAllNumeric utility", "[arma_tensor][utils]") {
  SECTION("All numeric columns extracted") {
    auto df = CreateTestDataFrame(25, 4);
    auto tensor = utils::ArmaTensorFromDataFrameAllNumeric(df);

    REQUIRE(tensor.n_rows() == 25);
    REQUIRE(tensor.n_cols() == 4);
  }
}

TEST_CASE("ArmaTensor Armadillo operations", "[arma_tensor]") {
  SECTION("Matrix operations on view") {
    std::vector<double> data = {1, 2, 3, 4, 5, 6, 7, 8, 9};  // 3x3 column-major
    ArmaTensor tensor(data.data(), 3, 3);

    const arma::mat& mat = tensor.mat();

    // Row slicing
    arma::rowvec row0 = mat.row(0);
    REQUIRE_THAT(row0(0), WithinAbs(1.0, 1e-10));
    REQUIRE_THAT(row0(1), WithinAbs(4.0, 1e-10));
    REQUIRE_THAT(row0(2), WithinAbs(7.0, 1e-10));

    // Column slicing
    arma::vec col1 = mat.col(1);
    REQUIRE_THAT(col1(0), WithinAbs(4.0, 1e-10));
    REQUIRE_THAT(col1(1), WithinAbs(5.0, 1e-10));
    REQUIRE_THAT(col1(2), WithinAbs(6.0, 1e-10));

    // Submatrix
    arma::mat sub = mat.submat(0, 0, 1, 1);
    REQUIRE(sub.n_rows == 2);
    REQUIRE(sub.n_cols == 2);
  }

  SECTION("Statistical operations") {
    std::vector<double> data = {1, 2, 3, 4, 5, 6};  // 3x2 column-major
    ArmaTensor tensor(data.data(), 3, 2);

    const arma::mat& mat = tensor.mat();

    // Mean
    arma::rowvec means = arma::mean(mat, 0);  // Column means
    REQUIRE_THAT(means(0), WithinAbs(2.0, 1e-10));  // mean(1,2,3)
    REQUIRE_THAT(means(1), WithinAbs(5.0, 1e-10));  // mean(4,5,6)

    // Sum
    double total = arma::accu(mat);
    REQUIRE_THAT(total, WithinAbs(21.0, 1e-10));  // 1+2+3+4+5+6
  }
}
