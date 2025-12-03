# Task 01: Setup OSQP-Eigen Dependency

## Objective
Install and configure osqp-eigen library for quadratic programming (required for MVO, Risk Parity, Max Diversification).

## Prerequisites
- None (first task)

## Steps

### 1. Clone and Build osqp-eigen
```bash
cd /home/adesola/EpochLab
git clone https://github.com/robotology/osqp-eigen.git
cd osqp-eigen
mkdir build && cd build
cmake -DCMAKE_INSTALL_PREFIX=/home/adesola/EpochLab/osqp-eigen-install ../
make -j$(nproc)
make install
```

### 2. Set Environment Variable
Add to `~/.bashrc` or export before building EpochScript:
```bash
export OsqpEigen_DIR=/home/adesola/EpochLab/osqp-eigen-install
```

### 3. Update EpochScript CMakeLists.txt
Add to `/home/adesola/EpochLab/EpochScript/CMakeLists.txt`:
```cmake
# Find OsqpEigen
find_package(OsqpEigen REQUIRED)

# Link to epoch_script library
target_link_libraries(epoch_script PUBLIC OsqpEigen::OsqpEigen)
```

### 4. Verify Installation
Create a test file to verify OSQP works:

```cpp
// test_osqp.cpp
#include <OsqpEigen/OsqpEigen.h>
#include <Eigen/Dense>
#include <iostream>

int main() {
    // Simple QP: minimize (1/2)x'Hx + f'x
    // H = [4 1; 1 2], f = [1; 1]
    // subject to: x1 + x2 = 1, x >= 0

    Eigen::SparseMatrix<double> H(2, 2);
    H.insert(0, 0) = 4.0;
    H.insert(0, 1) = 1.0;
    H.insert(1, 0) = 1.0;
    H.insert(1, 1) = 2.0;

    Eigen::VectorXd f(2);
    f << 1.0, 1.0;

    // Constraints: [1 1] * x = 1, x >= 0
    Eigen::SparseMatrix<double> A(3, 2);
    A.insert(0, 0) = 1.0; A.insert(0, 1) = 1.0;  // sum = 1
    A.insert(1, 0) = 1.0;  // x1 >= 0
    A.insert(2, 1) = 1.0;  // x2 >= 0

    Eigen::VectorXd lb(3), ub(3);
    lb << 1.0, 0.0, 0.0;
    ub << 1.0, 1e10, 1e10;

    OsqpEigen::Solver solver;
    solver.settings()->setVerbosity(false);
    solver.data()->setNumberOfVariables(2);
    solver.data()->setNumberOfConstraints(3);
    solver.data()->setHessianMatrix(H);
    solver.data()->setGradient(f);
    solver.data()->setLinearConstraintsMatrix(A);
    solver.data()->setLowerBound(lb);
    solver.data()->setUpperBound(ub);

    solver.initSolver();
    solver.solve();

    Eigen::VectorXd solution = solver.getSolution();
    std::cout << "Solution: " << solution.transpose() << std::endl;
    // Expected: approximately [0.25, 0.75]

    return 0;
}
```

## Deliverables
- [ ] osqp-eigen cloned and built
- [ ] Environment variable set
- [ ] CMakeLists.txt updated
- [ ] Test compilation succeeds
- [ ] EpochScript builds with OsqpEigen linked

## Notes
- osqp-eigen depends on OSQP and Eigen3 (should be auto-resolved)
- If vcpkg has osqp, you can try `vcpkg install osqp-eigen` instead
- Spectra is already in vcpkg.json (line 17) for eigenvalue computation
