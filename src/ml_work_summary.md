ML Work Status (handoff)
========================

Recent Changes (2025-11-29)
- **FIXED METADATA REGISTRATION BUG**: All ML transforms now have proper metadata registered with `ITransformRegistry`.
- Root cause: `REGISTER_TRANSFORM` macro only registers with `TransformRegistry` (factory), but metadata must ALSO be registered with `ITransformRegistry` via `Make*MetaData()` functions.
- Created `src/transforms/components/ml/rolling_transforms_metadata.h` - comprehensive metadata for all 27 rolling ML transforms.
- Added metadata registration calls in `registration.cpp` for:
  - Static ML: `MakeMLPreprocessMetaData()`, `MakeLightGBMMetaData()`, `MakeLiblinearMetaData()`, `MakeGMMMetaData()`, `MakeKMeansMetaData()`, `MakeDBSCANMetaData()`, `MakePCAMetaData()`, `MakeICAMetaData()`
  - Rolling ML: `MakeAllRollingMLMetaData()` (covers all rolling transforms)

Transform Inventory
- Static ML (all registered): LightGBM classifier/regressor; LIBLINEAR logistic_l1/logistic_l2/svr_l1/svr_l2; preprocessors ml_zscore/ml_minmax/ml_robust; clustering/decomposition/statistics kmeans_2-5, dbscan, gmm_2-5, hmm_2-5, pca, ica.
- Rolling ML (all registered): rolling_kmeans_2/3/4/5, rolling_dbscan, rolling_gmm_2/3/4/5, rolling_hmm_2/3/4/5, rolling_pca, rolling_ica, rolling_lightgbm_classifier/regressor, rolling_logistic_l1/l2, rolling_svr_l1/l2, rolling_ml_zscore/minmax/robust.
- Infra: `rolling_window_iterator.h`, `rolling_ml_base.h`, `rolling_ml_metadata.h`, `rolling_transforms_metadata.h`, plus the specific transform headers in `src/transforms/components/ml/`.

Registration Status
- `src/transforms/components/registration.cpp` now includes all metadata headers and registers both static and rolling ML transforms with both registries.
- Key files for metadata:
  - `ml/ml_preprocess_metadata.h` - ml_zscore, ml_minmax, ml_robust
  - `ml/lightgbm_metadata.h` - lightgbm_classifier, lightgbm_regressor
  - `ml/liblinear_metadata.h` - logistic_l1/l2, svr_l1/l2
  - `ml/rolling_transforms_metadata.h` - ALL rolling ML transforms
  - `statistics/gmm_metadata.h` - gmm_2/3/4/5
  - `statistics/clustering_metadata.h` - kmeans_2-5, dbscan, pca, ica

Test Status (2025-11-29)
- GMM tests: 6/6 passed
- LightGBM tests: 7/7 passed
- ml_preprocess tests: 9/10 passed (1 minor failure)
- LIBLINEAR tests: 6/9 passed (3 failures - output row count mismatch, not registration)
- Rolling ML tests: 9/18 passed (9 failures - test column name mismatches, not registration)

Outstanding / Yet To Do
- Fix rolling ML test failures: Tests expect different output column names than transforms produce:
  - Tests expect `decision_value`, transforms output `probability`
  - Tests expect `scaled_0`, transforms output `SLOT`
  - PCA test expects 4 columns but gets 3
- Fix LIBLINEAR test failures: Output row count mismatch (248 vs 500)

Known Bugs / Resolved
- RESOLVED: "Invalid Transform: rolling_lightgbm_classifier" and all other "Invalid Transform" errors - fixed by adding metadata registrations.
