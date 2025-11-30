#pragma once
//
// Transform Progress Emitter
// Lightweight emitter passed to transforms for internal progress reporting
//

#include "cancellation_token.h"
#include "event_dispatcher.h"

namespace epoch_script::runtime::events {

// ============================================================================
// Transform Progress Emitter
// ============================================================================

class TransformProgressEmitter {
public:
    TransformProgressEmitter(IEventDispatcherPtr dispatcher,
                             CancellationTokenPtr cancellationToken,
                             std::string node_id,
                             std::string transform_name);

    // Non-copyable but movable
    TransformProgressEmitter(const TransformProgressEmitter&) = delete;
    TransformProgressEmitter& operator=(const TransformProgressEmitter&) = delete;
    TransformProgressEmitter(TransformProgressEmitter&&) = default;
    TransformProgressEmitter& operator=(TransformProgressEmitter&&) = default;

    // ========================================================================
    // Asset Context Management (for per-asset transforms)
    // ========================================================================

    void SetAssetId(const std::string& asset_id);
    void ClearAssetId();
    [[nodiscard]] std::optional<std::string> GetAssetId() const;

    // ========================================================================
    // Cancellation Support
    // ========================================================================

    // Check if pipeline has been cancelled
    [[nodiscard]] bool IsCancelled() const noexcept;

    // Throw OperationCancelledException if cancelled
    void ThrowIfCancelled() const;

    // Throw with context message if cancelled
    void ThrowIfCancelled(const std::string& context) const;

    // ========================================================================
    // Progress Emission Methods
    // ========================================================================

    // Simple progress update (e.g., 5 of 100 items processed)
    void EmitProgress(size_t current, size_t total,
                      const std::string& message = "");

    // ML training epoch progress
    void EmitEpoch(size_t epoch, size_t total_epochs,
                   std::optional<double> loss = std::nullopt,
                   std::optional<double> accuracy = std::nullopt,
                   std::optional<double> learning_rate = std::nullopt);

    // Iteration progress (for iterative algorithms like optimization)
    void EmitIteration(size_t iteration,
                       std::optional<double> metric = std::nullopt,
                       const std::string& message = "");

    // Generic structured progress with custom metadata
    void EmitCustomProgress(const JsonMetadata& metadata,
                           const std::string& message = "");

    // Full control over the event
    void Emit(TransformProgressEvent event);

    // ========================================================================
    // Convenience Methods (emit + check cancellation)
    // ========================================================================

    // Emit epoch progress and check cancellation
    // Throws if cancelled, useful for ML training loops
    void EmitEpochOrCancel(size_t epoch, size_t total_epochs,
                           std::optional<double> loss = std::nullopt,
                           std::optional<double> accuracy = std::nullopt);

    // Emit iteration and check cancellation
    void EmitIterationOrCancel(size_t iteration,
                               std::optional<double> metric = std::nullopt,
                               const std::string& message = "");

    // ========================================================================
    // Getters
    // ========================================================================

    [[nodiscard]] const std::string& GetNodeId() const { return m_nodeId; }
    [[nodiscard]] const std::string& GetTransformName() const { return m_transformName; }

private:
    IEventDispatcherPtr m_dispatcher;
    CancellationTokenPtr m_cancellationToken;
    std::string m_nodeId;
    std::string m_transformName;
    std::optional<std::string> m_assetId;

    // Helper to create base event with common fields
    TransformProgressEvent MakeBaseEvent() const;
};

using TransformProgressEmitterPtr = std::shared_ptr<TransformProgressEmitter>;

// ============================================================================
// Factory Function
// ============================================================================

inline TransformProgressEmitterPtr MakeProgressEmitter(
    IEventDispatcherPtr dispatcher,
    CancellationTokenPtr cancellationToken,
    const std::string& node_id,
    const std::string& transform_name) {

    return std::make_shared<TransformProgressEmitter>(
        std::move(dispatcher),
        std::move(cancellationToken),
        node_id,
        transform_name
    );
}

// ============================================================================
// RAII Asset Context Guard
// ============================================================================

class AssetContextGuard {
public:
    AssetContextGuard(TransformProgressEmitter& emitter, const std::string& asset_id)
        : m_emitter(emitter) {
        m_emitter.SetAssetId(asset_id);
    }

    ~AssetContextGuard() {
        m_emitter.ClearAssetId();
    }

    // Non-copyable, non-movable
    AssetContextGuard(const AssetContextGuard&) = delete;
    AssetContextGuard& operator=(const AssetContextGuard&) = delete;

private:
    TransformProgressEmitter& m_emitter;
};

} // namespace epoch_script::runtime::events
