//
// ITransform progress helper method implementations
// Separated from header to avoid circular dependencies with TransformProgressEmitter
//

#include <epoch_script/transforms/core/itransform.h>

// Include the full definition of TransformProgressEmitter
#include "events/transform_progress_emitter.h"

namespace epoch_script::transform {

void ITransform::EmitProgress(size_t current, size_t total,
                              const std::string& message) const {
    if (m_progressEmitter) {
        m_progressEmitter->EmitProgress(current, total, message);
    }
}

void ITransform::EmitEpoch(size_t epoch, size_t total_epochs,
                           std::optional<double> loss,
                           std::optional<double> accuracy) const {
    if (m_progressEmitter) {
        m_progressEmitter->EmitEpoch(epoch, total_epochs, loss, accuracy);
    }
}

void ITransform::EmitIteration(size_t iteration,
                               std::optional<double> metric,
                               const std::string& message) const {
    if (m_progressEmitter) {
        m_progressEmitter->EmitIteration(iteration, metric, message);
    }
}

void ITransform::ThrowIfCancelled() const {
    if (m_progressEmitter) {
        m_progressEmitter->ThrowIfCancelled();
    }
}

bool ITransform::IsCancelled() const {
    return m_progressEmitter && m_progressEmitter->IsCancelled();
}

} // namespace epoch_script::transform
