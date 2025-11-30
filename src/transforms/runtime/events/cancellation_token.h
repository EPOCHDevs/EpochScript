#pragma once
//
// Thread-safe Cancellation Token for Pipeline Execution
// Allows external cancellation of running transforms
//

#include <atomic>
#include <memory>
#include <stdexcept>

namespace epoch_script::runtime::events {

// ============================================================================
// Exception thrown when cancellation is detected
// ============================================================================

class OperationCancelledException : public std::runtime_error {
public:
    OperationCancelledException()
        : std::runtime_error("Pipeline execution was cancelled") {}

    explicit OperationCancelledException(const std::string& message)
        : std::runtime_error(message) {}
};

// ============================================================================
// Thread-safe Cancellation Token
// ============================================================================

class CancellationToken {
public:
    CancellationToken() = default;

    // Non-copyable but movable
    CancellationToken(const CancellationToken&) = delete;
    CancellationToken& operator=(const CancellationToken&) = delete;
    CancellationToken(CancellationToken&&) = default;
    CancellationToken& operator=(CancellationToken&&) = default;

    // Request cancellation (called from outside, e.g., UI thread)
    void Cancel() noexcept {
        m_cancelled.store(true, std::memory_order_release);
    }

    // Check if cancelled (called from transforms between iterations)
    [[nodiscard]] bool IsCancelled() const noexcept {
        return m_cancelled.load(std::memory_order_acquire);
    }

    // Throw if cancelled (convenience for transforms)
    void ThrowIfCancelled() const {
        if (IsCancelled()) {
            throw OperationCancelledException();
        }
    }

    // Throw with custom message if cancelled
    void ThrowIfCancelled(const std::string& context) const {
        if (IsCancelled()) {
            throw OperationCancelledException(
                "Operation cancelled: " + context);
        }
    }

    // Reset for reuse (call only when no execution is in progress)
    void Reset() noexcept {
        m_cancelled.store(false, std::memory_order_release);
    }

    // Explicit conversion to bool
    explicit operator bool() const noexcept {
        return IsCancelled();
    }

private:
    std::atomic<bool> m_cancelled{false};
};

using CancellationTokenPtr = std::shared_ptr<CancellationToken>;

// ============================================================================
// Factory Function
// ============================================================================

inline CancellationTokenPtr MakeCancellationToken() {
    return std::make_shared<CancellationToken>();
}

// ============================================================================
// RAII Guard for Checking Cancellation
// ============================================================================

class CancellationGuard {
public:
    explicit CancellationGuard(CancellationTokenPtr token)
        : m_token(std::move(token)) {
        CheckCancellation();
    }

    ~CancellationGuard() noexcept(false) {
        // Check on destruction too (at end of scope)
        if (m_token && m_token->IsCancelled()) {
            // Only throw if we're not already unwinding due to an exception
            if (!std::uncaught_exceptions()) {
                throw OperationCancelledException();
            }
        }
    }

    void CheckCancellation() const {
        if (m_token) {
            m_token->ThrowIfCancelled();
        }
    }

    [[nodiscard]] bool IsCancelled() const noexcept {
        return m_token && m_token->IsCancelled();
    }

private:
    CancellationTokenPtr m_token;
};

} // namespace epoch_script::runtime::events
