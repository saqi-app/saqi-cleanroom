import Foundation

internal enum DiagnosticReadError: LocalizedError {
    case timedOut

    var errorDescription: String? {
        "Diagnostic file read timed out; waiting for the existing read to finish."
    }
}

/// Bounds observation without pretending that a blocked OS read is cancellable.
/// A timed-out read retains ownership until settlement; later callers cannot
/// accumulate filesystem threads, and its eventual result is never presented.
@MainActor
internal final class DiagnosticFileReader<Value: Sendable> {
    private let timeout: Duration
    private var flight: UUID?
    private var timedOut = false
    private var waiters: [CheckedContinuation<Result<Value, Error>, Never>] = []
    private var deadline: Task<Void, Never>?

    var isReadInFlight: Bool {
        flight != nil
    }

    init(timeout: Duration = .seconds(5)) {
        self.timeout = timeout
    }

    func read(operation: @escaping @Sendable () async throws -> Value) async -> Result<Value, Error> {
        if isReadInFlight, timedOut {
            return .failure(DiagnosticReadError.timedOut)
        }
        return await withCheckedContinuation { continuation in
            waiters.append(continuation)
            guard !isReadInFlight else { return }
            let identity = UUID()
            flight = identity
            timedOut = false
            deadline = Task {
                do { try await Task.sleep(for: timeout) } catch { return }
                guard flight == identity else { return }
                timedOut = true
                completeWaiters(.failure(DiagnosticReadError.timedOut))
            }
            Task {
                let result: Result<Value, Error>
                do { result = try await .success(operation()) } catch { result = .failure(error) }
                deadline?.cancel()
                deadline = nil
                flight = nil
                if !timedOut {
                    completeWaiters(result)
                }
            }
        }
    }

    private func completeWaiters(_ result: Result<Value, Error>) {
        let completed = waiters
        waiters.removeAll()
        for waiter in completed {
            waiter.resume(returning: result)
        }
    }
}
