import AppKit
import Darwin
import Foundation

internal enum MonitorProcessEnvironment {
    static let executableSearchPath = "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
}

extension RigStore {
    nonisolated static func run(
        _ executable: URL,
        _ arguments: [String],
        timeout: Duration = .seconds(120),
        outputLimit: Int = 1_048_576
    ) async throws -> Data {
        let worker = Task.detached {
            try await execute(executable, arguments, timeout: timeout, outputLimit: outputLimit)
        }
        return try await withTaskCancellationHandler {
            try await worker.value
        } onCancel: {
            worker.cancel()
        }
    }

    nonisolated private static func execute(
        _ executable: URL,
        _ arguments: [String],
        timeout: Duration,
        outputLimit: Int
    ) async throws -> Data {
        let process = Process()
        process.executableURL = executable
        process.arguments = arguments
        process.environment = sanitizedEnvironment()
        let outputPipe = Pipe()
        let errorPipe = Pipe()
        process.standardOutput = outputPipe
        process.standardError = errorPipe
        defer {
            try? outputPipe.fileHandleForReading.close()
            try? errorPipe.fileHandleForReading.close()
            try? outputPipe.fileHandleForWriting.close()
            try? errorPipe.fileHandleForWriting.close()
        }
        for pipe in [outputPipe, errorPipe] {
            let descriptor = pipe.fileHandleForReading.fileDescriptor
            guard fcntl(descriptor, F_SETFL, O_NONBLOCK) != -1 else {
                throw POSIXError(.EIO)
            }
        }
        try Task.checkCancellation()
        try process.run()
        do {
            try outputPipe.fileHandleForWriting.close()
            try errorPipe.fileHandleForWriting.close()
            return try await collect(process, outputPipe, errorPipe, timeout: timeout, limit: outputLimit)
        } catch {
            await stopChild(process)
            throw error
        }
    }

    nonisolated private static func collect(
        _ process: Process,
        _ outputPipe: Pipe,
        _ errorPipe: Pipe,
        timeout: Duration,
        limit: Int
    ) async throws -> Data {
        var output = Data()
        var error = Data()
        let deadline = ContinuousClock.now.advanced(by: timeout)
        while true {
            try Task.checkCancellation()
            guard ContinuousClock.now < deadline else {
                throw NSError(domain: "SaqiRigMonitor", code: 408, userInfo: [
                    NSLocalizedDescriptionKey:
                        "Command timed out; outcome unknown. Refresh status before retrying any control.",
                ])
            }
            let outputEnded = try drain(outputPipe, into: &output, limit: limit)
            let errorEnded = try drain(errorPipe, into: &error, limit: limit)
            if !process.isRunning, outputEnded, errorEnded {
                break
            }
            try await Task.sleep(for: .milliseconds(20))
        }
        guard process.terminationStatus == 0 else {
            throw NSError(
                domain: "SaqiRigMonitor",
                code: Int(process.terminationStatus),
                userInfo: [
                    NSLocalizedDescriptionKey: String(bytes: error.prefix(1000), encoding: .utf8)
                        ?? "Crawler command failed with non-UTF-8 diagnostics",
                ]
            )
        }
        return output
    }

    /// Bound each drain pass as well as total retained bytes so a noisy stream
    /// cannot starve stderr, cancellation, or the deadline check.
    nonisolated private static func drain(_ pipe: Pipe, into data: inout Data, limit: Int) throws -> Bool {
        var buffer = [UInt8](repeating: 0, count: 8192)
        let count = Darwin.read(pipe.fileHandleForReading.fileDescriptor, &buffer, buffer.count)
        if count == 0 {
            return true
        }
        if count < 0 {
            if errno == EAGAIN || errno == EINTR {
                return false
            }
            throw POSIXError(.EIO)
        }
        guard count <= limit - data.count else { throw CocoaError(.fileReadTooLarge) }
        data.append(contentsOf: buffer.prefix(count))
        return false
    }

    nonisolated private static func stopChild(_ process: Process) async {
        guard process.isRunning else { return }
        process.terminate()
        // Cleanup must continue even when the caller cancelled. Only signal the
        // command we launched, never the separately managed translation runner.
        await Task.detached {
            let deadline = ContinuousClock.now.advanced(by: .seconds(1))
            while process.isRunning, ContinuousClock.now < deadline {
                try? await Task.sleep(for: .milliseconds(20))
            }
            if process.isRunning {
                kill(process.processIdentifier, SIGKILL)
            }
        }.value
    }

    nonisolated static func isProvenPreMutationFailure(_ error: Error) -> Bool {
        let message = error.localizedDescription
        return message.contains("LAUNCHD_CONTROL_BUSY")
            || message.contains("CONCURRENCY_CONFIG_CHANGED")
    }

    nonisolated static func concurrencyControlLabel(
        provider: String,
        configured: Int?,
        requested: Int?,
        state: String?
    ) -> String {
        let name = provider.capitalized
        guard let requested else { return "\(name) target \(configured.map(String.init) ?? "—")" }
        switch state {
        case "pending_start":
            return "\(name) target \(requested) · pending start"
        case "restart_failed":
            return "\(name) target \(requested) desired · current \(configured.map(String.init) ?? "—")"
        case "outcome_unknown":
            return "\(name) target \(requested) · outcome unknown"
        case "applied":
            return "\(name) target \(requested) · awaiting runtime"
        default:
            return "\(name) target \(requested) · applying"
        }
    }

    nonisolated static func readHealth(configuration: MonitorConfiguration) throws -> PipelineHealth {
        let url = configuration.stateDirectory.appending(path: "health/latest.json")
        let values = try url.resourceValues(forKeys: [.fileSizeKey])
        guard let size = values.fileSize, size <= 1_048_576 else { throw CocoaError(.fileReadTooLarge) }
        let health = try JSONDecoder().decode(PipelineHealth.self, from: Data(contentsOf: url, options: .mappedIfSafe))
        try health.validate()
        return health
    }

    nonisolated static func readRuntimeStatus(
        configuration: MonitorConfiguration,
        now: Date = Date()
    ) throws -> RuntimeStatus {
        let url = configuration.stateDirectory.appending(path: "status.json")
        let values = try url.resourceValues(forKeys: [.fileSizeKey])
        guard let size = values.fileSize, size <= 1_048_576 else { throw CocoaError(.fileReadTooLarge) }
        let data = try Data(contentsOf: url, options: .mappedIfSafe)
        let raw = try JSONSerialization.jsonObject(with: data)
        guard !providerExecutionContainsPrivateCredentialField(raw) else {
            throw MonitorDataError.invalidRuntimeDocument
        }
        let status = try JSONDecoder().decode(
            RuntimeStatus.self,
            from: data
        )
        try status.validate(now: now)
        return status
    }

    nonisolated private static func providerExecutionContainsPrivateCredentialField(_ value: Any) -> Bool {
        guard let document = value as? [String: Any],
              let workload = document["workload"] as? [String: Any],
              let providerExecution = workload["providerExecution"],
              !(providerExecution is NSNull)
        else { return false }
        return containsPrivateCredentialField(providerExecution)
    }

    nonisolated private static func containsPrivateCredentialField(_ value: Any) -> Bool {
        let forbidden = Set(["accountGeneration", "materialGeneration", "accountIdentity"])
        if let dictionary = value as? [String: Any] {
            return dictionary.contains { element in
                forbidden.contains(element.key) || containsPrivateCredentialField(element.value)
            }
        }
        if let array = value as? [Any] {
            return array.contains(where: containsPrivateCredentialField)
        }
        return false
    }

    nonisolated static func sanitizedEnvironment(
        source: [String: String] = ProcessInfo.processInfo.environment
    ) -> [String: String] {
        var environment: [String: String] = [
            "HOME",
            "LANG",
            "LC_ALL",
            "LC_CTYPE",
            "NO_COLOR",
            "SSL_CERT_DIR",
            "SSL_CERT_FILE",
            "TMPDIR",
        ]
        .reduce(into: [:]) { result, key in
            if let value = source[key] {
                result[key] = value
            }
        }
        environment["PATH"] = MonitorProcessEnvironment.executableSearchPath
        return environment
    }

    func runControl(_ operation: @escaping @MainActor () async throws -> Void) {
        guard controlTask == nil else { return }
        controlTask = Task {
            defer { controlTask = nil }
            do { try await operation() } catch is CancellationError {} catch {
                phase = .failed("Control failed: \(error.localizedDescription)")
            }
        }
    }

    func performServiceAction(_ action: String, label: String) {
        runControl {
            self.phase = .applying(label)
            self.service = try await self.serviceControl(action)
            self.phase = .idle
            await self.refresh(forceService: true)
        }
    }

    func crawler(_ arguments: [String]) async throws {
        _ = try await Self.run(configuration.crawlerCLI, arguments + ["--state-dir", configuration.stateDirectory.path])
    }

    func serviceControl(_ action: String) async throws -> ServiceSnapshot {
        let arguments = [
            "service-control",
            "--action",
            action,
            "--config",
            configuration.rigConfig.path,
            "--label",
            configuration.serviceLabel,
        ]
        let timeout: Duration = action == "status" ? .seconds(15) : .seconds(120)
        let data = try await Self.run(configuration.crawlerCLI, arguments, timeout: timeout)
        let response = try JSONDecoder().decode(ServiceControlResponse.self, from: data)
        try response.result.validate()
        return response.result
    }
}
