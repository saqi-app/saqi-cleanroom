import AppKit
import Foundation

extension RigStore {
    var canRefreshDiagnostics: Bool {
        activeRefreshes == 0 && !refreshingDiagnostics && !phase.isApplying
    }

    var refreshDiagnosticsTitle: String {
        if refreshingDiagnostics || activeRefreshes > 0 {
            return "Refreshing…"
        }
        guard diagnosticError == nil, !healthRefreshFailed, !runtimeRefreshFailed, !serviceRefreshFailed,
              isFresh, let runtime, let service, runtime.matches(service: service)
        else { return "Retry" }
        return "Refresh"
    }

    var providerRates: [String: Double] {
        ProgressEstimator.rates(samples: samples)
    }

    func refreshDiagnostics() async {
        await refreshDiagnostics { await self.refresh(forceService: true) }
    }

    func refreshDiagnostics(read: () async -> Void) async {
        guard canRefreshDiagnostics else { return }
        refreshingDiagnostics = true
        refreshOutcome = "Refreshing diagnostics…"
        // Explicit observation retries may retry local history persistence now.
        // Automatic refresh retains its backoff; no runtime control is changed.
        nextHistoryImportAttempt = .distantPast
        defer { refreshingDiagnostics = false }
        await read()
        await retryStartupPreferenceObservation()
        if let startupPreferenceError {
            refreshOutcome = startupPreferenceError
        } else if let diagnosticError {
            refreshOutcome = diagnosticError
        } else if !healthRefreshFailed, !runtimeRefreshFailed, !serviceRefreshFailed,
                  isFresh, let runtime, let service, runtime.matches(service: service)
        {
            refreshOutcome = "Diagnostics refreshed"
        } else {
            refreshOutcome = "Diagnostics remain stale or mismatched"
        }
    }

    private func retryStartupPreferenceObservation() async {
        guard startupPreferenceError != nil else { return }
        let preference = startupPreference
        let result = await startupPreferenceReader.read {
            try await Task.detached { try preference.readVerified() }.value
        }
        switch result {
        case let .success(enabled):
            startsAtLogin = enabled
            if diagnosticError == startupPreferenceError {
                diagnosticError = nil
            }
            startupPreferenceError = nil
        case .failure:
            startupPreferenceError = "Monitor login preference unavailable"
        }
    }

    func refresh(forceService: Bool = false) async {
        if refreshingDiagnostics, !forceService {
            return
        }
        activeRefreshes += 1
        defer { activeRefreshes -= 1 }
        refreshGeneration += 1
        let generation = refreshGeneration
        let currentConfiguration = configuration
        async let healthResult = healthFileReader.read {
            try await Task.detached { try Self.readHealth(configuration: currentConfiguration) }.value
        }
        async let runtimeResult = runtimeFileReader.read {
            try await Task.detached { try Self.readRuntimeStatus(configuration: currentConfiguration) }.value
        }
        let (serviceResult, loadedService) = await serviceRefreshResult(force: forceService)
        guard generation == refreshGeneration else { return }
        let loadedRuntime = await runtimeResult
        guard generation == refreshGeneration else { return }
        applyRuntimeRefreshResult(loadedRuntime)
        let loadedHealth = await healthResult
        guard generation == refreshGeneration else { return }
        applyRefreshResults(loadedHealth, serviceResult, loadedService: loadedService)
        if case let .failure(error) = loadedRuntime {
            diagnosticError = error is DiagnosticReadError
                ? "Runtime status read timed out" : "Runtime status unavailable"
        }
        captureCoherentCodexTelemetry()
        applyPauseTelemetry()
    }

    func applyPauseTelemetry(now: Date = Date()) {
        guard !runtimeRefreshFailed, !serviceRefreshFailed,
              let runtime, let service, runtime.matches(service: service),
              let execution = runtime.providerExecution,
              execution.configDigest == runtime.configDigest,
              let provider = execution.providers.first(where: { $0.provider == "sol" })
        else { return }
        let age = now.timeIntervalSince1970 - Double(execution.observedAt) / 1000
        guard age >= -30, age <= 15 * 60 else { return }
        paused = provider.gates.operator.globalPaused
    }

    func persistSamples(now: Date = Date()) {
        guard historyPersistenceEnabled else { return }
        if pendingHistoryImport != nil || historyPersistenceFailed {
            guard now >= nextHistoryImportAttempt else { return }
        }
        do {
            if let defaults = pendingHistoryImport {
                nextHistoryImportAttempt = now.addingTimeInterval(30)
                let imported = try progressStore.load(legacyDefaults: defaults)
                samples = samples.reduce(into: imported) { history, sample in
                    history = ProgressEstimator.recording(sample, in: history, now: now)
                }
                pendingHistoryImport = nil
            }
            try progressStore.save(samples)
            historyPersistenceFailed = false
            if diagnosticError == "Progress history persistence unavailable" {
                diagnosticError = startupPreferenceError
            }
        } catch {
            historyPersistenceFailed = true
            nextHistoryImportAttempt = now.addingTimeInterval(30)
            diagnosticError = "Progress history persistence unavailable"
        }
    }

    private func serviceRefreshResult(force: Bool) async -> (Result<ServiceSnapshot, Error>, Bool) {
        if !force, let service, let lastServiceRefresh,
           Date().timeIntervalSince(lastServiceRefresh) < 10
        {
            return (.success(service), false)
        }
        do {
            return try await (.success(serviceControl("status")), true)
        } catch {
            return (.failure(error), true)
        }
    }

    func applyRuntimeRefreshResult(_ result: Result<RuntimeStatus, Error>) {
        switch result {
        case let .success(snapshot):
            runtime = snapshot
            providerExecutionHealth = snapshot.providerExecution
            runtimeRefreshFailed = false
        case .failure:
            // A transient read failure must not erase the most recent validated
            // runtime document. Presentation code labels retained data as last-known.
            runtimeRefreshFailed = true
        }
    }

    func applyRefreshResults(
        _ healthResult: Result<PipelineHealth, Error>,
        _ serviceResult: Result<ServiceSnapshot, Error>,
        loadedService: Bool
    ) {
        switch (healthResult, serviceResult) {
        case let (.success(snapshot), .success(serviceSnapshot)):
            health = snapshot
            service = serviceSnapshot
            diagnosticError = historyPersistenceFailed
                ? "Progress history persistence unavailable" : startupPreferenceError
            refreshedAt = Date()
            healthRefreshFailed = false
            serviceRefreshFailed = false
            if loadedService {
                lastServiceRefresh = Date()
            }
            recordProgressIfCurrent(snapshot: snapshot, service: serviceSnapshot)
            if pendingHistoryImport != nil || historyPersistenceFailed {
                persistSamples()
            }
        case let (.failure(error), .success(serviceSnapshot)):
            service = serviceSnapshot
            healthRefreshFailed = true
            serviceRefreshFailed = false
            diagnosticError = error is DiagnosticReadError
                ? "Diagnostics read timed out" : "Diagnostics unavailable"
        case let (.success(snapshot), .failure):
            health = snapshot
            healthRefreshFailed = false
            serviceRefreshFailed = true
            diagnosticError = "Service status unavailable"
        case (.failure, .failure):
            healthRefreshFailed = true
            serviceRefreshFailed = true
            diagnosticError = "Diagnostics unavailable"
        }
    }

    func captureCoherentCodexTelemetry() {
        guard let execution = providerExecutionHealth, let runtime, let service,
              runtime.matches(service: service),
              execution.configDigest == runtime.configDigest
        else { return }
        lastCoherentCodexTelemetry = CoherentCodexTelemetry(
            execution: execution,
            runtime: runtime,
            service: service
        )
    }

    private func recordProgressIfCurrent(snapshot: PipelineHealth, service: ServiceSnapshot) {
        let identityMatches = service.configDigest == snapshot.configDigest
            && service.ownerPid.map { snapshot.runId == "process:\($0)" } == true
        guard identityMatches, !snapshot.enrichmentProviders.isEmpty else { return }
        for provider in snapshot.enrichmentProviders
            where requestedConcurrency[provider.provider] == provider.invocationConcurrency
        {
            requestedConcurrency.removeValue(forKey: provider.provider)
            concurrencyApplicationState.removeValue(forKey: provider.provider)
        }
        let acceptedByModel = Dictionary(uniqueKeysWithValues: snapshot.enrichmentProviders.map { provider in
            (provider.modelKey, snapshot.queue(provider.workKind)?.succeeded ?? 0)
        })
        let sample = ProgressSample(
            acceptedByModel: acceptedByModel,
            configDigest: snapshot.configDigest,
            observedAt: Date(timeIntervalSince1970: Double(snapshot.observedAt) / 1000),
            eligibleModels: snapshot.enrichmentProviders.compactMap { provider in
                let queue = snapshot.queue(provider.workKind)
                let waiting = provider.wait != nil || (queue?.quotaWait ?? 0) > 0 || (queue?.retryWait ?? 0) > 0
                return provider.activeInvocations > 0 || (!waiting && (queue?.pending ?? 0) > 0)
                    ? provider.modelKey : nil
            }
        )
        let updated = ProgressEstimator.recording(sample, in: samples, now: Date())
        if updated != samples {
            samples = updated
            persistSamples()
        }
    }
}
