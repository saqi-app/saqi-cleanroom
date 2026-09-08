import AppKit
import Foundation

extension RigStore {
    var codexEnrichmentProviders: [ProviderHealth] {
        (health?.enrichmentProviders ?? []).filter { $0.provider == Self.codexProvider }
    }

    var concurrencyDiagnostics: String {
        guard let value = requestedConcurrency[Self.codexProvider] else { return "none" }
        return "codex=\(value) (\(concurrencyApplicationState[Self.codexProvider] ?? "unknown"))"
    }

    private var providerDiagnostics: String {
        codexEnrichmentProviders.map { provider in
            let queue = health?.queue(provider.workKind)
            let concurrency = [
                provider.activeInvocations,
                provider.selectedConcurrency,
                provider.invocationConcurrency,
            ].map(String.init).joined(separator: "/")
            return [
                "\(provider.provider): \(queue?.succeeded ?? 0)/\(queue?.total ?? 0)",
                "concurrency actual/selected/ceiling \(concurrency)",
                "wait \(provider.wait?.kind.rawValue ?? "none")",
                "disposition \(provider.lastDisposition)",
                "provider error \(Self.diagnosticCode(provider.providerErrorCode))",
                "retry at \(provider.retryAt.map(String.init) ?? "none")",
                "retry \(queue?.retryWait ?? 0)",
                "quota \(queue?.quotaWait ?? 0)",
                "dead \(queue?.deadLetter ?? 0)",
            ].joined(separator: ", ")
        }
        .joined(separator: "\n")
    }

    private var codexExecutionDiagnostics: String {
        guard let execution = providerExecutionHealth,
              let provider = execution.providers.first(where: { $0.provider == "sol" })
        else { return "Codex execution: unavailable" }
        let gates = provider.gates
        let current = runtime?.providerExecution == execution
        let admission = [
            "admission \(provider.admission.state)/\(provider.admission.primaryReason)",
            "recovery \(provider.admission.recovery)",
            "action \(provider.admission.operatorAction)",
            "retry at \(Self.diagnosticTime(provider.admission.retryAt))",
        ].joined(separator: ", ")
        let credentials = [
            "credentials \(provider.credentials.state)",
            "error \(Self.diagnosticCode(provider.credentials.errorCode))",
            "retry at \(Self.diagnosticTime(provider.credentials.retryAt))",
        ].joined(separator: ", ")
        let authentication = [
            "authentication \(gates.authentication.state)",
            "error \(Self.diagnosticCode(gates.authentication.errorCode))",
            "retry at \(Self.diagnosticTime(gates.authentication.retryAt))",
        ].joined(separator: ", ")
        let budgetCounts = [
            gates.budget.maximumOperations,
            gates.budget.remainingOperations,
            gates.budget.reservedOperations,
        ].map(String.init).joined(separator: "/")
        let providerGate = [
            "provider \(gates.provider.state)",
            "error \(Self.diagnosticCode(gates.provider.errorCode))",
            "retry at \(Self.diagnosticTime(gates.provider.retryAt))",
        ].joined(separator: ", ")
        let quota = [
            "quota \(gates.quota.state)",
            "error \(Self.diagnosticCode(gates.quota.errorCode))",
            "retry at \(Self.diagnosticTime(gates.quota.retryAt))",
            "next probe \(Self.diagnosticTime(gates.quota.nextProbeAt))",
        ].joined(separator: ", ")
        let resources = [
            "resources \(gates.resources.state)",
            "reasons \(gates.resources.reasons.joined(separator: ","))",
            "next probe \(Self.diagnosticTime(gates.resources.nextProbeAt))",
        ].joined(separator: ", ")
        let schedulerCounts = [
            gates.scheduler.activeInvocations,
            gates.scheduler.selectedConcurrency,
            gates.scheduler.configuredConcurrency,
        ].map(String.init).joined(separator: "/")
        let progressCounts = [
            provider.progress.accepted,
            provider.progress.activeInvocations,
            provider.progress.readyWork,
            provider.progress.delayedWork,
            provider.progress.terminalWork,
        ].map(String.init).joined(separator: "/")
        return [
            "Codex execution: current \(current)",
            admission,
            credentials,
            authentication,
            "budget \(gates.budget.state), maximum/remaining/reserved \(budgetCounts)",
            "operator global paused \(gates.operator.globalPaused), paid paused \(gates.operator.paidWorkPaused)",
            providerGate,
            quota,
            resources,
            "scheduler \(gates.scheduler.state), active/selected/configured \(schedulerCounts), " +
                "next wake \(Self.diagnosticTime(gates.scheduler.nextWakeAt))",
            "progress \(provider.progress.state), accepted/active/ready/delayed/terminal \(progressCounts), " +
                "last accepted \(Self.diagnosticTime(provider.progress.lastAcceptedAt))",
        ].joined(separator: "\n")
    }

    private var serviceRuntimeMatch: String {
        guard let service, let runtime else { return "unavailable" }
        return String(runtime.matches(service: service))
    }

    private var loadedDesiredConfigMatch: String {
        guard let service, let desired = service.desiredConfigDigest,
              let loaded = service.loadedConfigDigest
        else { return "unavailable" }
        return String(desired == loaded)
    }

    var diagnosticsText: String {
        let poem = health?.collectionPoemQueue
        return """
        Saqi monitor diagnostics
        Process: \(service?.actualState ?? "unavailable") · service/runtime match \(serviceRuntimeMatch)
        Pipeline: \(health?.state ?? "unavailable") · Fresh: \(isFresh) · Error present: \(diagnosticError != nil)
        Operational: \(operationalStatus.label) · self-healing \(operationalStatus.wait
            .map { !$0.requiresAttention } ?? false)
        Resource: \(runtime?.resourcePressure.state ?? "unavailable") · \(runtime?.resourcePressure.reasons
            .joined(separator: ",") ?? "none")
        Host admission: \(health?.providerHostAdmission?.label ?? "unavailable")
        Checks: \((health?.checks ?? [])
            .map { "\(Self.diagnosticCode($0.code)): \($0.state), retry at \(Self.diagnosticTime($0.retryAt))" }
            .joined(separator: "; "))
        Collection: \(poem?.succeeded ?? 0)/\(poem?.total ?? 0), active \(poem?.active ?? 0), retry \(poem?
            .retryWait ?? 0), quota \(poem?.quotaWait ?? 0), dead \(poem?.deadLetter ?? 0)
        Requested concurrency: \(concurrencyDiagnostics)
        Config: loaded/desired match \(loadedDesiredConfigMatch)
        \(providerDiagnostics.isEmpty ? "Enrichment: unavailable" : providerDiagnostics)
        \(codexExecutionDiagnostics)
        """
    }

    nonisolated private static func diagnosticCode(_ value: String?) -> String {
        guard let value,
              value.range(of: #"^[A-Z0-9_]{1,128}$"#, options: .regularExpression) != nil
        else { return value == nil ? "none" : "invalid" }
        return value
    }

    nonisolated private static func diagnosticTime(_ value: Int?) -> String {
        value.map(String.init) ?? "none"
    }

    func start() {
        guard serviceControlsAvailable else {
            phase = .failed("Service status must be current before starting")
            return
        }
        performServiceAction("start", label: "Starting")
    }

    func stop() {
        guard serviceControlsAvailable else {
            phase = .failed("Service status must be current before stopping")
            return
        }
        performServiceAction("stop", label: "Stopping after current work")
    }

    func restart() {
        guard serviceControlsAvailable else {
            phase = .failed("Service status must be current before restarting")
            return
        }
        performServiceAction("restart", label: "Restarting after current work")
    }

    func setConcurrency(_ provider: String, _ value: Int) {
        guard provider == Self.codexProvider else {
            phase = .failed("Unsupported provider")
            return
        }
        guard Self.concurrencyChoices.contains(value) else {
            phase = .failed("Unsupported concurrency")
            return
        }
        guard concurrencyControlsAvailable else {
            phase = .failed("Current runtime diagnostics are required before changing concurrency")
            return
        }
        runControl {
            self.requestedConcurrency[provider] = value
            self.concurrencyApplicationState[provider] = "applying"
            self.phase = .applying("Applying Codex concurrency \(value)")
            do {
                let response = try await self.requestConcurrency(provider: provider, value: value)
                try self.validateConcurrencyResponse(response, provider: provider, value: value)
                self.applyConcurrencyResponse(response, provider: provider)
                await self.refresh(forceService: true)
            } catch {
                // The CLI can durably save the requested value before its response is
                // interrupted. Keep showing the requested value beside the explicit
                // failure until health proves which configuration the service loaded.
                await self.refresh(forceService: true)
                if Self.isProvenPreMutationFailure(error) {
                    self.requestedConcurrency.removeValue(forKey: provider)
                    self.concurrencyApplicationState.removeValue(forKey: provider)
                } else {
                    self.concurrencyApplicationState[provider] = "outcome_unknown"
                }
                throw error
            }
        }
    }

    private func requestConcurrency(provider: String, value: Int) async throws -> ConcurrencyControlResponse {
        let data = try await Self.run(configuration.crawlerCLI, [
            "set-concurrency",
            "--config",
            configuration.rigConfig.path,
            "--provider",
            provider,
            "--value",
            String(value),
            "--label",
            configuration.serviceLabel,
        ])
        return try JSONDecoder().decode(ConcurrencyControlResponse.self, from: data)
    }

    private func validateConcurrencyResponse(
        _ response: ConcurrencyControlResponse,
        provider: String,
        value: Int
    ) throws {
        guard response.command == "set-concurrency" else { throw MonitorDataError.invalidServiceDocument }
        try validateCodexConcurrency(response.result.concurrency)
        try response.result.service?.validate()
        guard response.result.concurrency.provider == provider,
              response.result.concurrency.value == value,
              ["applied", "pending_start", "restart_failed", "status_unavailable"]
              .contains(response.result.applicationState)
        else { throw MonitorDataError.invalidServiceDocument }
        try validateServiceState(response.result)
    }

    private func validateCodexConcurrency(_ concurrency: ConcurrencyUpdateSnapshot) throws {
        guard concurrency.provider == Self.codexProvider,
              Self.concurrencyChoices.contains(concurrency.value),
              concurrency.previousValue > 0,
              concurrency.configDigest.range(of: #"^[a-f0-9]{64}$"#, options: .regularExpression) != nil,
              !concurrency.configPath.isEmpty
        else { throw MonitorDataError.invalidServiceDocument }
    }

    private func validateServiceState(_ result: ConcurrencyControlResult) throws {
        if result.applicationState == "applied" {
            guard result.service?.actualState == "running",
                  result.service?.loadedConfigDigest == result.concurrency.configDigest
            else { throw MonitorDataError.invalidServiceDocument }
        }
        if result.applicationState == "pending_start" {
            guard result.service?.actualState == "stopped", result.service?.serviceEnabled == .disabled
            else { throw MonitorDataError.invalidServiceDocument }
        }
    }

    private func applyConcurrencyResponse(_ response: ConcurrencyControlResponse, provider: String) {
        let result = response.result
        if let service = result.service {
            self.service = service
        }
        concurrencyApplicationState[provider] = result.applicationState
        guard ["restart_failed", "status_unavailable"].contains(result.applicationState) else {
            phase = .idle
            return
        }
        let restartError = result.restartError ?? "restart required"
        phase = .failed("Saved for Codex, but the running service did not apply it · \(restartError)")
    }

    func setStartsAtLogin(_ enabled: Bool) {
        do {
            try startupPreference.setEnabled(enabled)
            startsAtLogin = enabled
        } catch {
            phase = .failed("Startup preference failed: \(error.localizedDescription)")
        }
    }

    func pauseAfterCurrentWork() {
        runControl {
            self.phase = .applying("Pausing new work")
            try await self.crawler(["pause"])
            self.phase = .idle
            await self.refresh(forceService: true)
        }
    }

    func resume() {
        runControl {
            self.phase = .applying("Resuming")
            try await self.crawler(["resume"])
            self.phase = .idle
            await self.refresh(forceService: true)
        }
    }

    func copyDiagnostics() {
        NSPasteboard.general.clearContents()
        NSPasteboard.general.setString(diagnosticsText, forType: .string)
    }

    func openLogs() {
        NSWorkspace.shared.open(configuration.stateDirectory.appending(path: "logs"))
    }
}
