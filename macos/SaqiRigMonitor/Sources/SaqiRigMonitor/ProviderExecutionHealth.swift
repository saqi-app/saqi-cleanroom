import Foundation

internal struct ProviderExecutionHealth: Codable, Hashable {
    let configDigest: String
    let observedAt: Int
    let providers: [ProviderExecutionEntry]
    let runId: String
    let schemaId: String
    let schemaVersion: Int

    func validate(now: Date = Date()) throws {
        let nowMilliseconds = Int(now.timeIntervalSince1970 * 1000)
        guard schemaId == "saqi.provider-execution-health", schemaVersion == 1,
              configDigest.range(of: #"^[a-f0-9]{64}$"#, options: .regularExpression) != nil,
              observedAt >= 0, observedAt <= nowMilliseconds + 60_000,
              nowMilliseconds - observedAt <= 15 * 60 * 1000,
              !runId.isEmpty, runId.count <= 256,
              runId == runId.trimmingCharacters(in: .whitespacesAndNewlines),
              providers.count == 1,
              Set(providers.map(\.provider)).count == providers.count,
              providers.allSatisfy({ $0.isValid(observedAt: observedAt) })
        else { throw MonitorDataError.invalidHealthDocument }
    }
}

internal struct ProviderExecutionEntry: Codable, Hashable {
    let admission: ProviderExecutionAdmission
    let credentials: ProviderCredentialHealth
    let enabled: Bool
    let gates: ProviderExecutionGates
    let model: String
    let modelKey: String
    let progress: ProviderExecutionProgress
    let provider: String
    let sessions: ProviderExecutionSessions
    var throughput: ProviderPoemThroughput?

    func isValid(observedAt: Int) -> Bool {
        let (currentAndPrevious, firstOverflow) = sessions.activeCurrentAccountEpoch.addingReportingOverflow(
            sessions.activePreviousAccountEpoch
        )
        let (active, secondOverflow) = currentAndPrevious.addingReportingOverflow(sessions.activeUnattributed)
        return provider == "sol"
            && !model.isEmpty && model.count <= 128 && model == model.trimmingCharacters(in: .whitespacesAndNewlines)
            && !modelKey.isEmpty && modelKey.count <= 128
            && modelKey == modelKey.trimmingCharacters(in: .whitespacesAndNewlines)
            && admission.isValid(observedAt: observedAt)
            && credentials.isValid(observedAt: observedAt)
            && gates.isValid(observedAt: observedAt)
            && progress.isValid
            && sessions.isValid
            && throughput.map { value in
                value.isValid && [value.generated.lastAt, value.published.lastAt]
                    .allSatisfy { $0.map { $0 <= observedAt } != false }
            } != false
            && !firstOverflow && !secondOverflow
            && active == progress.activeInvocations
            && gates.scheduler.activeInvocations == progress.activeInvocations
            && (credentials.state != "account_switch_wait" || sessions.activePreviousAccountEpoch > 0)
            && (credentials.state != "ready" || sessions.activePreviousAccountEpoch == 0)
    }
}

internal struct ProviderPoemThroughput: Codable, Hashable {
    let coverage: ProviderPoemThroughputCoverage
    let generated: PoemMilestoneWindow
    let published: PoemMilestoneWindow
    let remaining: ProviderPoemRemaining

    var isValid: Bool {
        coverage.isValid && generated.isValid && published.isValid && remaining.isValid
    }
}

internal struct ProviderPoemThroughputCoverage: Codable, Hashable {
    let backfillComplete: Bool
    let highWatermark: Int
    let state: String

    var isValid: Bool {
        highWatermark >= 0 && ["backfilling", "complete"].contains(state)
            && (state == "complete") == backfillComplete
    }
}

internal struct PoemMilestoneWindow: Codable, Hashable {
    let last15m: Int
    let last1h: Int
    let last5m: Int
    let lastAt: Int?

    var isValid: Bool {
        [last15m, last1h, last5m].allSatisfy { $0 >= 0 }
            && last5m <= last15m && last15m <= last1h
            && lastAt.map { $0 >= 0 } != false
    }
}

internal struct ProviderPoemRemaining: Codable, Hashable {
    let active: Int
    let delayed: Int
    let endToEndPublication: Int
    let generatedAwaitingPublication: Int
    let generation: Int
    let ready: Int
    let terminalDead: Int

    var isValid: Bool {
        let values = [
            active,
            delayed,
            endToEndPublication,
            generatedAwaitingPublication,
            generation,
            ready,
            terminalDead,
        ]
        let (generationTotal, generationOverflow) = ready.addingReportingOverflow(delayed)
        let (generationWithActive, activeOverflow) = generationTotal.addingReportingOverflow(active)
        let (publicationTotal, publicationOverflow) = generation.addingReportingOverflow(generatedAwaitingPublication)
        return values.allSatisfy { $0 >= 0 }
            && !generationOverflow && !activeOverflow && generationWithActive == generation
            && !publicationOverflow && publicationTotal == endToEndPublication
    }
}

internal struct ProviderExecutionAdmission: Codable, Hashable {
    let operatorAction: String
    let primaryReason: String
    let recovery: String
    let retryAt: Int?
    let state: String

    func isValid(observedAt _: Int) -> Bool {
        ["open", "limited", "waiting", "closed", "disabled"].contains(state)
            && ["automatic", "operator", "configuration", "none"].contains(recovery)
            && [
                "none",
                "resume_all",
                "resume_paid",
                "arm_budget",
                "rearm_budget",
                "restore_auth",
                "enable_provider",
                "inspect_provider",
            ].contains(operatorAction)
            && [
                "active",
                "ready",
                "no_ready_work",
                "adaptive_capacity",
                "at_capacity",
                "launch_pacing",
                "error_dampener",
                "operator_paused",
                "paid_work_paused",
                "budget_unarmed",
                "budget_exhausted",
                "resource_wait",
                "auth_wait",
                "codex_quota_wait",
                "network_wait",
                "rate_limit_wait",
                "provider_backoff",
                "circuit_open",
                "disabled",
            ].contains(primaryReason)
            && retryAt.map { $0 >= 0 } != false
    }
}

internal struct ProviderCredentialHealth: Codable, Hashable {
    let accountEpoch: Int
    let change: String
    let changedAt: Int?
    let errorCode: String?
    let lastVerifiedAt: Int?
    let materialEpoch: Int
    let retryAt: Int?
    let state: String

    private var transitionIsConsistent: Bool {
        switch state {
        case "ready":
            change == "none" && errorCode == nil && retryAt == nil
        case "absent":
            change == "none" && errorCode != nil && retryAt == nil
        case "material_verification":
            change == "material_refresh" && changedAt != nil && errorCode != nil && retryAt != nil
        case "account_switch_wait", "account_verification":
            change == "account_switch" && changedAt != nil && errorCode != nil && retryAt != nil
        case "observation_wait":
            change == "none" && errorCode != nil && retryAt != nil
        default:
            false
        }
    }

    func isValid(observedAt: Int) -> Bool {
        accountEpoch >= 0 && materialEpoch >= 0
            && ["none", "material_refresh", "account_switch"].contains(change)
            && [
                "ready",
                "absent",
                "observation_wait",
                "material_verification",
                "account_switch_wait",
                "account_verification",
            ].contains(state)
            && changedAt.map { $0 >= 0 && $0 <= observedAt } != false
            && errorCode.map(isValidErrorCode) != false
            && lastVerifiedAt.map { $0 >= 0 && $0 <= observedAt } != false
            && retryAt.map { $0 > observedAt } != false
            && transitionIsConsistent
    }
}

internal struct ProviderExecutionProgress: Codable, Hashable {
    let accepted: Int
    let activeInvocations: Int
    let delayedWork: Int
    let lastAcceptedAt: Int?
    let readyWork: Int
    let state: String
    let terminalWork: Int

    var isValid: Bool {
        [accepted, activeInvocations, delayedWork, readyWork, terminalWork].allSatisfy { $0 >= 0 }
            && ["active", "recent", "stalled", "idle"].contains(state)
            && (state == "active") == (activeInvocations > 0)
            && lastAcceptedAt.map { $0 >= 0 } != false
    }
}

internal struct ProviderExecutionSessions: Codable, Hashable {
    let activeCurrentAccountEpoch: Int
    let activePreviousAccountEpoch: Int
    let activeUnattributed: Int

    var isValid: Bool {
        activeCurrentAccountEpoch >= 0 && activePreviousAccountEpoch >= 0 && activeUnattributed >= 0
    }
}

internal struct ProviderExecutionGates: Codable, Hashable {
    let authentication: RetryGate
    let budget: BudgetGate
    let `operator`: OperatorGate
    let provider: ProviderGate
    let quota: QuotaGate
    let resources: ResourceGate
    let scheduler: SchedulerGate

    func isValid(observedAt: Int) -> Bool {
        authentication.isValid(observedAt: observedAt)
            && budget.isValid
            && provider.isValid(observedAt: observedAt)
            && quota.isValid(observedAt: observedAt)
            && resources.isValid(observedAt: observedAt)
            && scheduler.isValid(observedAt: observedAt)
    }
}

internal struct RetryGate: Codable, Hashable {
    let errorCode: String?
    let retryAt: Int?
    let state: String

    func isValid(observedAt: Int) -> Bool {
        guard errorCode.map(isValidErrorCode) != false else { return false }
        return state == "ready" ? errorCode == nil && retryAt == nil
            : state == "waiting" && errorCode != nil && retryAt.map { $0 > observedAt } == true
    }
}

internal struct BudgetGate: Codable, Hashable {
    let budgetId: String?
    let maximumOperations: Int
    let remainingOperations: Int
    let reservedOperations: Int
    let state: String

    var isValid: Bool {
        guard ["unarmed", "active", "exhausted", "closed"].contains(state) else { return false }
        if state == "unarmed" {
            return budgetId == nil && maximumOperations == 0 && remainingOperations == 0
                && reservedOperations == 0
        }
        guard budgetId.flatMap(UUID.init(uuidString:)) != nil,
              maximumOperations > 0, maximumOperations.isMultiple(of: 3),
              reservedOperations >= 0, reservedOperations.isMultiple(of: 3),
              remainingOperations == maximumOperations - reservedOperations
        else { return false }
        return state == "active" ? remainingOperations >= 3
            : state != "exhausted" || remainingOperations < 3
    }
}

internal struct OperatorGate: Codable, Hashable {
    let globalPaused: Bool
    let paidWorkPaused: Bool
}

internal struct ProviderGate: Codable, Hashable {
    let errorCode: String?
    let retryAt: Int?
    let state: String

    func isValid(observedAt: Int) -> Bool {
        guard errorCode.map(isValidErrorCode) != false else { return false }
        return state == "ready" ? errorCode == nil && retryAt == nil
            : ["network_wait", "rate_limited", "backoff"].contains(state)
            && errorCode != nil && retryAt.map { $0 > observedAt } == true
    }
}

internal struct QuotaGate: Codable, Hashable {
    let errorCode: String?
    let nextProbeAt: Int?
    let retryAt: Int?
    let state: String

    func isValid(observedAt: Int) -> Bool {
        guard errorCode.map(isValidErrorCode) != false,
              nextProbeAt.map({ $0 >= 0 }) != false
        else { return false }
        if state == "clear" {
            return errorCode == nil && nextProbeAt == nil && retryAt == nil
        }
        return state == "waiting" && errorCode != nil && retryAt.map { $0 > observedAt } == true
    }
}

internal struct ResourceGate: Codable, Hashable {
    let nextProbeAt: Int?
    let reasons: [String]
    let state: String

    func isValid(observedAt _: Int) -> Bool {
        let allowed = Set([
            "DISK_PRESSURE",
            "MEMORY_PRESSURE",
            "PROCESS_MEMORY_PRESSURE",
            "FILE_DESCRIPTOR_PRESSURE",
            "RESOURCE_PROBE_FAILED",
        ])
        return reasons.count <= 5 && Set(reasons).isSubset(of: allowed) && Set(reasons).count == reasons.count
            && nextProbeAt.map({ $0 >= 0 }) != false
            && (state == "ready" ? reasons.isEmpty : state == "waiting" && !reasons.isEmpty)
    }
}

internal struct SchedulerGate: Codable, Hashable {
    let activeInvocations: Int
    let configuredConcurrency: Int
    let nextWakeAt: Int?
    let selectedConcurrency: Int
    let state: String

    func isValid(observedAt _: Int) -> Bool {
        activeInvocations >= 0 && configuredConcurrency >= 1 && configuredConcurrency <= 256
            && selectedConcurrency >= 1 && selectedConcurrency <= configuredConcurrency
            && activeInvocations <= configuredConcurrency
            && ["ready", "adaptive", "at_capacity", "launch_pacing", "error_dampener", "circuit_open"]
            .contains(state)
            && nextWakeAt.map { $0 >= 0 } != false
    }
}

private func isValidErrorCode(_ value: String) -> Bool {
    value.range(of: #"^[A-Z][A-Z0-9_]{1,127}$"#, options: .regularExpression) != nil
}
