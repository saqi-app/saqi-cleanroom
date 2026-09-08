import Foundation

internal enum OperationalWaitKind: String {
    case authentication = "authentication"
    case budget = "budget"
    case network = "network"
    case operatorPause = "operatorPause"
    case paidPause = "paidPause"
    case provider = "provider"
    case quota = "quota"
    case rateLimit = "rateLimit"
    case resource = "resource"
    case retry = "retry"
    case source = "source"
}

internal struct OperationalWait: Equatable {
    let action: String
    let kind: OperationalWaitKind
    let requiresAttention: Bool
}

internal enum MonitorSeverity {
    case attention
    case healthy
    case neutral
    case waiting
}

internal struct MonitorOperationalStatus: Equatable {
    let label: String
    let severity: MonitorSeverity
    let wait: OperationalWait?

    var requiresAttention: Bool {
        severity == .attention
    }

    static func evaluate(
        service: ServiceSnapshot?,
        health: PipelineHealth?,
        runtime: RuntimeStatus?,
        paused: Bool,
        now: Date = Date()
    ) -> Self {
        if let immediate = immediateStatus(service: service) {
            return immediate
        }
        guard let service else {
            return Self(label: "Diagnostics unavailable", severity: .attention, wait: nil)
        }
        let runtimeFallback = currentRuntimeStatus(runtime: runtime, service: service)
        if health == nil {
            if let runtimeFallback {
                return runtimeFallback
            }
            return Self(label: "Diagnostics unavailable", severity: .attention, wait: nil)
        }
        guard let health else {
            return Self(label: "Diagnostics unavailable", severity: .attention, wait: nil)
        }
        return evaluatedStatus(
            service: service,
            health: health,
            runtime: runtime,
            paused: paused,
            now: now
        )
    }

    private static func evaluatedStatus(
        service: ServiceSnapshot,
        health: PipelineHealth,
        runtime: RuntimeStatus?,
        paused: Bool,
        now: Date
    ) -> Self {
        let runtimeFallback = currentRuntimeStatus(runtime: runtime, service: service)
        let maximumAge = Double(max(15 * 60_000, (health.heartbeatIntervalMs ?? 5 * 60_000) * 3)) / 1000
        let healthAge = now.timeIntervalSince1970 - Double(health.observedAt) / 1000
        guard healthAge >= -30, healthAge <= maximumAge else {
            if let runtimeFallback {
                return runtimeFallback
            }
            return Self(label: "Diagnostics stale · attention needed", severity: .attention, wait: nil)
        }
        guard service.configDigest == health.configDigest,
              service.ownerPid.map({ health.runId == "process:\($0)" }) == true
        else {
            if let runtimeFallback {
                return runtimeFallback
            }
            return Self(label: "Starting · waiting for diagnostics", severity: .waiting, wait: nil)
        }
        if paused {
            return pausedStatus(health: health)
        }
        if health.state == "blocked" {
            return Self(label: "Pipeline blocked · attention needed", severity: .attention, wait: nil)
        }
        return runningStatus(service: service, health: health, runtime: runtime)
    }

    private static func currentRuntimeStatus(runtime: RuntimeStatus?, service: ServiceSnapshot) -> Self? {
        guard let runtime, runtime.matches(service: service) else { return nil }
        let active = runtime.providerExecution?.providers
            .first(where: { $0.provider == "sol" })?.progress.activeInvocations ?? 0
        if let provider = providerExecutionStatus(runtime: runtime, service: service, active: active) {
            return provider
        }
        return resourceStatus(runtime: runtime, service: service, active: active)
    }

    private static func runningStatus(
        service: ServiceSnapshot,
        health: PipelineHealth,
        runtime: RuntimeStatus?
    ) -> Self {
        let active = health.queues.reduce(0) { ProgressEstimator.saturatingAdd($0, $1.active) }
        let providerStatus = providerExecutionStatus(runtime: runtime, service: service, active: active)
        if providerStatus?.wait?.requiresAttention == true {
            return providerStatus ?? Self(label: "Diagnostics unavailable", severity: .attention, wait: nil)
        }
        if let waiting = resourceStatus(runtime: runtime, service: service, active: active) {
            return waiting
        }
        if let waiting = originStatus(health: health, active: active) {
            return waiting
        }
        if health.growth?.production.state == "stalled" {
            return Self(label: "Production stalled · attention needed", severity: .attention, wait: nil)
        }
        if let providerStatus {
            return providerStatus
        }
        return legacyRunningStatus(health: health, active: active)
    }

    private static func pausedStatus(health: PipelineHealth) -> Self {
        let active = health.queues.reduce(0) { ProgressEstimator.saturatingAdd($0, $1.active) }
        return Self(label: active > 0 ? "Pausing · \(active) active" : "Paused", severity: .neutral, wait: nil)
    }

    private static func resourceStatus(runtime: RuntimeStatus?, service: ServiceSnapshot, active: Int) -> Self? {
        guard let runtime, runtime.matches(service: service), runtime.resourcePressure.state == "resource_wait" else {
            return nil
        }
        let reason = runtime.resourcePressure.reasons.first ?? "RESOURCE_PRESSURE"
        let wait = OperationalWait(
            action: "\(reason.replacingOccurrences(of: "_", with: " ").capitalized); resuming automatically",
            kind: .resource,
            requiresAttention: false
        )
        return Self(
            label: active > 0 ? "Resource wait · \(active) draining" : wait.action,
            severity: .waiting,
            wait: wait
        )
    }

    private static func originStatus(health: PipelineHealth, active: Int) -> Self? {
        guard let origin = health.origins
            .filter({ effectiveOriginState($0) != "healthy" && effectiveOriginState($0) != "disabled" })
            .min(by: { originPriority($0) < originPriority($1) })
        else { return nil }
        let state = effectiveOriginState(origin)
        let human = state == "challenge_wait"
        let network = state == "network_wait"
        let disk = state == "disk_wait"
        let rate = state == "rate_wait"
        let wait = OperationalWait(
            action: human
                ? "Source challenge needs browser attention; automatic probes continue"
                : network
                ? "Internet unavailable; source retrying automatically"
                : disk
                ? "Source storage wait; retrying automatically"
                : rate
                ? "Source rate limit; retrying automatically"
                : "Source cooling down; retrying automatically",
            kind: network ? .network : disk ? .resource : rate ? .rateLimit : .source,
            requiresAttention: human
        )
        return Self(
            label: active > 0 ? "Running · \(active) active · \(wait.action)" : wait.action,
            severity: human ? .attention : (active > 0 ? .healthy : .waiting),
            wait: wait
        )
    }

    private static func effectiveOriginState(_ origin: OriginHealth) -> String {
        if let state = origin.state {
            return state
        }
        guard let reason = origin.stopReason else { return "healthy" }
        if reason.contains("HUMAN_REQUIRED") || reason.contains("CHALLENGE") {
            return "challenge_wait"
        }
        if reason.contains("NETWORK") {
            return "network_wait"
        }
        if reason.contains("RATE_LIMIT") {
            return "rate_wait"
        }
        if reason.contains("DISK") || reason.contains("STORAGE") || reason.contains("CAPACITY") {
            return "disk_wait"
        }
        return "blocked"
    }

    private static func originPriority(_ origin: OriginHealth) -> Int {
        switch effectiveOriginState(origin) {
        case "challenge_wait":
            0
        case "blocked":
            1
        case "disk_wait":
            2
        case "network_wait":
            3
        case "rate_wait":
            4
        default:
            5
        }
    }
}

internal struct PipelineGrowth: Codable, Hashable {
    let production: ProductionGrowth
}

internal struct ProductionGrowth: Codable, Hashable {
    let configured: Bool
    let lastSuccessAt: Int?
    let state: String

    var isOperational: Bool {
        configured && (state == "ready" || (state == "active" && lastSuccessAt != nil))
    }

    var label: String {
        guard configured else { return "Production · Off" }
        switch state {
        case "active":
            return "Production · Publishing"
        case "ready":
            return "Production · Ready"
        case "gated":
            return "Production · Gated"
        case "stalled":
            return "Production · Stalled"
        case "disabled":
            return "Production · Off"
        default:
            return "Production · Unknown"
        }
    }
}

internal struct HealthCheck: Codable, Hashable, Identifiable {
    let code: String
    let detail: String?
    let retryAt: Int?
    let state: String

    var id: String {
        code
    }

    var requiresAttention: Bool {
        state == "warning" || state == "blocked"
    }
}

internal struct ServiceControlResponse: Codable {
    let command: String
    let result: ServiceSnapshot
}

internal struct ConcurrencyControlResponse: Codable {
    let command: String
    let result: ConcurrencyControlResult
}

internal struct ConcurrencyControlResult: Codable {
    let applicationState: String
    let concurrency: ConcurrencyUpdateSnapshot
    let restartError: String?
    let service: ServiceSnapshot?
}

internal struct ConcurrencyUpdateSnapshot: Codable {
    let configDigest: String
    let configPath: String
    let previousValue: Int
    let provider: String
    let value: Int

    func validate() throws {
        guard provider == "sol",
              [2, 8, 16, 32, 128, 256].contains(value),
              previousValue > 0,
              configDigest.range(of: #"^[a-f0-9]{64}$"#, options: .regularExpression) != nil,
              !configPath.isEmpty
        else { throw MonitorDataError.invalidServiceDocument }
    }
}

internal struct ServiceSnapshot: Codable, Hashable {
    var sourceIdentityStatus: ServiceSourceIdentityStatus?
    let action: String
    let actualState: String
    let allowedActions: [String]
    let configDigest: String
    let desiredConfigDigest: String?
    let detail: String?
    let loadedConfigDigest: String?
    let observedAt: String
    let ownerPid: Int?
    let runId: String?
    let schemaId: String
    let schemaVersion: Int
    let serviceEnabled: ServiceEnablement?

    var onlySourceVerificationUnavailable: Bool {
        sourceIdentityStatus == .retainedUnverified && actualState == "running_outdated"
            && detail == "SOURCE_IDENTITY_UNVERIFIED"
    }

    func allows(_ action: String) -> Bool {
        if sourceIdentityStatus == .retainedUnverified, action == "start" || action == "restart" {
            return false
        }
        return allowedActions.contains(action)
    }

    func validate() throws {
        guard schemaId == "saqi.launchd-control", schemaVersion == 1,
              ["fenced", "running", "running_outdated", "starting", "stopped"].contains(actualState),
              configDigest.range(of: #"^[a-f0-9]{64}$"#, options: .regularExpression) != nil
        else { throw MonitorDataError.invalidServiceDocument }
    }
}

internal enum ServiceSourceIdentityStatus: String, Codable {
    case configured = "configured"
    case retainedUnverified = "retained_unverified"
}

internal enum ServiceEnablement: Codable, Hashable {
    case disabled
    case enabled

    init(from decoder: Decoder) throws {
        let enabled = try decoder.singleValueContainer().decode(Bool.self)
        self = enabled ? .enabled : .disabled
    }

    func encode(to encoder: Encoder) throws {
        var value = encoder.singleValueContainer()
        try value.encode(self == .enabled)
    }
}
