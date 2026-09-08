import Foundation

internal struct QueueHealth: Codable, Hashable, Identifiable {
    let active: Int
    let deadLetter: Int
    let kind: String
    let lastSuccessAt: Int?
    let pending: Int
    let quotaWait: Int
    let retryWait: Int
    let succeeded: Int
    let total: Int

    var id: String {
        kind
    }

    var remaining: Int {
        [pending, retryWait, quotaWait, active].reduce(0) { partial, value in
            let (sum, overflow) = partial.addingReportingOverflow(value)
            return overflow ? Int.max : sum
        }
    }

    var isValid: Bool {
        let counts = [active, deadLetter, pending, quotaWait, retryWait, succeeded, total]
        guard counts.allSatisfy({ $0 >= 0 && $0 <= total }) else { return false }
        var sum = 0
        for value in [active, deadLetter, pending, quotaWait, retryWait, succeeded] {
            let result = sum.addingReportingOverflow(value)
            guard !result.overflow else { return false }
            sum = result.partialValue
        }
        return sum <= total
    }
}

internal struct SolHealth: Codable, Hashable {
    let accepted: Int
    let activeInvocations: Int
    let invocationConcurrency: Int
    let lastDisposition: String
    let model: String
    let retryAt: Int?
    let selectedConcurrency: Int
    let semanticFailures: Int
}

internal struct ProviderHealth: Codable, Hashable, Identifiable {
    let accepted: Int
    let activeInvocations: Int
    let blockReason: String?
    let invocationConcurrency: Int
    let lastDisposition: String
    let model: String
    let modelKey: String
    let nextQuotaProbeAt: Int?
    let provider: String
    let providerErrorCode: String?
    let retryAt: Int?
    let selectedConcurrency: Int
    let semanticFailures: Int
    let unknownOperations: Int?

    var id: String {
        modelKey
    }

    var workKind: String {
        "poem-enrichment-sol"
    }

    var hasAuthenticationIssue: Bool {
        guard let providerErrorCode else { return false }
        return [
            "CODEX_CHATGPT_AUTH_REQUIRED",
            "CODEX_OAUTH_TOKEN_INVALIDATED",
            "CODEX_OAUTH_TOKEN_REVOKED",
            "ENRICHMENT_AUTH_REQUIRED",
        ].contains(providerErrorCode)
    }

    var isValid: Bool {
        accepted >= 0 && activeInvocations >= 0 && invocationConcurrency >= 1
            && invocationConcurrency <= 256 && selectedConcurrency >= 1
            && selectedConcurrency <= invocationConcurrency && semanticFailures >= 0
            && (unknownOperations ?? 0) >= 0
            && provider == "sol"
            && ["healthy", "idle", "network_wait", "neutral", "quota_wait", "rate_limited", "transient_error"]
            .contains(lastDisposition)
    }

    var wait: OperationalWait? {
        if blockReason == "paused" {
            return OperationalWait(
                action: "Paid translation paused",
                kind: .paidPause,
                requiresAttention: false
            )
        }
        if hasAuthenticationIssue {
            return OperationalWait(
                action: "Sign in to \(provider.capitalized); recovery is checked automatically",
                kind: .authentication,
                requiresAttention: true
            )
        }
        if lastDisposition == "network_wait"
            || providerErrorCode?.contains("NETWORK_UNAVAILABLE") == true
        {
            return OperationalWait(
                action: "Internet unavailable; retrying automatically",
                kind: .network,
                requiresAttention: false
            )
        }
        if lastDisposition == "quota_wait" || blockReason == "quota_wait" {
            return OperationalWait(
                action: "Provider quota wait; probing automatically",
                kind: .quota,
                requiresAttention: false
            )
        }
        if lastDisposition == "rate_limited" || blockReason == "rate_limited" {
            return OperationalWait(
                action: "Provider rate limit; resuming automatically",
                kind: .rateLimit,
                requiresAttention: false
            )
        }
        if blockReason == "disk_pressure" {
            return OperationalWait(
                action: "Disk pressure; resuming after headroom recovers",
                kind: .resource,
                requiresAttention: false
            )
        }
        if blockReason == "provider_wait" || providerErrorCode != nil {
            return OperationalWait(
                action: "Provider unavailable; retrying automatically",
                kind: .provider,
                requiresAttention: false
            )
        }
        return nil
    }

    func activityLabel(now: Date = Date()) -> String {
        let activity = "\(activeInvocations) active · selected \(selectedConcurrency) · target \(invocationConcurrency)"
        if blockReason == "paused" {
            return "paid work paused · \(activity)"
        }
        if let providerErrorCode {
            let issue = hasAuthenticationIssue ? "sign-in required" : "provider issue"
            guard let retryAt else { return "\(issue) · \(providerErrorCode) · \(activity)" }
            let retryDate = Date(timeIntervalSince1970: Double(retryAt) / 1000)
            if retryDate <= now {
                return "\(issue) · retry ready · \(activity)"
            }
            return "\(issue) · retries \(retryDate.formatted(.relative(presentation: .numeric))) · \(activity)"
        }
        guard lastDisposition == "quota_wait" || lastDisposition == "rate_limited" else {
            return activity
        }
        let disposition = lastDisposition == "quota_wait" ? "quota" : "rate limited"
        guard let retryAt else { return "\(disposition) · \(activity)" }
        let retryDate = Date(timeIntervalSince1970: Double(retryAt) / 1000)
        return "\(disposition) · retries \(retryDate.formatted(.relative(presentation: .numeric)))"
    }
}

internal struct ProviderHostAdmissionHealth: Codable, Hashable {
    let activeProcesses: Int
    let maximumProcesses: Int
    let remainingProcesses: Int

    var isValid: Bool {
        activeProcesses >= 0 && maximumProcesses > 0
            && activeProcesses <= maximumProcesses
            && remainingProcesses == maximumProcesses - activeProcesses
    }

    var label: String {
        "Host · \(activeProcesses) active · \(maximumProcesses) shared capacity"
    }
}

internal struct PipelineHealth: Decodable, Hashable {
    let checks: [HealthCheck]
    let schemaId: String
    let schemaVersion: Int
    let configDigest: String
    let growth: PipelineGrowth?
    let heartbeatIntervalMs: Int?
    let lastProgressAt: Int?
    let observedAt: Int
    let origins: [OriginHealth]
    let providerHostAdmission: ProviderHostAdmissionHealth?
    let providers: [ProviderHealth]
    let queues: [QueueHealth]
    let runId: String
    let sol: SolHealth?
    let state: String

    var duplicateConflict: HealthCheck? {
        checks.first { $0.code.hasSuffix("_POEM_DUPLICATE") }
    }

    var collectionPoemQueue: QueueHealth? {
        queues.first { $0.kind.hasSuffix("_poem_detail") }
    }

    var collectionAuthorQueue: QueueHealth? {
        queues.first { $0.kind.hasSuffix("_author_manifest") }
    }

    var productionOperational: Bool {
        growth?.production.isOperational == true
    }

    var productionLabel: String {
        growth?.production.label ?? "Production · Unknown"
    }

    var enrichmentProviders: [ProviderHealth] {
        if !providers.isEmpty {
            return providers
        }
        guard let sol else { return [] }
        return [
            ProviderHealth(
                accepted: sol.accepted,
                activeInvocations: sol.activeInvocations,
                blockReason: nil,
                invocationConcurrency: sol.invocationConcurrency,
                lastDisposition: sol.lastDisposition,
                model: sol.model,
                modelKey: "sol-5.6",
                nextQuotaProbeAt: nil,
                provider: "sol",
                providerErrorCode: nil,
                retryAt: sol.retryAt,
                selectedConcurrency: sol.selectedConcurrency,
                semanticFailures: sol.semanticFailures,
                unknownOperations: nil
            ),
        ]
    }

    var attentionCheck: HealthCheck? {
        checks.filter { !$0.code.hasSuffix("_POEM_DUPLICATE") && $0.requiresAttention }
            .min { lhs, rhs in
                (lhs.state == "blocked" ? 0 : 1) < (rhs.state == "blocked" ? 0 : 1)
            }
    }

    init(
        checks: [HealthCheck],
        schemaId: String,
        schemaVersion: Int,
        configDigest: String,
        growth: PipelineGrowth?,
        heartbeatIntervalMs: Int?,
        lastProgressAt: Int?,
        observedAt: Int,
        origins: [OriginHealth],
        providers: [ProviderHealth],
        queues: [QueueHealth],
        runId: String,
        sol: SolHealth?,
        state: String,
        providerHostAdmission: ProviderHostAdmissionHealth? = nil
    ) {
        self.checks = checks
        self.schemaId = schemaId
        self.schemaVersion = schemaVersion
        self.configDigest = configDigest
        self.growth = growth
        self.heartbeatIntervalMs = heartbeatIntervalMs
        self.lastProgressAt = lastProgressAt
        self.observedAt = observedAt
        self.origins = origins
        self.providerHostAdmission = providerHostAdmission
        self.providers = providers
        self.queues = queues
        self.runId = runId
        self.sol = sol
        self.state = state
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: PipelineHealthCodingKey.self)
        checks = try values.decodeIfPresent([HealthCheck].self, forKey: .checks) ?? []
        schemaId = try values.decode(String.self, forKey: .schemaId)
        schemaVersion = try values.decode(Int.self, forKey: .schemaVersion)
        configDigest = try values.decode(String.self, forKey: .configDigest)
        growth = try values.decodeIfPresent(PipelineGrowth.self, forKey: .growth)
        heartbeatIntervalMs = try values.decodeIfPresent(Int.self, forKey: .heartbeatIntervalMs)
        lastProgressAt = try values.decodeIfPresent(Int.self, forKey: .lastProgressAt)
        observedAt = try values.decode(Int.self, forKey: .observedAt)
        origins = try values.decodeIfPresent([OriginHealth].self, forKey: .origins) ?? []
        providerHostAdmission = try values.decodeIfPresent(
            ProviderHostAdmissionHealth.self,
            forKey: .providerHostAdmission
        )
        providers = try values.decodeIfPresent([ProviderHealth].self, forKey: .providers) ?? []
        queues = try values.decode([QueueHealth].self, forKey: .queues)
        runId = try values.decode(String.self, forKey: .runId)
        sol = try values.decodeIfPresent(SolHealth.self, forKey: .sol)
        state = try values.decode(String.self, forKey: .state)
    }

    func queue(_ kind: String) -> QueueHealth? {
        queues.first { $0.kind == kind }
    }

    func validate() throws {
        guard schemaId == "saqi.pipeline-health", schemaVersion == 1 else {
            throw MonitorDataError.unsupportedHealthSchema
        }
        guard configDigest.range(of: #"^[a-f0-9]{64}$"#, options: .regularExpression) != nil,
              observedAt >= 0,
              ["healthy", "degraded", "blocked"].contains(state),
              checks.allSatisfy({ ["ok", "warning", "blocked"].contains($0.state) }),
              growth
              .map({ ["active", "ready", "gated", "stalled", "disabled"].contains($0.production.state) }) != false,
              queues.allSatisfy(\.isValid),
              Set(queues.map(\.kind)).count == queues.count,
              Set(enrichmentProviders.map(\.modelKey)).count == enrichmentProviders.count,
              enrichmentProviders.allSatisfy(\.isValid),
              origins.allSatisfy(\.isValid),
              providerHostAdmission.map(\.isValid) != false,
              heartbeatIntervalMs.map({ $0 >= 1000 && $0 <= 24 * 60 * 60 * 1000 }) != false
        else { throw MonitorDataError.invalidHealthDocument }
    }
}

private enum PipelineHealthCodingKey: String, CodingKey {
    case checks = "checks"
    case schemaId = "schemaId"
    case schemaVersion = "schemaVersion"
    case configDigest = "configDigest"
    case growth = "growth"
    case heartbeatIntervalMs = "heartbeatIntervalMs"
    case lastProgressAt = "lastProgressAt"
    case observedAt = "observedAt"
    case origins = "origins"
    case providerHostAdmission = "providerHostAdmission"
    case providers = "providers"
    case queues = "queues"
    case runId = "runId"
    case sol = "sol"
    case state = "state"
}
