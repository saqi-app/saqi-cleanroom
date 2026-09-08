import Foundation

internal enum MonitorDataError: Error {
    case invalidHealthDocument
    case invalidRuntimeDocument
    case invalidServiceDocument
    case unsupportedHealthSchema
}

internal struct ProgressSample: Codable, Hashable {
    let acceptedByModel: [String: Int]
    let configDigest: String
    let modelEligibility: ModelEligibility
    let observedAt: Date

    init(
        acceptedByModel: [String: Int],
        configDigest: String,
        observedAt: Date,
        modelEligibility: ModelEligibility = .legacy
    ) {
        self.acceptedByModel = acceptedByModel
        self.configDigest = configDigest
        self.modelEligibility = modelEligibility
        self.observedAt = observedAt
    }

    init(acceptedByModel: [String: Int], configDigest: String, observedAt: Date, eligibleModels: [String]) {
        self.init(
            acceptedByModel: acceptedByModel,
            configDigest: configDigest,
            observedAt: observedAt,
            modelEligibility: .restricted(eligibleModels)
        )
    }

    init(from decoder: Decoder) throws {
        let values = try decoder.container(keyedBy: ProgressSampleCodingKey.self)
        acceptedByModel = try values.decode([String: Int].self, forKey: .acceptedByModel)
        configDigest = try values.decode(String.self, forKey: .configDigest)
        observedAt = try values.decode(Date.self, forKey: .observedAt)
        let models = try values.decodeIfPresent([String].self, forKey: .eligibleModels)
        modelEligibility = models.map(ModelEligibility.restricted) ?? .legacy
    }

    func encode(to encoder: Encoder) throws {
        var values = encoder.container(keyedBy: ProgressSampleCodingKey.self)
        try values.encode(acceptedByModel, forKey: .acceptedByModel)
        try values.encode(configDigest, forKey: .configDigest)
        try values.encode(observedAt, forKey: .observedAt)
        if case let .restricted(models) = modelEligibility {
            try values.encode(models, forKey: .eligibleModels)
        }
    }
}

private enum ProgressSampleCodingKey: String, CodingKey {
    case acceptedByModel = "acceptedByModel"
    case configDigest = "configDigest"
    case eligibleModels = "eligibleModels"
    case observedAt = "observedAt"
}

internal enum ModelEligibility: Hashable {
    case legacy
    case restricted([String])

    func permits(_ model: String) -> Bool {
        switch self {
        case .legacy:
            true
        case let .restricted(models):
            models.contains(model)
        }
    }
}

internal struct ProgressHistory: Codable {
    let schemaVersion: Int
    let samples: [ProgressSample]
}

internal enum ProgressEstimator {
    static let minimumAccepted = 2
    static let minimumInterval: TimeInterval = 10 * 60

    static func rates(samples: [ProgressSample]) -> [String: Double] {
        guard let last = samples.last else { return [:] }
        return last.acceptedByModel.reduce(into: [:]) { rates, entry in
            var interval: TimeInterval = 0
            var completed = 0
            for (earlier, later) in zip(samples, samples.dropFirst()) {
                guard earlier.configDigest == last.configDigest,
                      later.configDigest == last.configDigest,
                      let before = earlier.acceptedByModel[entry.key],
                      let after = later.acceptedByModel[entry.key], after >= before
                else { continue }
                let elapsed = later.observedAt.timeIntervalSince(earlier.observedAt)
                guard elapsed > 0 else { continue }
                guard earlier.modelEligibility == .legacy
                    || later.modelEligibility == .legacy
                    || (earlier.modelEligibility.permits(entry.key) && later.modelEligibility.permits(entry.key))
                else { continue }
                interval += elapsed
                completed += after - before
            }
            let rate = Double(completed) / interval * 3600
            guard interval >= minimumInterval, completed >= minimumAccepted,
                  rate.isFinite, rate > 0, rate <= 1_000_000_000
            else { return }
            rates[entry.key] = rate
        }
    }

    static func recording(
        _ sample: ProgressSample,
        in input: [ProgressSample],
        now: Date
    ) -> [ProgressSample] {
        guard sample.observedAt <= now.addingTimeInterval(30),
              sample.acceptedByModel.values.allSatisfy({ $0 >= 0 })
        else { return input }
        var output = input
        if let last = output.last {
            let counterRegressed = sample.acceptedByModel.contains { entry in
                entry.value < (last.acceptedByModel[entry.key] ?? entry.value)
            }
            if last.configDigest != sample.configDigest
                || counterRegressed
                || (sample.observedAt <= last.observedAt && sample != last)
            {
                output.removeAll()
            }
        }
        let bucket = Int(sample.observedAt.timeIntervalSince1970 / 30)
        let lastBucket = output.last.map { Int($0.observedAt.timeIntervalSince1970 / 30) }
        if lastBucket == bucket, !output.isEmpty {
            output[output.count - 1] = sample
        } else if output.last != sample {
            output.append(sample)
        }
        output.removeAll { $0.observedAt < now.addingTimeInterval(-6 * 3600) }
        if output.count > 256 {
            output.removeFirst(output.count - 256)
        }
        return output
    }

    static func eta(health: PipelineHealth?, rates: [String: Double]) -> String {
        guard let health, health.state != "blocked", !health.enrichmentProviders.isEmpty else {
            return "ETA unavailable · diagnostics incomplete"
        }
        guard health.growth?.production.state != "stalled" else {
            return "ETA unavailable · production stalled"
        }
        let tracking = trackEstimate(health: health, rates: rates)
        if let interruption = tracking.interruption {
            return interruption
        }
        return estimateLabel(tracker: tracking.estimate)
    }

    static func eta(provider: ProviderExecutionEntry) -> String? {
        let waitLabels = [
            "operator_paused": "ETA paused",
            "paid_work_paused": "ETA paused",
            "budget_unarmed": "ETA waiting for translation budget",
            "budget_exhausted": "ETA waiting for translation budget",
            "auth_wait": "ETA waiting for Sol sign-in",
            "codex_quota_wait": "ETA waiting on quota",
            "network_wait": "ETA waiting for internet",
            "rate_limit_wait": "ETA waiting to retry",
            "provider_backoff": "ETA waiting to retry",
            "circuit_open": "ETA waiting to retry",
            "resource_wait": "ETA waiting to retry",
            "disabled": "ETA disabled",
        ]
        if let waitLabel = waitLabels[provider.admission.primaryReason] {
            return waitLabel
        }
        guard let throughput = provider.throughput else { return nil }
        guard provider.progress.state != "stalled" else {
            return "ETA unavailable · translation stalled"
        }
        guard throughput.coverage.backfillComplete else {
            return throughput.published.last1h > 0
                ? "ETA indexing · \(throughput.published.last1h)/hour published"
                : "ETA indexing · no publications in last hour"
        }
        let remaining = throughput.remaining.endToEndPublication
        if remaining == 0 {
            return throughput.remaining.terminalDead == 0
                ? "Local work complete"
                : "Complete · \(throughput.remaining.terminalDead) unresolved"
        }
        guard throughput.published.last1h > 0 else { return "ETA learning · no publications in last hour" }
        let hours = Double(remaining) / Double(throughput.published.last1h)
        guard hours.isFinite, hours > 0 else { return "ETA learning" }
        return estimateLabel(tracker: EtaEstimate(hours: [hours]))
    }

    private static func trackEstimate(health: PipelineHealth, rates: [String: Double]) -> EtaTrackingResult {
        var tracker = EtaEstimate()
        for provider in health.enrichmentProviders {
            guard let queue = health.queue(provider.workKind) else {
                return EtaTrackingResult(estimate: tracker, interruption: "ETA unavailable · diagnostics incomplete")
            }
            tracker.unresolved = saturatingAdd(tracker.unresolved, queue.deadLetter)
            guard queue.remaining > 0 else { continue }
            if let waiting = waitingLabel(provider: provider, queue: queue, observedAt: health.observedAt) {
                return EtaTrackingResult(estimate: tracker, interruption: waiting)
            }
            guard let hours = estimatedHours(queue: queue, rate: rates[provider.modelKey]) else {
                return EtaTrackingResult(estimate: tracker, interruption: "ETA learning")
            }
            tracker.hours.append(hours)
            tracker.hasConditionalDelay = tracker.hasConditionalDelay || queue.quotaWait > 0 || queue.retryWait > 0
        }
        return EtaTrackingResult(estimate: tracker, interruption: nil)
    }

    private static func estimateLabel(tracker: EtaEstimate) -> String {
        guard let longest = tracker.hours.max() else {
            return tracker.unresolved == 0 ? "Local work complete" : "Complete · \(tracker.unresolved) unresolved"
        }
        let estimate = if longest < 1 {
            "<1 hour remaining"
        } else if longest < 48 {
            String(format: "~%.0f hours remaining", longest)
        } else {
            String(format: "~%.1f days remaining", longest / 24)
        }
        return tracker.hasConditionalDelay ? "\(estimate) · delays possible" : estimate
    }

    private static func waitingLabel(provider: ProviderHealth, queue: QueueHealth, observedAt: Int) -> String? {
        if provider.hasAuthenticationIssue {
            return "ETA waiting for \(provider.provider.capitalized) sign-in"
        }
        if queue.quotaWait > 0, queue.active == 0 {
            return "ETA waiting on quota"
        }
        if queue.retryWait > 0, queue.active == 0 {
            return "ETA waiting to retry"
        }
        guard queue.active == 0 else { return nil }
        guard let lastSuccessAt = queue.lastSuccessAt,
              lastSuccessAt <= observedAt,
              observedAt - lastSuccessAt <= 15 * 60 * 1000
        else { return "ETA unavailable · idle" }
        return nil
    }

    private static func estimatedHours(queue: QueueHealth, rate: Double?) -> Double? {
        guard let rate, rate.isFinite, rate > 0 else { return nil }
        let result = Double(queue.remaining) / rate
        return result.isFinite && result > 0 ? result : nil
    }

    static func saturatingAdd(_ lhs: Int, _ rhs: Int) -> Int {
        let (sum, overflow) = lhs.addingReportingOverflow(rhs)
        return overflow ? Int.max : sum
    }
}

private struct EtaEstimate {
    var hasConditionalDelay = false
    var hours: [Double] = []
    var unresolved = 0
}

private struct EtaTrackingResult {
    let estimate: EtaEstimate
    let interruption: String?
}

internal enum ControlPhase: Equatable {
    case idle
    case applying(String)
    case failed(String)

    var label: String? {
        switch self {
        case .idle:
            nil
        case let .applying(action):
            action
        case let .failed(message):
            message
        }
    }

    var isApplying: Bool {
        if case .applying = self {
            return true
        }
        return false
    }
}

internal struct MonitorConfiguration: Hashable {
    let crawlerCLI: URL
    let rigConfig: URL
    let serviceLabel: String
    let stateDirectory: URL

    static func current(environment: [String: String] = ProcessInfo.processInfo.environment) -> Self {
        let home = FileManager.default.homeDirectoryForCurrentUser
        let repository = home.appending(path: "code/saqi")
        let installedCLI = home.appending(
            path: "Library/Application Support/Saqi/current/typescript/packages/crawler-local/dist/cli.js"
        )
        return Self(
            crawlerCLI: URL(filePath: environment["SAQI_CRAWLER_CLI"] ?? installedCLI.path),
            rigConfig: URL(filePath: environment["SAQI_RIG_CONFIG"] ?? repository
                .appending(path: ".runtime/saqi-unified-rig-live.json").path),
            serviceLabel: environment["SAQI_SERVICE_LABEL"] ?? "net.saqi.crawler",
            stateDirectory: URL(filePath: environment["SAQI_STATE_DIR"] ?? home.appending(path: "code/saqi-runtime")
                .path)
        )
    }
}
