import Foundation

internal struct ResourcePressureStatus: Codable, Hashable {
    let nextProbeAt: Int
    let reasons: [String]
    let state: String
}

internal struct RuntimeStatus: Codable, Hashable {
    let configDigest: String
    let observedAt: String
    let ownerPid: Int
    let resourcePressure: ResourcePressureStatus
    let runId: String
    let workload: RuntimeWorkload?

    var providerExecution: ProviderExecutionHealth? {
        workload?.providerExecution
    }

    init(
        configDigest: String,
        observedAt: String,
        ownerPid: Int,
        resourcePressure: ResourcePressureStatus,
        runId: String,
        workload: RuntimeWorkload? = nil
    ) {
        self.configDigest = configDigest
        self.observedAt = observedAt
        self.ownerPid = ownerPid
        self.resourcePressure = resourcePressure
        self.runId = runId
        self.workload = workload
    }

    func matches(service: ServiceSnapshot) -> Bool {
        configDigest == service.configDigest && ownerPid == service.ownerPid && runId == service.runId
    }

    func validate(now: Date = Date()) throws {
        let knownReasons = Set([
            "DISK_PRESSURE",
            "FILE_DESCRIPTOR_PRESSURE",
            "MEMORY_PRESSURE",
            "PROCESS_MEMORY_PRESSURE",
            "RESOURCE_PROBE_FAILED",
        ])
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let runtimeObservedAt = formatter.date(from: observedAt)
        guard configDigest.range(of: #"^[a-f0-9]{64}$"#, options: .regularExpression) != nil,
              let runtimeObservedAt, ownerPid > 0, !runId.isEmpty,
              resourcePressure.nextProbeAt >= 0,
              ["ready", "resource_wait"].contains(resourcePressure.state),
              resourcePressure.reasons.count <= 5,
              resourcePressure.reasons.allSatisfy(knownReasons.contains)
        else { throw MonitorDataError.invalidRuntimeDocument }
        let runtimeAge = now.timeIntervalSince(runtimeObservedAt)
        guard runtimeAge >= -30, runtimeAge <= 15 * 60 else {
            throw MonitorDataError.invalidRuntimeDocument
        }
        guard let providerExecution else { return }
        try providerExecution.validate(now: now)
        let runtimeObservedAtMilliseconds = Int(runtimeObservedAt.timeIntervalSince1970 * 1000)
        guard providerExecution.configDigest == configDigest,
              providerExecution.observedAt <= runtimeObservedAtMilliseconds
        else { throw MonitorDataError.invalidRuntimeDocument }
    }
}

internal struct RuntimeWorkload: Codable, Hashable {
    let providerExecution: ProviderExecutionHealth?
}
