import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    func validHealth(
        schemaVersion: Int = 1,
        queues: [QueueHealth] = [],
        providers: [ProviderHealth] = [],
        observedAt: Int = 1,
        runId: String = "run",
        origins: [OriginHealth] = [],
        providerHostAdmission: ProviderHostAdmissionHealth? = nil,
        growth: PipelineGrowth? = nil
    ) -> PipelineHealth {
        PipelineHealth(
            checks: [],
            schemaId: "saqi.pipeline-health",
            schemaVersion: schemaVersion,
            configDigest: String(repeating: "a", count: 64),
            growth: growth,
            heartbeatIntervalMs: nil,
            lastProgressAt: nil,
            observedAt: observedAt,
            origins: origins,
            providers: providers,
            queues: queues,
            runId: runId,
            sol: nil,
            state: "healthy",
            providerHostAdmission: providerHostAdmission
        )
    }

    func provider(
        key: String,
        provider: String,
        error: String? = nil,
        retryAt: Int? = nil,
        disposition: String = "idle",
        blockReason: String? = nil
    ) -> ProviderHealth {
        ProviderHealth(
            accepted: 0,
            activeInvocations: 0,
            blockReason: blockReason,
            invocationConcurrency: 128,
            lastDisposition: disposition,
            model: key,
            modelKey: key,
            nextQuotaProbeAt: nil,
            provider: provider,
            providerErrorCode: error,
            retryAt: retryAt,
            selectedConcurrency: 1,
            semanticFailures: 0,
            unknownOperations: 0
        )
    }

    func queue(
        kind: String,
        pending: Int,
        quota: Int = 0,
        active: Int = 0,
        lastSuccessAt: Int? = nil
    ) -> QueueHealth {
        QueueHealth(
            active: active,
            deadLetter: 0,
            kind: kind,
            lastSuccessAt: lastSuccessAt,
            pending: pending,
            quotaWait: quota,
            retryWait: 0,
            succeeded: 0,
            total: pending + quota + active
        )
    }

    func service(actualState: String, actions: [String]) -> ServiceSnapshot {
        ServiceSnapshot(
            action: "status",
            actualState: actualState,
            allowedActions: actions,
            configDigest: String(repeating: "a", count: 64),
            desiredConfigDigest: String(repeating: "a", count: 64),
            detail: nil,
            loadedConfigDigest: actualState == "stopped" ? nil : String(repeating: "a", count: 64),
            observedAt: "2026-08-26T00:00:00.000Z",
            ownerPid: 1,
            runId: "run",
            schemaId: "saqi.launchd-control",
            schemaVersion: 1,
            serviceEnabled: actualState == "stopped" ? .disabled : .enabled
        )
    }
}
