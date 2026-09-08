import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    @MainActor
    func testPauseTelemetryRequiresCurrentIdentityAndFreshness() {
        let now = Date()
        let store = presentationStore(entry: presentationEntry(), now: now)
        store.paused = true
        store.runtimeRefreshFailed = true
        store.applyPauseTelemetry(now: now)
        XCTAssertTrue(store.paused)
        store.runtimeRefreshFailed = false
        store.applyPauseTelemetry(now: now.addingTimeInterval(16 * 60))
        XCTAssertTrue(store.paused)
        store.applyPauseTelemetry(now: now)
        XCTAssertFalse(store.paused)
        store.paused = true
        store.service = replacementService()
        store.applyPauseTelemetry(now: now)
        XCTAssertTrue(store.paused)
    }

    @MainActor
    func testTransientRefreshFailuresRetainAndLabelLastKnownCodexTelemetry() throws {
        let now = Date(timeIntervalSince1970: 1_000_000)
        let store = presentationStore(entry: presentationEntry(), now: now)
        let originalRuntime = try XCTUnwrap(store.runtime)
        let originalHealth = try XCTUnwrap(store.health)
        let originalService = try XCTUnwrap(store.service)
        store.captureCoherentCodexTelemetry()

        store.applyRuntimeRefreshResult(.failure(MonitorDataError.invalidRuntimeDocument))
        store.applyRefreshResults(
            .failure(MonitorDataError.invalidHealthDocument),
            .failure(MonitorDataError.invalidServiceDocument),
            loadedService: true
        )

        XCTAssertEqual(store.runtime, originalRuntime)
        XCTAssertEqual(store.health, originalHealth)
        XCTAssertEqual(store.service, originalService)
        let value = try XCTUnwrap(store.codexExecutionPresentation(now: now))
        XCTAssertEqual(value.headline, "Last known · 1 minute ago · Translating · 1 active")
        XCTAssertTrue(value.progress.hasPrefix("Last known · 1 minute ago · "))
        XCTAssertTrue(value.concurrency.hasPrefix("Last known · 1 minute ago · "))
        XCTAssertTrue(value.budget.hasPrefix("Last known · 1 minute ago · "))
        XCTAssertTrue(value.accessibilityValue.contains("Last known"))
        XCTAssertFalse(store.serviceControlsAvailable)
        XCTAssertFalse(store.concurrencyControlsAvailable)
        XCTAssertEqual(store.eta, "ETA unavailable")
        XCTAssertFalse(store.rateLabel.contains("published"))
    }

    @MainActor
    func testConfigurationTransitionShowsTelemetryAsDifferentConfiguration() throws {
        let now = Date(timeIntervalSince1970: 1_000_000)
        let store = presentationStore(entry: presentationEntry(), now: now)
        store.captureCoherentCodexTelemetry()
        store.service = replacementService()

        let value = try XCTUnwrap(store.codexExecutionPresentation(now: now))
        XCTAssertEqual(value.headline, "Different configuration · 1 minute ago · Translating · 1 active")
        XCTAssertTrue(value.progress.hasPrefix("Different configuration · 1 minute ago · "))
        XCTAssertTrue(value.concurrency.hasPrefix("Different configuration · 1 minute ago · "))
        XCTAssertTrue(value.budget.hasPrefix("Different configuration · 1 minute ago · "))
        XCTAssertFalse(value.headline.hasPrefix("Translating"))
        XCTAssertFalse(store.serviceControlsAvailable)
        XCTAssertFalse(store.concurrencyControlsAvailable)
    }

    @MainActor
    func testNewRuntimeBeforeServiceUsesNeutralTransitionAndRecovers() throws {
        let now = Date(timeIntervalSince1970: 1_000_000)
        let store = presentationStore(entry: presentationEntry(), now: now)
        let replacementRuntime = try replacementRuntime(from: XCTUnwrap(store.runtime), now: now)

        store.applyRuntimeRefreshResult(.success(replacementRuntime))
        XCTAssertTrue(try XCTUnwrap(store.codexExecutionPresentation(now: now)).headline
            .hasPrefix("Different configuration · 1 minute ago · "))
        XCTAssertFalse(store.serviceControlsAvailable)
        XCTAssertFalse(store.concurrencyControlsAvailable)

        store.applyRefreshResults(
            .failure(MonitorDataError.invalidHealthDocument),
            .success(replacementService()),
            loadedService: true
        )
        XCTAssertEqual(store.codexExecutionPresentation(now: now)?.headline, "Translating · 1 active")
        XCTAssertFalse(store.serviceControlsAvailable)

        store.applyRefreshResults(
            .success(replacementHealth(now: now)),
            .success(replacementService()),
            loadedService: true
        )
        XCTAssertTrue(store.serviceControlsAvailable)
        XCTAssertTrue(store.concurrencyControlsAvailable)
    }

    @MainActor
    func testRetainedCodexTelemetryExpiresAfterOneHour() throws {
        let now = Date(timeIntervalSince1970: 1_000_000)
        let store = presentationStore(entry: presentationEntry(), now: now)
        store.captureCoherentCodexTelemetry()
        store.applyRuntimeRefreshResult(.failure(MonitorDataError.invalidRuntimeDocument))

        let value = try XCTUnwrap(store.codexExecutionPresentation(now: now.addingTimeInterval(61 * 60)))
        XCTAssertEqual(value.headline, "Codex historical details expired")
        XCTAssertEqual(value.progress, "Progress unavailable")
        XCTAssertEqual(value.concurrency, "Concurrency unavailable")
        XCTAssertEqual(value.budget, "Budget unavailable")
    }

    @MainActor
    func testFreshHealthRemainsAuthoritativeWhenRuntimeReadFails() {
        let now = Date()
        let store = presentationStore(entry: presentationEntry(), now: now)
        store.health = validHealth(
            observedAt: Int(now.timeIntervalSince1970 * 1000),
            runId: "process:1",
            growth: PipelineGrowth(
                production: ProductionGrowth(configured: true, lastSuccessAt: nil, state: "stalled")
            )
        )
        store.applyRuntimeRefreshResult(.failure(MonitorDataError.invalidRuntimeDocument))
        store.diagnosticError = "Runtime status unavailable"

        XCTAssertEqual(store.operationalStatus.label, "Production stalled · attention needed")
        XCTAssertEqual(store.eta, "ETA unavailable · production stalled")
        XCTAssertFalse(store.serviceControlsAvailable)
        XCTAssertFalse(store.concurrencyControlsAvailable)
    }

    @MainActor
    func testMismatchedBlockedHealthCannotOverrideCurrentRuntime() throws {
        let now = Date()
        let store = presentationStore(
            entry: presentationEntry(throughput: poemThroughput(publishedLastHour: 10, remaining: 30)),
            now: now
        )
        let runtime = try replacementRuntime(from: XCTUnwrap(store.runtime), now: now)
        store.applyRuntimeRefreshResult(.success(runtime))
        store.service = replacementService()
        store.health = mismatchedBlockedHealth(now: now)
        store.paused = true

        XCTAssertEqual(store.operationalStatus.label, "Starting · waiting for diagnostics")
        store.paused = false
        XCTAssertEqual(store.eta, "~3 hours remaining")
        XCTAssertEqual(store.rateLabel, "10/hour published · rolling 1h")
        XCTAssertFalse(store.serviceControlsAvailable)
        XCTAssertFalse(store.concurrencyControlsAvailable)
    }

    @MainActor
    func testFutureProgressSampleCannotProduceRate() {
        let now = Date()
        let store = presentationStore(entry: presentationEntry(), now: now)
        store.samples = [
            ProgressSample(
                acceptedByModel: ["sol-5.6": 10],
                configDigest: String(repeating: "a", count: 64),
                observedAt: now.addingTimeInterval(31)
            ),
        ]

        XCTAssertNil(store.acceptedPerHour)
    }

    private func replacementRuntime(from current: RuntimeStatus, now: Date) throws -> RuntimeStatus {
        let existing = try XCTUnwrap(current.providerExecution)
        let execution = ProviderExecutionHealth(
            configDigest: String(repeating: "b", count: 64),
            observedAt: Int(now.timeIntervalSince1970 * 1000) - 1000,
            providers: existing.providers,
            runId: existing.runId,
            schemaId: existing.schemaId,
            schemaVersion: existing.schemaVersion
        )
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        return RuntimeStatus(
            configDigest: String(repeating: "b", count: 64),
            observedAt: formatter.string(from: now),
            ownerPid: 2,
            resourcePressure: current.resourcePressure,
            runId: "replacement-run",
            workload: RuntimeWorkload(providerExecution: execution)
        )
    }

    private func replacementHealth(now: Date) -> PipelineHealth {
        PipelineHealth(
            checks: [],
            schemaId: "saqi.pipeline-health",
            schemaVersion: 1,
            configDigest: String(repeating: "b", count: 64),
            growth: nil,
            heartbeatIntervalMs: nil,
            lastProgressAt: nil,
            observedAt: Int(now.timeIntervalSince1970 * 1000),
            origins: [],
            providers: [provider(key: "sol-5.6", provider: "sol")],
            queues: [],
            runId: "process:2",
            sol: nil,
            state: "healthy"
        )
    }

    private func mismatchedBlockedHealth(now: Date) -> PipelineHealth {
        PipelineHealth(
            checks: [],
            schemaId: "saqi.pipeline-health",
            schemaVersion: 1,
            configDigest: String(repeating: "a", count: 64),
            growth: PipelineGrowth(
                production: ProductionGrowth(configured: true, lastSuccessAt: nil, state: "stalled")
            ),
            heartbeatIntervalMs: nil,
            lastProgressAt: nil,
            observedAt: Int(now.timeIntervalSince1970 * 1000),
            origins: [],
            providers: [provider(key: "sol-5.6", provider: "sol")],
            queues: [],
            runId: "process:1",
            sol: nil,
            state: "blocked"
        )
    }

    private func replacementService() -> ServiceSnapshot {
        ServiceSnapshot(
            action: "status",
            actualState: "running",
            allowedActions: [],
            configDigest: String(repeating: "b", count: 64),
            desiredConfigDigest: String(repeating: "b", count: 64),
            detail: nil,
            loadedConfigDigest: String(repeating: "b", count: 64),
            observedAt: "2026-09-04T00:00:00.000Z",
            ownerPid: 2,
            runId: "replacement-run",
            schemaId: "saqi.launchd-control",
            schemaVersion: 1,
            serviceEnabled: .enabled
        )
    }
}
