import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    func testRateRequiresBothTimeAndAcceptedThresholds() {
        let digest = String(repeating: "a", count: 64)
        let start = ProgressSample(
            acceptedByModel: ["sol-5.6": 100],
            configDigest: digest,
            observedAt: Date(timeIntervalSince1970: 0)
        )
        let learned = ProgressSample(
            acceptedByModel: ["sol-5.6": 110],
            configDigest: digest,
            observedAt: Date(timeIntervalSince1970: 900)
        )
        XCTAssertEqual(ProgressEstimator.rates(samples: [start, learned])["sol-5.6"], 40)
        let tooSoon = ProgressSample(
            acceptedByModel: ["sol-5.6": 110],
            configDigest: digest,
            observedAt: Date(timeIntervalSince1970: 599)
        )
        let tooFew = ProgressSample(
            acceptedByModel: ["sol-5.6": 101],
            configDigest: digest,
            observedAt: Date(timeIntervalSince1970: 900)
        )
        XCTAssertTrue(ProgressEstimator.rates(samples: [start, tooSoon]).isEmpty)
        XCTAssertTrue(ProgressEstimator.rates(samples: [start, tooFew]).isEmpty)
    }

    func testHighFrequencySamplesRetainLearningWindow() throws {
        let digest = String(repeating: "a", count: 64)
        let start = Date(timeIntervalSince1970: 1_000_000)
        let now = start.addingTimeInterval(1200)
        var history: [ProgressSample] = []
        for index in 0 ... 600 {
            let sample = ProgressSample(
                acceptedByModel: ["sol-5.6": index],
                configDigest: digest,
                observedAt: start.addingTimeInterval(Double(index * 2))
            )
            history = ProgressEstimator.recording(sample, in: history, now: now)
        }
        XCTAssertLessThanOrEqual(history.count, 256)
        let rate = try XCTUnwrap(ProgressEstimator.rates(samples: history)["sol-5.6"])
        XCTAssertEqual(rate, 1800, accuracy: 0.001)
    }

    func testHistoryNeverLearnsAcrossConfigOrCounterReset() {
        let start = Date(timeIntervalSince1970: 1_000_000)
        let now = start.addingTimeInterval(1000)
        var history: [ProgressSample] = []
        for sample in [
            ProgressSample(acceptedByModel: ["sol-5.6": 100], configDigest: "a", observedAt: start),
            ProgressSample(
                acceptedByModel: ["sol-5.6": 200],
                configDigest: "b",
                observedAt: start.addingTimeInterval(300)
            ),
            ProgressSample(
                acceptedByModel: ["sol-5.6": 0],
                configDigest: "a",
                observedAt: start.addingTimeInterval(600)
            ),
            ProgressSample(
                acceptedByModel: ["sol-5.6": 110],
                configDigest: "a",
                observedAt: start.addingTimeInterval(900)
            ),
        ] {
            history = ProgressEstimator.recording(sample, in: history, now: now)
        }
        XCTAssertTrue(ProgressEstimator.rates(samples: history).isEmpty)
    }

    func testRateExcludesProviderWaitTime() {
        let digest = String(repeating: "a", count: 64)
        let start = Date(timeIntervalSince1970: 1_000_000)
        let samples = [
            ProgressSample(
                acceptedByModel: ["sol-5.6": 0],
                configDigest: digest,
                observedAt: start,
                eligibleModels: ["sol-5.6"]
            ),
            ProgressSample(
                acceptedByModel: ["sol-5.6": 2],
                configDigest: digest,
                observedAt: start.addingTimeInterval(600),
                eligibleModels: ["sol-5.6"]
            ),
            ProgressSample(
                acceptedByModel: ["sol-5.6": 2],
                configDigest: digest,
                observedAt: start.addingTimeInterval(4200),
                eligibleModels: []
            ),
        ]
        XCTAssertEqual(ProgressEstimator.rates(samples: samples)["sol-5.6"], 12)
    }

    func testOperationalStatusDoesNotTreatProductionOffAsAttention() {
        let now = Date(timeIntervalSince1970: 1000)
        let health = validHealth(observedAt: 1_000_000, runId: "process:1")
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: health,
            runtime: nil,
            paused: false,
            now: now
        )
        XCTAssertEqual(result.severity, .healthy)
        XCTAssertEqual(result.label, "Running · healthy")
    }

    func testOperationalStatusReportsOwnedStartupAsWaiting() {
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "starting", actions: ["restart", "status", "stop"]),
            health: nil,
            runtime: nil,
            paused: false
        )
        XCTAssertEqual(result.severity, .waiting)
        XCTAssertEqual(result.label, "Starting · preparing lanes")
    }

    func testOperationalStatusClassifiesRecoverableNetworkWait() throws {
        let now = Date(timeIntervalSince1970: 1000)
        let sol = provider(
            key: "sol-5.6",
            provider: "sol",
            error: "ENRICHMENT_NETWORK_UNAVAILABLE",
            disposition: "network_wait"
        )
        let health = validHealth(
            queues: [queue(kind: sol.workKind, pending: 10)],
            providers: [sol],
            observedAt: 1_000_000,
            runId: "process:1"
        )
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: health,
            runtime: nil,
            paused: false,
            now: now
        )
        XCTAssertEqual(result.severity, .waiting)
        XCTAssertEqual(result.wait?.kind, .network)
        XCTAssertFalse(try XCTUnwrap(result.wait).requiresAttention)
    }

    func testOperationalStatusClassifiesSourceChallengeAsActionable() throws {
        let now = Date(timeIntervalSince1970: 1000)
        let health = validHealth(
            observedAt: 1_000_000,
            runId: "process:1",
            origins: [
                OriginHealth(
                    active: false,
                    consecutiveFailures: 1,
                    cooldownUntil: 2_000_000,
                    lastCompletedAt: nil,
                    nextAllowedAt: 2_000_000,
                    origin: "https://source.invalid",
                    stopReason: "SOURCE_HUMAN_REQUIRED"
                ),
            ]
        )
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: health,
            runtime: nil,
            paused: false,
            now: now
        )
        XCTAssertEqual(result.severity, .attention)
        XCTAssertEqual(result.wait?.kind, .source)
        XCTAssertTrue(try XCTUnwrap(result.wait).requiresAttention)
    }

    func testOperationalStatusUsesExplicitRestartedOriginStateWithoutStopReason() throws {
        let now = Date(timeIntervalSince1970: 1000)
        let health = validHealth(
            observedAt: 1_000_000,
            runId: "process:1",
            origins: [
                OriginHealth(
                    active: false,
                    consecutiveFailures: 0,
                    cooldownUntil: 0,
                    lastCompletedAt: nil,
                    nextAllowedAt: 2_000_000,
                    origin: "https://source.example",
                    stopReason: nil,
                    state: "network_wait"
                ),
            ]
        )
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: health,
            runtime: nil,
            paused: false,
            now: now
        )
        XCTAssertEqual(result.severity, .waiting)
        XCTAssertEqual(result.wait?.kind, .network)
        XCTAssertFalse(try XCTUnwrap(result.wait).requiresAttention)
    }

    func testOperationalStatusClassifiesResourceWaitAsSelfHealing() throws {
        let now = Date(timeIntervalSince1970: 1000)
        let health = validHealth(observedAt: 1_000_000, runId: "process:1")
        let runtime = RuntimeStatus(
            configDigest: String(repeating: "a", count: 64),
            observedAt: "ignored",
            ownerPid: 1,
            resourcePressure: ResourcePressureStatus(
                nextProbeAt: 1_005_000,
                reasons: ["MEMORY_PRESSURE"],
                state: "resource_wait"
            ),
            runId: "run"
        )
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: health,
            runtime: runtime,
            paused: false,
            now: now
        )
        XCTAssertEqual(result.severity, .waiting)
        XCTAssertEqual(result.wait?.kind, .resource)
        XCTAssertFalse(try XCTUnwrap(result.wait).requiresAttention)
    }

    func testOperationalStatusDetectsStaleHeartbeat() {
        let health = validHealth(observedAt: 1, runId: "process:1")
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: health,
            runtime: nil,
            paused: false,
            now: Date(timeIntervalSince1970: 901)
        )
        XCTAssertEqual(result.severity, .attention)
        XCTAssertTrue(result.label.contains("stale"))
    }

    func testETAReportsQuotaInsteadOfFalsePrecision() {
        let sol = provider(key: "sol-5.6", provider: "sol")
        let health = validHealth(
            queues: [queue(kind: sol.workKind, pending: 99, quota: 1)],
            providers: [sol]
        )
        XCTAssertEqual(
            ProgressEstimator.eta(health: health, rates: ["sol-5.6": 100]),
            "ETA waiting on quota"
        )
    }

    func testProviderOAuthIssueIsVisibleAndActionable() {
        let provider = provider(
            key: "sol-5.6",
            provider: "sol",
            error: "CODEX_OAUTH_TOKEN_REVOKED",
            retryAt: 2_000_000
        )
        XCTAssertTrue(provider.hasAuthenticationIssue)
        XCTAssertTrue(
            provider.activityLabel(now: Date(timeIntervalSince1970: 1000)).contains("sign-in required")
        )
        XCTAssertTrue(provider.activityLabel(now: Date(timeIntervalSince1970: 1000)).contains("0 active"))
        XCTAssertTrue(provider.activityLabel(now: Date(timeIntervalSince1970: 1000)).contains("selected 1"))
        XCTAssertTrue(provider.activityLabel(now: Date(timeIntervalSince1970: 1000)).contains("target 128"))
    }

    func testUnknownProviderIssueIsNotSilentlyReducedToActivity() {
        let provider = provider(key: "sol-5.6", provider: "sol", error: "FUTURE_PROVIDER_ERROR")
        XCTAssertFalse(provider.hasAuthenticationIssue)
        XCTAssertEqual(
            provider.activityLabel(),
            "provider issue · FUTURE_PROVIDER_ERROR · 0 active · selected 1 · target 128"
        )
    }

    func testETAReportsAuthenticationBeforeLearningOrRetry() {
        let sol = provider(
            key: "sol-5.6",
            provider: "sol",
            error: "ENRICHMENT_AUTH_REQUIRED"
        )
        let health = validHealth(
            queues: [queue(kind: sol.workKind, pending: 99)],
            providers: [sol]
        )
        XCTAssertEqual(ProgressEstimator.eta(health: health, rates: [:]), "ETA waiting for Sol sign-in")
    }

    func testETAReportsQuotaEvenWhileOneInvocationDrains() {
        let sol = provider(key: "sol-5.6", provider: "sol")
        let health = validHealth(
            queues: [queue(kind: sol.workKind, pending: 0, quota: 99, active: 1)],
            providers: [sol]
        )
        XCTAssertEqual(
            ProgressEstimator.eta(health: health, rates: ["sol-5.6": 100]),
            "~1 hours remaining · delays possible"
        )
    }
}
