import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    func testRetainedSourceStatusIsDecodedAndCannotEnableControl() throws {
        var snapshot = service(actualState: "running_outdated", actions: ["start", "restart", "stop"])
        snapshot.sourceIdentityStatus = .retainedUnverified
        let data = try JSONEncoder().encode(snapshot)
        let decoded = try JSONDecoder().decode(ServiceSnapshot.self, from: data)
        XCTAssertEqual(decoded.sourceIdentityStatus, .retainedUnverified)
        XCTAssertFalse(decoded.allows("start"))
        XCTAssertFalse(decoded.allows("restart"))
        XCTAssertTrue(decoded.allows("stop"))
        let invalid = try XCTUnwrap(String(data: data, encoding: .utf8))
            .replacingOccurrences(of: "retained_unverified", with: "unrecognized")
        XCTAssertThrowsError(try JSONDecoder().decode(ServiceSnapshot.self, from: Data(invalid.utf8)))
    }

    func testOperationalStatusLabelsIntentionalPaidPause() throws {
        let now = Date(timeIntervalSince1970: 1000)
        let sol = provider(key: "sol-5.6", provider: "sol", blockReason: "paused")
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
        XCTAssertEqual(result.label, "Paid translation paused")
        XCTAssertEqual(result.wait?.kind, .paidPause)
        XCTAssertFalse(try XCTUnwrap(result.wait).requiresAttention)
        XCTAssertTrue(sol.activityLabel().hasPrefix("paid work paused"))
    }

    func testOperationalStatusSurfacesProductionStallBeforeAutomaticWaits() {
        let now = Date(timeIntervalSince1970: 1000)
        let sol = provider(key: "sol-5.6", provider: "sol", blockReason: "paused")
        let health = validHealth(
            queues: [queue(kind: sol.workKind, pending: 10)],
            providers: [sol],
            observedAt: 1_000_000,
            runId: "process:1",
            growth: PipelineGrowth(
                production: ProductionGrowth(configured: true, lastSuccessAt: nil, state: "stalled")
            )
        )
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: health,
            runtime: nil,
            paused: false,
            now: now
        )
        XCTAssertEqual(result.severity, .attention)
        XCTAssertEqual(result.label, "Production stalled · attention needed")
        XCTAssertTrue(result.requiresAttention)
    }

    func testProviderOperatorBlockersOutrankProductionStallWhenWorkRemains() throws {
        let cases: [String: OperationalWaitKind] = [
            "auth_wait": .authentication,
            "budget_exhausted": .budget,
            "budget_unarmed": .budget,
            "circuit_open": .provider,
            "disabled": .provider,
        ]
        for (reason, kind) in cases {
            let result = providerOperationalStatus(
                reason: reason,
                operatorAction: operatorAction(for: reason),
                recovery: "operator",
                productionStalled: true
            )
            XCTAssertEqual(result.severity, .attention, reason)
            XCTAssertEqual(result.wait?.kind, kind, reason)
            XCTAssertTrue(try XCTUnwrap(result.wait, reason).requiresAttention, reason)
            XCTAssertTrue(result.label.contains("attention needed"), reason)
            XCTAssertNotEqual(result.label, "Production stalled · attention needed", reason)
        }
    }

    func testProviderAutomaticWaitsRemainNonAttention() throws {
        let cases: [String: OperationalWaitKind] = [
            "auth_wait": .authentication,
            "codex_quota_wait": .quota,
            "error_dampener": .provider,
            "launch_pacing": .retry,
            "network_wait": .network,
            "provider_backoff": .provider,
            "rate_limit_wait": .rateLimit,
            "resource_wait": .resource,
        ]
        for (reason, kind) in cases {
            let result = providerOperationalStatus(reason: reason)
            XCTAssertEqual(result.severity, .waiting, reason)
            XCTAssertFalse(result.requiresAttention, reason)
            XCTAssertEqual(result.wait?.kind, kind, reason)
            XCTAssertFalse(try XCTUnwrap(result.wait, reason).requiresAttention, reason)
            XCTAssertTrue(result.label.contains("automatically"), reason)
        }
    }

    func testProviderNonWaitingExecutionReasonsUseLegacyHealthyStatus() {
        for reason in ["active", "ready", "no_ready_work", "adaptive_capacity", "at_capacity"] {
            let result = providerOperationalStatus(reason: reason)
            XCTAssertEqual(result.severity, .healthy, reason)
            XCTAssertEqual(result.label, "Running · healthy", reason)
            XCTAssertNil(result.wait, reason)
        }
    }

    func testAutomaticProviderWaitDoesNotMaskProductionStall() {
        let result = providerOperationalStatus(reason: "codex_quota_wait", productionStalled: true)
        XCTAssertEqual(result.severity, .attention)
        XCTAssertEqual(result.label, "Production stalled · attention needed")
        XCTAssertNil(result.wait)
    }

    func testProviderIntentionalPausesRemainNeutral() throws {
        let cases: [String: OperationalWaitKind] = [
            "operator_paused": .operatorPause,
            "paid_work_paused": .paidPause,
        ]
        for (reason, kind) in cases {
            let result = providerOperationalStatus(reason: reason)
            XCTAssertEqual(result.severity, .neutral, reason)
            XCTAssertEqual(result.wait?.kind, kind, reason)
            XCTAssertFalse(try XCTUnwrap(result.wait, reason).requiresAttention, reason)
        }
    }

    func testProviderBlockerWithoutRemainingWorkDoesNotMaskProductionStall() {
        let result = providerOperationalStatus(
            reason: "budget_unarmed",
            operatorAction: "arm_budget",
            recovery: "operator",
            remainingWork: 0,
            productionStalled: true
        )
        XCTAssertEqual(result.severity, .attention)
        XCTAssertEqual(result.label, "Production stalled · attention needed")
        XCTAssertNil(result.wait)
    }

    func testServiceStateStillOutranksProviderBlockers() {
        let expected = [
            "fenced": "Fenced · attention needed",
            "running_outdated": "Running · restart needed",
            "starting": "Starting · preparing lanes",
            "stopped": "Stopped",
        ]
        for (state, label) in expected {
            let result = providerOperationalStatus(
                reason: "budget_unarmed",
                operatorAction: "arm_budget",
                recovery: "operator",
                serviceState: state
            )
            XCTAssertEqual(result.label, label, state)
            XCTAssertEqual(
                result.requiresAttention,
                state == "fenced" || state == "running_outdated",
                state
            )
        }
    }

    func testMissingPipelineDiagnosticsRequiresAttentionWhileServiceRuns() {
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: nil,
            runtime: nil,
            paused: false
        )

        XCTAssertEqual(result.label, "Diagnostics unavailable")
        XCTAssertTrue(result.requiresAttention)
    }

    func testTopLevelBlockedHealthRequiresAttentionWithoutBlockedCheck() {
        let now = Date(timeIntervalSince1970: 1000)
        let health = PipelineHealth(
            checks: [],
            schemaId: "saqi.pipeline-health",
            schemaVersion: 1,
            configDigest: String(repeating: "a", count: 64),
            growth: nil,
            heartbeatIntervalMs: nil,
            lastProgressAt: nil,
            observedAt: 1_000_000,
            origins: [],
            providers: [],
            queues: [],
            runId: "process:1",
            sol: nil,
            state: "blocked"
        )
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: health,
            runtime: nil,
            paused: false,
            now: now
        )

        XCTAssertEqual(result.label, "Pipeline blocked · attention needed")
        XCTAssertTrue(result.requiresAttention)
    }

    func testCurrentRuntimeProviderBlockerOutranksPriorProcessPipelineHealth() {
        let now = Date(timeIntervalSince1970: 1000)
        let result = MonitorOperationalStatus.evaluate(
            service: service(actualState: "running", actions: []),
            health: validHealth(observedAt: 1_000_000, runId: "process:999"),
            runtime: providerRuntime(
                reason: "paid_work_paused",
                operatorAction: "resume_paid",
                recovery: "operator",
                remainingWork: 4,
                now: now
            ),
            paused: false,
            now: now
        )

        XCTAssertEqual(result.label, "Paid translation paused")
        XCTAssertEqual(result.severity, .neutral)
        XCTAssertEqual(result.wait?.kind, .paidPause)
    }
}

extension ModelsTests {
    private func providerOperationalStatus(
        reason: String,
        operatorAction: String = "none",
        recovery: String = "automatic",
        remainingWork: Int = 1,
        productionStalled: Bool = false,
        serviceState: String = "running"
    ) -> MonitorOperationalStatus {
        let now = Date(timeIntervalSince1970: 1000)
        let growth = productionStalled
            ? PipelineGrowth(production: ProductionGrowth(configured: true, lastSuccessAt: nil, state: "stalled"))
            : nil
        return MonitorOperationalStatus.evaluate(
            service: service(actualState: serviceState, actions: []),
            health: validHealth(observedAt: 1_000_000, runId: "process:1", growth: growth),
            runtime: providerRuntime(
                reason: reason,
                operatorAction: operatorAction,
                recovery: recovery,
                remainingWork: remainingWork,
                now: now
            ),
            paused: false,
            now: now
        )
    }

    private func operatorAction(for reason: String) -> String {
        switch reason {
        case "auth_wait":
            "restore_auth"
        case "budget_exhausted":
            "rearm_budget"
        case "budget_unarmed":
            "arm_budget"
        case "disabled":
            "enable_provider"
        default:
            "inspect_provider"
        }
    }

    private func providerRuntime(
        reason: String,
        operatorAction: String,
        recovery: String,
        remainingWork: Int,
        now: Date
    ) -> RuntimeStatus {
        let observedAt = Int(now.timeIntervalSince1970 * 1000)
        let execution = ProviderExecutionHealth(
            configDigest: String(repeating: "a", count: 64),
            observedAt: observedAt,
            providers: [
                providerExecutionEntry(
                    reason: reason,
                    operatorAction: operatorAction,
                    recovery: recovery,
                    remainingWork: remainingWork
                ),
            ],
            runId: "provider-run",
            schemaId: "saqi.provider-execution-health",
            schemaVersion: 1
        )
        return RuntimeStatus(
            configDigest: String(repeating: "a", count: 64),
            observedAt: "2026-08-26T00:00:00.000Z",
            ownerPid: 1,
            resourcePressure: ResourcePressureStatus(nextProbeAt: 0, reasons: [], state: "ready"),
            runId: "run",
            workload: RuntimeWorkload(providerExecution: execution)
        )
    }

    private func providerExecutionEntry(
        reason: String,
        operatorAction: String,
        recovery: String,
        remainingWork: Int
    ) -> ProviderExecutionEntry {
        ProviderExecutionEntry(
            admission: ProviderExecutionAdmission(
                operatorAction: operatorAction,
                primaryReason: reason,
                recovery: recovery,
                retryAt: nil,
                state: recovery == "operator" ? "closed" : "waiting"
            ),
            credentials: ProviderCredentialHealth(
                accountEpoch: 1,
                change: "none",
                changedAt: nil,
                errorCode: nil,
                lastVerifiedAt: 1,
                materialEpoch: 1,
                retryAt: nil,
                state: "ready"
            ),
            enabled: true,
            gates: providerExecutionGates(),
            model: "gpt-5.6-sol",
            modelKey: "sol-5.6",
            progress: ProviderExecutionProgress(
                accepted: 0,
                activeInvocations: 0,
                delayedWork: 0,
                lastAcceptedAt: nil,
                readyWork: remainingWork,
                state: "idle",
                terminalWork: 0
            ),
            provider: "sol",
            sessions: ProviderExecutionSessions(
                activeCurrentAccountEpoch: 0,
                activePreviousAccountEpoch: 0,
                activeUnattributed: 0
            ),
            throughput: nil
        )
    }

    private func providerExecutionGates() -> ProviderExecutionGates {
        ProviderExecutionGates(
            authentication: RetryGate(errorCode: nil, retryAt: nil, state: "ready"),
            budget: BudgetGate(
                budgetId: nil,
                maximumOperations: 0,
                remainingOperations: 0,
                reservedOperations: 0,
                state: "unarmed"
            ),
            operator: OperatorGate(globalPaused: false, paidWorkPaused: false),
            provider: ProviderGate(errorCode: nil, retryAt: nil, state: "ready"),
            quota: QuotaGate(errorCode: nil, nextProbeAt: nil, retryAt: nil, state: "clear"),
            resources: ResourceGate(nextProbeAt: nil, reasons: [], state: "ready"),
            scheduler: SchedulerGate(
                activeInvocations: 0,
                configuredConcurrency: 1,
                nextWakeAt: nil,
                selectedConcurrency: 1,
                state: "ready"
            )
        )
    }
}
