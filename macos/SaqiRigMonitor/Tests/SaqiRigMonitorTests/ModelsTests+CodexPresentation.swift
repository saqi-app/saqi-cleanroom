import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    @MainActor
    func testCodexPresentationShowsCanonicalProgressBudgetAndAccessibleRetry() throws {
        let now = Date(timeIntervalSince1970: 1_000_000)
        let entry = presentationEntry(
            admission: ProviderExecutionAdmission(
                operatorAction: "none",
                primaryReason: "codex_quota_wait",
                recovery: "automatic",
                retryAt: 1_000_060_000,
                state: "waiting"
            ),
            budget: BudgetGate(
                budgetId: "private-budget-id",
                maximumOperations: 12,
                remainingOperations: 9,
                reservedOperations: 3,
                state: "active"
            ),
            quota: QuotaGate(
                errorCode: "CODEX_QUOTA_EXHAUSTED",
                nextProbeAt: 1_000_030_000,
                retryAt: 1_000_060_000,
                state: "waiting"
            )
        )
        let store = presentationStore(entry: entry, now: now)

        let value = try XCTUnwrap(store.codexExecutionPresentation(now: now))

        XCTAssertEqual(value.headline, "Codex quota reached")
        XCTAssertEqual(value.detail, "Next probe in 1 minute")
        XCTAssertEqual(value.progress, "5 accepted · 4 ready · 2 delayed · 1 terminal")
        XCTAssertEqual(value.concurrency, "1 active · selected 2 · ceiling 4")
        XCTAssertEqual(value.budget, "Budget · 9 of 12 operations available · 3 reserved")
        XCTAssertEqual(value.accessibilityLabel, "Codex translation status")
        XCTAssertTrue(value.accessibilityValue.contains(value.progress))
        XCTAssertTrue(value.accessibilityValue.contains(value.budget))
        XCTAssertFalse(value.accessibilityValue.contains("private-budget-id"))
    }

    @MainActor
    func testCodexPresentationDescribesSwitchDrainWithoutCredentialEpochs() throws {
        let now = Date(timeIntervalSince1970: 1_000_000)
        let credentials = ProviderCredentialHealth(
            accountEpoch: 918_273,
            change: "account_switch",
            changedAt: 999_000_000,
            errorCode: "CODEX_ACCOUNT_SWITCH_WAIT",
            lastVerifiedAt: 999_000_000,
            materialEpoch: 817_263,
            retryAt: 1_000_060_000,
            state: "account_switch_wait"
        )
        let entry = presentationEntry(
            admission: ProviderExecutionAdmission(
                operatorAction: "none",
                primaryReason: "auth_wait",
                recovery: "automatic",
                retryAt: 1_000_060_000,
                state: "waiting"
            ),
            credentials: credentials,
            sessions: ProviderExecutionSessions(
                activeCurrentAccountEpoch: 0,
                activePreviousAccountEpoch: 1,
                activeUnattributed: 0
            )
        )
        let value = try XCTUnwrap(presentationStore(entry: entry, now: now)
            .codexExecutionPresentation(now: now))

        XCTAssertEqual(value.headline, "Switching Codex credentials")
        XCTAssertEqual(value.credentialSwitch, "Credential switch · draining 1 earlier sessions")
        XCTAssertTrue(value.accessibilityValue.contains("Credential switch"))
        for forbidden in ["918273", "817263", "accountEpoch", "materialEpoch", "account identity"] {
            XCTAssertFalse(value.accessibilityValue.localizedCaseInsensitiveContains(forbidden))
        }
    }

    @MainActor
    func testCodexPresentationUsesCurrentRuntimeWhenPipelineHealthStillHasPriorPID() {
        let now = Date(timeIntervalSince1970: 1_000_000)
        let store = presentationStore(entry: presentationEntry(), now: now)
        store.health = validHealth(
            queues: [queue(kind: "poem-enrichment-sol", pending: 4, active: 1)],
            providers: [provider(key: "sol-5.6", provider: "sol")],
            observedAt: 1_000_000_000,
            runId: "process:999"
        )

        XCTAssertEqual(store.codexExecutionPresentation(now: now)?.headline, "Translating · 1 active")
        store.providerExecutionHealth = nil
        XCTAssertNil(store.codexExecutionPresentation(now: now))
    }

    @MainActor
    func testCodexPresentationRejectsStaleAndFutureExecutionSnapshots() {
        let now = Date(timeIntervalSince1970: 1_000_000)
        let stale = presentationStore(
            entry: presentationEntry(),
            now: now,
            executionOffsetMilliseconds: -16 * 60_000
        )
        XCTAssertEqual(stale.codexExecutionPresentation(now: now)?.headline, "Codex details stale")

        let future = presentationStore(
            entry: presentationEntry(),
            now: now,
            executionOffsetMilliseconds: 31_000
        )
        XCTAssertEqual(
            future.codexExecutionPresentation(now: now)?.headline,
            "Codex details unavailable · clock mismatch"
        )
    }

    @MainActor
    func testETAUsesCurrentBackendPoemThroughputWithPriorPIDHealth() {
        let now = Date()
        let throughput = poemThroughput(publishedLastHour: 10, remaining: 30)
        let store = presentationStore(
            entry: presentationEntry(throughput: throughput),
            now: now
        )
        store.health = validHealth(
            queues: [queue(kind: "poem-enrichment-sol", pending: 4, active: 1)],
            providers: [provider(key: "sol-5.6", provider: "sol")],
            observedAt: Int(now.timeIntervalSince1970 * 1000),
            runId: "process:999"
        )

        XCTAssertEqual(store.eta, "~3 hours remaining")
        XCTAssertEqual(store.rateLabel, "10/hour published · rolling 1h")
    }

    func testETAWaitsForCompleteMilestoneCoverage() {
        let throughput = poemThroughput(
            publishedLastHour: 10,
            remaining: 30,
            backfillComplete: false
        )

        XCTAssertEqual(
            ProgressEstimator.eta(provider: presentationEntry(throughput: throughput)),
            "ETA indexing · 10/hour published"
        )
    }

    func testETAExplainsIndexingWhenNoRecentPublicationExists() {
        let throughput = poemThroughput(
            publishedLastHour: 0,
            remaining: 30,
            backfillComplete: false
        )

        XCTAssertEqual(
            ProgressEstimator.eta(provider: presentationEntry(throughput: throughput)),
            "ETA indexing · no publications in last hour"
        )
    }

    @MainActor
    func testProductionStallSuppressesNumericETAAndLabelsLastMeasuredRate() {
        let now = Date()
        let store = presentationStore(
            entry: presentationEntry(throughput: poemThroughput(publishedLastHour: 10, remaining: 30)),
            now: now
        )
        store.health = validHealth(
            queues: [queue(kind: "poem-enrichment-sol", pending: 4, active: 1)],
            providers: [provider(key: "sol-5.6", provider: "sol")],
            observedAt: Int(now.timeIntervalSince1970 * 1000),
            runId: "process:1",
            growth: PipelineGrowth(
                production: ProductionGrowth(configured: true, lastSuccessAt: nil, state: "stalled")
            )
        )

        XCTAssertEqual(store.eta, "ETA unavailable · production stalled")
        XCTAssertEqual(store.rateLabel, "Last measured · 10/hour published · rolling 1h · stalled")
    }

    @MainActor
    func testRecentSampleRateRemainsVisibleAcrossDiagnosticIdentityRotation() {
        let now = Date()
        let store = presentationStore(entry: presentationEntry(), now: now)
        store.runtime = nil
        store.providerExecutionHealth = nil
        store.health = validHealth(
            queues: [queue(kind: "poem-enrichment-sol", pending: 4, active: 1)],
            providers: [provider(key: "sol-5.6", provider: "sol")],
            observedAt: Int(now.timeIntervalSince1970 * 1000),
            runId: "process:999"
        )
        let digest = String(repeating: "a", count: 64)
        store.samples = [
            ProgressSample(
                acceptedByModel: ["sol-5.6": 10],
                configDigest: digest,
                observedAt: now.addingTimeInterval(-10 * 60)
            ),
            ProgressSample(
                acceptedByModel: ["sol-5.6": 12],
                configDigest: digest,
                observedAt: now
            ),
        ]

        XCTAssertEqual(store.rateLabel, "Last measured · 12/hour accepted · 1/1 lanes measured")

        store.samples = store.samples.map { sample in
            ProgressSample(
                acceptedByModel: sample.acceptedByModel,
                configDigest: sample.configDigest,
                observedAt: sample.observedAt.addingTimeInterval(-2 * 60 * 60)
            )
        }
        XCTAssertEqual(store.rateLabel, "Rate unavailable · starting")
    }
}

extension ModelsTests {
    @MainActor
    func presentationStore(
        entry: ProviderExecutionEntry,
        now: Date,
        executionOffsetMilliseconds: Int = -1000
    ) -> RigStore {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        let defaults = UserDefaults(suiteName: UUID().uuidString) ?? .standard
        let store = RigStore(
            configuration: MonitorConfiguration(
                crawlerCLI: root.appending(path: "cli.js"),
                rigConfig: root.appending(path: "rig.json"),
                serviceLabel: "net.saqi.test",
                stateDirectory: root
            ),
            userDefaults: defaults,
            monitorContinuously: false
        )
        let digest = String(repeating: "a", count: 64)
        store.service = service(actualState: "running", actions: [])
        store.health = validHealth(
            queues: [queue(kind: "poem-enrichment-sol", pending: 4, active: 1)],
            providers: [provider(key: "sol-5.6", provider: "sol")],
            observedAt: Int(now.timeIntervalSince1970 * 1000),
            runId: "process:1"
        )
        let execution = ProviderExecutionHealth(
            configDigest: digest,
            observedAt: Int(now.timeIntervalSince1970 * 1000) + executionOffsetMilliseconds,
            providers: [entry],
            runId: "private-provider-run-id",
            schemaId: "saqi.provider-execution-health",
            schemaVersion: 1
        )
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        store.runtime = RuntimeStatus(
            configDigest: digest,
            observedAt: formatter.string(from: now),
            ownerPid: 1,
            resourcePressure: ResourcePressureStatus(nextProbeAt: 0, reasons: [], state: "ready"),
            runId: "run",
            workload: RuntimeWorkload(providerExecution: execution)
        )
        store.providerExecutionHealth = execution
        return store
    }

    func presentationEntry(
        admission: ProviderExecutionAdmission = ProviderExecutionAdmission(
            operatorAction: "none",
            primaryReason: "active",
            recovery: "none",
            retryAt: nil,
            state: "open"
        ),
        budget: BudgetGate = BudgetGate(
            budgetId: nil,
            maximumOperations: 0,
            remainingOperations: 0,
            reservedOperations: 0,
            state: "unarmed"
        ),
        credentials: ProviderCredentialHealth = ProviderCredentialHealth(
            accountEpoch: 1,
            change: "none",
            changedAt: nil,
            errorCode: nil,
            lastVerifiedAt: 999_000_000,
            materialEpoch: 1,
            retryAt: nil,
            state: "ready"
        ),
        quota: QuotaGate = QuotaGate(errorCode: nil, nextProbeAt: nil, retryAt: nil, state: "clear"),
        sessions: ProviderExecutionSessions = ProviderExecutionSessions(
            activeCurrentAccountEpoch: 1,
            activePreviousAccountEpoch: 0,
            activeUnattributed: 0
        ),
        throughput: ProviderPoemThroughput? = nil
    ) -> ProviderExecutionEntry {
        ProviderExecutionEntry(
            admission: admission,
            credentials: credentials,
            enabled: true,
            gates: ProviderExecutionGates(
                authentication: RetryGate(errorCode: nil, retryAt: nil, state: "ready"),
                budget: budget,
                operator: OperatorGate(globalPaused: false, paidWorkPaused: false),
                provider: ProviderGate(errorCode: nil, retryAt: nil, state: "ready"),
                quota: quota,
                resources: ResourceGate(nextProbeAt: nil, reasons: [], state: "ready"),
                scheduler: SchedulerGate(
                    activeInvocations: 1,
                    configuredConcurrency: 4,
                    nextWakeAt: nil,
                    selectedConcurrency: 2,
                    state: "adaptive"
                )
            ),
            model: "gpt-5.6-sol",
            modelKey: "sol-5.6",
            progress: ProviderExecutionProgress(
                accepted: 5,
                activeInvocations: 1,
                delayedWork: 2,
                lastAcceptedAt: 999_000_000,
                readyWork: 4,
                state: "active",
                terminalWork: 1
            ),
            provider: "sol",
            sessions: sessions,
            throughput: throughput
        )
    }

    func poemThroughput(
        publishedLastHour: Int,
        remaining: Int,
        backfillComplete: Bool = true
    ) -> ProviderPoemThroughput {
        ProviderPoemThroughput(
            coverage: ProviderPoemThroughputCoverage(
                backfillComplete: backfillComplete,
                highWatermark: 100,
                state: backfillComplete ? "complete" : "backfilling"
            ),
            generated: PoemMilestoneWindow(last15m: 4, last1h: 12, last5m: 1, lastAt: 999_000_000),
            published: PoemMilestoneWindow(
                last15m: 3,
                last1h: publishedLastHour,
                last5m: 1,
                lastAt: 999_000_000
            ),
            remaining: ProviderPoemRemaining(
                active: 1,
                delayed: 2,
                endToEndPublication: remaining,
                generatedAwaitingPublication: remaining - 7,
                generation: 7,
                ready: 4,
                terminalDead: 0
            )
        )
    }
}
