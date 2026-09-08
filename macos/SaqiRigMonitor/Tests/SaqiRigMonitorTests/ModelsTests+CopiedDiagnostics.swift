import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    @MainActor
    func testCopiedDiagnosticsAllowlistExcludesPrivateRuntimeIdentity() throws {
        let fixture = try copiedDiagnosticsFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }

        let copied = fixture.store.diagnosticsText

        XCTAssertTrue(copied.contains("service/runtime match true"))
        XCTAssertTrue(copied.contains("Config: loaded/desired match true"))
        XCTAssertTrue(copied.contains("Codex execution: current true"))
        XCTAssertTrue(copied.contains("admission waiting/auth_wait"))
        XCTAssertTrue(copied.contains("budget active, maximum/remaining/reserved 12/9/3"))
        XCTAssertTrue(copied.contains("scheduler adaptive, active/selected/configured 1/2/4"))
        XCTAssertTrue(copied.contains("credentials observation_wait, error invalid"))
        assertCopiedDiagnosticsExcludePrivateValues(copied, digest: fixture.digest)
    }

    @MainActor
    private func copiedDiagnosticsFixture() throws -> (digest: String, root: URL, store: RigStore) {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let defaults = try XCTUnwrap(UserDefaults(suiteName: UUID().uuidString))
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
        let execution = privateProviderExecution(digest: digest)
        configurePrivateDiagnostics(store, digest: digest, execution: execution)
        return (digest, root, store)
    }

    @MainActor
    private func configurePrivateDiagnostics(
        _ store: RigStore,
        digest: String,
        execution: ProviderExecutionHealth
    ) {
        store.service = ServiceSnapshot(
            action: "status",
            actualState: "running",
            allowedActions: ["restart", "status", "stop"],
            configDigest: digest,
            desiredConfigDigest: digest,
            detail: "private-service-detail",
            loadedConfigDigest: digest,
            observedAt: "2026-09-04T00:00:00.000Z",
            ownerPid: 424_242,
            runId: "private-runtime-run-id",
            schemaId: "saqi.launchd-control",
            schemaVersion: 1,
            serviceEnabled: .enabled
        )
        store.runtime = RuntimeStatus(
            configDigest: digest,
            observedAt: "2026-09-04T00:00:00.000Z",
            ownerPid: 424_242,
            resourcePressure: ResourcePressureStatus(nextProbeAt: 0, reasons: [], state: "ready"),
            runId: "private-runtime-run-id",
            workload: RuntimeWorkload(providerExecution: execution)
        )
        store.providerExecutionHealth = execution
        store.diagnosticError = "private-diagnostic-path"
    }

    private func privateProviderExecution(digest: String) -> ProviderExecutionHealth {
        ProviderExecutionHealth(
            configDigest: digest,
            observedAt: 1_000_000,
            providers: [
                privateProviderEntry(retryAt: 2_000_000),
            ],
            runId: "private-provider-run-id",
            schemaId: "saqi.provider-execution-health",
            schemaVersion: 1
        )
    }

    private func privateProviderEntry(retryAt: Int) -> ProviderExecutionEntry {
        ProviderExecutionEntry(
            admission: ProviderExecutionAdmission(
                operatorAction: "restore_auth",
                primaryReason: "auth_wait",
                recovery: "operator",
                retryAt: retryAt,
                state: "waiting"
            ),
            credentials: ProviderCredentialHealth(
                accountEpoch: 918_273,
                change: "none",
                changedAt: nil,
                errorCode: "private/error",
                lastVerifiedAt: nil,
                materialEpoch: 817_263,
                retryAt: retryAt,
                state: "observation_wait"
            ),
            enabled: true,
            gates: privateProviderGates(retryAt: retryAt),
            model: "private-model-identity",
            modelKey: "private-model-key",
            progress: ProviderExecutionProgress(
                accepted: 5,
                activeInvocations: 1,
                delayedWork: 2,
                lastAcceptedAt: 999_000,
                readyWork: 7,
                state: "active",
                terminalWork: 3
            ),
            provider: "sol",
            sessions: ProviderExecutionSessions(
                activeCurrentAccountEpoch: 1,
                activePreviousAccountEpoch: 0,
                activeUnattributed: 0
            ),
            throughput: nil
        )
    }

    private func privateProviderGates(retryAt: Int) -> ProviderExecutionGates {
        ProviderExecutionGates(
            authentication: RetryGate(errorCode: "private/error", retryAt: retryAt, state: "waiting"),
            budget: BudgetGate(
                budgetId: "private-budget-id",
                maximumOperations: 12,
                remainingOperations: 9,
                reservedOperations: 3,
                state: "active"
            ),
            operator: OperatorGate(globalPaused: false, paidWorkPaused: false),
            provider: ProviderGate(errorCode: nil, retryAt: nil, state: "ready"),
            quota: QuotaGate(errorCode: nil, nextProbeAt: nil, retryAt: nil, state: "clear"),
            resources: ResourceGate(nextProbeAt: nil, reasons: [], state: "ready"),
            scheduler: SchedulerGate(
                activeInvocations: 1,
                configuredConcurrency: 4,
                nextWakeAt: retryAt,
                selectedConcurrency: 2,
                state: "adaptive"
            )
        )
    }

    private func assertCopiedDiagnosticsExcludePrivateValues(_ copied: String, digest: String) {
        let values = [
            "424242",
            "private-runtime-run-id",
            "private-provider-run-id",
            "private-budget-id",
            "private-service-detail",
            "private-diagnostic-path",
            "private-model-identity",
            "private-model-key",
            "private/error",
            digest,
            "918273",
            "817263",
        ]
        for value in values {
            XCTAssertFalse(copied.contains(value), "Copied private value: \(value)")
        }
        let fields = [
            "accountEpoch",
            "materialEpoch",
            "budgetId",
            "configDigest",
            "ownerPid",
            "operationKey",
            "runId",
        ]
        for field in fields {
            XCTAssertFalse(copied.contains(field), "Copied private field: \(field)")
        }
    }
}
