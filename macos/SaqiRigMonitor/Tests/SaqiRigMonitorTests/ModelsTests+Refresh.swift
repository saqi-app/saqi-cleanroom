import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    @MainActor
    func testManualRefreshRechecksStartupFailureWithoutWritingControlsOrProjection() async throws {
        let (root, preference, store) = try startupRefreshFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        let launchAgent = preference.launchAgentURL
        let coherent = presentationStore(entry: presentationEntry(), now: Date())
        store.runtime = coherent.runtime
        store.paused = true
        store.startupPreferenceError = "Monitor login preference unavailable"
        store.diagnosticError = store.startupPreferenceError
        let health = try XCTUnwrap(coherent.health)
        let service = try XCTUnwrap(coherent.service)
        let ledger = root.appending(path: "ledger.sqlite3")
        let before = try Data(contentsOf: ledger)
        await store.refreshDiagnostics {
            store.applyRefreshResults(.success(health), .success(service), loadedService: true)
            XCTAssertEqual(store.diagnosticError, "Monitor login preference unavailable")
        }
        XCTAssertEqual(store.refreshOutcome, "Monitor login preference unavailable")
        XCTAssertNotNil(store.startupPreferenceError)
        XCTAssertFalse(store.startsAtLogin)
        XCTAssertTrue(store.canRefreshDiagnostics)
        XCTAssertEqual(try Data(contentsOf: ledger), before)
        XCTAssertFalse(FileManager.default.fileExists(atPath: launchAgent.path))
        XCTAssertTrue(store.paused)

        // An external explicit repair establishes the projection; Refresh itself never installs it.
        try preference.setEnabled(true)
        let repairedLedger = try Data(contentsOf: ledger)
        let repairedProjection = try Data(contentsOf: launchAgent)
        await store.refreshDiagnostics {
            store.applyRefreshResults(.success(health), .success(service), loadedService: true)
        }
        XCTAssertEqual(store.refreshOutcome, "Diagnostics refreshed")
        XCTAssertNil(store.startupPreferenceError)
        XCTAssertNil(store.diagnosticError)
        XCTAssertTrue(store.startsAtLogin)
        XCTAssertTrue(store.paused)
        XCTAssertEqual(try Data(contentsOf: ledger), repairedLedger)
        XCTAssertEqual(try Data(contentsOf: launchAgent), repairedProjection)
    }

    @MainActor
    private func startupRefreshFixture() throws -> (URL, MonitorStartupPreference, RigStore) {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        try createStartupLedger(root)
        try executeStartupSQL(root, sql: """
        INSERT INTO runtime_control VALUES ('legacy_monitor_autostart_imported',1),
          ('monitor_autostart_enabled',1),('global_paused',1),('paid_work_paused',1),('service_enabled',0);
        CREATE TABLE budget_fixture(reserved INTEGER);
        INSERT INTO budget_fixture VALUES(3);
        """)
        let launchAgent = root.appending(path: "monitor.plist")
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "app"),
            launchAgentURL: launchAgent
        )
        let store = RigStore(
            configuration: MonitorConfiguration(
                crawlerCLI: root.appending(path: "missing-cli"),
                rigConfig: root.appending(path: "rig.json"),
                serviceLabel: "net.saqi.test",
                stateDirectory: root
            ),
            monitorContinuously: false,
            startupPreference: preference
        )
        return (root, preference, store)
    }

    func testReadOnlyStartupVerificationDistinguishesDisabledUnknownAndMismatchedProjection() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        let projection = root.appending(path: "monitor.plist")
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "app"),
            launchAgentURL: projection
        )
        XCTAssertThrowsError(try preference.readVerified())
        try executeStartupSQL(root, sql: """
        INSERT INTO runtime_control VALUES ('legacy_monitor_autostart_imported',1),('monitor_autostart_enabled',0)
        """)
        XCTAssertFalse(try preference.readVerified())
        try Data("not a valid projection".utf8).write(to: projection)
        XCTAssertThrowsError(try preference.readVerified())
        try executeStartupSQL(
            root,
            sql: "UPDATE runtime_control SET enabled=1 WHERE control_key='monitor_autostart_enabled'"
        )
        XCTAssertThrowsError(try preference.readVerified())
        XCTAssertEqual(try Data(contentsOf: projection), Data("not a valid projection".utf8))
    }

    @MainActor
    func testLastManualRefreshDoesNotOverrideCurrentFreshnessLabel() {
        let status = RefreshDiagnosticsStatus(
            freshnessLabel: "Updated 20 minutes ago",
            outcome: "Diagnostics refreshed"
        )
        XCTAssertEqual(status.freshnessLabel, "Updated 20 minutes ago")
        XCTAssertEqual(status.historicalOutcomeLabel, "Last manual refresh · Diagnostics refreshed")
    }

    @MainActor
    func testDiagnosticRefreshReportsSuccessOnlyForCoherentReadback() async {
        let store = presentationStore(entry: presentationEntry(), now: Date())
        XCTAssertEqual(store.refreshDiagnosticsTitle, "Refresh")
        store.diagnosticError = "Progress history persistence unavailable"
        XCTAssertEqual(store.refreshDiagnosticsTitle, "Retry")
        store.diagnosticError = nil
        await store.refreshDiagnostics {}
        XCTAssertEqual(store.refreshOutcome, "Diagnostics refreshed")
        store.service = nil
        XCTAssertEqual(store.refreshDiagnosticsTitle, "Retry")
        await store.refreshDiagnostics {}
        XCTAssertEqual(store.refreshOutcome, "Diagnostics remain stale or mismatched")
    }

    @MainActor
    func testDiagnosticRefreshPreservesControlsAndReportsReadFailure() async {
        let store = presentationStore(entry: presentationEntry(), now: Date())
        store.paused = true
        let service = store.service
        let execution = store.providerExecutionHealth
        await store.refreshDiagnostics {
            XCTAssertFalse(store.canRefreshDiagnostics)
            XCTAssertEqual(store.refreshDiagnosticsTitle, "Refreshing…")
            store.diagnosticError = "Runtime status unavailable"
            store.nextHistoryImportAttempt = .distantFuture
            await store.refreshDiagnostics { XCTFail("Overlapping refresh must not run") }
            XCTAssertEqual(store.nextHistoryImportAttempt, .distantFuture)
        }
        XCTAssertEqual(store.refreshOutcome, "Runtime status unavailable")
        XCTAssertTrue(store.paused)
        XCTAssertEqual(store.service, service)
        XCTAssertEqual(store.providerExecutionHealth, execution)
        XCTAssertTrue(store.canRefreshDiagnostics)
        XCTAssertEqual(store.refreshDiagnosticsTitle, "Retry")
    }

    @MainActor
    func testDiagnosticRefreshRemainsAvailableWhenStaleButNotBusy() async {
        let store = presentationStore(entry: presentationEntry(), now: Date(timeIntervalSince1970: 1000))
        XCTAssertTrue(store.canRefreshDiagnostics)
        await store.refreshDiagnostics {}
        XCTAssertEqual(store.refreshOutcome, "Diagnostics remain stale or mismatched")
        store.activeRefreshes = 1
        await store.refreshDiagnostics { XCTFail("Background refresh must finish first") }
        XCTAssertFalse(store.canRefreshDiagnostics)
        store.activeRefreshes = 0
        store.phase = .applying("Stopping")
        await store.refreshDiagnostics { XCTFail("Mutation must finish first") }
        XCTAssertFalse(store.canRefreshDiagnostics)
    }
}
