import SQLite3
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    func testUncommittedStartupImportMarkerCannotAuthorizePreferenceOrProjection() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        try executeStartupSQL(root, sql: """
        INSERT INTO runtime_control VALUES ('legacy_monitor_autostart_imported',0),('monitor_autostart_enabled',1)
        """)
        let ledger = root.appending(path: "ledger.sqlite3")
        let before = try Data(contentsOf: ledger)
        let projection = root.appending(path: "monitor.plist")
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "app"),
            launchAgentURL: projection
        )
        XCTAssertThrowsError(try MonitorAutostartStore(stateDirectory: root).read())
        XCTAssertThrowsError(try preference.applyDefault())
        XCTAssertThrowsError(try preference.setEnabled(false))
        XCTAssertEqual(try Data(contentsOf: ledger), before)
        XCTAssertFalse(FileManager.default.fileExists(atPath: projection.path))
    }

    func testMonitorUsesTrustedPathInsteadOfMinimalOrInjectedShellPath() {
        for path in ["/usr/bin:/bin:/usr/sbin:/sbin", "/untrusted/bin"] {
            let environment = RigStore.sanitizedEnvironment(source: [
                "PATH": path,
                "HOME": "/fixture",
                "LANG": "en_US.UTF-8",
                "SECRET": "excluded",
            ])
            XCTAssertEqual(environment, [
                "PATH": "/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
                "HOME": "/fixture",
                "LANG": "en_US.UTF-8",
            ])
        }
    }

    func testStartupRepairsMissingTrustedLaunchEnvironment() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        let launchAgent = root.appending(path: "monitor.plist")
        let executable = root.appending(path: "app")
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: executable,
            launchAgentURL: launchAgent
        )
        let legacy: [String: Any] = [
            "Label": MonitorStartupPreference.launchAgentLabel,
            "RunAtLoad": true,
            "ProgramArguments": [executable.path],
        ]
        try PropertyListSerialization.data(fromPropertyList: legacy, format: .xml, options: 0)
            .write(to: launchAgent)
        try preference.setEnabled(true)
        let data = try Data(contentsOf: launchAgent)
        let installed = try XCTUnwrap(
            PropertyListSerialization.propertyList(from: data, format: nil) as? [String: Any]
        )
        XCTAssertEqual(installed["EnvironmentVariables"] as? [String: String], [
            "PATH": MonitorProcessEnvironment.executableSearchPath,
        ])
        XCTAssertTrue(preference.isEnabled)
        XCTAssertEqual(
            try FileManager.default.attributesOfItem(atPath: launchAgent.path)[.posixPermissions] as? Int,
            0o600
        )
        XCTAssertFalse(
            try FileManager.default.contentsOfDirectory(atPath: root.path)
                .contains { $0.hasSuffix(".tmp") }
        )
    }

    func testFailedLaunchAgentPublicationPreservesPreviousPlist() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let launchAgent = root.appending(path: "monitor.plist")
        let original = try PropertyListSerialization.data(
            fromPropertyList: ["Label": "previous-monitor"],
            format: .xml,
            options: 0
        )
        try original.write(to: launchAgent)
        let preference = MonitorStartupPreference(stateDirectory: root, launchAgentURL: launchAgent)
        let replacement = Data("replacement".utf8)
        var publicationAttempted = false
        XCTAssertThrowsError(
            try preference.writeDurably(replacement, to: launchAgent, permissions: 0o600) { temporary, destination in
                publicationAttempted = true
                XCTAssertEqual(temporary.deletingLastPathComponent(), destination.deletingLastPathComponent())
                XCTAssertEqual(try Data(contentsOf: temporary), replacement)
                XCTAssertEqual(try Data(contentsOf: destination), original)
                throw CocoaError(.fileWriteNoPermission)
            }
        )
        XCTAssertTrue(publicationAttempted)
        XCTAssertEqual(try Data(contentsOf: launchAgent), original)
        XCTAssertEqual(try FileManager.default.contentsOfDirectory(atPath: root.path), ["monitor.plist"])
    }

    func testRollbackMirrorFailureDoesNotVetoLaunchAgentProjection() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        let launchAgent = root.appending(path: "monitor.plist")
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "app"),
            launchAgentURL: launchAgent
        )
        // A dangling link occupies the mirror's name but cannot be replaced
        // by writeSentinel's move; unlike permissions this is deterministic.
        try FileManager.default.createSymbolicLink(
            at: root.appending(path: MonitorStartupPreference.enabledFile),
            withDestinationURL: root.appending(path: "missing-enabled-target")
        )
        try preference.setEnabled(true)
        XCTAssertEqual(try MonitorAutostartStore(stateDirectory: root).read(), .enabled)
        XCTAssertTrue(preference.isEnabled)
        try FileManager.default.createSymbolicLink(
            at: root.appending(path: MonitorStartupPreference.disabledFile),
            withDestinationURL: root.appending(path: "missing-disabled-target")
        )
        try preference.setEnabled(false)
        XCTAssertEqual(try MonitorAutostartStore(stateDirectory: root).read(), .disabled)
        XCTAssertFalse(FileManager.default.fileExists(atPath: launchAgent.path))
    }

    func testActualLaunchAgentWriteFailureStillThrows() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        let invalidParent = root.appending(path: "not-a-directory")
        try Data("occupied".utf8).write(to: invalidParent)
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "app"),
            launchAgentURL: invalidParent.appending(path: "monitor.plist")
        )
        XCTAssertThrowsError(try preference.setEnabled(true))
        XCTAssertEqual(try MonitorAutostartStore(stateDirectory: root).read(), .enabled)
        XCTAssertFalse(preference.isEnabled)
    }

    func createStartupLedger(_ root: URL) throws {
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        try executeStartupSQL(root, sql: """
        PRAGMA journal_mode = WAL;
        CREATE TABLE runtime_control(control_key TEXT PRIMARY KEY, enabled INTEGER CHECK(enabled IN (0, 1))) STRICT;
        """)
    }

    func executeStartupSQL(_ root: URL, sql: String) throws {
        var database: OpaquePointer?
        let result = sqlite3_open(root.appending(path: "ledger.sqlite3").path, &database)
        let connection = try XCTUnwrap(database)
        defer { sqlite3_close(connection) }
        XCTAssertEqual(result, SQLITE_OK)
        XCTAssertEqual(sqlite3_exec(connection, sql, nil, nil, nil), SQLITE_OK)
    }

    func testMonitorAutostartIgnoresLegacyEditsAfterImport() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        let store = MonitorAutostartStore(stateDirectory: root)
        XCTAssertTrue(try store.importLegacy())
        try Data("stale disabled".utf8).write(to: root.appending(path: MonitorStartupPreference.disabledFile))
        XCTAssertTrue(try store.importLegacy())
        try store.setEnabled(false)
        try FileManager.default.removeItem(at: root.appending(path: MonitorStartupPreference.disabledFile))
        XCTAssertFalse(try store.importLegacy())
    }

    func testMissingOrCorruptAutostartAuthorityDoesNotChangeLaunchAgent() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let launchAgent = root.appending(path: "monitor.plist")
        let original = Data("existing launch agent".utf8)
        try original.write(to: launchAgent)
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "app"),
            launchAgentURL: launchAgent
        )
        XCTAssertThrowsError(try preference.applyDefault())
        XCTAssertFalse(FileManager.default.fileExists(atPath: root.appending(path: "ledger.sqlite3").path))
        XCTAssertEqual(try Data(contentsOf: launchAgent), original)
        try createStartupLedger(root)
        try executeStartupSQL(root, sql: "INSERT INTO runtime_control VALUES ('legacy_monitor_autostart_imported', 1)")
        XCTAssertThrowsError(try preference.applyDefault())
        XCTAssertEqual(try Data(contentsOf: launchAgent), original)
    }

    func testReadingMonitorAutostartDoesNotImportOrInstall() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        let launchAgent = root.appending(path: "monitor.plist")
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "app"),
            launchAgentURL: launchAgent
        )
        XCTAssertFalse(preference.isEnabled)
        XCTAssertEqual(try MonitorAutostartStore(stateDirectory: root).read(), .uninitialized)
        XCTAssertFalse(FileManager.default.fileExists(atPath: launchAgent.path))
    }

    @MainActor
    func testMonitoringStoreReportsUnavailableStartupAuthority() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let launchAgent = root.appending(path: "test.plist")
        let store = try RigStore(
            configuration: MonitorConfiguration(
                crawlerCLI: root.appending(path: "cli.js"),
                rigConfig: root.appending(path: "rig.json"),
                serviceLabel: "net.saqi.test",
                stateDirectory: root
            ),
            userDefaults: XCTUnwrap(UserDefaults(suiteName: UUID().uuidString)),
            startupPreference: MonitorStartupPreference(
                stateDirectory: root,
                executableURL: root.appending(path: "app"),
                launchAgentURL: launchAgent
            )
        )
        store.monitorTask?.cancel()
        XCTAssertFalse(store.startsAtLogin)
        XCTAssertEqual(store.diagnosticError, "Monitor login preference unavailable")
        XCTAssertFalse(FileManager.default.fileExists(atPath: launchAgent.path))
    }

    @MainActor
    func testNonMonitoringStoreDoesNotMutateLoginLaunchAgent() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        let launchAgent = root.appending(path: "net.saqi.monitor.plist")
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "xctest"),
            launchAgentURL: launchAgent
        )
        let defaults = try XCTUnwrap(UserDefaults(suiteName: UUID().uuidString))

        _ = RigStore(
            configuration: MonitorConfiguration(
                crawlerCLI: root.appending(path: "cli.js"),
                rigConfig: root.appending(path: "rig.json"),
                serviceLabel: "net.saqi.test",
                stateDirectory: root
            ),
            userDefaults: defaults,
            monitorContinuously: false,
            startupPreference: preference
        )

        XCTAssertFalse(FileManager.default.fileExists(atPath: launchAgent.path))
        XCTAssertFalse(
            FileManager.default.fileExists(
                atPath: root.appending(path: MonitorStartupPreference.enabledFile).path
            )
        )
    }
}
