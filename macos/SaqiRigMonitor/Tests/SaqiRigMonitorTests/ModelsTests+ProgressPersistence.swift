import SQLite3
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    private func historyFixture() throws -> (URL, MonitorProgressStore, UserDefaults) {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        try createStartupLedger(root)
        try executeStartupSQL(root, sql: """
        CREATE TABLE monitor_progress_history (
          singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
          payload BLOB NOT NULL CHECK(length(payload) <= 131072),
          updated_at INTEGER NOT NULL CHECK(updated_at >= 0)
        ) STRICT;
        """)
        return try (root, MonitorProgressStore(stateDirectory: root), XCTUnwrap(UserDefaults(
            suiteName: UUID().uuidString
        )))
    }

    private func historyData() throws -> Data {
        let now = Date()
        return try JSONEncoder().encode(ProgressHistory(schemaVersion: 1, samples: [
            ProgressSample(
                acceptedByModel: ["sol": 1],
                configDigest: "config",
                observedAt: now.addingTimeInterval(-600)
            ),
            ProgressSample(acceptedByModel: ["sol": 3], configDigest: "config", observedAt: now),
        ]))
    }

    func testProgressHistoryImportsOnceAndRetainsRateAcrossRestart() throws {
        let (root, store, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        try defaults.set(historyData(), forKey: MonitorProgressStore.legacyKey)
        let imported = try store.load(legacyDefaults: defaults)
        XCTAssertEqual(ProgressEstimator.rates(samples: imported)["sol"], 12)
        XCTAssertNil(defaults.data(forKey: MonitorProgressStore.legacyKey))
        defaults.set(Data("stale".utf8), forKey: MonitorProgressStore.legacyKey)
        XCTAssertEqual(try MonitorProgressStore(stateDirectory: root).load(legacyDefaults: defaults), imported)
        try store.save([])
        XCTAssertEqual(try store.load(), [])
    }

    func testProgressHistoryReadOnlyLoadDoesNotImport() throws {
        let (root, store, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        let legacy = try historyData()
        defaults.set(legacy, forKey: MonitorProgressStore.legacyKey)
        XCTAssertEqual(try store.load(), [])
        XCTAssertEqual(defaults.data(forKey: MonitorProgressStore.legacyKey), legacy)
        XCTAssertEqual(try store.load(legacyDefaults: defaults).count, 2)
    }

    func testProgressHistoryFailurePreservesLegacyAndRejectsCorruptDatabasePayload() throws {
        let (root, store, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        let legacy = try historyData()
        defaults.set(legacy, forKey: MonitorProgressStore.legacyKey)
        try executeStartupSQL(root, sql: """
        CREATE TRIGGER reject_history BEFORE INSERT ON monitor_progress_history
        BEGIN SELECT RAISE(ABORT, 'test failure'); END;
        """)
        XCTAssertThrowsError(try store.load(legacyDefaults: defaults))
        XCTAssertEqual(defaults.data(forKey: MonitorProgressStore.legacyKey), legacy)
        try executeStartupSQL(root, sql: """
        DROP TRIGGER reject_history;
        INSERT INTO monitor_progress_history VALUES (1, X'626164', 1);
        """)
        XCTAssertThrowsError(try store.load(legacyDefaults: defaults))
        XCTAssertEqual(defaults.data(forKey: MonitorProgressStore.legacyKey), legacy)
    }

    func testInvalidLegacyHistoryIsImportedAsEmptyOnlyOnce() throws {
        let (root, store, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        defaults.set(Data(repeating: 1, count: 131_073), forKey: MonitorProgressStore.legacyKey)
        XCTAssertEqual(try store.load(legacyDefaults: defaults), [])
        try defaults.set(historyData(), forKey: MonitorProgressStore.legacyKey)
        XCTAssertEqual(try store.load(legacyDefaults: defaults), [])
    }

    func testHistoryMissingDatabaseDoesNotCreateStorageOrConsumeLegacy() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        let defaults = try XCTUnwrap(UserDefaults(suiteName: UUID().uuidString))
        try defaults.set(historyData(), forKey: MonitorProgressStore.legacyKey)
        let store = MonitorProgressStore(stateDirectory: root)
        XCTAssertThrowsError(try store.load(legacyDefaults: defaults))
        XCTAssertThrowsError(try store.save([]))
        XCTAssertNotNil(defaults.data(forKey: MonitorProgressStore.legacyKey))
        XCTAssertFalse(FileManager.default.fileExists(atPath: root.path))
    }

    func testHistoryLockFailurePreservesImportAndRecovery() throws {
        let (root, store, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        try defaults.set(historyData(), forKey: MonitorProgressStore.legacyKey)
        var database: OpaquePointer?
        XCTAssertEqual(sqlite3_open(root.appending(path: "ledger.sqlite3").path, &database), SQLITE_OK)
        defer { sqlite3_close(database) }
        XCTAssertEqual(sqlite3_exec(database, "BEGIN IMMEDIATE", nil, nil, nil), SQLITE_OK)
        XCTAssertThrowsError(try store.load(legacyDefaults: defaults))
        XCTAssertNotNil(defaults.data(forKey: MonitorProgressStore.legacyKey))
        XCTAssertEqual(sqlite3_exec(database, "ROLLBACK", nil, nil, nil), SQLITE_OK)
        XCTAssertThrowsError(try store.save([]))
        XCTAssertEqual(try store.load(legacyDefaults: defaults).count, 2)
    }

    @MainActor
    func testRefreshPersistenceRetriesFailedStartupImportWithoutLosingHistory() throws {
        let (root, persistence, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        try defaults.set(historyData(), forKey: MonitorProgressStore.legacyKey)
        try executeStartupSQL(root, sql: """
        CREATE TRIGGER reject_history BEFORE INSERT ON monitor_progress_history
        BEGIN SELECT RAISE(ABORT, 'temporary failure'); END;
        """)
        let store = historyRig(root: root, defaults: defaults)
        XCTAssertNotNil(store.pendingHistoryImport)
        let now = Date()
        store.samples = [
            ProgressSample(acceptedByModel: ["sol": 5], configDigest: "config", observedAt: now),
        ]
        store.persistSamples(now: now)
        XCTAssertNotNil(defaults.data(forKey: MonitorProgressStore.legacyKey))
        try executeStartupSQL(root, sql: "DROP TRIGGER reject_history")
        store.persistSamples(now: now.addingTimeInterval(1))
        XCTAssertNotNil(store.pendingHistoryImport)
        store.persistSamples(now: now.addingTimeInterval(31))
        XCTAssertNil(store.pendingHistoryImport)
        XCTAssertNil(defaults.data(forKey: MonitorProgressStore.legacyKey))
        XCTAssertEqual(try persistence.load().first?.acceptedByModel["sol"], 1)
        XCTAssertEqual(try persistence.load().last?.acceptedByModel["sol"], 5)
        XCTAssertNil(store.diagnosticError)
    }

    @MainActor
    func testManualRefreshRetriesHistoryWhileAutomaticRefreshRetainsBackoff() async throws {
        let (root, persistence, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        try defaults.set(historyData(), forKey: MonitorProgressStore.legacyKey)
        try executeStartupSQL(root, sql: """
        CREATE TRIGGER reject_history BEFORE INSERT ON monitor_progress_history
        BEGIN SELECT RAISE(ABORT, 'temporary failure'); END;
        """)
        let store = historyRig(root: root, defaults: defaults)
        let health = validHealth(queues: [], providers: [])
        let service = service(actualState: "running", actions: [])
        store.applyRefreshResults(.success(health), .success(service), loadedService: true)
        XCTAssertTrue(store.historyPersistenceFailed)
        XCTAssertEqual(store.diagnosticError, "Progress history persistence unavailable")
        try executeStartupSQL(root, sql: "DROP TRIGGER reject_history")
        store.applyRefreshResults(.success(health), .success(service), loadedService: true)
        XCTAssertNotNil(store.pendingHistoryImport, "Identical refresh must respect retry throttle")
        XCTAssertEqual(store.diagnosticError, "Progress history persistence unavailable")
        await store.refreshDiagnostics {
            store.applyRefreshResults(.success(health), .success(service), loadedService: true)
        }
        XCTAssertNil(store.pendingHistoryImport)
        XCTAssertFalse(store.historyPersistenceFailed)
        XCTAssertNil(store.diagnosticError)
        XCTAssertEqual(try persistence.load().count, 2)
    }

    @MainActor
    func testIdenticalRefreshRetriesSaveFailureAfterImportCompleted() throws {
        let (root, persistence, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        let store = historyRig(root: root, defaults: defaults)
        XCTAssertNil(store.pendingHistoryImport)
        try executeStartupSQL(root, sql: """
        CREATE TRIGGER reject_history BEFORE INSERT ON monitor_progress_history
        BEGIN SELECT RAISE(ABORT, 'temporary failure'); END;
        """)
        store.samples = [
            ProgressSample(acceptedByModel: ["sol": 5], configDigest: "config", observedAt: Date()),
        ]
        store.persistSamples()
        XCTAssertTrue(store.historyPersistenceFailed)
        XCTAssertNil(store.pendingHistoryImport)
        let health = validHealth(queues: [], providers: [])
        let service = service(actualState: "running", actions: [])
        store.applyRefreshResults(.success(health), .success(service), loadedService: true)
        XCTAssertEqual(store.diagnosticError, "Progress history persistence unavailable")
        try executeStartupSQL(root, sql: "DROP TRIGGER reject_history")
        store.applyRefreshResults(.success(health), .success(service), loadedService: true)
        XCTAssertTrue(store.historyPersistenceFailed, "Save failure retry must remain throttled")
        store.nextHistoryImportAttempt = .distantPast
        store.applyRefreshResults(.success(health), .success(service), loadedService: true)
        XCTAssertFalse(store.historyPersistenceFailed)
        XCTAssertNil(store.diagnosticError)
        XCTAssertEqual(try persistence.load().last?.acceptedByModel["sol"], 5)
    }

    @MainActor
    private func historyRig(root: URL, defaults: UserDefaults, monitorContinuously: Bool = true) -> RigStore {
        let store = RigStore(
            configuration: MonitorConfiguration(
                crawlerCLI: root.appending(path: "cli.js"),
                rigConfig: root.appending(path: "rig.json"),
                serviceLabel: "net.saqi.test",
                stateDirectory: root
            ),
            userDefaults: defaults,
            monitorContinuously: monitorContinuously,
            startupPreference: MonitorStartupPreference(
                stateDirectory: root,
                executableURL: root.appending(path: "app"),
                launchAgentURL: root.appending(path: "test.plist")
            )
        )
        store.monitorTask?.cancel()
        return store
    }

    @MainActor
    func testFailedInitialReadCannotOverwriteExistingHistory() throws {
        let (root, persistence, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        try defaults.set(historyData(), forKey: MonitorProgressStore.legacyKey)
        _ = try persistence.load(legacyDefaults: defaults)
        var database: OpaquePointer?
        XCTAssertEqual(sqlite3_open(root.appending(path: "ledger.sqlite3").path, &database), SQLITE_OK)
        defer { sqlite3_close(database) }
        XCTAssertEqual(sqlite3_exec(database, "BEGIN IMMEDIATE", nil, nil, nil), SQLITE_OK)
        let store = historyRig(root: root, defaults: defaults)
        XCTAssertNotNil(store.pendingHistoryImport)
        let now = Date()
        store.samples = [
            ProgressSample(acceptedByModel: ["sol": 5], configDigest: "config", observedAt: now),
        ]
        store.persistSamples(now: now)
        XCTAssertEqual(sqlite3_exec(database, "ROLLBACK", nil, nil, nil), SQLITE_OK)
        store.persistSamples(now: now.addingTimeInterval(31))
        XCTAssertNil(store.pendingHistoryImport)
        XCTAssertEqual(try persistence.load().first?.acceptedByModel["sol"], 1)
        XCTAssertEqual(try persistence.load().last?.acceptedByModel["sol"], 5)
    }

    @MainActor
    func testReadOnlyRigNeverRetriesImportOrPersistsSamples() throws {
        let (root, persistence, defaults) = try historyFixture()
        defer { try? FileManager.default.removeItem(at: root) }
        try defaults.set(historyData(), forKey: MonitorProgressStore.legacyKey)
        let store = historyRig(root: root, defaults: defaults, monitorContinuously: false)
        store.persistSamples()
        XCTAssertNil(store.pendingHistoryImport)
        XCTAssertNotNil(defaults.data(forKey: MonitorProgressStore.legacyKey))
        XCTAssertEqual(try persistence.load(), [])
    }
}
