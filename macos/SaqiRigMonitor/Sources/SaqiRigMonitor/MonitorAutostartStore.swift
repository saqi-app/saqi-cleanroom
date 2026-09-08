import Foundation
import SQLite3

internal enum MonitorAutostartError: Error {
    case databaseUnavailable
    case invalidControl
}

internal enum MonitorAutostartState {
    case uninitialized
    case enabled
    case disabled
}

internal struct MonitorAutostartStore {
    let stateDirectory: URL

    func read() throws -> MonitorAutostartState {
        try withDatabase(readOnly: true, operation: readControl)
    }

    func importLegacy() throws -> Bool {
        try withDatabase(readOnly: false) { database in
            try execute("BEGIN IMMEDIATE", database: database)
            do {
                let enabled: Bool
                let existing = try readControl(database)
                if existing != .uninitialized {
                    enabled = existing == .enabled
                } else {
                    enabled = try !legacyDisabled()
                    try execute(
                        """
                        INSERT INTO runtime_control(control_key, enabled)
                        VALUES ('monitor_autostart_enabled', \(enabled ? 1 : 0))
                        """,
                        database: database
                    )
                    try execute(
                        """
                        INSERT INTO runtime_control(control_key, enabled)
                        VALUES ('legacy_monitor_autostart_imported', 1)
                        """,
                        database: database
                    )
                }
                try execute("COMMIT", database: database)
                return enabled
            } catch {
                _ = sqlite3_exec(database, "ROLLBACK", nil, nil, nil)
                throw error
            }
        }
    }

    func setEnabled(_ enabled: Bool) throws {
        _ = try importLegacy()
        try withDatabase(readOnly: false) { database in
            try execute(
                """
                UPDATE runtime_control SET enabled = \(enabled ? 1 : 0)
                WHERE control_key = 'monitor_autostart_enabled'
                """,
                database: database
            )
            guard sqlite3_changes(database) == 1 else { throw MonitorAutostartError.invalidControl }
        }
    }

    private func readControl(_ database: OpaquePointer) throws -> MonitorAutostartState {
        let sql = """
        SELECT (SELECT enabled FROM runtime_control WHERE control_key = 'monitor_autostart_enabled'), enabled
        FROM runtime_control WHERE control_key = 'legacy_monitor_autostart_imported'
        """
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
            throw MonitorAutostartError.databaseUnavailable
        }
        defer { sqlite3_finalize(statement) }
        let result = sqlite3_step(statement)
        if result == SQLITE_DONE {
            return .uninitialized
        }
        guard result == SQLITE_ROW, sqlite3_column_type(statement, 0) == SQLITE_INTEGER,
              sqlite3_column_type(statement, 1) == SQLITE_INTEGER, sqlite3_column_int(statement, 1) == 1
        else {
            throw MonitorAutostartError.invalidControl
        }
        let value = sqlite3_column_int(statement, 0)
        guard value == 0 || value == 1 else { throw MonitorAutostartError.invalidControl }
        return value == 1 ? .enabled : .disabled
    }

    private func legacyDisabled() throws -> Bool {
        do {
            _ = try FileManager.default.attributesOfItem(
                atPath: stateDirectory.appending(path: MonitorStartupPreference.disabledFile).path
            )
            return true
        } catch let error as NSError where error.domain == NSCocoaErrorDomain
            && [NSFileNoSuchFileError, NSFileReadNoSuchFileError].contains(error.code)
        {
            return false
        }
    }

    private func execute(_ sql: String, database: OpaquePointer) throws {
        guard sqlite3_exec(database, sql, nil, nil, nil) == SQLITE_OK else {
            throw MonitorAutostartError.databaseUnavailable
        }
    }

    private func withDatabase<T>(readOnly: Bool, operation: (OpaquePointer) throws -> T) throws -> T {
        var connection: OpaquePointer?
        let flags = readOnly ? SQLITE_OPEN_READONLY : SQLITE_OPEN_READWRITE
        let result = sqlite3_open_v2(stateDirectory.appending(path: "ledger.sqlite3").path, &connection, flags, nil)
        guard let connection else { throw MonitorAutostartError.databaseUnavailable }
        defer { sqlite3_close(connection) }
        guard result == SQLITE_OK, sqlite3_busy_timeout(connection, 1000) == SQLITE_OK else {
            throw MonitorAutostartError.databaseUnavailable
        }
        if !readOnly {
            try execute("PRAGMA synchronous = FULL", database: connection)
        }
        return try operation(connection)
    }
}
