import Foundation
import SQLite3

internal struct MonitorProgressStore {
    static let legacyKey = "SaqiRigMonitor.progressHistory.v4"
    let stateDirectory: URL

    func load(legacyDefaults: UserDefaults? = nil) throws -> [ProgressSample] {
        try withDatabase(writable: legacyDefaults != nil) { database in
            guard let legacyDefaults else { return try read(database)?.samples ?? [] }
            try execute("BEGIN IMMEDIATE", database: database)
            do {
                let samples: [ProgressSample]
                if let existing = try read(database) {
                    samples = existing.samples
                } else {
                    samples = legacyDefaults.data(forKey: Self.legacyKey).flatMap { try? decode($0) } ?? []
                    try write(samples, database: database)
                }
                try execute("COMMIT", database: database)
                legacyDefaults.removeObject(forKey: Self.legacyKey)
                return samples
            } catch {
                _ = sqlite3_exec(database, "ROLLBACK", nil, nil, nil)
                throw error
            }
        }
    }

    func save(_ samples: [ProgressSample]) throws {
        try withDatabase(writable: true) { database in
            try execute("BEGIN IMMEDIATE", database: database)
            do {
                guard try read(database) != nil else { throw MonitorAutostartError.invalidControl }
                try write(samples, database: database)
                try execute("COMMIT", database: database)
            } catch {
                _ = sqlite3_exec(database, "ROLLBACK", nil, nil, nil)
                throw error
            }
        }
    }

    private func decode(_ data: Data) throws -> [ProgressSample] {
        guard data.count <= 131_072 else { throw MonitorAutostartError.invalidControl }
        let history = try JSONDecoder().decode(ProgressHistory.self, from: data)
        guard history.schemaVersion == 1 else { throw MonitorAutostartError.invalidControl }
        let now = Date()
        return history.samples.reduce(into: []) { result, sample in
            result = ProgressEstimator.recording(sample, in: result, now: now)
        }
    }

    private func read(_ database: OpaquePointer) throws -> ProgressHistory? {
        var statement: OpaquePointer?
        let sql = "SELECT payload FROM monitor_progress_history WHERE singleton = 1"
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
            throw MonitorAutostartError.databaseUnavailable
        }
        defer { sqlite3_finalize(statement) }
        let result = sqlite3_step(statement)
        if result == SQLITE_DONE {
            return nil
        }
        guard result == SQLITE_ROW, sqlite3_column_type(statement, 0) == SQLITE_BLOB,
              sqlite3_column_bytes(statement, 0) <= 131_072,
              let bytes = sqlite3_column_blob(statement, 0)
        else { throw MonitorAutostartError.invalidControl }
        return try ProgressHistory(
            schemaVersion: 1,
            samples: decode(Data(bytes: bytes, count: Int(sqlite3_column_bytes(statement, 0))))
        )
    }

    private func write(_ input: [ProgressSample], database: OpaquePointer) throws {
        var samples = input
        var data = try JSONEncoder().encode(ProgressHistory(schemaVersion: 1, samples: samples))
        while data.count > 131_072, !samples.isEmpty {
            samples.removeFirst()
            data = try JSONEncoder().encode(ProgressHistory(schemaVersion: 1, samples: samples))
        }
        var statement: OpaquePointer?
        let sql = """
        INSERT INTO monitor_progress_history(singleton, payload, updated_at) VALUES(1, ?, ?)
        ON CONFLICT(singleton) DO UPDATE SET payload = excluded.payload, updated_at = excluded.updated_at
        """
        guard sqlite3_prepare_v2(database, sql, -1, &statement, nil) == SQLITE_OK, let statement else {
            throw MonitorAutostartError.databaseUnavailable
        }
        defer { sqlite3_finalize(statement) }
        try data.withUnsafeBytes { bytes in
            guard sqlite3_bind_blob(statement, 1, bytes.baseAddress, Int32(bytes.count), nil) == SQLITE_OK,
                  sqlite3_bind_int64(statement, 2, Int64(Date().timeIntervalSince1970 * 1000)) == SQLITE_OK,
                  sqlite3_step(statement) == SQLITE_DONE
            else { throw MonitorAutostartError.databaseUnavailable }
        }
    }

    private func execute(_ sql: String, database: OpaquePointer) throws {
        guard sqlite3_exec(database, sql, nil, nil, nil) == SQLITE_OK else {
            throw MonitorAutostartError.databaseUnavailable
        }
    }

    private func withDatabase<T>(writable: Bool, operation: (OpaquePointer) throws -> T) throws -> T {
        var database: OpaquePointer?
        let flags = writable ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY
        let result = sqlite3_open_v2(stateDirectory.appending(path: "ledger.sqlite3").path, &database, flags, nil)
        guard let database else { throw MonitorAutostartError.databaseUnavailable }
        defer { sqlite3_close(database) }
        guard result == SQLITE_OK, sqlite3_busy_timeout(database, 250) == SQLITE_OK else {
            throw MonitorAutostartError.databaseUnavailable
        }
        if writable {
            try execute("PRAGMA synchronous = FULL", database: database)
        }
        return try operation(database)
    }
}
