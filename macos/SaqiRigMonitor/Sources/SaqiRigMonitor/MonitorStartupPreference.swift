import Darwin
import Foundation
import OSLog

internal struct MonitorStartupPreference {
    static let disabledFile = "MONITOR_AUTOSTART_DISABLED"
    static let enabledFile = "MONITOR_AUTOSTART_ENABLED"
    static let launchAgentLabel = "net.saqi.monitor"

    let executableURL: URL
    let launchAgentURL: URL
    let stateDirectory: URL

    var isEnabled: Bool {
        (try? controlStore.read()) == .enabled && installedLaunchAgentMatches()
    }

    private var controlStore: MonitorAutostartStore {
        MonitorAutostartStore(stateDirectory: stateDirectory)
    }

    private var disabledURL: URL {
        stateDirectory.appending(path: Self.disabledFile)
    }

    private var enabledURL: URL {
        stateDirectory.appending(path: Self.enabledFile)
    }

    private var launchAgent: [String: Any] {
        [
            "Label": Self.launchAgentLabel,
            "ProcessType": "Interactive",
            "ProgramArguments": [executableURL.path],
            "RunAtLoad": true,
            "EnvironmentVariables": ["PATH": MonitorProcessEnvironment.executableSearchPath],
        ]
    }

    init(
        stateDirectory: URL,
        executableURL: URL = Bundle.main.executableURL
            ?? URL(fileURLWithPath: CommandLine.arguments.first ?? "SaqiRigMonitor"),
        launchAgentURL: URL = FileManager.default.homeDirectoryForCurrentUser
            .appending(path: "Library/LaunchAgents/\(Self.launchAgentLabel).plist")
    ) {
        self.executableURL = executableURL.standardizedFileURL
        self.launchAgentURL = launchAgentURL.standardizedFileURL
        self.stateDirectory = stateDirectory.standardizedFileURL
    }

    func applyDefault() throws -> Bool {
        let enabled = try controlStore.importLegacy()
        try applyProjection(enabled)
        return isEnabled
    }

    func readVerified() throws -> Bool {
        switch try controlStore.read() {
        case .uninitialized:
            throw MonitorAutostartError.invalidControl
        case .enabled:
            guard installedLaunchAgentMatches() else { throw MonitorAutostartError.invalidControl }
            return true
        case .disabled:
            do {
                _ = try FileManager.default.attributesOfItem(atPath: launchAgentURL.path)
            } catch let error as NSError where error.domain == NSCocoaErrorDomain
                && [NSFileNoSuchFileError, NSFileReadNoSuchFileError].contains(error.code)
            {
                return false
            }
            throw MonitorAutostartError.invalidControl
        }
    }

    func setEnabled(_ enabled: Bool) throws {
        try controlStore.setEnabled(enabled)
        try applyProjection(enabled)
    }

    private func applyProjection(_ enabled: Bool) throws {
        if enabled {
            try installLaunchAgent()
        } else {
            try removeIfPresent(launchAgentURL)
        }
        // The launch agent is the operative projection. Rollback-only files
        // must not veto it after SQLite has committed the user's preference.
        do {
            if enabled {
                try removeIfPresent(disabledURL)
                try writeSentinel(enabledURL)
            } else {
                try writeSentinel(disabledURL)
                try removeIfPresent(enabledURL)
            }
        } catch {
            Logger(subsystem: "net.saqi.monitor", category: "startup")
                .warning("Monitor preference saved; legacy rollback mirror unavailable")
        }
    }

    private func installedLaunchAgentMatches() -> Bool {
        guard
            let data = try? Data(contentsOf: launchAgentURL),
            let value = try? PropertyListSerialization.propertyList(from: data, format: nil),
            let dictionary = value as? [String: Any],
            dictionary["Label"] as? String == Self.launchAgentLabel,
            dictionary["RunAtLoad"] as? Bool == true,
            dictionary["EnvironmentVariables"] as? [String: String]
            == ["PATH": MonitorProcessEnvironment.executableSearchPath],
            dictionary["ProgramArguments"] as? [String] == [executableURL.path]
        else { return false }
        return true
    }

    private func installLaunchAgent() throws {
        if installedLaunchAgentMatches() {
            return
        }
        let directory = launchAgentURL.deletingLastPathComponent()
        try FileManager.default.createDirectory(
            at: directory,
            withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700]
        )
        let data = try PropertyListSerialization.data(
            fromPropertyList: launchAgent,
            format: .xml,
            options: 0
        )
        try writeDurably(data, to: launchAgentURL, permissions: 0o600) { temporary, destination in
            // A same-directory rename replaces the projection without an unlink gap.
            guard Darwin.rename(temporary.path, destination.path) == 0 else {
                throw NSError(domain: NSPOSIXErrorDomain, code: Int(errno))
            }
        }
    }

    private func removeIfPresent(_ url: URL) throws {
        guard FileManager.default.fileExists(atPath: url.path) else { return }
        try FileManager.default.removeItem(at: url)
        try syncDirectory(url.deletingLastPathComponent())
    }

    private func writeSentinel(_ url: URL) throws {
        try FileManager.default.createDirectory(
            at: stateDirectory,
            withIntermediateDirectories: true,
            attributes: [.posixPermissions: 0o700]
        )
        if FileManager.default.fileExists(atPath: url.path) {
            return
        }
        try writeDurably(Data("enabled\n".utf8), to: url, permissions: 0o600) { temporary, destination in
            try FileManager.default.moveItem(at: temporary, to: destination)
        }
    }

    func writeDurably(
        _ data: Data,
        to url: URL,
        permissions: Int,
        publish: (URL, URL) throws -> Void
    ) throws {
        let directory = url.deletingLastPathComponent()
        let temporary = directory.appending(path: ".\(url.lastPathComponent).\(UUID().uuidString).tmp")
        guard
            FileManager.default.createFile(
                atPath: temporary.path,
                contents: data,
                attributes: [.posixPermissions: permissions]
            )
        else { throw CocoaError(.fileWriteUnknown) }
        var moved = false
        defer {
            if !moved {
                try? FileManager.default.removeItem(at: temporary)
            }
        }
        let handle = try FileHandle(forWritingTo: temporary)
        defer { try? handle.close() }
        try handle.synchronize()
        try publish(temporary, url)
        moved = true
        try syncDirectory(directory)
    }

    private func syncDirectory(_ url: URL) throws {
        let descriptor = Darwin.open(url.path, O_RDONLY)
        guard descriptor >= 0 else { throw CocoaError(.fileWriteUnknown) }
        defer { Darwin.close(descriptor) }
        guard Darwin.fsync(descriptor) == 0 else { throw CocoaError(.fileWriteUnknown) }
    }
}
