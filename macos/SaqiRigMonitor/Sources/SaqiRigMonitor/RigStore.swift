import AppKit
import Foundation

@MainActor
internal final class RigStore: ObservableObject {
    struct CoherentCodexTelemetry: Equatable {
        let execution: ProviderExecutionHealth
        let runtime: RuntimeStatus
        let service: ServiceSnapshot
    }

    static let codexProvider = "sol"
    static let codexConcurrencyHelp = "Set Codex translation concurrency. Safety gates still pause new paid work "
        + "when credentials, quota, budget, or host resources are unavailable."
    static let concurrencyChoices = [2, 4, 8, 16, 32, 128, 256]

    @Published var health: PipelineHealth?
    @Published var providerExecutionHealth: ProviderExecutionHealth?
    @Published var runtime: RuntimeStatus?
    @Published var service: ServiceSnapshot?
    @Published var paused = false
    @Published var phase: ControlPhase = .idle
    @Published var diagnosticError: String?
    @Published var startupPreferenceError: String?
    @Published var activeRefreshes = 0
    @Published var refreshingDiagnostics = false
    @Published var refreshOutcome: String?
    @Published var refreshedAt: Date?
    @Published var startsAtLogin = false
    @Published var requestedConcurrency: [String: Int] = [:]
    @Published var concurrencyApplicationState: [String: String] = [:]

    let configuration: MonitorConfiguration
    var samples: [ProgressSample] = []
    var controlTask: Task<Void, Never>?
    var monitorTask: Task<Void, Never>?
    var refreshGeneration = 0
    var lastServiceRefresh: Date?
    var lastCoherentCodexTelemetry: CoherentCodexTelemetry?
    var healthRefreshFailed = false
    var runtimeRefreshFailed = false
    var serviceRefreshFailed = false
    let healthFileReader = DiagnosticFileReader<PipelineHealth>()
    let runtimeFileReader = DiagnosticFileReader<RuntimeStatus>()
    let startupPreferenceReader = DiagnosticFileReader<Bool>()
    let progressStore: MonitorProgressStore
    let historyPersistenceEnabled: Bool
    var pendingHistoryImport: UserDefaults?
    var nextHistoryImportAttempt = Date.distantPast
    var historyPersistenceFailed = false
    let startupPreference: MonitorStartupPreference

    init(
        configuration: MonitorConfiguration = .current(),
        userDefaults: UserDefaults = .standard,
        monitorContinuously: Bool = true,
        startupPreference: MonitorStartupPreference? = nil
    ) {
        self.configuration = configuration
        historyPersistenceEnabled = monitorContinuously
        progressStore = MonitorProgressStore(stateDirectory: configuration.stateDirectory)
        pendingHistoryImport = monitorContinuously ? userDefaults : nil
        self.startupPreference = startupPreference ?? MonitorStartupPreference(
            stateDirectory: configuration.stateDirectory
        )
        if monitorContinuously {
            do {
                startsAtLogin = try self.startupPreference.applyDefault()
            } catch {
                startupPreferenceError = "Monitor login preference unavailable"
                diagnosticError = "Monitor login preference unavailable"
            }
        } else {
            startsAtLogin = self.startupPreference.isEnabled
        }
        do {
            samples = try progressStore.load(legacyDefaults: monitorContinuously ? userDefaults : nil)
            pendingHistoryImport = nil
        } catch {
            if monitorContinuously {
                historyPersistenceFailed = true
                diagnosticError = diagnosticError ?? "Progress history persistence unavailable"
            }
        }
        if monitorContinuously {
            monitorTask = Task { [weak self] in
                while !Task.isCancelled {
                    guard let self else { return }
                    await self.refresh()
                    do { try await Task.sleep(for: .seconds(2)) } catch { return }
                }
            }
        }
    }

    deinit {
        controlTask?.cancel()
        monitorTask?.cancel()
    }
}
