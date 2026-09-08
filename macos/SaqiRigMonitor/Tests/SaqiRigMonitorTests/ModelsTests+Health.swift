import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    func testMonitorStartupDefaultsOnAndPersistsOptOut() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "SaqiRigMonitor"),
            launchAgentURL: root.appending(path: "net.saqi.monitor.plist")
        )

        XCTAssertTrue(try preference.applyDefault())
        XCTAssertTrue(preference.isEnabled)
        try preference.setEnabled(false)
        XCTAssertFalse(preference.isEnabled)
        XCTAssertFalse(try preference.applyDefault())
        XCTAssertFalse(preference.isEnabled)
        try preference.setEnabled(true)
        XCTAssertTrue(preference.isEnabled)
    }

    func testMonitorStartupPersistentOptOutRepairsContradictorySentinels() throws {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: root) }
        try createStartupLedger(root)
        let preference = MonitorStartupPreference(
            stateDirectory: root,
            executableURL: root.appending(path: "SaqiRigMonitor"),
            launchAgentURL: root.appending(path: "net.saqi.monitor.plist")
        )

        try Data("enabled\n".utf8).write(to: root.appending(path: MonitorStartupPreference.enabledFile))
        let disabled = root.appending(path: MonitorStartupPreference.disabledFile)
        try Data("disabled\n".utf8).write(to: disabled)

        XCTAssertFalse(try preference.applyDefault())
        XCTAssertFalse(preference.isEnabled)
        XCTAssertFalse(
            FileManager.default.fileExists(
                atPath: root.appending(path: MonitorStartupPreference.enabledFile).path
            )
        )
    }

    func testQueueRemainingIncludesEveryAutomatableState() {
        let queue = QueueHealth(
            active: 2,
            deadLetter: 7,
            kind: "poem-enrichment-sol",
            lastSuccessAt: nil,
            pending: 10,
            quotaWait: 3,
            retryWait: 4,
            succeeded: 5,
            total: 31
        )
        XCTAssertEqual(queue.remaining, 19)
    }

    func testConfigurationHonorsExplicitEnvironment() {
        let value = MonitorConfiguration.current(environment: [
            "SAQI_CRAWLER_CLI": "/tmp/saqi-cli",
            "SAQI_RIG_CONFIG": "/tmp/saqi-rig.json",
            "SAQI_SERVICE_LABEL": "net.saqi.test",
            "SAQI_STATE_DIR": "/tmp/saqi-state",
        ])
        XCTAssertEqual(value.crawlerCLI.path, "/tmp/saqi-cli")
        XCTAssertEqual(value.rigConfig.path, "/tmp/saqi-rig.json")
        XCTAssertEqual(value.serviceLabel, "net.saqi.test")
        XCTAssertEqual(value.stateDirectory.path, "/tmp/saqi-state")
    }

    func testConfigurationDefaultsToImmutableInstalledCLI() {
        let value = MonitorConfiguration.current(environment: [:])
        XCTAssertTrue(
            value.crawlerCLI.path.hasSuffix(
                "Library/Application Support/Saqi/current/typescript/packages/crawler-local/dist/cli.js"
            )
        )
    }

    func testHealthRejectsUnknownSchema() throws {
        let value = validHealth(schemaVersion: 2)
        XCTAssertThrowsError(try value.validate())
    }

    func testHealthRejectsUnknownPipelineState() throws {
        let health = PipelineHealth(
            checks: [],
            schemaId: "saqi.pipeline-health",
            schemaVersion: 1,
            configDigest: String(repeating: "a", count: 64),
            growth: nil,
            heartbeatIntervalMs: nil,
            lastProgressAt: nil,
            observedAt: 1,
            origins: [],
            providers: [],
            queues: [],
            runId: "run",
            sol: nil,
            state: "guessing"
        )
        XCTAssertThrowsError(try health.validate())
    }

    func testHealthRejectsDuplicateQueueKinds() throws {
        let duplicate = queue(kind: "duplicate", pending: 1)
        XCTAssertThrowsError(try validHealth(queues: [duplicate, duplicate]).validate())
    }

    func testHealthRejectsImpossibleQueueCounts() throws {
        let queue = QueueHealth(
            active: 1,
            deadLetter: 0,
            kind: "test",
            lastSuccessAt: nil,
            pending: 1,
            quotaWait: 0,
            retryWait: 0,
            succeeded: 1,
            total: 2
        )
        XCTAssertThrowsError(try validHealth(queues: [queue]).validate())
    }

    func testHealthAcceptsKnownConsistentSchema() throws {
        XCTAssertNoThrow(try validHealth().validate())
    }

    func testHealthAcceptsMaximumWidgetConcurrency() throws {
        let value = ProviderHealth(
            accepted: 0,
            activeInvocations: 0,
            blockReason: nil,
            invocationConcurrency: 256,
            lastDisposition: "idle",
            model: "sol-5.6",
            modelKey: "sol-5.6",
            nextQuotaProbeAt: nil,
            provider: "sol",
            providerErrorCode: nil,
            retryAt: nil,
            selectedConcurrency: 256,
            semanticFailures: 0,
            unknownOperations: 0
        )
        XCTAssertNoThrow(try validHealth(providers: [value]).validate())
    }

    func testConcurrencyLabelsSeparateRuntimeDesiredAndUnknownOutcome() {
        XCTAssertEqual(
            RigStore.concurrencyControlLabel(
                provider: "sol",
                configured: 128,
                requested: nil,
                state: nil
            ),
            "Sol target 128"
        )
        XCTAssertEqual(
            RigStore.concurrencyControlLabel(
                provider: "sol",
                configured: 8,
                requested: 16,
                state: "restart_failed"
            ),
            "Sol target 16 desired · current 8"
        )
        XCTAssertEqual(
            RigStore.concurrencyControlLabel(
                provider: "Codex",
                configured: 8,
                requested: 32,
                state: "outcome_unknown"
            ),
            "Codex target 32 · outcome unknown"
        )
    }

    func testProviderActivityLabelCannotConfuseActiveTargetAndMaximum() {
        let value = ProviderHealth(
            accepted: 0,
            activeInvocations: 2,
            blockReason: "at_capacity",
            invocationConcurrency: 128,
            lastDisposition: "accepted",
            model: "sol-5.6",
            modelKey: "sol-5.6",
            nextQuotaProbeAt: nil,
            provider: "sol",
            providerErrorCode: nil,
            retryAt: nil,
            selectedConcurrency: 2,
            semanticFailures: 0,
            unknownOperations: 0
        )

        XCTAssertEqual(value.activityLabel(), "2 active · selected 2 · target 128")
    }

    func testFixedProviderActivityLabelShowsTargetWithoutMaximum() {
        let value = ProviderHealth(
            accepted: 0,
            activeInvocations: 36,
            blockReason: nil,
            invocationConcurrency: 128,
            lastDisposition: "accepted",
            model: "sol-5.6",
            modelKey: "sol-5.6",
            nextQuotaProbeAt: nil,
            provider: "sol",
            providerErrorCode: nil,
            retryAt: nil,
            selectedConcurrency: 128,
            semanticFailures: 0,
            unknownOperations: 0
        )

        XCTAssertEqual(value.activityLabel(), "36 active · selected 128 · target 128")
    }

    func testHostAdmissionLabelIsExplicitlySharedAndValidated() throws {
        let host = ProviderHostAdmissionHealth(
            activeProcesses: 31,
            maximumProcesses: 64,
            remainingProcesses: 33
        )
        XCTAssertTrue(host.isValid)
        XCTAssertEqual(host.label, "Host · 31 active · 64 shared capacity")
        XCTAssertFalse(
            ProviderHostAdmissionHealth(
                activeProcesses: 31,
                maximumProcesses: 64,
                remainingProcesses: 34
            ).isValid
        )
        XCTAssertNoThrow(try validHealth(providerHostAdmission: host).validate())
    }

    func testOnlyProvenPreMutationErrorsClearOptimisticConcurrency() {
        let busy = NSError(
            domain: "test",
            code: 1,
            userInfo: [NSLocalizedDescriptionKey: "LAUNCHD_CONTROL_BUSY: pid 123"]
        )
        let lost = NSError(
            domain: "test",
            code: 2,
            userInfo: [NSLocalizedDescriptionKey: "connection lost after save"]
        )
        XCTAssertTrue(RigStore.isProvenPreMutationFailure(busy))
        XCTAssertFalse(RigStore.isProvenPreMutationFailure(lost))
    }

    func testHealthyChecksDoNotRequireAttention() {
        let check = HealthCheck(code: "AUTH_READY", detail: nil, retryAt: nil, state: "ok")
        XCTAssertFalse(check.requiresAttention)
    }

    func testBlockedCheckTakesPriorityOverWarning() {
        let warning = HealthCheck(code: "WARNING", detail: nil, retryAt: nil, state: "warning")
        let blocked = HealthCheck(code: "BLOCKED", detail: nil, retryAt: nil, state: "blocked")
        let health = PipelineHealth(
            checks: [warning, blocked],
            schemaId: "saqi.pipeline-health",
            schemaVersion: 1,
            configDigest: String(repeating: "a", count: 64),
            growth: nil,
            heartbeatIntervalMs: nil,
            lastProgressAt: nil,
            observedAt: 1,
            origins: [],
            providers: [],
            queues: [],
            runId: "run",
            sol: nil,
            state: "blocked"
        )
        XCTAssertEqual(health.attentionCheck, blocked)
    }

    func testHealthAcceptsNullableCheckDetailFromPipelineContract() throws {
        let document = #"""
        {"checks":[{"code":"AUTH_READY","detail":null,"retryAt":null,"state":"ok"}],
        "schemaId":"saqi.pipeline-health","schemaVersion":1,
        "configDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "lastProgressAt":null,"observedAt":1,"providers":[],"queues":[],
        "runId":"run","sol":null,"state":"healthy"}
        """#
        let health = try JSONDecoder().decode(PipelineHealth.self, from: Data(document.utf8))
        XCTAssertNoThrow(try health.validate())
        XCTAssertNil(health.checks.first?.detail)
    }

    func testProductionDisabledCannotBeReportedAsVerified() throws {
        let document = #"""
        {"checks":[],"schemaId":"saqi.pipeline-health","schemaVersion":1,
        "configDigest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "growth":{"production":{"configured":false,"lastSuccessAt":null,"state":"disabled"}},
        "lastProgressAt":1,"observedAt":1,"providers":[],"queues":[],
        "runId":"run","sol":null,"state":"healthy"}
        """#
        let health = try JSONDecoder().decode(PipelineHealth.self, from: Data(document.utf8))
        XCTAssertFalse(health.productionOperational)
        XCTAssertEqual(health.productionLabel, "Production · Off")
    }

    func testProductionGrowthMapsEveryPipelineStateWithoutFalseGreen() {
        let cases = [
            ("active", "Production · Publishing", true),
            ("ready", "Production · Ready", true),
            ("gated", "Production · Gated", false),
            ("stalled", "Production · Stalled", false),
            ("disabled", "Production · Off", false),
        ]
        for (state, label, active) in cases {
            let production = ProductionGrowth(
                configured: state != "disabled",
                lastSuccessAt: state == "active" ? 1 : nil,
                state: state
            )
            XCTAssertEqual(production.label, label)
            XCTAssertEqual(production.isOperational, active)
        }
    }

    func testHealthExposesRejectedDuplicateIntegrityCheck() {
        let check = HealthCheck(
            code: "SOURCE_POEM_DUPLICATE",
            detail: "one canonical conflict rejected",
            retryAt: nil,
            state: "blocked"
        )
        let health = PipelineHealth(
            checks: [check],
            schemaId: "saqi.pipeline-health",
            schemaVersion: 1,
            configDigest: String(repeating: "a", count: 64),
            growth: nil,
            heartbeatIntervalMs: nil,
            lastProgressAt: nil,
            observedAt: 1,
            origins: [],
            providers: [],
            queues: [],
            runId: "run",
            sol: nil,
            state: "blocked"
        )
        XCTAssertEqual(health.duplicateConflict, check)
    }

    func testServiceSnapshotAllowsOnlyBackendActions() throws {
        let value = service(actualState: "running_outdated", actions: ["restart", "status", "stop"])
        XCTAssertNoThrow(try value.validate())
        XCTAssertTrue(value.allows("restart"))
        XCTAssertFalse(value.allows("start"))
    }

    func testServiceSnapshotRejectsUnknownState() throws {
        XCTAssertThrowsError(try service(actualState: "guessing", actions: []).validate())
    }

    func testServiceSnapshotAcceptsOwnedStartup() throws {
        let value = service(actualState: "starting", actions: ["restart", "status", "stop"])
        XCTAssertNoThrow(try value.validate())
        XCTAssertTrue(value.allows("stop"))
        XCTAssertFalse(value.allows("start"))
    }
}
