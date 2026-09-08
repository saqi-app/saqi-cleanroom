import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    func testRuntimeStatusReadsCurrentProviderExecutionHealth() throws {
        let fixture = try providerExecutionFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }
        try writeRuntimeStatus(
            at: fixture.root,
            observedAt: fixture.now,
            providerExecution: validProviderExecution(observedAt: fixture.observedAt)
        )

        let status = try RigStore.readRuntimeStatus(configuration: fixture.configuration, now: fixture.now)

        let provider = try XCTUnwrap(status.providerExecution?.providers.first)
        XCTAssertEqual(provider.provider, "sol")
        XCTAssertEqual(provider.admission.primaryReason, "active")
        XCTAssertEqual(provider.credentials.accountEpoch, 2)
        XCTAssertEqual(provider.credentials.materialEpoch, 3)
        XCTAssertEqual(provider.sessions.activeCurrentAccountEpoch, 1)
    }

    func testRuntimeStatusWithoutProviderExecutionUsesLegacyFallback() throws {
        let fixture = try providerExecutionFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }
        try writeRuntimeStatus(at: fixture.root, observedAt: fixture.now)

        let status = try RigStore.readRuntimeStatus(configuration: fixture.configuration, now: fixture.now)

        XCTAssertNil(status.providerExecution)
    }

    func testRuntimeStatusRejectsStaleDocumentWithoutProviderExecution() throws {
        let fixture = try providerExecutionFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }
        try writeRuntimeStatus(
            at: fixture.root,
            observedAt: fixture.now.addingTimeInterval(-16 * 60)
        )

        XCTAssertThrowsError(
            try RigStore.readRuntimeStatus(configuration: fixture.configuration, now: fixture.now)
        )
    }

    func testRuntimeStatusRejectsMalformedProviderExecutionCounts() throws {
        let fixture = try providerExecutionFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }
        var providerExecution = validProviderExecution(observedAt: fixture.observedAt)
        var providers = try XCTUnwrap(providerExecution["providers"] as? [[String: Any]])
        var provider = try XCTUnwrap(providers.first)
        provider["sessions"] = [
            "activeCurrentAccountEpoch": 0,
            "activePreviousAccountEpoch": 0,
            "activeUnattributed": 0,
        ]
        providers[0] = provider
        providerExecution["providers"] = providers
        try writeRuntimeStatus(at: fixture.root, observedAt: fixture.now, providerExecution: providerExecution)

        XCTAssertThrowsError(
            try RigStore.readRuntimeStatus(configuration: fixture.configuration, now: fixture.now)
        )
    }

    func testRuntimeStatusReadsAndValidatesPoemThroughput() throws {
        let fixture = try providerExecutionFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }
        var providerExecution = validProviderExecution(observedAt: fixture.observedAt)
        var providers = try XCTUnwrap(providerExecution["providers"] as? [[String: Any]])
        var provider = try XCTUnwrap(providers.first)
        provider["throughput"] = validPoemThroughput()
        providers[0] = provider
        providerExecution["providers"] = providers
        try writeRuntimeStatus(at: fixture.root, observedAt: fixture.now, providerExecution: providerExecution)

        let status = try RigStore.readRuntimeStatus(configuration: fixture.configuration, now: fixture.now)
        XCTAssertEqual(status.providerExecution?.providers.first?.throughput?.published.last1h, 12)

        var invalid = validPoemThroughput()
        var remaining = try XCTUnwrap(invalid["remaining"] as? [String: Any])
        remaining["generation"] = 99
        invalid["remaining"] = remaining
        provider["throughput"] = invalid
        providers[0] = provider
        providerExecution["providers"] = providers
        try writeRuntimeStatus(at: fixture.root, observedAt: fixture.now, providerExecution: providerExecution)
        XCTAssertThrowsError(
            try RigStore.readRuntimeStatus(configuration: fixture.configuration, now: fixture.now)
        )
    }

    func testRuntimeStatusRejectsPrivateCredentialMaterial() throws {
        let fixture = try providerExecutionFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }
        var providerExecution = validProviderExecution(observedAt: fixture.observedAt)
        var providers = try XCTUnwrap(providerExecution["providers"] as? [[String: Any]])
        var provider = try XCTUnwrap(providers.first)
        var credentials = try XCTUnwrap(provider["credentials"] as? [String: Any])
        credentials["accountGeneration"] = String(repeating: "f", count: 64)
        provider["credentials"] = credentials
        providers[0] = provider
        providerExecution["providers"] = providers
        try writeRuntimeStatus(at: fixture.root, observedAt: fixture.now, providerExecution: providerExecution)

        XCTAssertThrowsError(
            try RigStore.readRuntimeStatus(configuration: fixture.configuration, now: fixture.now)
        )
    }

    func testRuntimeStatusRejectsStaleProviderExecutionHealth() throws {
        let fixture = try providerExecutionFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }
        let staleObservedAt = fixture.observedAt - 16 * 60 * 1000
        try writeRuntimeStatus(
            at: fixture.root,
            observedAt: fixture.now,
            providerExecution: validProviderExecution(observedAt: staleObservedAt)
        )

        XCTAssertThrowsError(
            try RigStore.readRuntimeStatus(configuration: fixture.configuration, now: fixture.now)
        )
    }
}

extension ModelsTests {
    private func providerExecutionFixture() throws -> ProviderExecutionFixture {
        let root = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        try FileManager.default.createDirectory(at: root, withIntermediateDirectories: true)
        let now = Date(timeIntervalSince1970: 1_000_000)
        return ProviderExecutionFixture(
            configuration: MonitorConfiguration(
                crawlerCLI: root.appending(path: "cli.js"),
                rigConfig: root.appending(path: "rig.json"),
                serviceLabel: "net.saqi.test",
                stateDirectory: root
            ),
            now: now,
            observedAt: Int(now.timeIntervalSince1970 * 1000) - 1000,
            root: root
        )
    }

    private func writeRuntimeStatus(
        at root: URL,
        observedAt: Date,
        providerExecution: [String: Any] = [:]
    ) throws {
        let formatter = ISO8601DateFormatter()
        formatter.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        var document: [String: Any] = [
            "configDigest": String(repeating: "a", count: 64),
            "observedAt": formatter.string(from: observedAt),
            "ownerPid": 123,
            "resourcePressure": ["nextProbeAt": 0, "reasons": [], "state": "ready"],
            "runId": "run:123",
        ]
        if !providerExecution.isEmpty {
            document["workload"] = ["providerExecution": providerExecution]
        }
        let data = try JSONSerialization.data(withJSONObject: document, options: [.sortedKeys])
        try data.write(to: root.appending(path: "status.json"))
    }

    private func validProviderExecution(observedAt: Int) -> [String: Any] {
        [
            "configDigest": String(repeating: "a", count: 64),
            "observedAt": observedAt,
            "providers": [validProviderExecutionEntry(observedAt: observedAt)],
            "runId": "provider-run",
            "schemaId": "saqi.provider-execution-health",
            "schemaVersion": 1,
        ]
    }

    private func validProviderExecutionEntry(observedAt: Int) -> [String: Any] {
        [
            "admission": [
                "operatorAction": "none",
                "primaryReason": "active",
                "recovery": "none",
                "retryAt": NSNull(),
                "state": "open",
            ],
            "credentials": validCredentials(observedAt: observedAt),
            "debt": [
                "quarantinedOperations": 0,
                "recoverableUnknownOperations": 0,
                "semanticFailures": 0,
            ],
            "enabled": true,
            "gates": validProviderGates(),
            "model": "gpt-5.6-sol",
            "modelKey": "sol-5.6",
            "progress": [
                "accepted": 5,
                "activeInvocations": 1,
                "delayedWork": 0,
                "lastAcceptedAt": observedAt - 1000,
                "readyWork": 4,
                "state": "active",
                "terminalWork": 0,
            ],
            "provider": "sol",
            "sessions": [
                "activeCurrentAccountEpoch": 1,
                "activePreviousAccountEpoch": 0,
                "activeUnattributed": 0,
            ],
        ]
    }

    private func validCredentials(observedAt: Int) -> [String: Any] {
        [
            "accountEpoch": 2,
            "change": "none",
            "changedAt": NSNull(),
            "errorCode": NSNull(),
            "lastVerifiedAt": observedAt - 1000,
            "materialEpoch": 3,
            "retryAt": NSNull(),
            "state": "ready",
        ]
    }

    private func validProviderGates() -> [String: Any] {
        [
            "authentication": ["errorCode": NSNull(), "retryAt": NSNull(), "state": "ready"],
            "budget": [
                "budgetId": NSNull(),
                "maximumOperations": 0,
                "remainingOperations": 0,
                "reservedOperations": 0,
                "state": "unarmed",
            ],
            "operator": ["globalPaused": false, "paidWorkPaused": false],
            "provider": ["errorCode": NSNull(), "retryAt": NSNull(), "state": "ready"],
            "quota": [
                "errorCode": NSNull(),
                "nextProbeAt": NSNull(),
                "retryAt": NSNull(),
                "state": "clear",
            ],
            "resources": ["nextProbeAt": NSNull(), "reasons": [], "state": "ready"],
            "scheduler": [
                "activeInvocations": 1,
                "configuredConcurrency": 8,
                "nextWakeAt": NSNull(),
                "selectedConcurrency": 8,
                "state": "ready",
            ],
        ]
    }

    private func validPoemThroughput() -> [String: Any] {
        [
            "coverage": ["backfillComplete": true, "highWatermark": 100, "state": "complete"],
            "generated": ["last15m": 4, "last1h": 15, "last5m": 1, "lastAt": 999_000],
            "published": ["last15m": 3, "last1h": 12, "last5m": 1, "lastAt": 999_000],
            "remaining": [
                "active": 1,
                "delayed": 2,
                "endToEndPublication": 17,
                "generatedAwaitingPublication": 10,
                "generation": 7,
                "ready": 4,
                "terminalDead": 0,
            ],
        ]
    }
}

private struct ProviderExecutionFixture {
    let configuration: MonitorConfiguration
    let now: Date
    let observedAt: Int
    let root: URL
}
