import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    @MainActor
    func testCodexConcurrencyChoicesMatchRuntimeContract() {
        let provider = RigStore.codexProvider
        let choices = RigStore.concurrencyChoices
        XCTAssertEqual(provider, "sol")
        XCTAssertEqual(choices, [2, 4, 8, 16, 32, 128, 256])
    }

    @MainActor
    func testRetiredProvidersCannotSurfaceOrMutateExecutableControls() throws {
        let fixture = try codexControlFixture()
        defer { try? FileManager.default.removeItem(at: fixture.root) }
        fixture.store.health = validHealth(providers: [
            provider(key: "retired-v1", provider: "retired"),
            provider(key: "sol-5.6", provider: "sol"),
        ])
        fixture.store.requestedConcurrency = ["retired": 16, "sol": 4]
        fixture.store.concurrencyApplicationState = ["retired": "applied", "sol": "applying"]

        XCTAssertEqual(fixture.store.codexEnrichmentProviders.map(\.provider), ["sol"])
        XCTAssertEqual(fixture.store.concurrencyDiagnostics, "codex=4 (applying)")

        fixture.store.setConcurrency("retired", 8)

        XCTAssertEqual(fixture.store.phase, .failed("Unsupported provider"))
        XCTAssertEqual(fixture.store.requestedConcurrency["retired"], 16)
    }

    @MainActor
    func testCodexControlLabelAndHelpCopyUseCodexConcept() {
        let help = RigStore.codexConcurrencyHelp
        XCTAssertEqual(
            RigStore.concurrencyControlLabel(
                provider: "Codex",
                configured: 2,
                requested: 4,
                state: "restart_failed"
            ),
            "Codex target 4 desired · current 2"
        )
        XCTAssertEqual(
            help,
            "Set Codex translation concurrency. Safety gates still pause new paid work "
                + "when credentials, quota, budget, or host resources are unavailable."
        )
    }

    @MainActor
    private func codexControlFixture() throws -> (root: URL, store: RigStore) {
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
        return (root, store)
    }
}
