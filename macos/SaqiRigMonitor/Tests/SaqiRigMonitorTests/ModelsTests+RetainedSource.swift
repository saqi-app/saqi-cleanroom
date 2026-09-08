import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    @MainActor
    func testCoherentRetainedSourceKeepsNumericTelemetryAndDisablesUnsafeActions() throws {
        let now = Date()
        let store = presentationStore(entry: presentationEntry(budget: BudgetGate(
            budgetId: "fixture-budget",
            maximumOperations: 12,
            remainingOperations: 9,
            reservedOperations: 3,
            state: "active"
        )), now: now)
        store.service = retainedSourceService(
            state: "running_outdated",
            detail: "SOURCE_IDENTITY_UNVERIFIED",
            actions: ["start", "stop", "restart"]
        )
        let snapshot = try XCTUnwrap(store.service)
        XCTAssertTrue(try XCTUnwrap(store.runtime).matches(service: snapshot))
        let presentation = try XCTUnwrap(store.codexExecutionPresentation(now: now))
        XCTAssertEqual(presentation.progress, "5 accepted · 4 ready · 2 delayed · 1 terminal")
        XCTAssertEqual(presentation.concurrency, "1 active · selected 2 · ceiling 4")
        XCTAssertEqual(presentation.budget, "Budget · 9 of 12 operations available · 3 reserved")
        XCTAssertEqual(snapshot.sourceIdentityStatus, .retainedUnverified)
        XCTAssertEqual(store.operationalStatus.label, "Source identity unverified")
        XCTAssertFalse(snapshot.allows("start"))
        XCTAssertFalse(snapshot.allows("restart"))
        XCTAssertTrue(snapshot.allows("stop"))
    }

    @MainActor
    func testRetainedSourceExplainsStatusETAAndRateWithoutAskingForRestart() {
        let store = presentationStore(entry: presentationEntry(), now: Date())
        store.service = retainedSourceService(state: "running_outdated", detail: "SOURCE_IDENTITY_UNVERIFIED")
        store.health = nil
        store.runtime = nil
        XCTAssertEqual(store.operationalStatus.label, "Source identity unverified")
        XCTAssertEqual(store.eta, "ETA waiting for source identity verification")
        XCTAssertEqual(store.rateLabel, "Rate unavailable · source identity unverified")
        XCTAssertFalse(store.service?.allows("restart") ?? true)
    }

    @MainActor
    func testRetainedSourceDoesNotHideStrongerOperationalFailures() {
        let store = presentationStore(entry: presentationEntry(), now: Date())
        store.health = nil
        store.runtime = nil
        store.service = retainedSourceService(state: "fenced", detail: "SERVICE_PID_MISMATCH")
        XCTAssertEqual(store.operationalStatus.label, "Fenced · attention needed")
        XCTAssertEqual(store.eta, "ETA blocked")
        XCTAssertEqual(store.rateLabel, "Rate unavailable · blocked")
        store.service = retainedSourceService(state: "stopped", detail: nil)
        XCTAssertEqual(store.operationalStatus.label, "Stopped")
        XCTAssertEqual(store.eta, "ETA stopped")
        XCTAssertEqual(store.rateLabel, "Rate unavailable · stopped")
        store.service = retainedSourceService(state: "running_outdated", detail: "SERVICE_CONFIG_MISMATCH")
        XCTAssertEqual(store.operationalStatus.label, "Running · restart needed")
        XCTAssertEqual(store.eta, "ETA restart required")
        XCTAssertEqual(store.rateLabel, "Rate unavailable · restart required")
    }

    private func retainedSourceService(
        state: String,
        detail: String?,
        actions: [String] = ["stop"]
    ) -> ServiceSnapshot {
        let base = service(actualState: state, actions: actions)
        return ServiceSnapshot(
            sourceIdentityStatus: .retainedUnverified,
            action: base.action,
            actualState: base.actualState,
            allowedActions: base.allowedActions,
            configDigest: base.configDigest,
            desiredConfigDigest: base.desiredConfigDigest,
            detail: detail,
            loadedConfigDigest: base.loadedConfigDigest,
            observedAt: base.observedAt,
            ownerPid: base.ownerPid,
            runId: base.runId,
            schemaId: base.schemaId,
            schemaVersion: base.schemaVersion,
            serviceEnabled: base.serviceEnabled
        )
    }
}
