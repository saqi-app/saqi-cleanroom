import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    @MainActor
    func testAggregateTelemetryRetainsCountersWithHonestProvenance() throws {
        let now = Date()
        let store = presentationStore(entry: presentationEntry(), now: now)
        let health = try XCTUnwrap(store.health)
        XCTAssertNil(store.aggregateTelemetryLabel(now: now))

        store.healthRefreshFailed = true
        XCTAssertEqual(store.aggregateTelemetryLabel(now: now), "Last known · 1 minute ago")
        XCTAssertEqual(store.health, health)

        store.healthRefreshFailed = false
        XCTAssertEqual(
            store.aggregateTelemetryLabel(now: now.addingTimeInterval(15 * 60 + 30)),
            "Last known · 16 minutes ago"
        )
        store.service = nil
        XCTAssertEqual(store.aggregateTelemetryLabel(now: now), "Last known · 1 minute ago")
    }

    @MainActor
    func testAggregateTelemetryLabelsConfigurationMismatch() {
        let now = Date()
        let store = presentationStore(entry: presentationEntry(), now: now)
        store.health = validHealth(observedAt: Int(now.timeIntervalSince1970 * 1000), runId: "process:999")
        XCTAssertEqual(store.aggregateTelemetryLabel(now: now), "Different configuration · 1 minute ago")
    }

    func testKnownETAWaitDoesNotRequireThroughputCounters() {
        for (reason, label) in [
            "budget_exhausted": "ETA waiting for translation budget",
            "auth_wait": "ETA waiting for Sol sign-in",
            "paid_work_paused": "ETA paused",
        ] {
            let provider = presentationEntry(admission: ProviderExecutionAdmission(
                operatorAction: "none",
                primaryReason: reason,
                recovery: "operator",
                retryAt: nil,
                state: "waiting"
            ))
            XCTAssertNil(provider.throughput)
            XCTAssertEqual(ProgressEstimator.eta(provider: provider), label)
        }
    }
}
