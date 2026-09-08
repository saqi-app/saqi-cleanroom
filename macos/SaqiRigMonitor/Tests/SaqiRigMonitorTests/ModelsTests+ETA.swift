import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    @MainActor
    func testETAExplainsProviderOperatorBlockerBeforeProductionStall() {
        let cases = [
            ("budget_exhausted", "rearm_budget", "ETA waiting for translation budget"),
            ("budget_unarmed", "arm_budget", "ETA waiting for translation budget"),
            ("auth_wait", "restore_auth", "ETA waiting for Sol sign-in"),
        ]
        for (reason, action, expected) in cases {
            let now = Date()
            let entry = presentationEntry(admission: ProviderExecutionAdmission(
                operatorAction: action,
                primaryReason: reason,
                recovery: "operator",
                retryAt: nil,
                state: "blocked"
            ))
            let store = presentationStore(entry: entry, now: now)
            store.health = validHealth(
                queues: [queue(kind: "poem-enrichment-sol", pending: 4)],
                providers: [provider(key: "sol-5.6", provider: "sol")],
                observedAt: Int(now.timeIntervalSince1970 * 1000),
                runId: "process:1",
                growth: PipelineGrowth(
                    production: ProductionGrowth(configured: true, lastSuccessAt: nil, state: "stalled")
                )
            )
            XCTAssertEqual(store.eta, expected, reason)
            store.runtimeRefreshFailed = true
            XCTAssertEqual(store.eta, "ETA unavailable · production stalled", reason)
        }
    }

    func testETARejectsInfiniteRateAndUsesSubHourCopy() {
        let sol = provider(key: "sol-5.6", provider: "sol")
        let health = validHealth(
            queues: [queue(kind: sol.workKind, pending: 1, active: 1)],
            providers: [sol]
        )
        XCTAssertEqual(
            ProgressEstimator.eta(health: health, rates: ["sol-5.6": .infinity]),
            "ETA learning"
        )
        XCTAssertEqual(
            ProgressEstimator.eta(health: health, rates: ["sol-5.6": 100]),
            "<1 hour remaining"
        )
    }

    func testETARejectsStalledHistoricalLane() {
        let sol = provider(key: "sol-5.6", provider: "sol")
        let observedAt = 2_000_000
        let health = validHealth(
            queues: [queue(kind: sol.workKind, pending: 100, lastSuccessAt: 1)],
            providers: [sol],
            observedAt: observedAt
        )
        XCTAssertEqual(
            ProgressEstimator.eta(health: health, rates: ["sol-5.6": 100]),
            "ETA unavailable · idle"
        )
    }

    func testETANeverTreatsMissingQueueAsComplete() {
        let sol = provider(key: "sol-5.6", provider: "sol")
        XCTAssertEqual(
            ProgressEstimator.eta(health: validHealth(providers: [sol]), rates: ["sol-5.6": 100]),
            "ETA unavailable · diagnostics incomplete"
        )
    }

    func testLegacyETARejectsNumericEstimateWhileProductionIsStalled() {
        let sol = provider(key: "sol-5.6", provider: "sol")
        let health = validHealth(
            queues: [queue(kind: sol.workKind, pending: 100, active: 1)],
            providers: [sol],
            growth: PipelineGrowth(
                production: ProductionGrowth(configured: true, lastSuccessAt: nil, state: "stalled")
            )
        )

        XCTAssertEqual(
            ProgressEstimator.eta(health: health, rates: ["sol-5.6": 100]),
            "ETA unavailable · production stalled"
        )
    }
}
