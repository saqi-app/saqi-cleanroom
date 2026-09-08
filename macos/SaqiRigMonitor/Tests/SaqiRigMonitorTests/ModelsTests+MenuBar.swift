import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    func testMenuBarAttentionUsesExistingOperationalStatus() {
        let status = MonitorOperationalStatus(label: "Production stalled", severity: .attention, wait: nil)
        let presentation = MenuBarPresentation(status: status)
        XCTAssertTrue(presentation.requiresAttention)
        XCTAssertEqual(presentation.symbol, "exclamationmark.circle.fill")
        XCTAssertEqual(presentation.accessibilityLabel, "Saqi · Production stalled")
    }

    func testMenuBarPreservesNormalIconForNonAttentionStatuses() {
        for severity in [MonitorSeverity.healthy, .neutral, .waiting] {
            let presentation = MenuBarPresentation(status: MonitorOperationalStatus(
                label: "Current status",
                severity: severity,
                wait: nil
            ))
            XCTAssertFalse(presentation.requiresAttention)
            XCTAssertEqual(presentation.symbol, "books.vertical.fill")
            XCTAssertEqual(presentation.accessibilityLabel, "Saqi · Current status")
        }
    }
}
