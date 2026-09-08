import Foundation
import XCTest
@testable import SaqiRigMonitor

private actor SuspendedDiagnosticRead {
    private var continuation: CheckedContinuation<Int, Never>?
    private(set) var calls = 0

    func read() async -> Int {
        calls += 1
        return await withCheckedContinuation { continuation = $0 }
    }

    func settle(_ value: Int) {
        continuation?.resume(returning: value)
        continuation = nil
    }
}

extension ModelsTests {
    @MainActor
    func testConcurrentDiagnosticObserversShareOneUnderlyingRead() async {
        let gate = SuspendedDiagnosticRead()
        let reader = DiagnosticFileReader<Int>(timeout: .milliseconds(20))
        async let first = reader.read { await gate.read() }
        async let second = reader.read { await gate.read() }
        let results = await [first, second]
        for result in results {
            XCTAssertThrowsError(try result.get())
        }
        let calls = await gate.calls
        XCTAssertEqual(calls, 1)
        await gate.settle(0)
    }

    @MainActor
    func testHungDiagnosticReadTimesOutWithoutAccumulatingReadsAndDiscardsLateValue() async throws {
        let gate = SuspendedDiagnosticRead()
        let reader = DiagnosticFileReader<Int>(timeout: .milliseconds(20))
        let first = await reader.read { await gate.read() }
        XCTAssertThrowsError(try first.get())
        XCTAssertTrue(reader.isReadInFlight)
        for _ in 0 ..< 128 {
            let retry = await reader.read { await gate.read() }
            XCTAssertThrowsError(try retry.get())
        }
        let calls = await gate.calls
        XCTAssertEqual(calls, 1)
        await gate.settle(42)
        for _ in 0 ..< 1000 where reader.isReadInFlight {
            await Task.yield()
        }
        XCTAssertFalse(reader.isReadInFlight)
        // The timed-out value cannot be returned to a later observer.
        let recovered = await reader.read { 99 }
        XCTAssertEqual(try recovered.get(), 99)
    }

    @MainActor
    func testIndependentDiagnosticReadsRemainAvailableWhenOneFileIsHung() async throws {
        let gate = SuspendedDiagnosticRead()
        let hung = DiagnosticFileReader<Int>(timeout: .milliseconds(20))
        let healthy = DiagnosticFileReader<Int>(timeout: .seconds(1))
        async let failed = hung.read { await gate.read() }
        let value = await healthy.read { 7 }
        XCTAssertEqual(try value.get(), 7)
        let result = await failed
        XCTAssertThrowsError(try result.get())
        await gate.settle(0)
    }
}
