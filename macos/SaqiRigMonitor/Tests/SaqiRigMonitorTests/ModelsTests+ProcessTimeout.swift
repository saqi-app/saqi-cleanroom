import Darwin
import Foundation
import XCTest
@testable import SaqiRigMonitor

extension ModelsTests {
    func testTimeoutKillsOnlyItsUnresponsiveChild() async throws {
        let pidFile = FileManager.default.temporaryDirectory.appending(path: UUID().uuidString)
        defer { try? FileManager.default.removeItem(at: pidFile) }
        let arguments = [
            "-e",
            "$SIG{TERM} = 'IGNORE'; open(my $f, '>', $ARGV[0]) or die; print $f $$; close $f; sleep 30;",
            pidFile.path,
        ]
        do {
            _ = try await RigStore.run(
                URL(fileURLWithPath: "/usr/bin/perl"),
                arguments,
                timeout: .milliseconds(300)
            )
            XCTFail("Expected timeout")
        } catch {
            XCTAssertTrue(error.localizedDescription.contains("outcome unknown"))
        }
        let pid = try XCTUnwrap(Int32(String(contentsOf: pidFile, encoding: .utf8)))
        let deadline = ContinuousClock.now.advanced(by: .seconds(2))
        while kill(pid, 0) == 0, ContinuousClock.now < deadline {
            try await Task.sleep(for: .milliseconds(20))
        }
        XCTAssertEqual(kill(pid, 0), -1)
        XCTAssertEqual(errno, ESRCH)
    }

    func testSubprocessCapturesBothStreamsWithoutDeadlock() async throws {
        let output = try await RigStore.run(URL(fileURLWithPath: "/usr/bin/perl"), [
            "-e",
            "print STDERR 'e' x 100000; print STDOUT 'o' x 100000;",
        ])
        XCTAssertEqual(output.count, 100_000)
    }

    func testSubprocessTimeoutIsNotProvenPreMutationFailure() async {
        do {
            let arguments = ["-e", "$SIG{TERM} = 'IGNORE'; sleep 30;"]
            _ = try await RigStore.run(
                URL(fileURLWithPath: "/usr/bin/perl"),
                arguments,
                timeout: .milliseconds(100)
            )
            XCTFail("Expected timeout")
        } catch {
            XCTAssertTrue(error.localizedDescription.contains("outcome unknown"))
            XCTAssertFalse(RigStore.isProvenPreMutationFailure(error))
        }
    }

    func testSubprocessCapsEachStreamBeforeExit() async {
        for stream in ["STDOUT", "STDERR"] {
            do {
                let arguments = ["-e", "print \(stream) 'x' x 100000; sleep 30;"]
                _ = try await RigStore.run(
                    URL(fileURLWithPath: "/usr/bin/perl"),
                    arguments,
                    timeout: .seconds(3),
                    outputLimit: 1024
                )
                XCTFail("Expected bounded output failure")
            } catch {
                XCTAssertEqual((error as NSError).code, CocoaError.fileReadTooLarge.rawValue)
            }
        }
    }

    func testSubprocessCancellationPropagatesToDetachedWorker() async {
        let task = Task {
            try await RigStore.run(URL(fileURLWithPath: "/usr/bin/perl"), ["-e", "sleep 30;"])
        }
        try? await Task.sleep(for: .milliseconds(100))
        task.cancel()
        do {
            _ = try await task.value
            XCTFail("Expected cancellation")
        } catch {
            XCTAssertTrue(error is CancellationError)
        }
    }
}
