// swift-tools-version: 6.0
import PackageDescription

internal let package = Package(
    name: "SaqiRigMonitor",
    platforms: [.macOS(.v14)],
    products: [.executable(name: "SaqiRigMonitor", targets: ["SaqiRigMonitor"])],
    targets: [
        .executableTarget(name: "SaqiRigMonitor"),
        .testTarget(name: "SaqiRigMonitorTests", dependencies: ["SaqiRigMonitor"]),
    ]
)
