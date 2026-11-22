// swift-tools-version: 6.3

import PackageDescription

let package = Package(
    name: "SwiftDataCollection",
    platforms: [
        .iOS(.v17),
        .macOS(.v14),
    ],
    products: [
        .library(
            name: "SwiftDataCollection",
            targets: ["SwiftDataCollection"]
        ),
        .library(
            name: "ElectricSwiftDataCollection",
            targets: ["ElectricSwiftDataCollection"]
        ),
        .library(
            name: "FetchSwiftDataCollection",
            targets: ["FetchSwiftDataCollection"]
        ),
        .library(
            name: "NeonRealtimeSwiftDataCollection",
            targets: ["NeonRealtimeSwiftDataCollection"]
        ),
    ],
    dependencies: [
        .package(url: "https://github.com/casvandergun/electric-swift.git", exact: "0.1.3"),
        .package(url: "https://github.com/casvandergun/neon-realtime-swift.git", exact: "0.2.1"),
    ],
    targets: [
        .target(
            name: "SwiftDataCollection",
            dependencies: []
        ),
        .target(
            name: "ElectricSwiftDataCollection",
            dependencies: [
                "SwiftDataCollection",
                .product(name: "ElectricSwift", package: "electric-swift"),
            ],
            path: "Sources/Adapters/ElectricSwiftDataCollection"
        ),
        .target(
            name: "FetchSwiftDataCollection",
            dependencies: ["SwiftDataCollection"],
            path: "Sources/Adapters/FetchSwiftDataCollection"
        ),
        .target(
            name: "NeonRealtimeSwiftDataCollection",
            dependencies: [
                "SwiftDataCollection",
                .product(name: "NeonRealtimeSwift", package: "neon-realtime-swift"),
            ],
            path: "Sources/Adapters/NeonRealtimeSwiftDataCollection"
        ),
        .testTarget(
            name: "SwiftDataCollectionTests",
            dependencies: ["SwiftDataCollection", "ElectricSwiftDataCollection", "FetchSwiftDataCollection"],
            path: "Tests/SwiftDataCollectionTests"
        ),
        .testTarget(
            name: "NeonRealtimeSwiftDataCollectionTests",
            dependencies: [
                "SwiftDataCollection",
                "NeonRealtimeSwiftDataCollection",
                .product(name: "NeonRealtimeSwift", package: "neon-realtime-swift"),
            ]
        ),
    ]
)
