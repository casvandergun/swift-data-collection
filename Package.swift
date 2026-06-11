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
    ],
    dependencies: [
        .package(url: "https://github.com/casvandergun/electric-swift.git", exact: "0.1.0"),
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
        .testTarget(
            name: "SwiftDataCollectionTests",
            dependencies: ["SwiftDataCollection", "ElectricSwiftDataCollection", "FetchSwiftDataCollection"],
            path: "Tests/SwiftDataCollectionTests"
        ),
    ]
)
