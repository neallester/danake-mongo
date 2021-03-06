// swift-tools-version:4.0
// The swift-tools-version declares the minimum version of Swift required to build this package.

import PackageDescription

let package = Package(
    name: "danakeMongo",
    products: [
        // Products define the executables and libraries produced by a package, and make them visible to other packages.
        .library(
            name: "danakeMongo",
            targets: ["danakeMongo"]),
    ],
    dependencies: [
        // Dependencies declare other packages that this package depends on.
        // .package(url: /* package url */, from: "1.0.0"),
        .package(url: "https://github.com/neallester/danake-sw.git", .branch("master")),
        .package(url: "https://github.com/mongodb/mongo-swift-driver.git", .revision("be4f099802d023a1847ab091af2cc61e247ac23f")),
    ],
    targets: [
        // Targets are the basic building blocks of a package. A target can define a module or a test suite.
        // Targets can depend on other targets in this package, and on products in packages which this package depends on.
        .target(
            name: "danakeMongo",
            dependencies: ["danake", "MongoSwift"]),
        .testTarget(
            name: "danakeMongoTests",
            dependencies: ["danakeMongo"]),
    ]
)
