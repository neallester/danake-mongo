// Generated using Sourcery 0.13.0 — https://github.com/krzysztofzablocki/Sourcery
// DO NOT EDIT

import XCTest
@testable import danakeTests

extension DanakeMongoTests {
  static var allTests = [
    ("testConnection", testConnection),
    ("testBson", testBson),
    ("testCount", testCount),
    ("testDanakeMetadata", testDanakeMetadata),
    ("testDanakeMongoCreation", testDanakeMongoCreation),
  ]
}


XCTMain([
  testCase(DanakeMongoTests.allTests),
])
