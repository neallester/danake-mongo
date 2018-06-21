// Generated using Sourcery 0.13.0 — https://github.com/krzysztofzablocki/Sourcery
// DO NOT EDIT

import XCTest
@testable import danakeMongoTests

extension DanakeMongoTests {
  static var allTests = [
    ("testConnection", testConnection),
    ("testBson", testBson),
    ("testDanakeMetadata", testDanakeMetadata),
    ("testDanakeMongoCreation", testDanakeMongoCreation),
    ("testIsValidCacheName", testIsValidCacheName),
    ("testCollectionFor", testCollectionFor),
    ("testCRUD", testCRUD),
    ("testScan", testScan),
    ("testParallelTests", testParallelTests),
    ("testSampleCompany", testSampleCompany),
  ]
}


XCTMain([
  testCase(DanakeMongoTests.allTests),
])
