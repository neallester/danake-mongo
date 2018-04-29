import XCTest

import danakeMongoTests

var tests = [XCTestCaseEntry]()
tests += danakeMongoTests.allTests()
XCTMain(tests)