import XCTest
@testable import danakeMongo
import MongoKitten
import danake

/*
 
    REQUIRED PREPARATION
 
    Place a file named danake-mongo-connection.txt containing a Mongo connection string to a working
    Mongo database in the home directory of the user running these tests.
 
    e.g. mongodb://<username>:<password>@hosted_database.mongodb.net:27017
 
*/

final class DanakeMongoTests: XCTestCase {
    
    func testConnection() throws {
        
        if let connectionString = connectionString() {
            do {
                let _ = try MongoKitten.Database(connectionString)
            } catch {
                XCTFail ("Program With Connection String: \(error)")
            }
        }
    }
    
    private func connectionString() -> String? {
        var connectionString: String? = nil
        if #available(OSX 10.12, *) {
            let fm = FileManager.default
            let url = fm.homeDirectoryForCurrentUser.appendingPathComponent("danake-mongo-connection.txt")
            do {
                connectionString = try String (contentsOf: url).trimmingCharacters(in: .whitespacesAndNewlines)
            } catch {
                XCTFail ("Need Mongo DB connection string in \(url)")
            }
        } else {
            XCTFail ("Test Needs OSX 10.12 or greater")
        }
        return connectionString
    }
    
    public func testBson() throws {
        let encoder = BSONEncoder()
        let myStruct = MyStruct()
        var document = try encoder.encode(myStruct)
        let id = UUID(uuidString: "B6A42FA9-73A8-475A-85CD-C85353A24225")!
        document["_id"] = id.uuidString // This is how we'll set the _id
        if let connectionString = connectionString() {
            let db = try MongoKitten.Database(connectionString)
            let collection = db["myStructs"]
            try collection.insert(document)
        }
        XCTAssertNotNil (document)
    }
    
    public func testCount() throws {
        if let connectionString = connectionString() {
            let db = try MongoKitten.Database(connectionString)
            let collection = db["myStructs"]
            try XCTAssertEqual (1, collection.count())
            let query: Query = "_id" == "B6A42FA9-73A8-475A-85CD-C85353A24225"
            let document = try collection.findOne(query)!
            let decoder = BSONDecoder()
            let myStruct = try decoder.decode (MyStruct.self, from: document)
            XCTAssertEqual (0, myStruct.myInt)
        } else {
            XCTFail()
        }

    }
    
    public func testDanakeMetadata() throws {
        let metadata = DanakeMetadata()
        let encoder = JSONEncoder()
        let json = try String (data: encoder.encode(metadata), encoding: .utf8)!
        XCTAssertEqual ("{\"id\":\"\(metadata.id.uuidString)\"}", json)
        let decoder = JSONDecoder()
        let decodedMetadata = try decoder.decode(DanakeMetadata.self, from: json.data(using: .utf8)!)
        XCTAssertEqual (metadata.id.uuidString, decodedMetadata.id.uuidString)
    }
    
    public func testDanakeMongoCreation() {
        clearTestDatabase()
        let logger = danake.InMemoryLogger()
        do {
            let _ = try MongoAccessor (dbConnectionString: "xhbpaiewerjjlsizppskne320982734qpeijfz1209873.com", logger: logger)
            XCTFail("Expected Error")
        } catch {
            XCTAssertEqual ("invalidDatabase(Optional(\"\"))", "\(error)")
        }
        do {
            let _ = try MongoAccessor (dbConnectionString: "mongodb://www.mysafetyprogram.com:27017", logger: logger)
            XCTFail("Expected Error")
        } catch {
            XCTAssertEqual ("invalidDatabase(Optional(\"\"))", "\(error)")
        }
        if let connectionString = connectionString() {
            do {
                var accessor = try MongoAccessor (dbConnectionString: connectionString, logger: logger)
                XCTAssertTrue (logger === accessor.logger as! InMemoryLogger)
                let metadataCollection = accessor.database[MongoAccessor.metadataCollectionName]
                try XCTAssertEqual (1, metadataCollection.count())
                let hashCode = accessor.hashValue()
                accessor = try MongoAccessor (dbConnectionString: connectionString, logger: logger)
                try XCTAssertEqual (1, metadataCollection.count())
                XCTAssertEqual (hashCode, accessor.hashValue())
                // Add a second metadata document which should cause subsequent accessor creation to fail
                let anotherMetadata = DanakeMetadata()
                let encoder = BSONEncoder()
                var metadataDocument = try encoder.encode(anotherMetadata)
                metadataDocument[MongoAccessor.kittenIdFieldName] = anotherMetadata.id.uuidString
                try metadataCollection.insert(metadataDocument)
            } catch {
                XCTFail("No Error expected but got \(error)")
            }
            do {
                let database = try MongoKitten.Database(connectionString)
                let metadataCollection = database[MongoAccessor.metadataCollectionName]
                var count = try metadataCollection.count()
                let endTime = Date().timeIntervalSince1970 + 30.0
                while count != 2 && (Date().timeIntervalSince1970 < endTime) {
                    usleep (100000)
                    count = try metadataCollection.count()
                }
            } catch {
                XCTFail("No Error expected but got \(error)")
            }
            do {
                let _ = try MongoAccessor (dbConnectionString: connectionString, logger: logger)
                XCTFail ("Expected Error")
            } catch {
                XCTAssertEqual ("multipleMetadata(2)", "\(error)")
            }
            // TODO Test Case where  metadata document retrieval fails; need MongoKitten.Database with mocking capability
            
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }
    
    public func testIsValidCollectionName() {
        if let connectionString = connectionString() {
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, logger: nil)
                XCTAssertEqual ("error(\"name may not be empty\")", "\(accessor.isValidCollectionName(""))")
                XCTAssertEqual ("error(\"name cannot start with \\\"system\\\"\")", "\(accessor.isValidCollectionName("system"))")
                XCTAssertEqual ("error(\"name cannot start with \\\"system\\\"\")", "\(accessor.isValidCollectionName("systemCollection"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCollectionName(".name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCollectionName("3name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCollectionName("-name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCollectionName("*name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCollectionName("^name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCollectionName("$name"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCollectionName("name$"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCollectionName("name "))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCollectionName("name\0"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCollectionName("name$ after"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCollectionName("name after"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCollectionName("name\0after"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCollectionName("name$\0 after"))")
                XCTAssertEqual ("ok", "\(accessor.isValidCollectionName("_"))")
                XCTAssertEqual ("ok", "\(accessor.isValidCollectionName("a"))")
                XCTAssertEqual ("ok", "\(accessor.isValidCollectionName("collection_name"))")
                XCTAssertEqual ("ok", "\(accessor.isValidCollectionName("_collection_name"))")

            } catch {
                XCTFail("No Error expected but got \(error)")
            }
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }
    
    public func clearTestDatabase () {
        if let connectionString = connectionString() {
            do {
                let database = try MongoKitten.Database(connectionString)
                let collection = database[MongoAccessor.metadataCollectionName]
                try collection.remove()
            } catch {}
        }
    }
    
    public struct MyStruct : Codable {
        
        var myInt = 0
        var myString = ""
    }
    
    
    
    

}
