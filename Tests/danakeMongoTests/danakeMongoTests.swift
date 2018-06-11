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
            let _ = try MongoAccessor (dbConnectionString: "xhbpaiewerjjlsizppskne320982734qpeijfz1209873.com", maxConnections: 40, logger: logger)
            XCTFail("Expected Error")
        } catch {
            XCTAssertEqual ("invalidDatabase(Optional(\"\"))", "\(error)")
        }
        do {
            let _ = try MongoAccessor (dbConnectionString: "mongodb://www.mysafetyprogram.com:27017", maxConnections: 40, logger: logger)
            XCTFail("Expected Error")
        } catch {
            XCTAssertEqual ("invalidDatabase(Optional(\"\"))", "\(error)")
        }
        if let connectionString = connectionString() {
            do {
                var accessor = try MongoAccessor (dbConnectionString: connectionString, maxConnections: 40, logger: logger)
                XCTAssertTrue (logger === accessor.logger as! InMemoryLogger)
                let metadataCollection = accessor.database[MongoAccessor.metadataCollectionName]
                try XCTAssertEqual (1, metadataCollection.count())
                let hashCode = accessor.hashValue
                accessor = try MongoAccessor (dbConnectionString: connectionString, maxConnections: 40, logger: logger)
                try XCTAssertEqual (1, metadataCollection.count())
                XCTAssertEqual (hashCode, accessor.hashValue)
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
                    usleep (3000000)
                    count = try metadataCollection.count()
                }
                XCTAssertEqual (2, count)
            } catch {
                XCTFail("No Error expected but got \(error)")
            }
            do {
                let _ = try MongoAccessor (dbConnectionString: connectionString, maxConnections: 40, logger: logger)
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
    
    public func testIsValidCacheName() {
        clearTestDatabase()
        if let connectionString = connectionString() {
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, maxConnections: 40, logger: nil)
                XCTAssertEqual ("error(\"name may not be empty\")", "\(accessor.isValidCacheName(""))")
                XCTAssertEqual ("error(\"name cannot start with \\\"system\\\"\")", "\(accessor.isValidCacheName("system"))")
                XCTAssertEqual ("error(\"name cannot start with \\\"system\\\"\")", "\(accessor.isValidCacheName("systemCache"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCacheName(".name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCacheName("3name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCacheName("-name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCacheName("*name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCacheName("^name"))")
                XCTAssertEqual ("error(\"name must start with one of the following characters: _abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ\")", "\(accessor.isValidCacheName("$name"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCacheName("name$"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCacheName("name "))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCacheName("name\0"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCacheName("name$ after"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCacheName("name after"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCacheName("name\0after"))")
                XCTAssertEqual ("error(\"name cannot contain \\'$\\', \\' \\' or the null character (\\\\0)\")", "\(accessor.isValidCacheName("name$\0 after"))")
                XCTAssertEqual ("ok", "\(accessor.isValidCacheName("_"))")
                XCTAssertEqual ("ok", "\(accessor.isValidCacheName("a"))")
                XCTAssertEqual ("ok", "\(accessor.isValidCacheName("Cache_name"))")
                XCTAssertEqual ("ok", "\(accessor.isValidCacheName("_Cache_name"))")

            } catch {
                XCTFail("No Error expected but got \(error)")
            }
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }
    
    public func testCRUD() {
        clearTestDatabase()
        if let connectionString = connectionString() {
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, maxConnections: 40, logger: nil)
                let logger = InMemoryLogger()
                let database = danake.Database(accessor: accessor, schemaVersion: 1, logger: logger)
                let cache = EntityCache<MyStruct>(database: database, name: myStructCacheName)
                let myStruct = MyStruct (myInt: 10, myString: "10")
                let batch = EventuallyConsistentBatch()
                var entity: Entity<MyStruct>? = cache.new(batch: batch, item: myStruct)
                let id = entity!.id
                batch.commitSync()
                entity = nil
                cache.waitWhileCached (id: id)
                entity = cache.get (id: id).item()
                entity!.sync() { myStruct in
                    XCTAssertEqual (10, myStruct.myInt)
                    XCTAssertEqual ("10", myStruct.myString)
                }
                entity!.update(batch: batch) { myStruct in
                    myStruct.myInt = 20
                    myStruct.myString = "20"
                }
                batch.commitSync()
                entity = nil
                cache.waitWhileCached (id: id)
                entity = cache.get (id: id).item()
                entity!.sync() { myStruct in
                    XCTAssertEqual (20, myStruct.myInt)
                    XCTAssertEqual ("20", myStruct.myString)
                }
                entity?.remove(batch: batch)
                batch.commitSync()
                entity = nil
                cache.waitWhileCached (id: id)
                entity = cache.get (id: id).item()
                XCTAssertNil (entity)
            } catch {
                XCTFail("No Error expected but got \(error)")
            }
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }

    public func testScan() {
        clearTestDatabase()
        if let connectionString = connectionString() {
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, maxConnections: 40, logger: nil)
                let logger = InMemoryLogger()
                let database = danake.Database(accessor: accessor, schemaVersion: 1, logger: logger)
                let cache = EntityCache<MyStruct>(database: database, name: myStructCacheName)
                let batch = EventuallyConsistentBatch()
                let myStruct1 = MyStruct (myInt: 10, myString: "10")
                var entity1: Entity<MyStruct>? = cache.new(batch: batch, item: myStruct1)
                let id1 = entity1!.id
                let myStruct2 = MyStruct (myInt: 20, myString: "20")
                var entity2: Entity<MyStruct>? = cache.new(batch: batch, item: myStruct2)
                let id2 = entity2!.id
                batch.commitSync()
                entity1 = nil
                entity2 = nil
                cache.waitWhileCached (id: id1)
                cache.waitWhileCached (id: id2)
                let entities = cache.scan().item()!
                XCTAssertEqual (2, entities.count)
                var found1 = false
                var found2 = false
                for entity in entities {
                    if entity.id.uuidString == id1.uuidString {
                        found1 = true
                        entity.sync() { myStruct in
                            XCTAssertEqual (10, myStruct.myInt)
                            XCTAssertEqual ("10", myStruct.myString)
                        }
                    } else if entity.id.uuidString == id2.uuidString {
                        found2 = true
                        entity.sync() { myStruct in
                            XCTAssertEqual (20, myStruct.myInt)
                            XCTAssertEqual ("20", myStruct.myString)
                        }
                    }
                }
                XCTAssertTrue (found1)
                XCTAssertTrue (found2)
            } catch {
                XCTFail("No Error expected but got \(error)")
            }
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }
    
    public func testParallelTests() throws {
//        if let connectionString = connectionString() {
//            let logger = ConsoleLogger()
//            let accessor = try MongoAccessor (dbConnectionString: connectionString, maxConnections: 40, logger: nil)
// See https://github.com/OpenKitten/MongoKitten/issues/170
//            XCTAssertTrue (ParallelTest.performTest (accessor: accessor, repetitions: 5, logger: nil))
//        } else {
//            XCTFail("Expected connectionString")
//        }
    }

    public func clearTestDatabase () {
        do {
            if let connectionString = connectionString() {
                let database = try MongoKitten.Database(connectionString)
                clearCollection(database: database, name: MongoAccessor.metadataCollectionName)
                clearCollection(database: database, name: myStructCacheName)
            } else {
                XCTFail ("Expected connectionString")
            }
        } catch {
            XCTFail("Expected success but got \(error)")
        }
    }
    
    public func clearCollection (database: MongoKitten.Database, name: String) {
        do {
            let collection = database[name]
            try collection.remove()
        } catch {
            XCTFail ("Expected success but got \(error)")
        }
    }
    
    public struct MyStruct : Codable {
        
        init (myInt: Int = 0, myString: String = "") {
            self.myInt = myInt
            self.myString = myString
        }
        
        var myInt: Int
        var myString: String
    }
    
    public let myStructCacheName = "myStruct"

    
    
    
    

}
