import XCTest
@testable import danakeMongo
import danake
import MongoSwift
import ManagedPool

/*
 
    REQUIRED PREPARATION
 
    Place a file named danake-mongo-connection.txt containing a Mongo connection string to a working
    Mongo database in the home directory of the user running these tests.
 
    e.g. mongodb://<username>:<password>@hosted_database.mongodb.net:27017
 
*/

final class DanakeMongoTests: XCTestCase {
    
    override func setUp() {
        MongoSwift.initialize()
    }

    
    func testConnection() throws {
        if let connectionString = connectionString() {
            do {
                let client = try MongoClient (connectionString: connectionString)
                let database = try client.db (DanakeMongoTests.testDbName)
                let _ = try database.listCollections()
                XCTAssertTrue (true)
            } catch {
                XCTFail ("Program With Connection String: \(error)")
            }
        }
    }

    private func connectionString() -> String? {
        MongoSwift.initialize()
        var connectionString: String? = nil
        #if os(Linux)
            sleep (1)
        #endif
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
        let document = try encoder.encode(myStruct)
        XCTAssertNotNil (document)
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
            let _ = try MongoAccessor (dbConnectionString: "xhbpaiewerjjlsizppskne320982734qpeijfz1209873.com", databaseName: DanakeMongoTests.testDbName, logger: logger)
            XCTFail("Expected Error")
        } catch {
            XCTAssertEqual ("invalidUri(message: \"Invalid URI Schema, expecting \\'mongodb://\\' or \\'mongodb+srv://\\'\")", "\(error)")
        }
        do {
            let _ = try MongoAccessor (dbConnectionString: "mongodb://www.mysafetyprogram.com:27017", databaseName: DanakeMongoTests.testDbName, logger: logger)
            XCTFail("Expected Error")
        } catch {}
        if let connectionString = connectionString() {
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: logger)
                XCTAssertTrue (logger === accessor.logger as! InMemoryLogger)
                let client = try MongoClient (connectionString: connectionString)
                let database = try client.db (DanakeMongoTests.testDbName)
                let metadataCollection = try database.collection (MongoAccessor.metadataCollectionName)
                try XCTAssertEqual (1, metadataCollection.count())
                let hashCode = accessor.hashValue
                let decoder = BSONDecoder()
                for document in try metadataCollection.find() {
                    let metadata = try decoder.decode(DanakeMetadata.self, from: document)
                    XCTAssertEqual (hashCode, metadata.id.uuidString)
                }
                XCTAssertEqual (1, accessor.existingCollections.count)
                XCTAssertTrue (accessor.existingCollections.contains(MongoAccessor.metadataCollectionName))
                let accessor2 = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: logger)
                XCTAssertTrue (logger === accessor2.logger as! InMemoryLogger)
                let metadataCollection2 = try database.collection (MongoAccessor.metadataCollectionName)
                try XCTAssertEqual (1, metadataCollection2.count())
                let hashCode2 = accessor2.hashValue
                for document in try metadataCollection2.find() {
                    let metadata = try decoder.decode(DanakeMetadata.self, from: document)
                    XCTAssertEqual (hashCode2, metadata.id.uuidString)
                }
                XCTAssertEqual (1, accessor2.existingCollections.count)
                XCTAssertTrue (accessor2.existingCollections.contains (MongoAccessor.metadataCollectionName))
                let _ = try database.createCollection("testCollection")
                let accessor3 = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: logger)
                XCTAssertTrue (logger === accessor3.logger as! InMemoryLogger)
                let metadataCollection3 = try database.collection (MongoAccessor.metadataCollectionName)
                try XCTAssertEqual (1, metadataCollection3.count())
                let hashCode3 = accessor3.hashValue
                for document in try metadataCollection3.find() {
                    let metadata = try decoder.decode(DanakeMetadata.self, from: document)
                    XCTAssertEqual (hashCode3, metadata.id.uuidString)
                }
                XCTAssertEqual (2, accessor3.existingCollections.count)
                XCTAssertTrue (accessor3.existingCollections.contains (MongoAccessor.metadataCollectionName))
                XCTAssertTrue (accessor3.existingCollections.contains ("testCollection"))
                // Add a second metadata document which should cause subsequent accessor creation to fail
                let anotherMetadata = DanakeMetadata()
                let encoder = BSONEncoder()
                let metadataDocument = try encoder.encode(anotherMetadata)
                try metadataCollection2.insertOne(metadataDocument)
                do {
                    let _ = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: logger)
                    XCTFail ("Expected error")
                } catch {
                    XCTAssertEqual ("metadataCount(2)", "\(error)")
                }

            } catch {
                XCTFail("No Error expected but got \(error)")
            }
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }

    public func testIsValidCacheName() {
        if let connectionString = connectionString() {
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: nil)
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
    }
    
    public func testCollectionFor() {
        clearTestDatabase()
        var stage = 1
        if let connectionString = connectionString() {
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: nil)
                stage = 2
                accessor.newCollectionsSync() { newCollections in
                    XCTAssertEqual (0, newCollections.count)
                }
                let collectionName = "myCollection"
                let newCollection = try accessor.collectionFor (name: collectionName).collection
                XCTAssertEqual (collectionName, newCollection.name)
                stage = 3
                accessor.newCollectionsSync() { newCollections in
                    XCTAssertEqual (1, newCollections.count)
                    XCTAssertTrue (newCollections.contains (collectionName))
                }
                var indexCount = 0;
                for index in try newCollection.listIndexes() {
                    XCTAssertEqual ("_id_", index["name"] as! String)
                    indexCount = indexCount + 1
                }
                XCTAssertEqual (1, indexCount)
                stage = 4
                let accessor2 = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: nil)
                XCTAssertEqual (2, accessor2.existingCollections.count)
                XCTAssertTrue (accessor2.existingCollections.contains (collectionName))
                XCTAssertTrue (accessor2.existingCollections.contains(MongoAccessor.metadataCollectionName))
                stage = 5
                let newCollection2 = try accessor2.collectionFor (name: collectionName).collection
                stage = 6
                accessor2.newCollectionsSync() { newCollections in
                    XCTAssertEqual (0, newCollections.count)
                }
                stage = 7
                XCTAssertEqual (newCollection2.name, collectionName)
                indexCount = 0;
                for index in try newCollection2.listIndexes() {
                    XCTAssertEqual ("_id_", index["name"] as! String)
                    indexCount = indexCount + 1
                }
                XCTAssertEqual (1, indexCount)
            } catch {
                XCTFail("Stage \(stage): No Error expected but got \(error)")
            }
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }

    public func testCRUD() {
        clearTestDatabase()
        if let connectionString = connectionString() {
            var stage = 1
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: nil)
                let logger = InMemoryLogger()
                let database = danake.Database(accessor: accessor, schemaVersion: 1, logger: logger)
                let cache = EntityCache<MyStruct>(database: database, name: myStructCacheName)
                let myStruct = MyStruct (myInt: 10, myString: "10")
                let batch = EventuallyConsistentBatch()
                var entity: Entity<MyStruct>? = cache.new(batch: batch, item: myStruct)
                let id = entity!.id
                stage = 2
                batch.commitSync()
                stage = 3
                entity = nil
                cache.waitWhileCached (id: id)
                entity = try cache.getSync (id: id)
                entity!.sync() { myStruct in
                    XCTAssertEqual (10, myStruct.myInt)
                    XCTAssertEqual ("10", myStruct.myString)
                }
                stage = 4
                entity!.update(batch: batch) { myStruct in
                    myStruct.myInt = 20
                    myStruct.myString = "20"
                }
                batch.commitSync()
                stage = 5
                entity = nil
                cache.waitWhileCached (id: id)
                entity = try cache.getSync (id: id)
                entity!.sync() { myStruct in
                    XCTAssertEqual (20, myStruct.myInt)
                    XCTAssertEqual ("20", myStruct.myString)
                }
                entity?.remove(batch: batch)
                batch.commitSync()
                stage = 6
                entity = nil
                cache.waitWhileCached (id: id)
		do {
		   let _ = try cache.getSync (id: id)
		   XCTFail ("Expected error")
		} catch {
		  XCTAssertEqual ("unknownUUID(\(id.uuidString))", "\(error)")
		}
                XCTAssertEqual (0, accessor.connectionPool.status().checkedOut)
            } catch {
                XCTFail("Stage \(stage): No Error expected but got \(error)")
            }
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }

    public func testScan() {
        clearTestDatabase()
        var stage = 1
        if let connectionString = connectionString() {
            do {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: nil)
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
                stage = 2
                batch.commitSync()
                stage = 3
                entity1 = nil
                entity2 = nil
                cache.waitWhileCached (id: id1)
                cache.waitWhileCached (id: id2)
                let entities = try cache.scanSync()
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
                XCTFail("Stage \(stage): No Error expected but got \(error)")
            }
        } else {
            XCTFail("No Connection String")
        }
        clearTestDatabase()
    }
    
    public func testCheckIn() throws {
        if let connectionString = connectionString() {
            let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: nil)
            let c1 = try accessor.connectionPool.checkOut()
            var status = accessor.poolStatus()
            XCTAssertEqual (1, status.checkedOut)
            XCTAssertEqual (0, status.cached)
            accessor.checkIn(c1)
            status = accessor.poolStatus()
            XCTAssertEqual (0, status.checkedOut)
            XCTAssertEqual (1, status.cached)
        } else {
            XCTFail("Expected connectionString")
        }
    }
    
    public func testErrorLogging() throws {
        if let connectionString = connectionString() {
            let logger = InMemoryLogger(level: .warning)
            let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, maximumConnections: 1, logger: logger)
            let c1 = try accessor.connectionPool.checkOut()
            logger.sync() { entries in
                XCTAssertEqual (1, entries.count)
                XCTAssertEqual ("EMERGENCY|MongoAccessor.Type.logPoolError|poolEmpty", entries[0].asTestString())
            }
            accessor.checkIn(c1)
            
        } else {
            XCTFail("Expected connectionString")
        }
    }
    
    public func testStatusReport() throws {
        if let connectionString = connectionString() {
            let logger = InMemoryLogger(level: .info)
            let options = ConnectionPoolOptions(maximumConnections: 5, statusReportInterval: 0.0000001)
            let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, connectionPoolOptions: options, logger: logger)
            var actualLogEntry: String? = nil
            var foundEntry = logger.waitForEntry(intervalUseconds: 100, timeoutSeconds: 100) { entries in
                actualLogEntry = entries.last!.asTestString()
                return actualLogEntry == "INFO|MongoAccessor.logStatusReport|status|maximumConnections=5;checkedOut=0;cached=0;firstExpires=nil;lastExpires=nil"
            }
            if !foundEntry {
                print ("Unexpected Entry 1: \(String(describing: actualLogEntry))")
            }
            XCTAssertTrue (foundEntry)
            let c1 = try accessor.connectionPool.checkOut()
            foundEntry = false
            actualLogEntry = nil
            foundEntry = logger.waitForEntry(intervalUseconds: 100, timeoutSeconds: 100) { entries in
                actualLogEntry = entries.last!.asTestString()
                return actualLogEntry == "INFO|MongoAccessor.logStatusReport|status|maximumConnections=5;checkedOut=1;cached=0;firstExpires=nil;lastExpires=nil"
            }
            if !foundEntry {
                print ("Unexpected Entry 2: \(String(describing: actualLogEntry))")
            }
            XCTAssertTrue (foundEntry)
            accessor.checkIn(c1)
            foundEntry = false
            actualLogEntry = nil
            foundEntry = logger.waitForEntry(intervalUseconds: 100, timeoutSeconds: 100) { entries in
                actualLogEntry = entries.last!.asTestString()
                return entries.last!.asTestString().contains ("INFO|MongoAccessor.logStatusReport|status|maximumConnections=5;checkedOut=0;cached=1;firstExpires=") &&
                       entries.last!.asTestString().contains (";lastExpires=") &&
                       !entries.last!.asTestString().contains (";lastExpires=nil")
            }
            if !foundEntry {
                print ("Unexpected Entry 3: \(String(describing: actualLogEntry))")
            }
            XCTAssertTrue (foundEntry)
        } else {
            XCTFail("Expected connectionString")
        }
        
    }

    public func testSampleCompany() throws {
        
        class SampleMongoAccessor : MongoAccessor, SampleAccessor {
            func employeesForCompany(cache: EntityCache<SampleEmployee>, company: SampleCompany) throws -> [Entity<SampleEmployee>] {

	    	enum TestErrors: Error {
		     case testError
		}

                var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
                var isConnectionOk = true
                defer {
                    if let connection = connection {
                        self.connectionPool.checkIn(connection.poolObject, isOK: isConnectionOk)
                    }
                }
                do {
                    connection = try collectionFor(name: cache.name)
                    if let connection = connection {
                        let query: Document = [ "item.company.id" : company.id.uuidString]
                        let documents = try connection.collection.find(query);
                        let result: [Entity<SampleEmployee>] = try entityForDocuments(documents, cache: cache, type: Entity<SampleEmployee>.self)
                        return result
                    } else {
                        isConnectionOk = false
			throw TestErrors.testError
                    }
                } catch {
                    isConnectionOk = false
                    throw error
                }
            }
        }
        
        if let connectionString = connectionString() {
            let accessor = try SampleMongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: nil)
            XCTAssertTrue (SampleUsage.runSample (accessor: accessor))
            XCTAssertEqual (0, accessor.connectionPool.status().checkedOut)
        } else {
            XCTFail("Expected connectionString")
        }
    }
    
    public func testParallelTests() throws {        
        let runTest = true;
        var repetitions = 5
        #if os(Linux)
        // The test often throws a Signal 4 or Signal 9 Error on Linux
        // Appears to be a problem with the mongo-swift-driver which is still
        // immature. Hoping they'll fix it and then I can remove this iff statement.
//            if Calendar.current.dateComponents([.weekday], from: Date()).weekday != 2 {
//                runTest = false
//            } else {
            repetitions = 1
            sleep (3)
//            }
        #endif
        if runTest {
            if let connectionString = connectionString() {
                let accessor = try MongoAccessor (dbConnectionString: connectionString, databaseName: DanakeMongoTests.testDbName, logger: nil)
                XCTAssertTrue (ParallelTest.performTest (accessor: accessor, repetitions: repetitions, logger: nil))
                XCTAssertEqual (0, accessor.connectionPool.status().checkedOut)
            } else {
                XCTFail("Expected connectionString")
            }
            #if os(Linux)
                sleep (3)
            #endif
            MongoSwift.cleanup()
        }
    }
    
    
    public func clearTestDatabase () {
        do {
            if let connectionString = connectionString() {
                let client = try MongoClient (connectionString: connectionString)
                let database = try client.db (DanakeMongoTests.testDbName)
                for collectionDocument in try database.listCollections() {
                    if let name: String = collectionDocument ["name"] as? String {
                        let command: Document = [ "drop" : name]
                        try database.runCommand(command)
                        
                    }
                }
            } else {
                XCTFail ("Expected connectionString")
            }
        } catch {
            XCTFail("Expected success but got \(error)")
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
    public static let testDbName = "danake"

    
    
    
    

}
