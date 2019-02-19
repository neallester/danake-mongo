
import danake
import Foundation
import MongoSwift
import ManagedPool

class DanakeMetadata : Codable {
    
    init() {
        id = UUID()
    }
    
    let id: UUID
    
}

enum DanakeMongoError : Error {
    case metadataCount (Int)
    case metadataRetrievalError
    case unableToCreateCollection (String)
}

/**
    Options for tuning the behavior the connection pool used by the MongoAccessor.
*/
public struct ConnectionPoolOptions {

/**
     - parameter maximumConnections: The maximum number of database connections which may be open at one time.
     - parameter minimumCached: The minimum number of open connections to retain in the cache. Actual cache count may drop below this
                                under high demand. **Default = 0**
     - parameter reservedCacheCapacity: The initial capacity reserved for the cache beyond **minimumCached**.
                                        That is, initial cache reservedCapcity = (**minimumCached** + **reservedCacheCapcity**) or
                                        **capacity**, whichever is less. **Default = 30**.
     - parameter idleTimeout: Connections will be removed from the cache if they are not used within **idleTimeout** seconds
                              of their checkIn. 0.0 means connections live forever (not recommended). **Default = 300.0**
     - parameter statusReportInterval: Status reports will be logged (.info) every **statusReportInterval** seconds. 0.0 indicates no reports. Ignored if logger is nil. **Default = 300.0**
     - parameter timeout: The maximum number of seconds the MongoAccesor will wait for an object in the pool to become available.
                          **Default = 60.0**
*/
    public init (maximumConnections: Int, minimumCached: Int = 0, reservedCacheCapacity: Int = 30, idleTimeout: TimeInterval = 300.0, timeout: TimeInterval = 60.0, statusReportInterval: TimeInterval = 300.0) {
        MongoSwift.initialize()
        self.maximumConnections = maximumConnections
        self.minimumCached = minimumCached
        self.reservedCacheCapacity = reservedCacheCapacity
        self.idleTimeout = idleTimeout
        self.statusReportInterval = statusReportInterval
        self.timeout = timeout
    }
    
    internal let maximumConnections: Int
    internal let minimumCached: Int
    internal let reservedCacheCapacity: Int
    internal let idleTimeout: TimeInterval
    internal let timeout: TimeInterval
    internal let statusReportInterval: TimeInterval
    
}
    
/**
    A Danake DatabaseAccessor for the Mongo Database; used as a database access delegate by a Database object.
 
    Important Note: Call MongoSwift.cleanup() exactly once at end of application execution.
 
*/
open class MongoAccessor : SynchronousAccessor {

/**
     - parameter dbConnectionString: A string defining [connection parameters](https://docs.mongodb.com/manual/reference/connection-string/)
                                     to a Mongo database.
     - parameter databaseName: Name of the database to connect to.
     - parameter maximumConnections: The maximum number of database connections which may be open at one time. **Default = 30**
     - parameter logger: The logger to use for reporting database activity and errors.
*/
    convenience init (dbConnectionString: String, databaseName: String, maximumConnections: Int = 30, logger: danake.Logger?) throws {
        try self.init (dbConnectionString: dbConnectionString, databaseName: databaseName, connectionPoolOptions: ConnectionPoolOptions(maximumConnections: maximumConnections), clientOptions: nil, databaseOptions: nil, logger: logger)
    }

/**
     - parameter dbConnectionString: A string defining [connection parameters](https://docs.mongodb.com/manual/reference/connection-string/)
     to a Mongo database.
     - parameter databaseName: Name of the database to connect to.
     - parameter connectionPoolOptions: Options for tuning the behavior of the connection pool.
     - parameter clientOptions: Options for tuning the behavior of MongoDB client sessions.
     - parameter databaseOptions: Options for tuning the behavior of MongoDB database sessions.
     - parameter logger: The logger to use for reporting database activity and errors.
*/
    init (dbConnectionString: String, databaseName: String, connectionPoolOptions: ConnectionPoolOptions, clientOptions: ClientOptions? = nil, databaseOptions: DatabaseOptions? = nil, logger: danake.Logger?) throws {
        self.dbConnectionString = dbConnectionString
        self.databaseName = databaseName
        self.clientOptions = clientOptions
        self.databaseOptions = databaseOptions
        self.logger = logger
        
        let newConnectionClosure: () throws -> MongoDatabase = {
            let client = try MongoClient (dbConnectionString, options: clientOptions)
            return client.db (databaseName, options: databaseOptions)
        }
        let database = try newConnectionClosure()
        do {
            let metadataCollection = try database.createCollection (MongoAccessor.metadataCollectionName)
            let newMetadata = DanakeMetadata()
            let encoder = BSONEncoder()
            let document = try encoder.encode(newMetadata)
            try metadataCollection.insertOne(document)
            hashValue = newMetadata.id.uuidString
        } catch {
            let metadataCollection = database.collection (MongoAccessor.metadataCollectionName)
            let metadataCount = try metadataCollection.count()
            switch metadataCount {
            case 1:
                let cursor = try metadataCollection.find()
                if let metadataDocument = cursor.next() {
                    let decoder = BSONDecoder()
                    let metadata = try decoder.decode(DanakeMetadata.self, from: metadataDocument)
                    hashValue = metadata.id.uuidString
                } else {
                    throw DanakeMongoError.metadataRetrievalError
                }
            default:
                throw DanakeMongoError.metadataCount(metadataCount)
            }
            
        }
        var existingCollections = Set<String>()
        for collectionDocument in try database.listCollections()  {
            if let name: String = collectionDocument ["name"] as? String {
                existingCollections.insert(name)
            }
        }
        self.existingCollections = existingCollections

        var logPoolErrorClosure: ((ManagedPool<MongoDatabase>.ManagedPoolError) -> ())? = nil
        if let logger = logger {
            logPoolErrorClosure = { poolError in
                var level = LogLevel.error
                switch poolError {
                case .creationError, .activationError, .deactivationError:
                    break
                case .wrongPool, .poolEmpty, .timeout:
                    level = .emergency
                }
                logger.log(level: level, source: MongoAccessor.self, featureName: "logPoolError", message: "\(poolError)", data: nil)
            }
        }
        connectionPool = ManagedPool<MongoDatabase>(capacity: connectionPoolOptions.maximumConnections, minimumCached: connectionPoolOptions.minimumCached, reservedCacheCapacity: connectionPoolOptions.reservedCacheCapacity, idleTimeout: connectionPoolOptions.idleTimeout, timeout: connectionPoolOptions.timeout, onError: logPoolErrorClosure, create: newConnectionClosure)
        if let _ = logger {
            if connectionPoolOptions.statusReportInterval > 0.0 {
                self.logStatusReport(nextReport: connectionPoolOptions.statusReportInterval)
            }
        }

    }
    
    public func getImplementation<T>(type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) throws -> Entity<T>? where T : Decodable, T : Encodable {
        var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
        var isConnectionOk = true
        defer {
            if let connection = connection {
                connectionPool.checkIn(connection.poolObject, isOK: isConnectionOk)
            }
        }
        do {
            let query = selectId(id)
            connection = try collectionFor (name: cache.name)
            if let connection = connection {
                let resultCursor = try connection.collection.find(query)
                var entity: Entity<T>? = nil
                if let resultDocument = resultCursor.next() {
                    let bsonDecoder = decoder(cache: cache)
                    switch entityCreation.entity(creator: { try bsonDecoder.decode(type, from: resultDocument) }) {
                    case .ok (let newEntity):
                        entity = newEntity
                    case .error(let errorMessage):
                        throw AccessorError.creation(errorMessage)
                    }
                }
                return entity
            }
            throw AccessorError.creation("UnknownError") // Should never happen ever
        } catch {
            isConnectionOk = false
            throw error
        }
    }
    
    public func scanImplementation<T>(type: Entity<T>.Type, cache: EntityCache<T>) throws -> [Entity<T>] where T : Decodable, T : Encodable {
        var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
        var isConnectionOk = true
        defer {
            if let connection = connection {
                connectionPool.checkIn(connection.poolObject, isOK: isConnectionOk)
            }
        }
        do {
            connection = try collectionFor(name: cache.name)
            if let connection = connection {
                let documents = try connection.collection.find();
                let result: [Entity<T>] = try entityForDocuments(documents, cache: cache, type: type)
                return result
            }
            throw AccessorError.creation("UnexpectedError") // Should not occur ever
        } catch {
            isConnectionOk = false
            throw error
        }
    }
    
    public func isValidCacheName(_ name: CacheName) -> ValidationResult {
        if name.count == 0 {
            return .error ("name may not be empty")
        }
        if name.hasPrefix("system") {
            return .error ("name cannot start with \"system\"")
        }
        let legalFirstCharacters = "_abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
        let firstCharacter = name[name.startIndex]
        if !legalFirstCharacters.contains(firstCharacter) {
            return .error ("name must start with one of the following characters: \(legalFirstCharacters)")
        }
        let illegalCharacters = " $\0"
        var containsIllegalCharacters = false
        for character in illegalCharacters {
            containsIllegalCharacters = containsIllegalCharacters || name.contains(character)
        }
        if containsIllegalCharacters {
            return .error ("name cannot contain '$', ' ' or the null character (\\0)")
        }
        return .ok
    }
 
    public func addActionImplementation(wrapper: EntityPersistenceWrapper, callback: @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> () {
        let document = try self.documentForWrapper(wrapper)
        return {
            var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
            var isConnectionOk = true
            defer {
                if let connection = connection {
                    self.connectionPool.checkIn(connection.poolObject, isOK: isConnectionOk)
                }
            }
            do {
                connection = try self.collectionFor(name: wrapper.cacheName)
                if let connection = connection {
                    try connection.collection.insertOne(document)
                    callback (.ok)
                } else {
                    callback (.error ("NoCollection"))
                }
            } catch {
                isConnectionOk = false
                callback (.error ("\(error)"))
            }
        }
    }
    
    public func updateActionImplementation(wrapper: EntityPersistenceWrapper, callback: @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> () {
            let document = try self.documentForWrapper(wrapper)
            let query = selectId (wrapper.id)
            return {
                var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
                var isConnectionOk = true
                defer {
                    if let connection = connection {
                        self.connectionPool.checkIn(connection.poolObject, isOK: isConnectionOk)
                    }
                }
                do {
                    connection = try self.collectionFor(name: wrapper.cacheName)
                    if let connection = connection {
                        try connection.collection.replaceOne(filter: query, replacement: document)
                        callback (.ok)
                    } else {
                        callback (.error ("NoCollection"))
                    }
                } catch {
                    isConnectionOk = false
                    callback (.error ("\(error)"))
                }
            }

    }
    

    
    
    
    public func removeActionImplementation(wrapper: EntityPersistenceWrapper, callback: @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> () {
        let query = selectId (wrapper.id)
        return {
            var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
            var isConnectionOk = true
            defer {
                if let connection = connection {
                    self.connectionPool.checkIn(connection.poolObject, isOK: isConnectionOk)
                }
            }
            do {
                connection = try self.collectionFor(name: wrapper.cacheName)
                if let connection = connection {
                    try connection.collection.deleteOne(query)
                    callback (.ok)
                } else {
                    callback (.error ("NoCollection"))
                }
            } catch {
                isConnectionOk = false
                callback (.error ("\(error)"))
            }
        }

    }
    
    public func encoder() -> BSONEncoder {
        return BSONEncoder()
    }
    
/**
     - parameter documents: A MongoCursor containing the documents representing the Entities to be created
     - parameter cache: The cache for the type of Entities to be created.
     - parameter type: The type of the Entities
     - returns: an Array of Entity<T> created from the provided documents.
*/
    public func entityForDocuments<T: Codable> (_ documents: MongoCursor<Document>, cache: EntityCache<T>, type: Entity<T>.Type) throws -> [Entity<T>] {
        var result: [Entity<T>] = []
        for document in documents {
            let bsonDecoder = decoder(cache: cache)
            switch entityCreation.entity(creator: { try bsonDecoder.decode(type, from: document) }) {
            case .ok (let newEntity):
                result.append (newEntity)
            case .error(let errorMessage):
                logger?.log (level: .error, source: self, featureName: "entityForDocument", message: "decodingError", data: [("cacheName", cache.name), ("documentId", document["id"] as? CustomStringConvertible), (name: "errorMessage", value: errorMessage)])
            }
        }
        return result
    }

    public func decoder<T> (cache: EntityCache<T>) -> BSONDecoder {
        var userInfo: [CodingUserInfoKey : Any] = [:]
        userInfo[Database.cacheKey] = cache
        userInfo[Database.parentDataKey] = DataContainer()
        if let closure = cache.userInfoClosure {
            closure (&userInfo)
        }
        let result = BSONDecoder()
        result.userInfo = userInfo
        return result
    }

/**
     - returns: a MongoCollection and the associated database connection (wrapped in a PoolObject). The connection must be returned to the
     connectionPool using checkIn() when finished using the MongoCollection
     - parameter name: The name of the collection.
*/
    public func collectionFor (name: String) throws -> (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>) {
        let databaseObject = try connectionPool.checkOut()
        var errorMessage: String? = nil
        var newCollection: MongoCollection<Document>? = nil
        var wasExisting = false
        var wasPreviouslyCreated = false
        do {
            if existingCollections.contains(name) {
                wasExisting = true
                newCollection = databaseObject.object.collection (name)
            } else {
                try newCollectionsQueue.sync {
                    if newCollections.contains(name) {
                        wasPreviouslyCreated = true
                        newCollection = databaseObject.object.collection (name)
                    } else {
                        let createdCollection = try databaseObject.object.createCollection(name)
                        newCollection = createdCollection
                        newCollections.insert(name)
                    }
                }
            }
        } catch {
            connectionPool.checkIn(databaseObject, isOK: false)
            errorMessage = "\(error)"
        }
        if let collection = newCollection {
            return (collection: collection, poolObject: databaseObject)
        } else {
            logger?.log(level: .emergency, source: self, featureName: "collectionFor", message: "unableToCreateCollection", data: [(name:"name", value: name), (name:"wasExisting", value: wasExisting), (name: "wasPreviouslyCreated", value: wasPreviouslyCreated), (name: "errorMessage", value: "\(errorMessage ?? "Unknown")")])
            throw DanakeMongoError.unableToCreateCollection (name)
        }
    }
    
/**
     - parameter wrapper: An EntityPersistentWrapper (type erased wrapper for an Entity<T>
     - returns: A mongo Document representing the wrapped entity.
*/
    public func documentForWrapper (_ wrapper: EntityPersistenceWrapper) throws -> Document {
        var result = try encoder().encode(wrapper)
        result [MongoAccessor.mongoIdFieldName] = wrapper.id.uuidString
        return result
    }

/**
     - parameter id: Id of the Entity to be selected for
     - returns: A Document query which selects an Entity by id
*/
    public func selectId (_ id: UUID) -> Document {
        return [MongoAccessor.mongoIdFieldName : id.uuidString]
    }
    
    public func poolStatus() -> ManagedPool<Database>.StatusReport {
        return connectionPool.status()
    }

/**
     Return a connection to the pool.
     - parameter poolObject: The object to be returned to the pool.
     - parameter isOK: Is the connection ok to return to the cache?
*/
    public func checkIn (_ poolObject: PoolObject<MongoDatabase>, isOK: Bool = true) {
        connectionPool.checkIn(poolObject, isOK: isOK)
    }
    
/**
     Prepare for deinitialization. Failure to call this function before dereferencing may cause a memory
     leak due to the strong reference held by the dispatch jobs used to periodically manage the connectionPool
     and log periodic status reports. Note that the invalidated accessor will remain in memory until the existing
     dipatch jobs run.
*/
    public func invalidate() {
        connectionPool.invalidate()
        statusReportQueue.sync {
            isInvalidated = true
        }
    }

    internal func newCollectionsSync (closure: (Set<String>) -> ()) {
        newCollectionsQueue.sync() {
            closure (self.newCollections)
        }
    }
    
    private func logStatusReport(nextReport: TimeInterval) {
        if hasStatusReportStarted {
            let status = connectionPool.status()
            logger?.log(level: .info, source: self, featureName: "logStatusReport", message: "status", data: [(name: "maximumConnections", value: connectionPool.capacity), (name: "checkedOut", value: status.checkedOut), (name: "cached", value: status.cached), (name: "firstExpires", value: status.firstExpires?.timeIntervalSince1970), (name: "lastExpires", value: status.lastExpires?.timeIntervalSince1970)])
        } else {
            hasStatusReportStarted = true
        }
        if !isInvalidated {
            statusReportQueue.asyncAfter(deadline: DispatchTime.now() + nextReport) {
                self.logStatusReport(nextReport: nextReport)
            }
        }
    }
    
    public let logger: danake.Logger?
    public let hashValue: String
    public let entityCreation = EntityCreation()

    
    internal var hasStatusReportStarted = false
    internal let connectionPool: ManagedPool<MongoDatabase>
    internal let existingCollections: Set<String>
    internal let clientOptions: ClientOptions?
    internal let databaseOptions: DatabaseOptions?
    internal let dbConnectionString: String
    internal let databaseName: String

    private var newCollections = Set<String>()
    private var isInvalidated = false
    private let newCollectionsQueue = DispatchQueue (label: "newCollections")
    private let statusReportQueue = DispatchQueue (label: "statusReport")

    static let metadataCollectionName = "danakeMetadata"
    static let mongoIdFieldName = "_id"
    
    
}
