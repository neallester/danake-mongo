
import danake
import Foundation
import MongoSwift

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
     - parameter timeout: The maximum number of seconds the MongoAccesor will wait for an object in the pool to become available.
                          **Default = 60.0**
*/
    public init (maximumConnections: Int, minimumCached: Int = 0, reservedCacheCapacity: Int = 30, idleTimeout: TimeInterval = 300.0, timeout: TimeInterval = 60.0) {
        self.maximumConnections = maximumConnections
        self.minimumCached = minimumCached
        self.reservedCacheCapacity = reservedCacheCapacity
        self.idleTimeout = idleTimeout
        self.timeout = timeout
    }
    
    internal let maximumConnections: Int
    internal let minimumCached: Int
    internal let reservedCacheCapacity: Int
    internal let idleTimeout: TimeInterval
    internal let timeout: TimeInterval
    
}
    
/**
    A Danake DatabaseAccessor for the Mongo Database; used as a database access delegate by a Database object.
*/
public final class MongoAccessor : DatabaseAccessor {

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
            let client = try MongoClient (connectionString: dbConnectionString, options: clientOptions)
            return try client.db (databaseName, options: databaseOptions)
        }
        let database = try newConnectionClosure()
        do {
            let metadataCollection = try database.createCollection (MongoAccessor.metadataCollectionName)
            let newMetadata = DanakeMetadata()
            let encoder = BsonEncoder()
            let document = try encoder.encode(newMetadata)
            try metadataCollection.insertOne(document)
            hashValue = newMetadata.id.uuidString
        } catch {
            let metadataCollection = try database.collection (MongoAccessor.metadataCollectionName)
            let metadataCount = try metadataCollection.count()
            switch metadataCount {
            case 1:
                let cursor = try metadataCollection.find()
                if let metadataDocument = cursor.next() {
                    let decoder = BsonDecoder()
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
            let name: String = try collectionDocument.get("name")
            existingCollections.insert(name)
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
    }
    
    public func get<T>(type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) -> RetrievalResult<Entity<T>> where T : Decodable, T : Encodable {
        var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
        do {
            let query = selectId(id)
            connection = try collectionFor (name: cache.name)
            if let connection = connection {
                let resultCursor = try connection.collection.find(query)
                var entity: Entity<T>? = nil
                if let resultDocument = resultCursor.next() {
                    let bsonDecoder = decoder(cache: cache)
                    entity = try bsonDecoder.decode(type, from: resultDocument)
                }
                connectionPool.checkIn(connection.poolObject)
                return .ok (entity)
            } else {
                return .error ("NoCollection")
            }
        } catch {
            if let connection = connection {
                connectionPool.checkIn(connection.poolObject, isOK: false)
            }
            return .error ("\(error)")
        }
    }
    
    public func scan<T>(type: Entity<T>.Type, cache: EntityCache<T>) -> DatabaseAccessListResult<Entity<T>> where T : Decodable, T : Encodable {
        var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
        do {
            connection = try collectionFor(name: cache.name)
            if let connection = connection {
                let documents = try connection.collection.find();
                var result: [Entity<T>] = []
                for document in documents {
                    let bsonDecoder = decoder(cache: cache)
                    try result.append (bsonDecoder.decode(type, from: document))
                }
                connectionPool.checkIn(connection.poolObject)
                return .ok (result)
            } else {
                return .error ("NoCollection")
            }
        } catch {
            if let connection = connection {
                connectionPool.checkIn(connection.poolObject, isOK: false)
            }
            return .error ("\(error)")
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
    
    public func addAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        do {
            let document = try self.documentForWrapper(wrapper)
            let result: () -> DatabaseUpdateResult = {
                var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
                do {
                    connection = try self.collectionFor(name: wrapper.cacheName)
                    if let connection = connection {
                        try connection.collection.insertOne(document)
                        self.connectionPool.checkIn(connection.poolObject)
                        return .ok
                    } else {
                        return .error ("NoCollection")
                    }
                } catch {
                    if let connection = connection {
                        self.connectionPool.checkIn(connection.poolObject, isOK: false)
                    }
                    return .error ("\(error)")
                }
            }
            return .ok (result)
        } catch {
            return .error ("\(error)")
        }
    }
    
    public func updateAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        do {
            let document = try self.documentForWrapper(wrapper)
            let query = selectId (wrapper.id)
            let result: () -> DatabaseUpdateResult = {
                var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
                do {
                    connection = try self.collectionFor(name: wrapper.cacheName)
                    if let connection = connection {
                        try connection.collection.replaceOne(filter: query, replacement: document)
                        self.connectionPool.checkIn(connection.poolObject)
                        return .ok
                    } else {
                        return .error ("NoCollection")
                    }
                } catch {
                    if let connection = connection {
                        self.connectionPool.checkIn(connection.poolObject, isOK: false)
                    }
                    return .error ("\(error)")
                }
            }
            return .ok (result)
        } catch {
            return .error ("\(error)")
        }
    }
    
    public func removeAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        let query = selectId (wrapper.id)
        let result: () -> DatabaseUpdateResult = {
            var connection: (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>)? = nil
            do {
                connection = try self.collectionFor(name: wrapper.cacheName)
                if let connection = connection {
                    try connection.collection.deleteOne(query)
                    self.connectionPool.checkIn(connection.poolObject)
                    return .ok
                } else {
                    return .error ("NoCollection")
                }
            } catch {
                if let connection = connection {
                    self.connectionPool.checkIn(connection.poolObject, isOK: false)
                }
                return .error ("\(error)")
            }
        }
        return .ok (result)
    }
    
    public func encoder() -> BsonEncoder {
        return BsonEncoder()
    }

    private func decoder<T> (cache: EntityCache<T>) -> BsonDecoder {
        var userInfo: [CodingUserInfoKey : Any] = [:]
        userInfo[Database.cacheKey] = cache
        userInfo[Database.parentDataKey] = DataContainer()
        if let closure = cache.userInfoClosure {
            closure (&userInfo)
        }
        let result = BsonDecoder()
        result.userInfo = userInfo
        return result
    }
    
    internal func collectionFor (name: String) throws -> (collection: MongoCollection<Document>, poolObject: PoolObject<MongoDatabase>) {
        let databaseObject = try connectionPool.checkOut()
        var errorMessage: String? = nil
        var newCollection: MongoCollection<Document>? = nil
        var wasExisting = false
        var wasPreviouslyCreated = false
        do {
            if existingCollections.contains(name) {
                wasExisting = true
                newCollection = try databaseObject.object.collection (name)
            } else {
                try newCollectionsQueue.sync {
                    if newCollections.contains(name) {
                        wasPreviouslyCreated = true
                        newCollection = try databaseObject.object.collection (name)
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
    
    internal func documentForWrapper (_ wrapper: EntityPersistenceWrapper) throws -> Document {
        var result = try encoder().encode(wrapper)
        result [MongoAccessor.mongoIdFieldName] = wrapper.id.uuidString
        return result
    }

    private func selectId (_ id: UUID) -> Document {
        return [MongoAccessor.mongoIdFieldName : id.uuidString]
    }

    internal func newCollectionsSync (closure: (Set<String>) -> ()) {
        newCollectionsQueue.sync() {
            closure (self.newCollections)
        }
    }
    
    private var newCollections = Set<String>()

    
    public let logger: danake.Logger?
    public let hashValue: String

    internal let existingCollections: Set<String>
    private let newCollectionsQueue = DispatchQueue (label: "newCollections")
    internal let clientOptions: ClientOptions?
    internal let databaseOptions: DatabaseOptions?
    internal let dbConnectionString: String
    internal let databaseName: String
    private let connectionPool: ManagedPool<MongoDatabase>

    static let metadataCollectionName = "danakeMetadata"
    static let mongoIdFieldName = "_id"
    
    
}
