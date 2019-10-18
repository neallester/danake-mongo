
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
/**
    A Danake DatabaseAccessor for the Mongo Database; used as a database access delegate by a Database object.
 
    Important Note: Call MongoSwift.cleanup() exactly once at end of application execution.
 
*/
open class MongoAccessor : SynchronousAccessor {

/**
     - parameter dbConnectionString: A string defining [connection parameters](https://docs.mongodb.com/manual/reference/connection-string/)
     to a Mongo database.
     - parameter databaseName: Name of the database to connect to.
     - parameter clientOptions: Options for tuning the behavior of MongoDB client sessions.
     - parameter databaseOptions: Options for tuning the behavior of MongoDB database sessions.
     - parameter logger: The logger to use for reporting database activity and errors.
*/
    init (dbConnectionString: String, databaseName: String, clientOptions: ClientOptions? = nil, databaseOptions: DatabaseOptions? = nil, logger: danake.Logger?) throws {
        self.dbConnectionString = dbConnectionString
        self.databaseName = databaseName
        self.clientOptions = clientOptions
        self.databaseOptions = databaseOptions
        self.logger = logger
        let client = try SyncMongoClient (dbConnectionString, options: clientOptions)
        database = client.db (databaseName)
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
            existingCollections.insert(collectionDocument.name)
        }
        self.existingCollections = existingCollections
    }
    
    public func getImplementation<T>(type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) throws -> Entity<T>? where T : Decodable, T : Encodable {
        let query = selectId(id)
        let resultCursor = try collectionFor (name: cache.name).find(query)
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
    
    public func scanImplementation<T>(type: Entity<T>.Type, cache: EntityCache<T>) throws -> [Entity<T>] where T : Decodable, T : Encodable {
        let documents = try collectionFor(name: cache.name).find();
        let result: [Entity<T>] = try entityForDocuments(documents, cache: cache, type: type)
        return result
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
            do {
                try self.collectionFor(name: wrapper.cacheName).insertOne(document)
                callback (.ok)
            } catch {
                callback (.error ("\(error)"))
            }
        }
    }
    
    public func updateActionImplementation(wrapper: EntityPersistenceWrapper, callback: @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> () {
            let document = try self.documentForWrapper(wrapper)
            let query = selectId (wrapper.id)
            return {
                do {
                    try self.collectionFor(name: wrapper.cacheName).replaceOne(filter: query, replacement: document)
                    callback (.ok)
                } catch {
                    callback (.error ("\(error)"))
                }
            }

    }
    

    
    
    
    public func removeActionImplementation(wrapper: EntityPersistenceWrapper, callback: @escaping ((DatabaseUpdateResult) -> ())) throws -> () -> () {
        let query = selectId (wrapper.id)
        return {
            do {
                try self.collectionFor(name: wrapper.cacheName).deleteOne(query)
                callback (.ok)
            } catch {
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
    public func entityForDocuments<T: Codable> (_ documents: SyncMongoCursor<Document>, cache: EntityCache<T>, type: Entity<T>.Type) throws -> [Entity<T>] {
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
    public func collectionFor (name: String) throws -> SyncMongoCollection<Document> {
        var newCollection: SyncMongoCollection<Document>? = nil
        var wasExisting = false
        var wasPreviouslyCreated = false
        var errorMessage: String? = nil
        do {
            if existingCollections.contains(name) {
                wasExisting = true
                newCollection = database.collection (name)
            } else {
                try newCollectionsQueue.sync {
                    if newCollections.contains(name) {
                        wasPreviouslyCreated = true
                        newCollection = database.collection (name)
                    } else {
                        let createdCollection = try database.createCollection(name)
                        newCollection = createdCollection
                        newCollections.insert(name)
                    }
                }
            }
        } catch {
            errorMessage = "\(error)"
        }
        if let collection = newCollection {
            return collection
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
        result [MongoAccessor.mongoIdFieldName] = BSON.string (wrapper.id.uuidString)
        return result
    }

/**
     - parameter id: Id of the Entity to be selected for
     - returns: A Document query which selects an Entity by id
*/
    public func selectId (_ id: UUID) -> Document {
        return [MongoAccessor.mongoIdFieldName : BSON.string (id.uuidString)]
    }
    
    deinit {
        cleanupMongoSwift()
    }

    internal func newCollectionsSync (closure: (Set<String>) -> ()) {
        newCollectionsQueue.sync() {
            closure (self.newCollections)
        }
    }
    
    public let logger: danake.Logger?
    public let hashValue: String
    public let entityCreation = EntityCreation()

    
    internal var hasStatusReportStarted = false
    internal let database: SyncMongoDatabase
    internal let existingCollections: Set<String>
    internal let clientOptions: ClientOptions?
    internal let databaseOptions: DatabaseOptions?
    internal let dbConnectionString: String
    internal let databaseName: String

    private var newCollections = Set<String>()
    private let newCollectionsQueue = DispatchQueue (label: "newCollections")

    static let metadataCollectionName = "danakeMetadata"
    static let mongoIdFieldName = "_id"
    
    
}
