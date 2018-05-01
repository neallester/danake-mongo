
import MongoKitten
import danake
import danake
import Foundation
import BSON

class DanakeMetadata : Codable {
    
    init() {
        id = UUID()
    }
    
    let id: UUID
    
}

enum DanakeMongoError : Error {
    case multipleMetadata (Int)
    case metadataRetrievalError
}

class MongoAccessor : DatabaseAccessor {
    
    init (dbConnectionString: String, logger: danake.Logger?) throws {
        self.logger = logger
        database = try MongoKitten.Database(dbConnectionString)
        let metadataCollection = database[MongoAccessor.metadataCollectionName]
        let metadataCount = try metadataCollection.count()
        switch metadataCount {
        case 0:
            let newMetadata = DanakeMetadata()
            hashValueCache = newMetadata.id.uuidString
            let encoder = BSONEncoder()
            var document = try encoder.encode(newMetadata)
            document[MongoAccessor.kittenIdFieldName] = newMetadata.id.uuidString
            try metadataCollection.insert(document)
        case 1:
            let metadataDocument = try metadataCollection.findOne()
            if let metadataDocument = metadataDocument {
                let decoder = BSONDecoder()
                let metadata = try decoder.decode(DanakeMetadata.self, from: metadataDocument)
                hashValueCache = metadata.id.uuidString
            } else {
                throw DanakeMongoError.metadataRetrievalError
            }
        default:
            throw DanakeMongoError.multipleMetadata(metadataCount)
        }
    }
    
    func get<T>(type: Entity<T>.Type, collection: PersistentCollection<T>, id: UUID) -> RetrievalResult<Entity<T>> where T : Decodable, T : Encodable {
        return .error ("not implemented")
    }
    
    func scan<T>(type: Entity<T>.Type, collection: PersistentCollection<T>) -> DatabaseAccessListResult<Entity<T>> where T : Decodable, T : Encodable {
        return .error ("not implemented")
    }
    
    func isValidCollectionName(name: CollectionName) -> ValidationResult {
        return .error ("not implemented")
    }
    
    func hashValue() -> String {
        return hashValueCache
    }
    
    func addAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        return .error ("not implemented")
    }
    
    func updateAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        return .error ("not implemented")
    }
    
    func removeAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        return .error ("not implemented")
    }
    
    public let logger: danake.Logger?
    internal let database: MongoKitten.Database
    let hashValueCache: String
    
    static let metadataCollectionName = "danakeMetadata"
    static let kittenIdFieldName = "_id"
    
    
}
