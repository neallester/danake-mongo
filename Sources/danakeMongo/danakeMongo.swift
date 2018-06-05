
import MongoKitten
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
    
    func get<T>(type: Entity<T>.Type, collection: EntityCache<T>, id: UUID) -> RetrievalResult<Entity<T>> where T : Decodable, T : Encodable {
        return .error ("not implemented")
    }
    
    func scan<T>(type: Entity<T>.Type, collection: EntityCache<T>) -> DatabaseAccessListResult<Entity<T>> where T : Decodable, T : Encodable {
        return .error ("not implemented")
    }
    
    func isValidCollectionName(_ name: CollectionName) -> ValidationResult {
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
