
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
    
    init (dbConnectionString: String, maxConnections: Int, logger: danake.Logger?) throws {
        self.logger = logger
        connectionGate = DispatchSemaphore (value: maxConnections)
        database = try MongoKitten.Database(dbConnectionString)
        let metadataCollection = database[MongoAccessor.metadataCollectionName]
        let metadataCount = try metadataCollection.count()
        switch metadataCount {
        case 0:
            let newMetadata = DanakeMetadata()
            hashValue = newMetadata.id.uuidString
            let encoder = BSONEncoder()
            var document = try encoder.encode(newMetadata)
            document[MongoAccessor.kittenIdFieldName] = newMetadata.id.uuidString
            try metadataCollection.insert(document)
        case 1:
            let metadataDocument = try metadataCollection.findOne()
            if let metadataDocument = metadataDocument {
                let decoder = BSONDecoder()
                let metadata = try decoder.decode(DanakeMetadata.self, from: metadataDocument)
                hashValue = metadata.id.uuidString
            } else {
                throw DanakeMongoError.metadataRetrievalError
            }
        default:
            throw DanakeMongoError.multipleMetadata(metadataCount)
        }
    }
    
    func get<T>(type: Entity<T>.Type, cache: EntityCache<T>, id: UUID) -> RetrievalResult<Entity<T>> where T : Decodable, T : Encodable {
        var inGate = false
        let query = selectId(id)
        let collection = self.database[cache.name]
        do {
            connectionGate.wait()
            inGate = true
            let document = try collection.findOne(query);
            connectionGate.signal()
            inGate = false
            if let document = document {
                let bsonDecoder = decoder(cache: cache)
                let entity = try bsonDecoder.decode(type, from: document)
                return .ok (entity)
            }
            return .ok (nil)
        } catch {
            if inGate {
                connectionGate.signal()
            }
            return .error ("\(error)")
        }
    }
    
    func scan<T>(type: Entity<T>.Type, cache: EntityCache<T>) -> DatabaseAccessListResult<Entity<T>> where T : Decodable, T : Encodable {
        var inGate = false
        let collection = self.database[cache.name]
        do {
            connectionGate.wait()
            inGate = true
            let documents = try collection.find();
            connectionGate.signal()
            inGate = false
            var result: [Entity<T>] = []
            for document in documents {
                let bsonDecoder = decoder(cache: cache)
                try result.append (bsonDecoder.decode(type, from: document))
            }
            return .ok (result)
        } catch {
            if inGate {
                connectionGate.signal()
            }
            return .error ("\(error)")
        }
    }
    
    func isValidCacheName(_ name: CacheName) -> ValidationResult {
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
    
    func addAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        do {
            var document = try encoder().encode(wrapper)
            document[MongoAccessor.kittenIdFieldName] = wrapper.id.uuidString
            let result: () -> DatabaseUpdateResult = {
                var inGate = false
                do {
                    let collection = self.database[wrapper.cacheName]
                    self.connectionGate.wait()
                    inGate = true
                    try collection.insert(document)
                    self.connectionGate.signal()
                    inGate = false
                    return .ok
                } catch {
                    if inGate {
                        self.connectionGate.signal()
                    }
                    return .error ("\(error)")
                }
            }
            return .ok (result)
        } catch {
            return .error ("\(error)")
        }
    }
    
    func updateAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        do {
            var document = try encoder().encode(wrapper)
            document[MongoAccessor.kittenIdFieldName] = wrapper.id.uuidString
            let query = selectId(wrapper.id)
            let result: () -> DatabaseUpdateResult = {
                var inGate = false
                do {
                    let collection = self.database[wrapper.cacheName]
                    self.connectionGate.wait()
                    inGate = true
                    try collection.update(query, to: document)
                    self.connectionGate.signal()
                    inGate = false
                    return .ok
                } catch {
                    if inGate {
                        self.connectionGate.signal()
                    }
                    return .error ("\(error)")
                }
            }
            return .ok (result)
        } catch {
            return .error ("\(error)")
        }
    }
    
    func removeAction(wrapper: EntityPersistenceWrapper) -> DatabaseActionResult {
        do {
            let query = selectId(wrapper.id)
            let result: () -> DatabaseUpdateResult = {
                var inGate = false
                do {
                    let collection = self.database[wrapper.cacheName]
                    self.connectionGate.wait()
                    inGate = true
                    try collection.remove(query)
                    self.connectionGate.signal()
                    inGate = false
                    return .ok
                } catch {
                    if inGate {
                        self.connectionGate.signal()
                    }
                    return .error ("\(error)")
                }
            }
            return .ok (result)
        }
    }
    
    public func encoder() -> BSONEncoder {
        return BSONEncoder()
    }
    
    private func decoder<T> (cache: EntityCache<T>) -> BSONDecoder {
        var userInfo: [CodingUserInfoKey : Any] = [:]
        userInfo[Database.cacheKey] = cache
        userInfo[Database.parentDataKey] = DataContainer()
        if let closure = cache.userInfoClosure {
            closure (&userInfo)
        }
        return BSONDecoder(userInfo: userInfo)
    }
    
    private func selectId (_ id: UUID) -> Query {
        return MongoAccessor.kittenIdFieldName == id.uuidString
    }
    
    public let logger: danake.Logger?
    internal let database: MongoKitten.Database
    let hashValue: String
    let connectionGate: DispatchSemaphore
    
    static let metadataCollectionName = "danakeMetadata"
    static let kittenIdFieldName = "_id"
    
    
}
