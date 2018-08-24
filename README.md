# danake-mongo
A Mongo DB Accessor for the [danake-sw persistence framework](https://github.com/neallester/danake-sw). Currently tested on OSX and Linux.
## Installation
```swift
    .Package(url: "https://github.com/neallester/danake-mongo.git, .branch("master")),
```
## Usage
```
import danakeMongo

let logger = ConsoleLogger()
let mongoAccessor = try MongoAccessor(dbConnectionString: "connectionString", databaseName: "dbName", logger: logger)
```
The **mongoAccessor** object may be used to create a DatabaseAccessor (see  [danake-sw](https://github.com/neallester/danake-sw) for details).

See class [MongoAccessor](https://github.com/neallester/danake-mongo/blob/master/Sources/danakeMongo/danakeMongo.swift) for additional initialization options.



