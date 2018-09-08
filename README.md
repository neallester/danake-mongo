# danake-mongo
A Mongo DB Accessor for the [danake-sw persistence framework](https://github.com/neallester/danake-sw). Currently tested on OSX and Ubuntu 16.04.
## Installation
1. Install the [MongoDb C Driver](https://github.com/mongodb/mongo-swift-driver#first-install-the-mongodb-c-driver)
1. Add the following your project's Package.swift package description:
```swift
    .Package(url: "https://github.com/neallester/danake-mongo.git, .branch("master")),
```
## Usage
```
import danakeMongo

let logger = ConsoleLogger()
let mongoAccessor = try MongoAccessor(dbConnectionString: "connectionString", databaseName: "dbName", logger: logger)
```
The **mongoAccessor** object may be used to create a Danake Framework Database (see  [danake-sw](https://github.com/neallester/danake-sw) for details).

See class [MongoAccessor](https://github.com/neallester/danake-mongo/blob/master/Sources/danakeMongo/danakeMongo.swift) for additional initialization options.

**Important Note:** Call MongoSwift.cleanup() exactly once at end of application execution.



