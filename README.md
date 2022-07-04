# py-mongo-sync

It synchronizes data from a replica set to another MongoDB deployment, e.g., standalone, replica set, and sharded cluster.

It's oplog-based and provides a realtime data synchronization.

It's written in Python 2.7.

## Support

- MongoDB 2.4
- MongoDB 2.6
- MongoDB 3.0
- MongoDB 3.2
- MongoDB 3.4

## Features

- initial sync and oplog based incremental sync
- sync the specified databases and collections
- concurrent oplog replaying

## Requirements

See [requirements](./requirements.txt) for details.

- gevent
- toml
- mmh3
- pymongo

    Always use pymongo 3.5.1.

    Refer to [https://api.mongodb.com/python/3.6.0/changelog.html](https://api.mongodb.com/python/3.6.0/changelog.html)

    > Version 3.6 adds support for MongoDB 3.6, drops support for CPython 3.3 (PyPy3 is still supported), and drops support for MongoDB versions older than 2.6. If connecting to a MongoDB 2.4 server or older, PyMongo now throws a ConfigurationError.

## Notice

- source **MUST** be a replica set
- ignore system databases
    - admin
    - local
- ignore system collections
    - system.\*
- create users for destination manually if necessary
- suggest to authenticate with administrator if source enabled authentication
- not support geospatial index

if the source is a sharded cluster

- first, stop the balancer
- then, start a seprate sync process for each shard

## Configurations

Use [TOML](https://github.com/toml-lang/toml) as configuration file format.

Refer to [mongo_conf.toml](example/mongo_conf.toml).

### src

Source config items.

- src.hosts - hostportstr of a member of replica set
- src.username - username
- src.password - password
- src.authdb - authentiction database

### dst

Destination config items.

- dst.mongo.hosts
- dst.mongo.authdb
- dst.mongo.username
- dst.mongo.password

### sync

Custom options for synchronization.

`sync.dbs` specfies the databases to sync.
`sync.dbs.colls` specifies the collections to sync.

- sync.dbs - databases to sync, sync all databases if not specify
    - sync.dbs.db - source database name
    - sync.dbs.rename_db - destination database name, stay the same if not specify
    - sync.dbs.colls - collectons to sync, sync all collections if not specify

`coll` in `sync.dbs.colls` element specifies the collection to sync.
`fileds` in `sync.dbs.colls` element specifies the fields of current collection to sync.

### log

- log.filepath - log file path, write to stdout if empty or not set

## Usage 

Command options has functional limitations.
It's strongly recommended that use config file.

### sync

```bash
usage: sync.py [-h] [-f [CONFIG]] [--src [SRC]] [--src-authdb [SRC_AUTHDB]]
               [--src-username [SRC_USERNAME]] [--src-password [SRC_PASSWORD]]
               [--dst [DST]] [--dst-authdb [DST_AUTHDB]]
               [--dst-username [DST_USERNAME]] [--dst-password [DST_PASSWORD]]
               [--start-optime [START_OPTIME]]
               [--optime-logfile [OPTIME_LOGFILE]] [--logfile [LOGFILE]]

Sync data from a replica-set to another MongoDB/Elasticsearch.

optional arguments:
  -h, --help            show this help message and exit
  -f [CONFIG], --config [CONFIG]
                        configuration file, note that command options will
                        override items in config file
  --src [SRC]           source should be hostportstr of a replica-set member
  --src-authdb [SRC_AUTHDB]
                        src authentication database, default is 'admin'
  --src-username [SRC_USERNAME]
                        src username
  --src-password [SRC_PASSWORD]
                        src password
  --dst [DST]           destination should be hostportstr of a mongos or
                        mongod instance
  --dst-authdb [DST_AUTHDB]
                        dst authentication database, default is 'admin', for
                        MongoDB
  --dst-username [DST_USERNAME]
                        dst username, for MongoDB
  --dst-password [DST_PASSWORD]
                        dst password, for MongoDB
  --start-optime [START_OPTIME]
                        timestamp in second, indicates oplog based increment
                        sync
  --optime-logfile [OPTIME_LOGFILE]
                        optime log file path, use this as start optime if
                        without '--start-optime'
  --logfile [LOGFILE]   log file path

```

## TODO List

- [ ] command options tuning
- [ ] config file format tuning
- [ ] sync sharding config (enableSharding & shardCollection)
