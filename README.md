# py-mongo-sync

An oplog based realtime sync tool written in Python that could sync data from MongoDB to MongoDB and Elasticsearch.


## Features

- full sync (including data and indexes) and oplog based incremental sync
- sync the specified databases
- sync the specified collections
- sync the sepcified fileds in collections to Elasticsearch
- rename the destination index name for Elasticsearch


## Requirements

- source is a replica set of MongoDB v2.4 or later
- pymongo 3.0 or later
- gevent (for async IO and better performance, optional)


## Notice

- the following databases is ignored
    - admin
    - local
- the following collections is ignored
    - system.users
    - system.profile
- create users for destination manually if necessary
- authenticate with superuser if source authentication is enabled
- sync sharded-cluster by running a process for each shard, first of all, guarantee that balancer is off
- not support geospatial index


## Usage 

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


## Configurations

Use [TOML](https://github.com/toml-lang/toml) as configuration file format.
Refer to [conf_example.toml](example/conf_example.toml).

### src
Source config items.

- src.hosts - hostportstr of a member of replica set
- src.authdb - authentiction database
- src.username - username
- src.password - password

### dst
Destination config items.

- dst.type - Destination type, either 'mongo' or 'es'

- dst.mongo
    - dst.mongo.hosts - MongoDB hostportstr
    - dst.mongo.authdb - MongoDB authentication database
    - dst.mongo.username - MongoDB username
    - dst.mongo.password - MongoDB password

- dst.es
    - dst.es.hosts - Elasticsearch hosts

### sync
Custom sync options.

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


##  Release Notes

- v1.0.0
    - sync data between MongoDB
    - support TokuMX as source

- v2.0.0
    - support sync data from MongoDB to Elasticsearch
    - never support TokuMX as source
    - 'check' tool is inavailable now (TODO)

