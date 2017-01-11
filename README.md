# py-mongo-sync

A oplog based realtime sync tool for MongoDB written in Python.
Source should be a member of replica set and destination could be a mongod/replica-set/sharded-cluster.


## Features

- full sync including data and indexes and oplog based increment sync
- sync the specified databases or collections
- source could be MongoDB(v2.4 or later) and TokuMX


## Notes

- the following databases are ignored: 'admin', 'local'
- the following collections are ignored: 'system.users', 'system.profile'
- you need to create users for destination manually
- if authencation is enabled, you should use a root user to authenticate


## Not Supported

- geospatial index
- users information


## Dependency

- pymongo 3.0 or later


## Usage 

### sync

```bash
usage: sync.py [-h] --from [FROM] [--src-authdb [SRC_AUTHDB]]
               [--src-username [SRC_USERNAME]] [--src-password [SRC_PASSWORD]]
               [--src-engine [SRC_ENGINE]] --to [TO]
               [--dst-authdb [DST_AUTHDB]] [--dst-username [DST_USERNAME]]
               [--dst-password [DST_PASSWORD]] [--dbs DBS [DBS ...]]
               [--colls COLLS [COLLS ...]] [--start-optime [START_OPTIME]]
               [--write-concern [WRITE_CONCERN]] [--log [LOG]]

Sync data from a replica-set to another mongod/replica-set/sharded-cluster.

optional arguments:
  -h, --help            show this help message and exit
  --from [FROM]         the source must be a member of replica-set
  --src-authdb [SRC_AUTHDB]
                        authentication database, default is 'admin'
  --src-username [SRC_USERNAME]
                        src username
  --src-password [SRC_PASSWORD]
                        src password
  --src-engine [SRC_ENGINE]
                        src engine, the value could be mongodb or tokumx,
                        default is mongodb
  --to [TO]             the destionation should be a mongos or mongod instance
  --dst-authdb [DST_AUTHDB]
                        authentication database, default is 'admin'
  --dst-username [DST_USERNAME]
                        dst username
  --dst-password [DST_PASSWORD]
                        dst password
  --dbs DBS [DBS ...]   databases to sync, conflict with --colls
  --colls COLLS [COLLS ...]
                        collections to sync, conflict with --dbs
  --start-optime [START_OPTIME]
                        start optime, a timestamp value in second for MongoDB
                        or a 'YYYYmmddHHMMSS' value for TokuMX
  --write-concern [WRITE_CONCERN]
                        write concern, default 1
  --log [LOG]           log file path

```

### validate

```bash
usage: check_consistency.py [-h] --from [FROM] [--src-authdb [SRC_AUTHDB]]
                            [--src-username [SRC_USERNAME]]
                            [--src-password [SRC_PASSWORD]] --to [TO]
                            [--dst-authdb [DST_AUTHDB]]
                            [--dst-username [DST_USERNAME]]
                            [--dst-password [DST_PASSWORD]]
                            [--dbs DBS [DBS ...]]

Check data consistency including data and indexes.

optional arguments:
  -h, --help            show this help message and exit
  --from [FROM]         the source must be a mongod instance of replica-set
  --src-authdb [SRC_AUTHDB]
                        authentication database, default is 'admin'
  --src-username [SRC_USERNAME]
                        src username
  --src-password [SRC_PASSWORD]
                        src password
  --to [TO]             the destionation should be a mongos or mongod instance
  --dst-authdb [DST_AUTHDB]
                        authentication database, default is 'admin'
  --dst-username [DST_USERNAME]
                        dst username
  --dst-password [DST_PASSWORD]
                        dst password
  --dbs DBS [DBS ...]   databases to check
```

