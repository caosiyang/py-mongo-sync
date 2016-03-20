# py-mongo-sync

A oplog based realtime sync tool for MongoDB written in Python.
Source should be a member of replica set and destination could be a mongod/mongos instance or member of replica set.


## Feature

- full sync including data and indexes
- oplog based increment sync
- a specified collection sync with an optional query
- support authentication for source and destination
- source engine could be MongoDB(v2.4 or later) and TokuMX


## Note

- the following databases are ignored: 'admin', 'local'
- the following collections are ignored: 'system.users', 'system.profile'
- you need to create users for destination manually
- if authencation is enabled, you need to specify a root user(authenticate on 'admin')


## Not Supported

- geospatial index
- users information


## Dependency

- pymongo 3.0 or later


## Usage 

```bash
# python main.py --help

usage: main.py [-h] --from [FROM] [--src-username [SRC_USERNAME]]
               [--src-password [SRC_PASSWORD]] [--src-engine [SRC_ENGINE]]
               --to [TO] [--dst-username [DST_USERNAME]]
               [--dst-password [DST_PASSWORD]] [--db [DB]] [--coll [COLL]]
               [--query [QUERY]] [--start-optime [START_OPTIME]]
               [--write-concern [WRITE_CONCERN]] [--log [LOG]]

Sync data from a replica-set to another mongod/replica-set/sharded-cluster.

optional arguments:
  -h, --help            show this help message and exit
  --from [FROM]         the source must be a mongod instance of replica-set
  --src-username [SRC_USERNAME]
                        src username
  --src-password [SRC_PASSWORD]
                        src password
  --src-engine [SRC_ENGINE]
                        src engine, the value could be mongodb or tokumx,
                        default is mongodb
  --to [TO]             the destionation should be a mongos or mongod instance
  --dst-username [DST_USERNAME]
                        dst username
  --dst-password [DST_PASSWORD]
                        dst password
  --db [DB]             the database to sync
  --coll [COLL]         the collection to sync
  --query [QUERY]       json query
  --start-optime [START_OPTIME]
                        start optime, a timestamp value in second for MongoDB
                        or a 'YYYYmmddHHMMSS' value for TokuMX
  --write-concern [WRITE_CONCERN]
                        write concern, default 1
  --log [LOG]           log file path


usage: main.py [-h] --from [FROM] [--src-username [SRC_USERNAME]]
               [--src-password [SRC_PASSWORD]] --to [TO]
               [--dst-username [DST_USERNAME]] [--dst-password [DST_PASSWORD]]
               [--db [DB]] [--coll [COLL]] [--query [QUERY]]
               [--start-optime [START_OPTIME]]
               [--write-concern [WRITE_CONCERN]] [--log [LOG]]

```

```bash
# python check_consistency.py --help
usage: check_consistency.py [-h] --from [FROM] [--src-username [SRC_USERNAME]]
                            [--src-password [SRC_PASSWORD]] --to [TO]
                            [--dst-username [DST_USERNAME]]
                            [--dst-password [DST_PASSWORD]]

Check data consistency including data and indexes.

optional arguments:
  -h, --help            show this help message and exit
  --from [FROM]         the source must be a mongod instance of replica-set
  --src-username [SRC_USERNAME]
                        src username
  --src-password [SRC_PASSWORD]
                        src password
  --to [TO]             the destionation should be a mongos or mongod instance
  --dst-username [DST_USERNAME]
                        dst username
  --dst-password [DST_PASSWORD]
                        dst password
```
