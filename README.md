# py-mongo-sync

An oplog-based realtime sync tool for MongoDB written in Python.


## Features

- full sync including data and indexes
- oplog-based incremental sync
- sync the specified databases
- sync the specified collections
- support MongoDB(v2.4 or later) and TokuMX


## Notes

- source must be a replica set
- Ignore the following databases:
    - admin
    - local
- ignore the following collections
    - system.users
    - system.profile
- create users for destination manually if necessary
- run as superuser through setting '--src-username' and '--src-password' if source authentication is enabled
- you could sync a sharded-cluster by running a process for each shard, first of all, guarantee that balancer is off


## Not Supported

- geospatial index


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
               [--colls COLLS [COLLS ...]] [--src-db [SRC_DB]]
               [--dst-db [DST_DB]] [--start-optime [START_OPTIME]]
               [--log [LOG]]

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
  --to [TO]             the destination should be a mongos or mongod instance
  --dst-authdb [DST_AUTHDB]
                        authentication database, default is 'admin'
  --dst-username [DST_USERNAME]
                        dst username
  --dst-password [DST_PASSWORD]
                        dst password
  --dbs DBS [DBS ...]   databases to sync, conflict with --colls
  --colls COLLS [COLLS ...]
                        collections to sync, conflict with --dbs
  --src-db [SRC_DB]     src database name, work with '--dst-db', conflict with
                        '--dbs' and '--colls'
  --dst-db [DST_DB]     dst database name, work with '--src-db', conflict with
                        '--dbs' and '--colls'
  --start-optime [START_OPTIME]
                        start optime, a timestamp value in second for MongoDB
                        or a 'YYYYmmddHHMMSS' value for TokuMX
  --log [LOG]           log file path

```

### check

```bash
usage: check.py [-h] --from [FROM] [--src-authdb [SRC_AUTHDB]]
                [--src-username [SRC_USERNAME]]
                [--src-password [SRC_PASSWORD]] --to [TO]
                [--dst-authdb [DST_AUTHDB]] [--dst-username [DST_USERNAME]]
                [--dst-password [DST_PASSWORD]] [--dbs DBS [DBS ...]]
                [--src-db [SRC_DB]] [--dst-db [DST_DB]]

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
  --src-db [SRC_DB]     src database to check, work with '--dst-db', conflict
                        with '--dbs'
  --dst-db [DST_DB]     dst database to check, work with '--src-db', conflict
                        with '--dbs'
```

