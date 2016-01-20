# Python Mongo Sync

MongoDB同步工具，提供基于oplog的实时数据同步。 
源端必须是复制集成员，目标端可以是单实例/复制集/分片集群。注意，源端不支持master/slave成员。


## Feature

- 全量数据或指定集合的实时同步
- 对于指定集合，支持按照query条件进行同步
- 索引同步
- 支持源端和目标端用户认证
- 支持MongoDB v2.4/2.6/3.x


## Requirement

- pymongo 3.x


## Note

- 不同步admin/local数据库
- 不同步system.users/system.profile集合
- 同步过程会执行复制集命令，如果开启认证，建议以具备root权限的数据库用户进行同步
- 用户认证默认在admin数据库完成，且不支持指定其他认证数据库


## Usage 

```bash

# python main.py --help
usage: main.py [-h] --from [FROM] [--src-username [SRC_USERNAME]]
               [--src-password [SRC_PASSWORD]] --to [TO]
               [--dst-username [DST_USERNAME]] [--dst-password [DST_PASSWORD]]
               [--db [DB]] [--coll [COLL]] [--query [QUERY]]
               [--start-optime [START_OPTIME]]
               [--write-concern [WRITE_CONCERN]] [--log [LOG]]

Sync data from a replica-set to another mongod/replica-set/sharded-cluster.

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
  --db [DB]             the database to sync
  --coll [COLL]         the collection to sync
  --query [QUERY]       json query
  --start-optime [START_OPTIME]
                        start optime
  --write-concern [WRITE_CONCERN]
                        write concern, default 1
  --log [LOG]           log file path

```
