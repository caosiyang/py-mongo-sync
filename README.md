# Python Mongo Sync

MongoDB同步工具，提供基于oplog的实时数据同步。  
源端是复制集成员，目标端可以是单实例/复制集/分片集群。注意，源端不支持master/slave成员。
全面监控MongoDB v3.0


## Feature

- 支持实时同步
- 支持全量数据或指定集合的同步
- 如果指定集合，允许按照query条件同步
- 全量同步过程根据实际情况选择批量写入或采用多进程并发写，提高同步效率


## Requirement

- pymongo 3.0


## Usage 

```bash
# python main.py -h
usage: main.py [-h] --from [FROM] --to [TO] [--db [DB]] [--coll [COLL]]
               [--query [QUERY]] [--start-optime [START_OPTIME]]
               [--write-concern [WRITE_CONCERN]] [--log [LOG]]

Sync data from a replica-set to another mongod/replica-set/sharded-cluster.

optional arguments:
  -h, --help            show this help message and exit
  --from [FROM]         the source must be a mongod instance of replica-set
  --to [TO]             the destionation should be a mongos or mongod instance
  --db [DB]             the database to sync
  --coll [COLL]         the collection to sync
  --query [QUERY]       query, JSON format
  --start-optime [START_OPTIME]
                        start optime
  --write-concern [WRITE_CONCERN]
                        write concern, default = 1
  --log [LOG]           log file path

```
