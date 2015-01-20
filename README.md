# Python Mongo Sync

A MongoDB sync tool that can sync data from a replica-set to other mongos/mongod/replica set. It suppport a oplog-based realtime data sync.  
The soucre must be a member of a replica set and the destination can be a mongos/mongod/replica-set-member.

MongoDB从源端到目标端的数据同步工具，同步过程包括全量数据导入和基于oplog的实时数据同步。  
源端是复制集成员，暂不支持Master/Slave；目标端是mongos/mongod/复制集成员。


## Feature

* 支持实时同步
* 支持全量数据或指定集合的同步（TODO指定数据库）
* 如果指定集合，允许按照query条件同步
* 全量同步过程根据实际情况选择批量写入或采用多进程并发写，提高同步效率


## Requirement

* 源端必须是复制集成员


## Usage 

```
# python main.py -h
usage: main.py [-h] --from [FROM] --to [TO] [--db [DB]] [--coll [COLL]]
               [--query [QUERY]] [--log [LOG]]

Sync data from a replica-set to another mongos/mongod instance.

optional arguments:
  -h, --help       show this help message and exit
  --from [FROM]    the source must be a mongod instance of replica-set
  --to [TO]        the destionation should be a mongos or mongod instance
  --db [DB]        the database to sync
  --coll [COLL]    the collection to sync
  --query [QUERY]  json query
  --log [LOG]      log file path
```

## TODO

* 支持指定数据库的同步
* 提高容错性，同步过程遇到服务或网络中断，自动重连
* 对于集合内有唯一索引的情况，自动解决DuplicateKey问题
* 支持自定义oplog同步实现
