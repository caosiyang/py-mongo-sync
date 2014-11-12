Python Mongo Sync
====

A MongoDB sync tool that can sync data from a replica-set to other mongos/mongod/replica set. It suppport a oplog-based realtime data sync.  
The soucre must be a member of a replica set and the destination can be a mongos/mongod/replica-set-member.

MongoDB从源端到目标端的数据同步工具，同步过程包括一次全量数据导入和基于oplog的实时数据同步。  
源端是复制集成员，暂不支持Master/Slave；  
目标端可以是mongos/mongod/复制集成员。

## Feature
* real-time sync
* support multiple source
* support synchronization for the specified collections

## Requirement
* source(s) should be one or more replica set(s)

## Usage 
1. edit setting.py to configure
 * source
 * destination
 * username (optional)
 * password (optional)
 * collections (optional)
 * ingore indexes (optional)

2. run program
```python
# python main.py
```
