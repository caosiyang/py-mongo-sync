Python Mongo Sync
====

A MongoDB sync tool that can sync data from a replica-set to other mongos/mongod/replica set. It suppport a oplog-based realtime data sync.  
The soucre must be a member of a replica set and the destination can be a mongos/mongod/replica-set-member.

MongoDB从源端到目标端的数据同步工具，同步过程包括一次全量数据导入和基于oplog的实时数据同步。  
源端是复制集成员，暂不支持Master/Slave；  
目标端可以是mongos/mongod/复制集成员。

## Usage
1.edit setting.py to configure the source and destination  
编辑setting.py，配置源端和目标端

2.run program  
运行程序，执行main.py
```
# python main.py
```
