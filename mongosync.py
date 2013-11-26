#!/usr/bin/env python

# filename: mongosync.py
# summary: mongo synchronize tool
# author: caosiyang
# date: 2013/09/16

import os
import sys
import types
import time
import shutil
import argparse
from pymongo import MongoClient
from pymongo.database import Database
from utils import *
from mongo_sync_utils import *
from bson.timestamp import Timestamp


class OplogParser:
    @property
    def open(self):
        return True

class MongoSynchronizer:
    def __init__(self, src_host=None, src_port=None, dst_host=None, dst_port=None, dbs=[], **kwargs):
        self.src_host = src_host
        self.src_port = src_port
        self.dst_host = dst_host
        self.dst_port = dst_port
        self.dbs = dbs[:]
        self.last_optime = None
        assert self.src_host
        assert self.src_port
        assert self.dst_host
        assert self.dst_port
        assert self.dbs
        self.username = kwargs.get('username')
        self.password = kwargs.get('password')
        try:
            self.src_mc = MongoClient(self.src_host, self.src_port)
            if self.username and self.password:
                self.src_mc.admin.authenticate(self.username, self.password)
                print 'auth with %s %s' % (self.username, self.password)
            self.dst_mc = MongoClient(self.dst_host, self.dst_port)
        except Exception, e:
            raise e

    def __del__(self):
        self.src_mc.close()
        self.dst_mc.close()

    def run(self):
        """Start synchronizing data.
        """
        # check and initialize config
        self.init_sync_config()

        # get last optime
        self.last_optime = self.get_optime()
        assert self.last_optime
        print 'last optime: ', self.last_optime

        if self.username and self.password:
            res = db_export(self.src_host, self.src_port, username=self.username, password=self.password)
        else:
            res = db_export(self.src_host, self.src_port)
        if not res:
            error_exit('export data failed')
        print 'export data done'

        if not db_import(self.dst_host, self.dst_port, self.dbs):
            error_exit('import data failed')
        print 'import data done'

        # start running real-time synchronaztion
        self.oplog_sync()

    def load_config(self, filepath):
        """Load config.
        """
        pass

    def init_sync_config(self):
        """Initialize synchronization config on destination mongo instance.
        """
        # write 'SyncTo' config
        db = self.dst_mc['local']
        coll = db['qiyi.mongosync_config']
        config = coll.find_one({'_id': 'mongosync'})
        current_source = '%s:%d' % (self.src_host, self.src_port)
        if config:
            old_source = config.get('syncTo')
            if old_source:
                print 'Config has already existed, syncTo %s' % old_source
            if current_source != old_source:
                error('sycnTo config conflicted')
                return False
        else:
            coll.insert({'_id': 'mongosync', 'syncTo': '%s:%d' % (self.src_host, self.src_port)})
            print 'Add new config, syncTo %s:%d' % (self.src_host, self.src_port)

        # create capped collection for store oplog

        return True
    
    def get_optime(self):
        """Get optime of last write operation.
        """
        db = self.src_mc['admin']
        rs_status = db.command({'replSetGetStatus': 1})
        members = rs_status.get('members')
        if not members:
            return None
        for member in members:
            role = member.get('stateStr')
            if role == 'PRIMARY':
                optime = member.get('optime')
                return optime

    def oplog_sync(self):
        db = self.src_mc['local']
        coll = db['oplog.rs']
        cursor = coll.find({'ts': {'$gte': self.last_optime}}, tailable=True)

        # make sure oplog is OK
        if cursor.count == 0 or cursor[0]['ts'] != self.last_optime:
            error('oplog is out of date')
            return False

        n = 0
        while True:
            if not cursor.alive:
                error('cursor is dead')
                break
            try:
                oplog = cursor.next()
                if oplog:
                    n = n + 1
                    print '+' * 40, n
                    #print n
                    print 'op:', oplog['op']
                    # parse
                    ts = oplog['ts']
                    op = oplog['op'] # 'n' or 'i' or 'u' or 'c' or 'd'
                    ns = oplog['ns']
                    try:
                        dbname = ns.split('.', 1)[0]
                        db = self.dst_mc[dbname]
                        if op == 'i': # insert
                            collname = ns.split('.', 1)[1]
                            coll = db[collname]
                            coll.insert(oplog['o'])
                            print 'ns: %s' % ns
                        elif op == 'u': # update
                            collname = ns.split('.', 1)[1]
                            coll = db[collname]
                            coll.update(oplog['o2'], oplog['o'])
                            print 'ns: %s' % ns
                        elif op == 'd': # delete
                            collname = ns.split('.', 1)[1]
                            coll = db[collname]
                            coll.remove(oplog['o'])
                            print 'ns: %s' % ns
                        elif op == 'c': # command
                            db.command(oplog['o'])
                            print 'db: %s' % dbname
                        elif op == 'n': # no-op
                            print 'no-op'
                        else:
                            print 'unknown command: %s' % oplog
                        # update local.qiyi.oplog
                        db = self.dst_mc['local']
                        coll = db['qiyi.mongosync_oplog']
                        coll.insert(oplog)
                        print 'apply oplog done: %s' % oplog
                    except Exception, e:
                        print e
                        error('apply oplog failed: %s' % oplog)
            except Exception, e:
                time.sleep(0.1)

def parse_args():
    """Parse and check arguments.
    """
    parser = argparse.ArgumentParser(description='Synchronization from a replicaSet to another mongo instance.')
    parser.add_argument('--from', nargs='?', required=True, help='the source mongo instance')
    parser.add_argument('--to', nargs='?', required=True, help='the destination mongo instance')
    parser.add_argument('--db', nargs='+', required=True, help='the names of databases to be synchronized')
    parser.add_argument('--oplog', action='store_true', help='enable continuous synchronization')
    parser.add_argument('--username', nargs='?', required=False, help='username')
    parser.add_argument('--password', nargs='?', required=False, help='password')
    args = vars(parser.parse_args())
    src_host = args['from'].split(':', 1)[0]
    src_port = int(args['from'].split(':', 1)[1])
    dst_host = args['to'].split(':', 1)[0]
    dst_port = int(args['to'].split(':', 1)[1])
    db = args['db']
    username = args['username']
    password = args['password']
    assert src_host
    assert src_port
    assert dst_host
    assert dst_port
    assert db
    return src_host, src_port, dst_host, dst_port, db, username, password

def main():
    # parse and check arguments
    src_host, src_port, dst_host, dst_port, db, username, password = parse_args()

    #syncer = MongoSynchronizer('servicecloud-test-dev09.qiyi.virtual', 27018, '127.0.0.1', 27017, db)
    syncer = MongoSynchronizer(src_host, src_port, dst_host, dst_port, db, username=username, password=password)
    syncer.run()
    sys.exit(0)

if __name__ == '__main__':
    main()
