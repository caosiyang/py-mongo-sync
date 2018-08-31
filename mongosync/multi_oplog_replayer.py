import pymongo
import gevent
import mongo_utils
from mongosync.db import Mongo
from mongosync.logger import Logger

log = Logger.get()


class MultiOplogReplayer:
    """ Concurent oplog replaying for MongoDB.
    """
    def __init__(self, mc, n_writers):
        assert isinstance(mc, Mongo)
        assert n_writers > 0
        self._mc = mc
        self._writers = n_writers
        self._pool = gevent.pool.Pool(self._writers)
        self._map = {}
        self._count = 0
        self._last_optime = None

    def clear(self):
        self._map.clear()
        self._count = 0

    def push(self, oplog):
        op = oplog['op']
        ns = oplog['ns']
        if ns not in self._map:
            self._map[ns] = []
        if op == 'u':
            self._map[ns].append(pymongo.UpdateOne({'_id': oplog['o2']['_id']}, oplog['o']))
        elif op == 'i':
            self._map[ns].append(pymongo.ReplaceOne({'_id': oplog['o']['_id']}, oplog['o'], upsert=True))
        elif op == 'd':
            self._map[ns].append(pymongo.DeleteOne({'_id': oplog['o']['_id']}))
        else:
            log.error('invaid op: %s' % oplog)
            assert op in ['i', 'u', 'd']
        self._count += 1
        self._last_optime = oplog['ts']

    def apply(self):
        for ns, reqs in self._map.iteritems():
            dbname, collname = mongo_utils.parse_namespace(ns)
            self._pool.spawn(self._mc.bulk_write, dbname, collname, reqs)
        self._pool.join()

    def count(self):
        return self._count

    def last_optime(self):
        return self._last_optime
