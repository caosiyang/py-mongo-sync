import pymongo
import gevent
import mmh3
import mongo_utils
from mongosync.db import Mongo
from mongosync.logger import Logger

log = Logger.get()


class OplogVector(object):
    """ A set of oplogs with same namespace.
    """
    def __init__(self, dbname, collname):
        self._dbname = dbname
        self._collname = collname
        self._oplogs = []


class MultiOplogReplayer(object):
    """ Concurrent oplog replayer for MongoDB.
    """
    def __init__(self, mc, n_writers=10, batch_size=40):
        """
        Parameter:
          - n_writers: maximum coroutine count
          - batch_size: maximum oplog count in a batch, 40 is empiric value
        """
        assert isinstance(mc, Mongo)
        assert n_writers > 0
        assert batch_size > 0
        self._mc = mc
        self._pool = gevent.pool.Pool(n_writers)
        self._batch_size = batch_size
        self._map = {}
        self._count = 0
        self._last_optime = None

    def clear(self):
        """ Clear oplogs.
        """
        self._map.clear()
        self._count = 0

    def push(self, oplog):
        """ Push oplog and group by namespace.
        """
        ns = oplog['ns']
        if ns not in self._map:
            self._map[ns] = []
        self._map[ns].append(oplog)
        self._count += 1
        self._last_optime = oplog['ts']

    def apply(self):
        """ Apply oplogs.
        """
        oplog_vecs = []
        for ns, oplogs in self._map.iteritems():
            dbname, collname = mongo_utils.parse_namespace(ns)
            n = len(oplogs) / self._batch_size + 1
            if n == 1:
                vec = OplogVector(dbname, collname)
                for oplog in oplogs:
                    op = self.__convert(oplog)
                    assert op is not None
                    vec._oplogs.append(op)
                oplog_vecs.append(vec)
            else:
                vecs = [OplogVector(dbname, collname) for i in xrange(n)]
                for oplog in oplogs:
                    op = self.__convert(oplog)
                    assert op is not None
                    # filter of UpdateOne/ReplaceOne/DeleteOne is {'_id': ObjectID}
                    # @ref https://github.com/mongodb/mongo-python-driver/blob/master/pymongo/operations.py
                    m = self.__hash(op._filter['_id'])
                    vecs[m % n]._oplogs.append(op)
                oplog_vecs.extend(vecs)

        for vec in oplog_vecs:
            if vec._oplogs:
                self._pool.spawn(self._mc.bulk_write, vec._dbname, vec._collname, vec._oplogs)
        self._pool.join()

    def count(self):
        """ Return count of oplogs.
        """
        return self._count

    def last_optime(self):
        """ Return timestamp of the last oplog.
        """
        return self._last_optime

    def __convert(self, oplog):
        """ Convert oplog to operation that supports bulk write.
        """
        op = oplog['op']
        if op == 'u':
            # it could be an update or replace
            # @ref https://docs.mongodb.com/manual/reference/limits/#naming-restrictions
            is_update = False
            for key in oplog['o'].iterkeys():
                if key[0] == '$':
                    is_update = True
                    break
            if is_update:
                return pymongo.operations.UpdateOne({'_id': oplog['o2']['_id']}, oplog['o'])
            else:
                return pymongo.operations.ReplaceOne({'_id': oplog['o2']['_id']}, oplog['o'], upsert=True)
        elif op == 'i':
            return pymongo.operations.ReplaceOne({'_id': oplog['o']['_id']}, oplog['o'], upsert=True)
        elif op == 'd':
            return pymongo.operations.DeleteOne({'_id': oplog['o']['_id']})
        else:
            log.error('invaid op: %s' % oplog)
            return None

    def __hash(self, oid):
        """ Hash ObjectID with murmurhash3.
        """
        try:
            # str(oid) may contain non-ascii characters
            m = mmh3.hash(str(oid), signed=False)
        except Exception as e:
            m = 0
        return m
