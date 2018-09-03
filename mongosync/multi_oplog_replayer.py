import pymongo
import gevent
import mmh3
import mongo_utils
from mongosync.db import Mongo
from mongosync.logger import Logger

log = Logger.get()


class OplogVector:
    """ A set of oplogs with same namespace.
    """
    def __init__(self):
        self._dbname = ''
        self._collname = ''
        self._oplogs = []

class MultiOplogReplayer:
    """ Concurent oplog replaying for MongoDB.
    """
    def __init__(self, mc, n_writers):
        assert isinstance(mc, Mongo)
        assert n_writers > 0
        self._mc = mc
        self._pool = gevent.pool.Pool(n_writers)
        self._map = {}
        self._count = 0
        self._last_optime = None

    def clear(self):
        self._map.clear()
        self._count = 0

    #def push(self, oplog):
    #    op = oplog['op']
    #    ns = oplog['ns']
    #    if ns not in self._map:
    #        self._map[ns] = []
    #    if op == 'u':
    #        self._map[ns].append(pymongo.UpdateOne({'_id': oplog['o2']['_id']}, oplog['o']))
    #    elif op == 'i':
    #        self._map[ns].append(pymongo.ReplaceOne({'_id': oplog['o']['_id']}, oplog['o'], upsert=True))
    #    elif op == 'd':
    #        self._map[ns].append(pymongo.DeleteOne({'_id': oplog['o']['_id']}))
    #    else:
    #        log.error('invaid op: %s' % oplog)
    #        assert op in ['i', 'u', 'd']
    #    self._count += 1
    #    self._last_optime = oplog['ts']

    def push(self, oplog):
        op = oplog['op']
        ns = oplog['ns']
        if ns not in self._map:
            self._map[ns] = []
        self._map[ns].append(oplog)
        self._count += 1
        self._last_optime = oplog['ts']

    #def apply(self):
    #    for ns, reqs in self._map.iteritems():
    #        dbname, collname = mongo_utils.parse_namespace(ns)
    #        self._pool.spawn(self._mc.bulk_write, dbname, collname, reqs)
    #    self._pool.join()

    def apply(self):
        oplog_vecs = []
        for ns, oplogs in self._map.iteritems():
            dbname, collname = mongo_utils.parse_namespace(ns)
            n = len(oplogs) / 40 + 1  # 40 is empiric value
            if n == 1:
                vec = OplogVector()
                vec._dbname = dbname
                vec._collname = collname
                for oplog in oplogs:
                    op = oplog['op']
                    if op == 'u':
                        is_replace = True
                        for key in oplog['o'].iterkeys():
                            if key and key[0] == '$':
                                is_replace = False
                        if not is_replace:
                            vec._oplogs.append(pymongo.UpdateOne({'_id': oplog['o2']['_id']}, oplog['o']))
                        else:
                            vec._oplogs.append(pymongo.ReplaceOne({'_id': oplog['o2']['_id']}, oplog['o'], upsert=True))
                    elif op == 'i':
                        vec._oplogs.append(pymongo.ReplaceOne({'_id': oplog['o']['_id']}, oplog['o'], upsert=True))
                    elif op == 'd':
                        vec._oplogs.append(pymongo.DeleteOne({'_id': oplog['o']['_id']}))
                    else:
                        log.error('invaid op: %s' % oplog)
                        assert op in ['i', 'u', 'd']
                oplog_vecs.append(vec)
            else:
                vecs = []
                for i in range(0, n):
                    vec = OplogVector()
                    vec._dbname = dbname
                    vec._collname = collname
                    vecs.append(vec)
                for oplog in oplogs:
                    op = oplog['op']
                    if op == 'u':
                        m = self.hash(oplog['o2']['_id'])
                        is_replace = True
                        for key in oplog['o'].iterkeys():
                            if key and key[0] == '$':
                                is_replace = False
                                break
                        if not is_replace:
                            vecs[m % n]._oplogs.append(pymongo.UpdateOne({'_id': oplog['o2']['_id']}, oplog['o']))
                        else:
                            vecs[m % n]._oplogs.append(pymongo.ReplaceOne({'_id': oplog['o2']['_id']}, oplog['o'], upsert=True))
                    elif op == 'i':
                        m = self.hash(oplog['o']['_id'])
                        vecs[m % n]._oplogs.append(pymongo.ReplaceOne({'_id': oplog['o']['_id']}, oplog['o'], upsert=True))
                    elif op == 'd':
                        m = self.hash(oplog['o']['_id'])
                        vecs[m % n]._oplogs.append(pymongo.DeleteOne({'_id': oplog['o']['_id']}))
                    else:
                        log.error('invaid op: %s' % oplog)
                        assert op in ['i', 'u', 'd']
                oplog_vecs.extend(vecs)

        for vec in oplog_vecs:
            if vec._oplogs:
                self._pool.spawn(self._mc.bulk_write, vec._dbname, vec._collname, vec._oplogs)
        self._pool.join()

    def hash(self, oid):
        """ Hash ObjectID.
        """
        try:
            # str(oid) may contain non-ascii characters
            m = mmh3.hash(str(oid), signed=False)
        except:
            m = 0
        return m

    def count(self):
        return self._count

    def last_optime(self):
        return self._last_optime
