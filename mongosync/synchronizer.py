import sys
import time
import datetime
import exceptions
import bson
from mongosync.config import Config
from mongosync.logger import Logger
from mongo_utils import get_optime, gen_namespace

try:
    import gevent
except ImportError:
    pass

log = Logger.get()


class Synchronizer(object):
    """ Common synchronizer.

    Other synchronizer entities should implement methods:
        - __init__
        - __del__
        - _sync_database
        - _sync_collection
        - _sync_oplog
    """
    def __init__(self, conf):
        if not isinstance(conf, Config):
            raise Exception('invalid config type')
        self._conf = conf

        self._ignore_dbs = ['admin', 'local']
        self._ignore_colls = ['system.indexes', 'system.profile', 'system.users']
        self._last_optime = None  # optime of the last oplog has been replayed
        self._last_logtime = time.time()  # use in oplog replay
        self._log_interval = 1  # default 1s

    @property
    def from_to(self):
        return "%s => %s" % (self._conf.src_conf.hosts, self._conf.dst_conf.hosts)

    @property
    def log_interval(self):
        return self._log_interval

    @log_interval.setter
    def log_interval(self, n_secs):
        if n_secs < 0:
            n_secs = 0
        self._log_interval = n_secs

    def run(self):
        """ Start to sync.
        """
        # never drop database automatically
        # you should clear the databases manually if necessary
        try:
            self._sync()
        except exceptions.KeyboardInterrupt:
            log.info('keyboard interrupt')

    def _sync(self):
        """ Sync databases and oplog.
        """
        if self._conf.start_optime:
            # TODO optimize
            log.info("locating oplog, it will take a while")
            oplog_start = bson.timestamp.Timestamp(int(self._conf.start_optime), 0)
            doc = self._src.client()['local']['oplog.rs'].find_one({'ts': {'$gte': oplog_start}})
            if not doc:
                log.error('no oplogs newer than the specified oplog')
                return
            oplog_start = doc['ts']
            log.info('start timestamp is %s actually' % oplog_start)
            self._last_optime = oplog_start
            self._sync_oplog(oplog_start)
        else:
            oplog_start = get_optime(self._src.client())
            if not oplog_start:
                log.error('get oplog_start failed, terminate')
                sys.exit(1)
            self._last_optime = oplog_start
            self._sync_databases()
            self._sync_oplog(oplog_start)

    def _sync_databases(self):
        """ Sync databases excluding 'admin' and 'local'.
        """
        host, port = self._src.client().address
        log.info('sync databases from %s:%d' % (host, port))
        for dbname in self._src.client().database_names():
            if dbname in self._ignore_dbs:
                log.info("skip database '%s'" % dbname)
                continue
            if not self._conf.data_filter.valid_db(dbname):
                log.info("skip database '%s'" % dbname)
                continue
            self._sync_database(dbname)
        log.info('all databases done')

    def _sync_database(self, dbname):
        """ Sync a database.
        """
        raise Exception('you should implement %s.%s' % (self.__class__.__name__, self._sync_database.__name__))

    def _sync_collections(self, dbname):
        """ Sync collections in the database excluding system collections.
        """
        collnames = self._src.client()[dbname].collection_names(include_system_collections=False)
        for collname in collnames:
            if collname in self._ignore_colls:
                log.info("skip collection '%s'" % gen_namespace(dbname, collname))
                continue
            if not self._conf.data_filter.valid_coll(dbname, collname):
                log.info("skip collection '%s'" % gen_namespace(dbname, collname))
                continue
            self._sync_collection(dbname, collname)

    def _sync_collection(self, dbname, collname):
        """ Sync a collection until success.
        """
        raise Exception('you should implement %s.%s' % (self.__class__.__name__, self._sync_collection.__name__))

    def _sync_oplog(self, oplog_start):
        """ Replay oplog.
        """
        raise Exception('you should implement %s.%s' % (self.__class__.__name__, self._sync_oplog.__name__))

    def _print_progress(self):
        """ Print progress.
        """
        now = time.time()
        if now - self._last_logtime > self._log_interval:
            log.info('sync to %s, %s, %s' % (datetime.datetime.fromtimestamp(self._last_optime.time), self.from_to, self._last_optime))
            self._last_logtime = now
