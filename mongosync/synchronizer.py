import sys
import time
import datetime
import exceptions
from mongosync.config import Config
from mongosync.logger import Logger
from mongosync.mongo_utils import get_optime, gen_namespace
from mongosync.optime_logger import OptimeLogger

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

        if conf.optime_logfilepath:
            self._optime_logger = OptimeLogger(conf.optime_logfilepath)
        else:
            self._optime_logger = None
        self._optime_log_interval = 10  # default 10s
        self._last_optime = None  # optime of the last oplog has been replayed
        self._last_optime_logtime = time.time()

        self._log_interval = 2  # default 2s
        self._last_logtime = time.time()  # use in oplog replay

    @property
    def from_to(self):
        return "%s => %s" % (self._conf.src_hostportstr, self._conf.dst_hostportstr)

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
            oplog_start = self._conf.start_optime
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
            if self._optime_logger:
                self._optime_logger.write(oplog_start)
                log.info('first %s' % oplog_start)
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

    def _log_progress(self, tag=''):
        """ Print progress periodically.
        """
        now = time.time()
        if now - self._last_logtime >= self._log_interval:
            delay = now - self._last_optime.time
            time_unit = 'second' if delay <= 1 else 'seconds'
            if tag:
                log.info('%s - sync to %s - %d %s delay - %s - %s' % (self.from_to,
                                                                      datetime.datetime.fromtimestamp(self._last_optime.time),
                                                                      delay,
                                                                      time_unit,
                                                                      self._last_optime,
                                                                      tag))
            else:
                log.info('%s - sync to %s - %d %s delay - %s' % (self.from_to,
                                                                 datetime.datetime.fromtimestamp(self._last_optime.time),
                                                                 delay,
                                                                 time_unit,
                                                                 self._last_optime))
            self._last_logtime = now

    def _log_optime(self, optime):
        """ Record optime periodically.
        """
        if not self._optime_logger:
            return
        now = time.time()
        if now - self._last_optime_logtime >= self._optime_log_interval:
            self._optime_logger.write(optime)
            self._last_optime_logtime = now
            log.info("flush optime into file '%s': %s" % (self._optime_logger.filepath, optime))
