import sys
import time
import datetime
import exceptions
import gevent
from mongosync.config import Config
from mongosync.logger import Logger
from mongosync.mongo_utils import get_optime
from mongosync.optime_logger import OptimeLogger
from mongosync.progress_logger import LoggerThread

log = Logger.get()


class Stage(object):
    STOPPED = 0
    INITIAL_SYNC = 1
    POST_INITIAL_SYNC = 2
    OPLOG_SYNC = 3


class CommonSyncer(object):
    """ Common database synchronizer.

    Specific database synchronizer should implement the following methods:
        - __init__
        - _initial_sync
        - _sync_collection
        - _sync_large_collection
        - _replay_oplog
    """
    def __init__(self, conf):
        if not isinstance(conf, Config):
            raise RuntimeError('invalid config type')
        self._conf = conf

        self._ignore_dbs = ['admin', 'local']
        self._ignore_colls = ['system.indexes', 'system.profile', 'system.users']

        if conf.optime_logfilepath:
            self._optime_logger = OptimeLogger(conf.optime_logfilepath)
        else:
            self._optime_logger = None
        self._optime_log_interval = 10  # default 10s
        self._last_optime = None  # optime of the last oplog was applied
        self._last_optime_logtime = time.time()

        self._log_interval = 2  # default 2s
        self._last_logtime = time.time()  # use in oplog replay

        # for large collections
        self._n_workers = 8  # multi-process
        self._large_coll_docs = 1000000  # 100w

        self._initial_sync_start_optime = None
        self._initial_sync_end_optime = None

        self._stage = Stage.STOPPED

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
        # clear data manually if necessary
        try:
            self._sync()
        except exceptions.KeyboardInterrupt:
            log.info('keyboard interrupt')

    def _sync(self):
        """ Sync databases and oplog.
        """
        if self._conf.start_optime:
            log.info("locating oplog, it will take a while")
            doc = self._src.client()['local']['oplog.rs'].find_one({'ts': {'$gte': self._conf.start_optime}})
            if not doc:
                log.error('oplog is stale')
                return
            start_optime = doc['ts']
            log.info('start timestamp is %s actually' % start_optime)
            self._stage = Stage.OPLOG_SYNC
            self._replay_oplog(start_optime)
        else:
            # initial sync
            self._initial_sync_start_optime = get_optime(self._src.client())
            self._stage = Stage.INITIAL_SYNC

            self._initial_sync()

            self._stage = Stage.POST_INITIAL_SYNC
            self._initial_sync_end_optime = get_optime(self._src.client())

            # oplog sync
            if self._optime_logger:
                self._optime_logger.write(self._initial_sync_start_optime)
            self._replay_oplog(self._initial_sync_start_optime)

    def _collect_colls(self):
        """ Collect collections to sync.
        """
        colls = []
        for dbname in self._src.client().database_names():
            if dbname in self._ignore_dbs:
                continue
            if not self._conf.data_filter.valid_db(dbname):
                continue
            for collname in self._src.client()[dbname].collection_names(include_system_collections=False):
                if collname in self._ignore_colls:
                    continue
                if not self._conf.data_filter.valid_coll(dbname, collname):
                    continue
                colls.append((dbname, collname))
        return colls

    def _split_coll(self, namespace_tuple, n_partitions):
        """ Split a collection into n partitions.

        Return a list of split points.

        splitPointCount = partitionCount - 1
        splitPointCount = keyTotalCount / (keyCount + 1)
        keyCount = maxChunkSize / (2 * avgObjSize)
        =>
        maxChunkSize = (keyTotalCount / (partionCount - 1) - 1) * 2 * avgObjSize

        Note: maxChunkObjects is default 250000.
        """
        if n_partitions <= 1:
            raise RuntimeError('n_partitions need greater than 1, but %s' % n_partitions)

        dbname, collname = namespace_tuple
        ns = '.'.join(namespace_tuple)
        db = self._src.client()[dbname]
        collstats = db.command('collstats', collname)

        if 'avgObjSize' not in collstats:  # empty collection
            return []

        n_points = n_partitions - 1
        max_chunk_size = ((collstats['count'] / (n_partitions - 1) - 1) * 2 * collstats['avgObjSize']) / 1024 / 1024

        if max_chunk_size <= 0:
            return []

        res = db.command('splitVector', ns, keyPattern={'_id': 1}, maxSplitPoints=n_points, maxChunkSize=max_chunk_size, maxChunkObjects=collstats['count'])

        if res['ok'] != 1:
            return []
        else:
            return [doc['_id'] for doc in res['splitKeys']]

    def _initial_sync(self):
        """ Initial sync.
        """
        def classify(ns_tuple, large_colls, small_colls):
            """ Find out large and small collections.
            """
            if self._is_large_collection(ns_tuple):
                points = self._split_coll(ns_tuple, self._n_workers)
                if points:
                    large_colls.append((ns_tuple, points))
                else:
                    small_colls.append(ns_tuple)
            else:
                small_colls.append(ns_tuple)

        large_colls = []
        small_colls = []

        pool = gevent.pool.Pool(8)
        colls = self._collect_colls()
        for ns in colls:
            dbname, collname = ns
            log.info('%d\t%s.%s' % (self._src.client()[dbname][collname].count(), dbname, collname))
            pool.spawn(classify, ns, large_colls, small_colls)
        pool.join()

        if len(large_colls) + len(small_colls) != len(colls):
            raise RuntimeError('classify collections error')

        log.info('large collections: %s' % ['.'.join(ns) for ns, points in large_colls])
        log.info('small collections: %s' % ['.'.join(ns) for ns in small_colls])

        # create progress logger
        self._progress_logger = LoggerThread(len(colls))
        self._progress_logger.start()

        # small collections first
        pool = gevent.pool.Pool(8)
        for res in pool.imap(self._sync_collection, small_colls):
            if res is not None:
                sys.exit(1)

        # then large collections
        for ns, points in large_colls:
            self._sync_large_collection(ns, points)

    def _sync_collection(self, namespace_tuple):
        """ Sync a collection until success.
        """
        raise NotImplementedError('you should implement %s.%s' % (self.__class__.__name__, self._sync_collection.__name__))

    def _is_large_collection(self, namespace_tuple):
        """ Check if large collection or not.
        """
        dbname, collname = namespace_tuple
        return True if self._src.client()[dbname][collname].count() > self._large_coll_docs else False

    def _sync_large_collection(self, namespace_tuple):
        """ Sync large collection until success.
        """
        raise NotImplementedError('you should implement %s.%s' % (self.__class__.__name__, self._sync_large_collection.__name__))

    def _replay_oplog(self, oplog_start):
        """ Replay oplog.
        """
        raise NotImplementedError('you should implement %s.%s' % (self.__class__.__name__, self._replay_oplog.__name__))

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
