import sys
import time
import datetime
import exceptions
import Queue
import threading
import gevent
from mongosync.config import Config
from mongosync.logger import Logger
from mongosync.mongo_utils import get_optime, gen_namespace
from mongosync.optime_logger import OptimeLogger

log = Logger.get()


class Progress(object):
    def __init__(self, ns, curr, total, done, start_time):
        self.ns = ns
        self.curr = curr
        self.total = total
        self.done = done
        self.start_time = start_time


class ProgressLogger(threading.Thread):
    def __init__(self, queue, n_colls, **kwargs):
        self.queue = queue
        self.n_colls = n_colls
        self.map = {}
        super(ProgressLogger, self).__init__(**kwargs)

    def run(self):
        n_colls_done = 0
        while n_colls_done < self.n_colls:
            p = self.queue.get()
            self.map[p.ns] = p

            if p.done:
                n_colls_done += 1
                del self.map[p.ns]
                time_used = time.time() - p.start_time
                sys.stdout.write('\r\33[K')
                sys.stdout.write('\r[\033[32m OK \033[0m]\t[%d/%d]\t%s\t%d/%d\t%.1fs\n' % (n_colls_done, self.n_colls, p.ns, p.curr, p.total, time_used))
                sys.stdout.flush()

            continue

            s = ''
            for ns, p in self.map.iteritems():
                s += '|| %s  %d/%d  %.1f%% ' % (ns, p.curr, p.total, float(p.curr)/p.total*100)
            if len(s) > 0:
                s += '||'
                sys.stdout.write('\r%s' % s)
                sys.stdout.flush()


class CommonSyncer(object):
    """ Common database synchronizer.

    Specific database synchronizer should implement the following methods:
        - __init__
        - _initial_sync
        - _sync_collection
        - _replay_oplog
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

        self._progress_queue = Queue.Queue()

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
            oplog_start = self._conf.start_optime
            doc = self._src.client()['local']['oplog.rs'].find_one({'ts': {'$gte': oplog_start}})
            if not doc:
                log.error('oplog is stale')
                return
            oplog_start = doc['ts']
            log.info('start timestamp is %s actually' % oplog_start)
            self._last_optime = oplog_start
            self._replay_oplog(oplog_start)
        else:
            oplog_start = get_optime(self._src.client())
            if not oplog_start:
                log.error('get oplog_start failed, terminate')
                sys.exit(1)
            self._last_optime = oplog_start
            self._initial_sync()
            if self._optime_logger:
                self._optime_logger.write(oplog_start)
                log.info('first %s' % oplog_start)
            self._replay_oplog(oplog_start)

    def _initial_sync(self):
        """ Initial sync.
        """
        colls = []
        for dbname in self._src.client().database_names():
            if dbname in self._ignore_dbs:
                log.info("skip database '%s'" % dbname)
                continue
            if not self._conf.data_filter.valid_db(dbname):
                log.info("skip database '%s'" % dbname)
                continue
            for collname in self._src.client()[dbname].collection_names(include_system_collections=False):
                if collname in self._ignore_colls:
                    log.info("skip collection '%s'" % gen_namespace(dbname, collname))
                    continue
                if not self._conf.data_filter.valid_coll(dbname, collname):
                    log.info("skip collection '%s'" % gen_namespace(dbname, collname))
                    continue
                colls.append((dbname, collname))

        t = ProgressLogger(self._progress_queue, len(colls))
        t.start()

        pool = gevent.pool.Pool(8)
        for res in pool.imap(self._sync_collection, colls):
            if res is not None:
                sys.exit(1)

        t.join()

    def _sync_collection(self, dbname, collname):
        """ Sync a collection until success.
        """
        raise Exception('you should implement %s.%s' % (self.__class__.__name__, self._sync_collection.__name__))

    def _replay_oplog(self, oplog_start):
        """ Replay oplog.
        """
        raise Exception('you should implement %s.%s' % (self.__class__.__name__, self._replay_oplog.__name__))

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
