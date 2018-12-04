import time
import multiprocessing
import gevent
import pymongo
from mongosync import mongo_utils
from mongosync.logger import Logger
from mongosync.config import MongoConfig
from mongosync.common_syncer import CommonSyncer
from mongosync.mongo.handler import MongoHandler
from mongosync.multi_oplog_replayer import MultiOplogReplayer

log = Logger.get()


class MongoSyncer(CommonSyncer):
    """ MongoDB synchronizer.
    """
    def __init__(self, conf):
        CommonSyncer.__init__(self, conf)

        if not isinstance(self._conf.src_conf, MongoConfig):
            raise RuntimeError('invalid src config type')
        self._src = MongoHandler(self._conf.src_conf)
        if not self._src.connect():
            raise RuntimeError('connect to mongodb(src) failed: %s' % self._conf.src_hostportstr)
        if not isinstance(self._conf.dst_conf, MongoConfig):
            raise RuntimeError('invalid dst config type')
        self._dst = MongoHandler(self._conf.dst_conf)
        if not self._dst.connect():
            raise RuntimeError('connect to mongodb(dst) failed: %s' % self._conf.dst_hostportstr)
        self._multi_oplog_replayer = MultiOplogReplayer(self._dst, 10)

    def _create_index(self, namespace_tuple):
        """ Create indexes.
        """
        def format(key_direction_list):
            """ Format key and direction of index.
            """
            res = []
            for key, direction in key_direction_list:
                if isinstance(direction, float) or isinstance(direction, long):
                    direction = int(direction)
                res.append((key, direction))
            return res

        dbname, collname = namespace_tuple
        dst_dbname, dst_collname = self._conf.db_coll_mapping(dbname, collname)
        index_info = self._src.client()[dbname][collname].index_information()
        for name, info in index_info.iteritems():
            keys = info['key']
            options = {}
            options['name'] = name
            if 'unique' in info:
                options['unique'] = info['unique']
            if 'sparse' in info:
                options['sparse'] = info['sparse']
            if 'expireAfterSeconds' in info:
                options['expireAfterSeconds'] = info['expireAfterSeconds']
            if 'partialFilterExpression' in info:
                options['partialFilterExpression'] = info['partialFilterExpression']
            if 'dropDups' in info:
                options['dropDups'] = info['dropDups']

            ## create indexes before import documents, so not need 'background' option
            # if 'background' in info:
            #     options['background'] = info['background']

            # for text index
            if 'weights' in info:
                options['weights'] = info['weights']
            if 'default_language' in info:
                options['default_language'] = info['default_language']
            if 'language_override' in info:
                options['language_override'] = info['language_override']

            self._dst.create_index(dst_dbname, dst_collname, format(keys), **options)

    def _sync_collection(self, namespace_tuple):
        """ Sync a collection until success.
        """
        # create indexes first
        self._create_index(namespace_tuple)

        src_dbname, src_collname = namespace_tuple
        dst_dbname, dst_collname = self._conf.db_coll_mapping(src_dbname, src_collname)
        src_ns = '%s.%s' % (src_dbname, src_collname)

        total = self._src.client()[src_dbname][src_collname].count()
        self._progress_logger.register(src_ns, total)

        while True:
            try:
                cursor = self._src.client()[src_dbname][src_collname].find(filter=None,
                                                                           cursor_type=pymongo.cursor.CursorType.EXHAUST,
                                                                           no_cursor_timeout=True,
                                                                           modifiers={'$snapshot': True})

                reqs = []
                reqs_max = 100
                groups = []
                groups_max = 10
                n = 0

                for doc in cursor:
                    reqs.append(pymongo.ReplaceOne({'_id': doc['_id']}, doc, upsert=True))
                    if len(reqs) == reqs_max:
                        groups.append(reqs)
                        reqs = []
                    if len(groups) == groups_max:
                        threads = [gevent.spawn(self._dst.bulk_write, dst_dbname, dst_collname, groups[i]) for i in xrange(groups_max)]
                        gevent.joinall(threads)
                        groups = []

                    n += 1
                    if n % 10000 == 0:
                        self._progress_logger.add(src_ns, n)
                        n = 0

                if len(groups) > 0:
                    threads = [gevent.spawn(self._dst.bulk_write, dst_dbname, dst_collname, groups[i]) for i in xrange(len(groups))]
                    gevent.joinall(threads)
                if len(reqs) > 0:
                    self._dst.bulk_write(dst_dbname, dst_collname, reqs)

                self._progress_logger.add(src_ns, n, done=True)
                return
            except pymongo.errors.AutoReconnect:
                self._src.reconnect()

    def _sync_large_collection(self, namespace_tuple, split_points):
        """ Sync large collection.
        """
        # create indexes first
        self._create_index(namespace_tuple)

        dbname, collname = namespace_tuple
        ns = '.'.join(namespace_tuple)

        log.info('pending to sync %s with %d processes' % (ns, len(split_points) + 1))

        coll = self._src.client()[dbname][collname]
        total = coll.count()
        self._progress_logger.register(ns, total)

        prog_q = multiprocessing.Queue()
        res_q = multiprocessing.Queue()

        proc_logging = multiprocessing.Process(target=logging_progress, args=(ns, total, prog_q))
        proc_logging.start()

        queries = []
        lower_bound = None
        for point in split_points:
            if lower_bound is None:
                queries.append({'_id': {'$lt': point}})
            else:
                queries.append({'_id': {'$gte': lower_bound, '$lt': point}})
            lower_bound = point
        queries.append({'_id': {'$gte': lower_bound}})

        procs = []
        for query in queries:
            p = multiprocessing.Process(target=self._sync_collection_with_query, args=(namespace_tuple, query, prog_q, res_q))
            p.start()
            procs.append(p)
            log.info('start process %s with query %s' % (p.name, query))

        for p in procs:
            p.join()

        n_docs = 0
        for p in procs:
            n_docs += res_q.get()
        self._progress_logger.add(ns, n_docs, done=True)

        prog_q.put(True)
        prog_q.close()
        prog_q.join_thread()
        proc_logging.join()

    def _sync_collection_with_query(self, namespace_tuple, query, prog_q, res_q):
        """ Sync collection with query.
        """
        self._src.reconnect()
        self._dst.reconnect()

        src_dbname, src_collname = namespace_tuple
        dst_dbname, dst_collname = self._conf.db_coll_mapping(src_dbname, src_collname)

        while True:
            try:
                cursor = self._src.client()[src_dbname][src_collname].find(filter=query,
                                                                           cursor_type=pymongo.cursor.CursorType.EXHAUST,
                                                                           no_cursor_timeout=True,
                                                                           # snapshot cause blocking, maybe bug
                                                                           # modifiers={'$snapshot': True}
                                                                           )
                total = 0
                n = 0
                reqs = []
                reqs_max = 100
                groups = []
                groups_max = 10

                for doc in cursor:
                    reqs.append(pymongo.ReplaceOne({'_id': doc['_id']}, doc, upsert=True))
                    if len(reqs) == reqs_max:
                        groups.append(reqs)
                        reqs = []
                    if len(groups) == groups_max:
                        threads = [gevent.spawn(self._dst.bulk_write, dst_dbname, dst_collname, groups[i]) for i in xrange(groups_max)]
                        gevent.joinall(threads)
                        groups = []

                    n += 1
                    total += 1
                    if n % 10000 == 0:
                        prog_q.put(n)
                        n = 0

                if len(groups) > 0:
                    threads = [gevent.spawn(self._dst.bulk_write, dst_dbname, dst_collname, groups[i]) for i in xrange(len(groups))]
                    gevent.joinall(threads)
                if len(reqs) > 0:
                    self._dst.bulk_write(dst_dbname, dst_collname, reqs)

                if n > 0:
                    prog_q.put(n)
                res_q.put(total)

                prog_q.close()
                prog_q.join_thread()
                res_q.close()
                res_q.join_thread()
                return
            except pymongo.errors.AutoReconnect:
                self._src.reconnect()

    def _replay_oplog(self, start_optime):
        """ Replay oplog.
        """
        self._last_optime = start_optime

        n_total = 0
        n_skip = 0

        while True:
            # try to get cursor until success
            try:
                start_optime_valid = False
                need_log = False
                host, port = self._src.client().address
                log.info('try to sync oplog from %s on %s:%d' % (self._last_optime, host, port))
                cursor = self._src.tail_oplog(start_optime)

                while True:
                    try:
                        if need_log:
                            self._log_optime(self._last_optime)
                            self._log_progress()
                            need_log = False

                        if not cursor.alive:
                            log.error('cursor is dead')
                            raise pymongo.errors.AutoReconnect

                        oplog = cursor.next()
                        n_total += 1

                        # check start optime once
                        if not start_optime_valid:
                            if oplog['ts'] == self._last_optime:
                                log.info('oplog is ok: %s' % self._last_optime)
                                start_optime_valid = True
                            else:
                                log.error('oplog %s is stale, terminate' % self._last_optime)
                                return

                        if oplog['op'] == 'n':  # no-op
                            self._last_optime = oplog['ts']
                            need_log = True
                            continue

                        # validate oplog only for mongodb
                        if not self._conf.data_filter.valid_oplog(oplog):
                            n_skip += 1
                            self._last_optime = oplog['ts']
                            need_log = True
                            continue

                        dbname, collname = mongo_utils.parse_namespace(oplog['ns'])
                        dst_dbname, dst_collname = self._conf.db_coll_mapping(dbname, collname)
                        if dst_dbname != dbname or dst_collname != collname:
                            oplog['ns'] = '%s.%s' % (dst_dbname, dst_collname)

                        if self._multi_oplog_replayer:
                            if mongo_utils.is_command(oplog):
                                self._multi_oplog_replayer.apply()
                                self._multi_oplog_replayer.clear()
                                self._dst.replay_oplog(oplog)
                                self._last_optime = oplog['ts']
                                need_log = True
                            else:
                                self._multi_oplog_replayer.push(oplog)
                                if self._multi_oplog_replayer.count() >= 1000:
                                    self._multi_oplog_replayer.apply()
                                    self._multi_oplog_replayer.clear()
                                    self._last_optime = oplog['ts']
                                    need_log = True
                        else:
                            self._dst.replay_oplog(oplog)
                            self._last_optime = oplog['ts']
                            need_log = True
                    except StopIteration as e:
                        if self._multi_oplog_replayer.count() > 0:
                            self._multi_oplog_replayer.apply()
                            self._multi_oplog_replayer.clear()
                            self._last_optime = self._multi_oplog_replayer.last_optime()
                            need_log = True
                        # no more oplogs, wait a moment
                        time.sleep(0.1)
                        self._log_optime(self._last_optime)
                        self._log_progress('latest')
                    except pymongo.errors.AutoReconnect as e:
                        log.error(e)
                        self._src.reconnect()
                        break
            except IndexError as e:
                log.error(e)
                log.error('%s not found, terminate' % self._last_optime)
                return


def logging_progress(ns, total, prog_q):
    curr = 0
    while True:
        m = prog_q.get()
        if isinstance(m, bool):
            return
        curr += m
        s = '\t%s\t%d/%d\t[%.2f%%]' % (
                ns,
                curr,
                total,
                float(curr)/total*100 if total > 0 else float(curr+1)/(total+1)*100)
        log.info(s)
