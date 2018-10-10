import time
import pymongo
from mongosync import mongo_utils
from mongosync.logger import Logger
from mongosync.config import MongoConfig
from mongosync.common_syncer import CommonSyncer, Progress
from mongosync.mongo.handler import MongoHandler
from mongosync.multi_oplog_replayer import MultiOplogReplayer

try:
    import gevent
except ImportError:
    pass

log = Logger.get()


class MongoSyncer(CommonSyncer):
    """ MongoDB synchronizer.
    """
    def __init__(self, conf):
        CommonSyncer.__init__(self, conf)

        if not isinstance(self._conf.src_conf, MongoConfig):
            raise Exception('invalid src config type')
        self._src = MongoHandler(self._conf.src_conf)
        if not self._src.connect():
            raise Exception('connect to mongodb(src) failed: %s' % self._conf.src_hostportstr)
        if not isinstance(self._conf.dst_conf, MongoConfig):
            raise Exception('invalid dst config type')
        self._dst = MongoHandler(self._conf.dst_conf)
        if not self._dst.connect():
            raise Exception('connect to mongodb(dst) failed: %s' % self._conf.dst_hostportstr)
        self._multi_oplog_replayer = None
        engine = self._dst.client()['admin'].command('serverStatus')['storageEngine']['name']
        if self._conf.asyncio and engine.lower() == 'wiredtiger':
            self._multi_oplog_replayer = MultiOplogReplayer(self._dst, 10)

    def _initial_sync(self):
        """ Initial sync to MongoDB.
        """
        # Q: Why create indexes first?
        # A: Create indexes may failed if import the documents first.
        #    For example, create an unique index without 'dropDups' option and duplicate key error occured.
        #    Duplicate key document could be produced in initial sync.
        self._sync_indexes()
        CommonSyncer._initial_sync(self)

    def _sync_indexes(self):
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

        for dbname in self._src.client().database_names():
            if dbname in self._ignore_dbs:
                continue
            if not self._conf.data_filter.valid_db(dbname):
                continue
            for collname in self._src.client()[dbname].collection_names(include_system_collections=False):
                if collname in self._ignore_colls:
                    continue
                if not self._conf.data_filter.valid_index(dbname, collname):
                    continue
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
                    # create indexes before import documents, so not need 'background' option
                    # if 'background' in info:
                    #     options['background'] = info['background']
                    self._dst.create_index(dst_dbname, dst_collname, format(keys), **options)

    def _sync_collection(self, namespace_tuple):
        """ Sync a collection until success.
        """
        src_dbname, src_collname = namespace_tuple[0], namespace_tuple[1]
        dst_dbname, dst_collname = self._conf.db_coll_mapping(src_dbname, src_collname)
        src_ns = '%s.%s' % (src_dbname, src_collname)
        dst_ns = '%s.%s' % (dst_dbname, dst_collname)
        start_time = time.time()

        while True:
            try:
                cursor = self._src.client()[src_dbname][src_collname].find(filter=None,
                                                                           cursor_type=pymongo.cursor.CursorType.EXHAUST,
                                                                           no_cursor_timeout=True,
                                                                           modifiers={'$snapshot': True})
                count = cursor.count()

                n = 0
                reqs = []
                reqs_max = 100
                groups = []
                groups_max = 10

                for doc in cursor:
                    if self._conf.asyncio:
                        reqs.append(pymongo.ReplaceOne({'_id': doc['_id']}, doc, upsert=True))
                        if len(reqs) == reqs_max:
                            groups.append(reqs)
                            reqs = []
                        if len(groups) == groups_max:
                            threads = [gevent.spawn(self._dst.bulk_write, dst_dbname, dst_collname, groups[i]) for i in xrange(groups_max)]
                            gevent.joinall(threads)
                            groups = []
                    else:
                        reqs.append(pymongo.ReplaceOne({'_id': doc['_id']}, doc, upsert=True))
                        if len(reqs) == reqs_max:
                            self._dst.bulk_write(dst_dbname, dst_collname, reqs)
                            reqs = []
                    n += 1
                    if n % 10000 == 0:
                        self._progress_queue.put(Progress(src_ns, n, count, False, start_time))
                        log.info('\t%s\t%d/%d\t(%.1f%%)' % (src_ns, n, count, float(n)/count*100))

                if self._conf.asyncio:
                    if len(groups) > 0:
                        threads = [gevent.spawn(self._dst.bulk_write, dst_dbname, dst_collname, groups[i]) for i in xrange(len(groups))]
                        gevent.joinall(threads)
                    if len(reqs) > 0:
                        self._dst.bulk_write(dst_dbname, dst_collname, reqs)
                else:
                    if len(reqs) > 0:
                        self._dst.bulk_write(dst_dbname, dst_collname, reqs)

                self._progress_queue.put(Progress(src_ns, n, count, True, start_time))
                log.info("[ OK ] sync collection\t%s\t%d/%d\t(%.1f%%)" % (
                    src_ns if src_ns == dst_ns else '%s => %s' % (src_ns, dst_ns),
                    n,
                    count,
                    float(n)/count*100 if count > 0 else float(n+1)/(count+1)*100))
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
