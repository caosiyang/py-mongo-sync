import sys
import time
import datetime
import exceptions
import multiprocessing
import Queue
import pymongo
import bson
import mongo_helper
import filter
from multiprocessing import Process
from pymongo import ReplaceOne
from doc_writer import DocWriter
from logger import Logger

class MongoSynchronizer(object):
    """ MongoDB synchronizer.
    """
    def __init__(self, src_hostportstr, dst_hostportstr, **kwargs):
        """ Constructor.
        """
        if not src_hostportstr:
            raise Exception('src hostportstr is empty')
        if not dst_hostportstr:
            raise Exception('dst hostportstr is empty')

        self._src_mc = None
        self._dst_mc = None
        self._filter = None
        self._w = 1 # write concern, default 1
        self._start_optime = None # if true, only sync oplog
        self._last_optime = None # optime of the last oplog has been replayed
        self._logger = Logger.get()
        self._log_interval = 1
        self._last_logtime = None # use in oplog replay

        self._ignore_dbs = ['admin', 'local']
        self._ignore_colls = ['system.indexes', 'system.profile', 'system.users']

        self._src_engine = kwargs.get('src_engine')
        self._src_authdb = kwargs.get('src_authdb')
        self._src_username = kwargs.get('src_username')
        self._src_password = kwargs.get('src_password')

        self._dst_authdb = kwargs.get('dst_authdb')
        self._dst_username = kwargs.get('dst_username')
        self._dst_password = kwargs.get('dst_password')
        self._dbs = kwargs.get('dbs', [])
        self._colls = kwargs.get('colls', [])
        self._src_db = kwargs.get('src_db', '')
        self._dst_db = kwargs.get('dst_db', '')
        self._ignore_indexes = kwargs.get('ignore_indexes')
        self._start_optime = kwargs.get('start_optime')

        if self.rename_db_mode:
            assert len(self._dbs) == 0
            self._dbs.append(self._src_db)

        if self._colls:
            self._filter = filter.CollectionFilter()
            self._filter.add_target_collections(self._colls)
        elif self._dbs:
            self._filter = filter.DatabaseFilter()
            self._filter.add_target_databases(self._dbs)

        # init src mongo client
        self._src_host = src_hostportstr.split(':')[0]
        self._src_port = int(src_hostportstr.split(':')[1])
        self._src_mc = mongo_helper.mongo_connect(
                self._src_host,
                self._src_port,
                authdb=self._src_authdb,
                username=self._src_username,
                password=self._src_password,
                w=self._w)

        # init dst mongo client
        self._dst_host = dst_hostportstr.split(':')[0]
        self._dst_port = int(dst_hostportstr.split(':')[1])
        self._dst_mc = mongo_helper.mongo_connect(
                self._dst_host,
                self._dst_port,
                authdb=self._dst_authdb,
                username=self._dst_username,
                password=self._dst_password,
                w=self._w)

        self._dst_is_mongos = self._dst_mc.is_mongos

        self._src_version = mongo_helper.get_version(self._src_host, self._src_port)
        self._dst_version = mongo_helper.get_version(self._dst_host, self._dst_port)

    def __del__(self):
        """ Destructor.
        """
        if self._src_mc:
            self._src_mc.close()
        if self._dst_mc:
            self._dst_mc.close()

    @property
    def rename_db_mode(self):
        return self._src_db and self._dst_db

    @property
    def src_hostportstr(self):
        return '%s:%d' % (self._src_host, self._src_port)

    @property
    def dst_hostportstr(self):
        return '%s:%d' % (self._dst_host, self._dst_port)

    @property
    def from_to(self):
        return "%s => %s" % (self.src_hostportstr, self.dst_hostportstr)

    @property
    def log_interval(self):
        return self._log_interval

    @log_interval.setter
    def log_interval(self, n_secs):
        if n_secs < 0:
            n_secs = 0
        self._log_interval = n_secs

    def _sync(self):
        """ Sync databases and oplog.
        """
        if self._start_optime:
            self._logger.info("locating oplog, it will take a while")
            if self._src_engine == 'mongodb':
                oplog_start = bson.timestamp.Timestamp(int(self._start_optime), 0)
                doc = self._src_mc['local']['oplog.rs'].find_one({'ts': {'$gte': oplog_start}})
                if not doc:
                    self._logger.error('specified oplog not found')
                    return
                oplog_start = doc['ts']
            elif self._src_engine == 'tokumx':
                timestamp_str = self._start_optime
                if len(timestamp_str) != 14:
                    self._logger.error("invalid --start-optime, start optime for TokuMX should be 14 characters, like 'YYYYmmddHHMMSS'")
                    return
                oplog_start = datetime.datetime(int(timestamp_str[0:4]), int(timestamp_str[4:6]), int(timestamp_str[6:8]),
                        int(timestamp_str[8:10]), int(timestamp_str[10:12]),int(timestamp_str[12:14]))
                doc = self._src_mc['local']['oplog.rs'].find_one({'ts': {'$gte': oplog_start}})
                if not doc:
                    self._logger.error('specified oplog not found')
                    return
                oplog_start = doc['ts']
            self._logger.info('start timestamp is %s actually' % oplog_start)
            self._last_optime = oplog_start
            self._sync_oplog(oplog_start)
        else:
            oplog_start = None
            if self._src_engine == 'mongodb':
                oplog_start = mongo_helper.get_optime(self._src_mc)
            elif self._src_engine == 'tokumx':
                oplog_start = mongo_helper.get_optime_tokumx(self._src_mc)
            if not oplog_start:
                self._logger.error('[%s] get oplog_start failed, terminate' % self._current_process_name)
                sys.exit(1)
            self._last_optime = oplog_start
            self._sync_databases()
            self._sync_oplog(oplog_start)

    def _sync_databases(self):
        """ Sync databases except 'admin' and 'local'.
        """
        host, port = self._src_mc.address
        self._logger.info('[%s] sync databases from %s:%d' % (self._current_process_name, host, port))
        ignore_dbnames = ['admin', 'local']
        for dbname in self._src_mc.database_names():
            if dbname not in ignore_dbnames:
                if self._filter and not self._filter.valid_database(dbname):
                    continue
                self._sync_database(dbname)
        self._logger.info('[%s] all databases done' % self._current_process_name)

    def _sync_database(self, dbname):
        """ Sync a database.
        """
        # Q: Why create indexes first?
        # A: It may occured that create indexes failed after you have imported the data,
        #    for example, when you create an unique index without 'dropDups' option and get a 'duplicate keys' error.
        #    Because when you export and import data, duplicate key document may be produced.
        #    Another reason is for TokuMX. It does not support 'dropDups' option for an uniuqe index.
        #    The 'duplicate keys' error must cause index creation failed.
        self._logger.info("[%s] sync database '%s'" % (self._current_process_name, dbname))
        self._sync_indexes(dbname)
        self._sync_collections(dbname)

    def _sync_collections(self, dbname):
        """ Sync all collections in the database except system collections.
        """
        collnames = self._src_mc[dbname].collection_names(include_system_collections=False)
        for collname in collnames:
            if self._filter and not self._filter.valid_collection('%s.%s' % (dbname, collname)):
                continue
            if collname in self._ignore_colls:
                continue
            if self.rename_db_mode:
                assert dbname == self._src_db
                self._sync_collection(dbname, collname, self._dst_db, collname)
            else:
                self._sync_collection(dbname, collname, dbname, collname)

    def _sync_collection(self, src_dbname, src_collname, dst_dbname, dst_collname):
        """ Sync a collection through batch write.
        """
        self._logger.info("[%s] sync collection '%s.%s'" % (self._current_process_name, src_dbname, src_collname))
        while True:
            try:
                n = 0
                #docs = [] 
                reqs = []
                batchsize = 1000
                cursor = self._src_mc[src_dbname][src_collname].find(filter=None,
                                                             cursor_type=pymongo.cursor.CursorType.EXHAUST,
                                                             no_cursor_timeout=True,
                                                             modifiers={'$snapshot': True})

                if self._src_engine == 'tokumx':
                    # TokuMX 'count' command may be very slow, use 'collStats' command instead
                    count = self._src_mc[src_dbname].command({'collStats': src_collname})['count']
                else:
                    count = cursor.count()

                if count == 0:
                    self._logger.info('[%s] \t skip empty collection' % (self._current_process_name))
                    return
                for doc in cursor:
                    #docs.append(doc)
                    #if len(docs) == batchsize:
                    #    self._dst_mc[dst_dbname][dst_collname].insert_many(docs)
                    #    docs = []
                    reqs.append(ReplaceOne({'_id': doc['_id']}, doc, upsert=True))
                    if len(reqs) == batchsize:
                        self._bulk_write(dst_dbname, dst_collname, reqs, ordered=False)
                        reqs = []
                    n += 1
                    if n % 10000 == 0:
                        self._logger.info('[%s] \t %s.%s %d/%d (%.2f%%)' % (self._current_process_name, src_dbname, src_collname, n, count, float(n)/count*100))
                #if len(docs) > 0:
                #    self._dst_mc[dst_dbname][dst_collname].insert_many(docs)
                if len(reqs) > 0:
                    self._bulk_write(dst_dbname, dst_collname, reqs, ordered=False)
                    self._logger.info('[%s] \t %s.%s %d/%d (%.2f%%)' % (self._current_process_name, src_dbname, src_collname, n, count, float(n)/count*100))
                return
            except pymongo.errors.AutoReconnect:
                self._src_mc.close()
                self._src_mc = self.reconnect(self._src_host,
                                              self._src_port,
                                              username=self._src_username,
                                              password=self._src_password,
                                              w=self._w)

    def _sync_collection_mp2(self, dbname, collname):
        """ Sync a collection with multi-processes.
        Deprecated.
        Without fully test.
        """
        dw = DocWriter(self._dst_host, self._dst_port, dbname, collname)
        n = 0
        cursor = self._src_mc[dbname][collname].find(
                filter=None,
                cursor_type=pymongo.cursor.CursorType.EXHAUST,
                no_cursor_timeout=True,
                modifiers={'$snapshot': True})
        for doc in cursor:
            dw.write(doc)
            n += 1
            if n % 10000 == 0:
                self._logger.info('[%s] >> %d' % (self._current_process_name, n))
        dw.close()
        self._logger.info('[%s] >> %d all done' % (self._current_process_name, n))

    def _sync_collection_mp(self, dbname, collname):
        """ Sync a collection with multi-processes.
        Deprecated.
        Without fully test.
        """
        self._logger.info('>>>> %s.%s' % (dbname, collname))
        doc_q = multiprocessing.Queue()
        ev = multiprocessing.Event()
        ev.clear()
        processes = []
        for i in range(0, 4):
            p = multiprocessing.Process(target=self._write_document, args=(dbname, collname, doc_q, ev))
            p.start()
            processes.append(p)
        n = 0
        cursor = self._src_mc[dbname][collname].find(
                filter=None,
                cursor_type=pymongo.cursor.CursorType.EXHAUST,
                no_cursor_timeout=True,
                modifiers={'$snapshot': True})
        for doc in cursor:
            while doc_q.qsize() > 10000:
                time.sleep(0.2) # wait subprocess consume
            doc_q.put(doc)
            n += 1
            if n % 10000 == 0:
                self._logger.info('[%s] push %d, size: %d' % (self._current_process_name, n, doc_q.qsize()))
        ev.set()
        for p in processes:
            p.join()
        self._logger.info('==== %s.%s %d, qsize %d' % (dbname, collname, n, doc_q.qsize()))

    def _write_document(self, dbname, collname, q, ev):
        """ Write document to destination in subprocess.
        """
        n = 0
        while True:
            try:
                doc = q.get(block=True, timeout=0.1)
                while True:
                    try:
                        self._dst_mc[dbname][collname].replace_one({'_id': doc['_id']}, doc, upsert=True)
                        break
                    except pymongo.errors.DuplicateKeyError as e:
                        # TODO
                        # through unique index, delete old, insert new
                        self._logger.error(e)
                        self._logger.info(doc)
                        break
                    except pymongo.errors.AutoReconnect:
                        self._dst_mc = self.reconnect(
                                self._dst_host,
                                self._dst_port,
                                username=self._dst_username,
                                password=self._dst_password,
                                w=self._w)
                    except Exception as e:
                        self._logger.error('%s' % e)
                n += 1
            except Queue.Empty:
                if ev.is_set():
                    self._logger.info('==== %s write %d' % (self._current_process_name, n))
                    sys.exit(0)

    #def _sync_oplog_mp(self, dst_host, dst_port, src_host, src_port, oplog_start):
    #    """ Sync oplog with 2 processes.
    #    """
    #    self._logger.info('>>>> %s.%s' % ('local', 'oplog.rs'))
    #    oplog_q = multiprocessing.Queue()
    #    ev = multiprocessing.Event()
    #    ev.clear()
    #
    #    # create writer process
    #    processes = []
    #    for i in range(0, 1):
    #        p = multiprocessing.Process(target=self._write_oplog, args=(oplog_q, ev, dst_host, dst_port))
    #        p.start()
    #        processes.append(p)
    #
    #    n = 0
    #    mc = mongo_helper.mongo_connect(src_host, src_port)
    #    cursor = mc['local']['oplog.rs'].find({'ts': {'$gte': oplog_start}}, cursor_type=pymongo.cursor.CursorType.TAILABLE, no_cursor_timeout=True)
    #    while True:
    #        for oplog in cursor:
    #            while oplog_q.qsize() > 20000:
    #                time.sleep(0.2) # wait subprocess consume
    #            oplog_q.put(oplog)
    #            n += 1
    #            if n % 10000 == 0:
    #                self._logger.info('[%s] push %d oplog, qsize: %d' % (self._current_process_name, n, oplog_q.qsize()))
    #        else:
    #            time.sleep(0.1)
    #    #ev.set()
    #    #for p in processes:
    #    #    p.join()
    #    #self._logger.info('==== %s.%s %d, qsize %d' % (dbname, collname, n, doc_q.qsize()))

    #def _write_oplog(self, q, ev, dst_host, dst_port):
    #    """ Write document to destination in subprocess.
    #    """
    #    mc = mongo_helper.mongo_connect(dst_host, dst_port, w=1)
    #    n = 0
    #    while True:
    #        try:
    #            oplog = q.get(block=True, timeout=0.1)
    #            # use while make sure oplog is applied successfully
    #            while True:
    #                try:
    #                    # parse oplog
    #                    ts = oplog['ts']
    #                    op = oplog['op'] # 'n' or 'i' or 'u' or 'c' or 'd'
    #                    ns = oplog['ns']
    #                    dbname = ns.split('.', 1)[0]
    #                    if op == 'i': # insert
    #                        collname = ns.split('.', 1)[1]
    #                        mc[dbname][collname].save(oplog['o'])
    #                    elif op == 'u': # update
    #                        collname = ns.split('.', 1)[1]
    #                        mc[dbname][collname].update(oplog['o2'], oplog['o'])
    #                    elif op == 'd': # delete
    #                        collname = ns.split('.', 1)[1]
    #                        mc[dbname][collname].remove(oplog['o'])
    #                    elif op == 'c': # command
    #                        mc[dbname].command(oplog['o'])
    #                    elif op == 'n': # no-op
    #                        self._logger.info('no-op')
    #                    else:
    #                        self._logger.error('unknown command: %s' % oplog)

    #                    self._last_optime = ts
    #                    n += 1
    #                    if n % 1000 == 0:
    #                        self._logger.info('apply %d oplog, %s, %s' % (n, datetime.datetime.fromtimestamp(ts.time), ts))
    #                    break
    #                except pymongo.errors.AutoReconnect:
    #                    self._dst_mc = self.reconnect(self._dst_host, self._dst_port, w=self._w)
    #                except pymongo.errors.DuplicateKeyError as e:
    #                    # TODO
    #                    # through unique index, delete old, insert new
    #                    self._logger.error(e)
    #                    self._logger.error(oplog)
    #                    break
    #                except Exception as e:
    #                    self._logger.error(e)
    #                    self._logger.error(oplog)
    #                    break
    #        except Queue.Empty:
    #            if ev.is_set():
    #                self._logger.info('==== %s write %d' % (self._current_process_name, n))
    #                sys.exit(0)

    def _sync_indexes(self, dbname):
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

        if self._ignore_indexes:
            return

        for collname in self._src_mc[dbname].collection_names(include_system_collections=False):
            if self._filter and not self._filter.valid_index('%s.%s' % (dbname, collname)):
                continue
            if collname in self._ignore_colls:
                continue
            index_info = self._src_mc[dbname][collname].index_information()
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
                # create indexes before import documents, ignore 'background' option
                #if 'background' in info:
                #    options['background'] = info['background']
                if self.rename_db_mode:
                    assert dbname == self._src_db
                    self._dst_mc[self._dst_db][collname].create_index(format(keys), **options)
                else:
                    self._dst_mc[dbname][collname].create_index(format(keys), **options)

    def _sync_oplog(self, oplog_start):
        """ Replay oplog.
        """
        try:
            host, port = self._src_mc.address
            self._logger.info('try to sync oplog from %s on %s:%d' % (oplog_start, host, port))
            # set codec options to guarantee the order of keys in command
            coll = self._src_mc['local'].get_collection('oplog.rs', codec_options=bson.codec_options.CodecOptions(document_class=bson.son.SON))
            cursor = coll.find({'ts': {'$gte': oplog_start}}, cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT, no_cursor_timeout=True)

            # New in version 3.2
            if mongo_helper.version_higher_or_equal(self._src_version, '3.2.0'):
                cursor.max_await_time_ms(3000)

            if cursor.next()['ts'] != oplog_start:
                self._logger.error('%s is stale, terminate' % oplog_start)
                return
        except IndexError as e:
            self._logger.error(e)
            self._logger.error('%s not found, terminate' % oplog_start)
            return
        except Exception as e:
            self._logger.error(e)
            raise e

        self._logger.info('replaying oplog')
        self._set_last_logtime()
        n_total = 0
        n_skip = 0
        n_replayed = 0
        while True:
            try:
                if not cursor.alive:
                    self._logger.error('cursor is dead')
                    raise pymongo.errors.AutoReconnect

                # get an oplog
                oplog = cursor.next()
                n_total += 1

                # validate oplog only for mongodb
                if self._filter and self._src_engine == 'mongodb' and not self._filter.valid_oplog(oplog):
                    n_skip += 1
                    continue

                # guarantee that replay oplog successfully
                recovered = False
                while True:
                    try:
                        if recovered:
                            self._logger.info('recovered at %s' % oplog['ts'])
                            recovered = False
                        self._replay_oplog(oplog)
                        n_replayed += 1
                        if n_replayed % 1000 == 0:
                            self._print_progress(oplog)
                        break
                    except pymongo.errors.DuplicateKeyError as e:
                        # TODO
                        # through unique index, delete old, insert new
                        #self._logger.error(oplog)
                        #self._logger.error(e)
                        break
                    except pymongo.errors.AutoReconnect as e:
                        self._logger.error(e)
                        self._logger.error('interrupted at %s' % oplog['ts'])
                        self._dst_mc = self.reconnect(
                                self._dst_host,
                                self._dst_port,
                                username=self._dst_username,
                                password=self._dst_password,
                                w=self._w)
                        if self._dst_mc:
                            recovered = True
                    except pymongo.errors.WriteError as e:
                        self._logger.error('interrupted at %s' % oplog['ts'])
                        self._logger.error(e)

                        # For case:
                        #   Update the values of shard key fields when syncing from replica set to sharded cluster.
                        #
                        # Once you shard a collection, the shard key and the shard key values are immutable.
                        # Reference: https://docs.mongodb.com/manual/core/sharding-shard-key/
                        if self._dst_is_mongos and oplog['op'] == 'u' and 'the (immutable) field' in str(e):
                            db, coll = mongo_helper.parse_namespace(oplog['ns'])
                            old_doc = self._dst_mc[db][coll].find_one(oplog['o2'])
                            if not old_doc:
                                self._logger.error('replay update failed: document not found:', oplog['o2'])
                                sys.exit(1)
                            if '$set' in oplog['o']:
                                new_doc = old_doc.update(oplog['o']['$set'])
                            else:
                                new_doc = oplog['o']

                            # TODO: here need a transaction to delete old and insert new
                            # delete old document
                            res = self._dst_mc[db][coll].delete_one(oplog['o2'])
                            if res.deleted_count != 1:
                                self._logger.error('replay update failed: delete old document failed:', oplog['o2'])
                                sys.exit(1)
                            # insert new document
                            res = self._dst_mc[db][coll].insert_one(new_doc)
                            if not res.inserted_id:
                                self._logger.error('replay update failed: insert new document failed:', new_doc)
                                sys.exit(1)

                        recovered = True

            except StopIteration as e:
                # there is no oplog to replay now, wait a moment
                time.sleep(0.1)
                self._print_progress(None)
            except pymongo.errors.AutoReconnect:
                self._src_mc.close()
                self._src_mc = self.reconnect(
                        host,
                        port,
                        username=self._src_username,
                        password=self._src_password,
                        w=self._w)
                # set codec options  to guarantee the order of keys in command
                coll = self._src_mc['local'].get_collection('oplog.rs', codec_options=bson.codec_options.CodecOptions(document_class=bson.son.SON))
                cursor = coll.find({'ts': {'$gte': self._last_optime}}, cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT, no_cursor_timeout=True)

                # New in version 3.2
                if mongo_helper.version_higher_or_equal(self._src_version, '3.2.0'):
                    cursor.max_await_time_ms(3000)

                if cursor.next()['ts'] != self._last_optime:
                    self._logger.error('%s is stale, terminate' % self._last_optime)
                    return

    def _replay_oplog(self, oplog):
        if self._src_engine == 'mongodb':
            self._replay_oplog_mongodb(oplog)
        elif self._src_engine == 'tokumx':
            self._replay_oplog_tokumx(oplog)

    def _print_progress(self, oplog):
        if oplog:
            ts = oplog['ts']
            if self._src_engine == 'mongodb':
                self._logger.info('%s, sync to %s, %s' % (self.from_to, datetime.datetime.fromtimestamp(ts.time), ts))
            elif self._src_engine == 'tokumx':
                self._logger.info('%s, sync to %s' % (self.from_to, ts))
            self._set_last_logtime()
        else:
            now = time.time()
            if now - self._last_logtime > self._log_interval:
                self._logger.info('%s, no more oplog, sync to %s, %s' % (self.from_to, datetime.datetime.fromtimestamp(self._last_optime.time), self._last_optime))
                self._set_last_logtime()

    def _replay_oplog_mongodb(self, oplog):
        """ Replay oplog on destination if source is MongoDB.
        """
        # parse
        ts = oplog['ts']
        op = oplog['op'] # 'n' or 'i' or 'u' or 'c' or 'd'
        ns = oplog['ns']
        dbname = ns.split('.', 1)[0]

        if self.rename_db_mode:
            assert dbname == self._src_db
            dbname = self._dst_db

        if op == 'i': # insert
            collname = ns.split('.', 1)[1]
            self._dst_mc[dbname][collname].insert_one(oplog['o'])
            #self._dst_mc[dbname][collname].replace_one({'_id': oplog['o']['_id']}, oplog['o'], upsert=True)
        elif op == 'u': # update
            collname = ns.split('.', 1)[1]
            self._dst_mc[dbname][collname].update(oplog['o2'], oplog['o'])
        elif op == 'd': # delete
            collname = ns.split('.', 1)[1]
            self._dst_mc[dbname][collname].delete_one(oplog['o'])
        elif op == 'c': # command
            # FIX ISSUE #4 and #5
            # if use option '--colls' to sync target collections,
            # commands on other collections in the same database may replay failed
            try:
                self._dst_mc[dbname].command(oplog['o'])
            except pymongo.errors.OperationFailure as e:
                self._logger.error('%s: %s' % (e, op))
        elif op == 'n': # no-op
            pass
        else:
            self._logger.error('unknown operation: %s' % oplog)
        self._last_optime = ts

    def _replay_oplog_tokumx(self, oplog):
        """ Replay oplog on destination if source is TokuMX.
        """
        for op in oplog['ops']:
            if op['op'] == 'i':
                dbname = op['ns'].split('.', 1)[0]
                collname = op['ns'].split('.', 1)[1]
                if self.rename_db_mode:
                    assert dbname == self._src_db
                    dbname = self._dst_db
                self._dst_mc[dbname][collname].insert_one(op['o'])
            elif op['op'] == 'u':
                dbname = op['ns'].split('.', 1)[0]
                collname = op['ns'].split('.', 1)[1]
                if self.rename_db_mode:
                    assert dbname == self._src_db
                    dbname = self._dst_db
                self._dst_mc[dbname][collname].update({'_id': op['o']['_id']}, op['o2'])
            elif op['op'] == 'ur':
                dbname = op['ns'].split('.', 1)[0]
                collname = op['ns'].split('.', 1)[1]
                if self.rename_db_mode:
                    assert dbname == self._src_db
                    dbname = self._dst_db
                self._dst_mc[dbname][collname].update({'_id': op['pk']['']}, op['m'])
            elif op['op'] == 'd':
                dbname = op['ns'].split('.', 1)[0]
                collname = op['ns'].split('.', 1)[1]
                if self.rename_db_mode:
                    assert dbname == self._src_db
                    dbname = self._dst_db
                self._dst_mc[dbname][collname].remove({'_id': op['o']['_id']})
            elif op['op'] == 'c':
                dbname = op['ns'].split('.', 1)[0]
                if self.rename_db_mode:
                    assert dbname == self._src_db
                    dbname = self._dst_db
                self._dst_mc[dbname].command(op['o'])
            elif op['op'] == 'n':
                pass
            else:
                self._logger.error('unknown operation: %s' % op)
        self._last_optime = oplog['ts']

    @property
    def _current_process_name(self):
        return multiprocessing.current_process().name

    def run(self):
        """ Start data synchronization.
        """
        # never drop database automatically
        # you should clear the databases manually if necessary
        try:
            self._sync()
        except exceptions.KeyboardInterrupt:
            self._logger.info('terminating')

    def reconnect(self, host, port, **kwargs):
        """ Try to reconnect until success.
        """
        while True:
            try:
                self._logger.info('try to reconnect %s:%d' % (host, port))
                mc = mongo_helper.mongo_connect(host, port, **kwargs)
                mc.database_names() # excute command to confirm connection availability
                return mc
            except Exception as e:
                self._logger.error('reconnect failed: %s' % e)

    def _bulk_write(self, dbname, collname, requests, ordered=True, bypass_document_validation=False):
        """ Try to bulk write until success.
        """
        while True:
            try:
                res = self._dst_mc[dbname][collname].bulk_write(requests,
                        ordered=ordered,
                        bypass_document_validation=bypass_document_validation)
                return
            except pymongo.errors.AutoReconnect:
                # reconnect and rewrite
                self._dst_mc.close()
                self._dst_mc = self.reconnect(self._dst_host,
                        self._dst_port,
                        username=self._dst_username,
                        password=self._dst_password,
                        w=self._w)
            except pymongo.errors.BulkWriteError as e:
                self._handle_bulk_write_error(dbname, collname, requests)
                return

    def _handle_bulk_write_error(self, dbname, collname, requests):
        """ Write documents one by one to locate the error(s).
        """
        for op in requests:
            while True:
                try:
                    res = self._dst_mc[dbname][collname].replace_one(op._filter, op._doc)
                    break
                except pymongo.errors.AutoReconnect:
                    self._dst_mc.close()
                    self._dst_mc = self.reconnect(self._dst_host,
                            self._dst_port,
                            username=self._dst_username,
                            password=self._dst_password,
                            w=self._w)
                except Exception as e:
                    self._logger.error('%s when excuting %s' % (e, op))
                    break

    def _set_last_logtime(self):
        """ Set last logtime when replaying oplog.
        """
        self._last_logtime = time.time()
