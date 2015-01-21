import sys
import time
import datetime
import logging
import exceptions
import multiprocessing
import Queue
import pymongo
import mongo_helper
import filter
from multiprocessing import Process
from doc_writer import DocWriter

class MongoSynchronizer(object):
    """ MongoDB multi-source synchronizer.
    """
    def __init__(self, src_hostportstr, dst_hostportstr, **kwargs):
        """ Constructor.
        """
        if not src_hostportstr:
            raise Exception('source hostportstr is empty')
        if not dst_hostportstr:
            raise Exception('destination hostportstr is empty')

        self._src_mc = None
        self._dst_mc = None
        self._is_dst_mongos = False
        self._filter = None
        self._query = None
        self._last_optime = None # optime of the last oplog has applied
        self._logger = logging.getLogger()

        self._dst_username = kwargs.get('dst_username')
        self._dst_password = kwargs.get('dst_password')
        self._collections = kwargs.get('collections')
        self._ignore_indexes = kwargs.get('ignore_indexes')
        self._query = kwargs.get('query')

        if self._collections:
            self._filter = filter.CollectionFilter()
            self._filter.add_target_collections(self._collections)

        # init source mongo clients
        self._src_host = src_hostportstr.split(':')[0]
        self._src_port = int(src_hostportstr.split(':')[1])
        self._src_mc = mongo_helper.mongo_connect(self._src_host, self._src_port)

        # init destination mongo client
        self._dst_host = dst_hostportstr.split(':')[0]
        self._dst_port = int(dst_hostportstr.split(':')[1])
        self._dst_mc = mongo_helper.mongo_connect(self._dst_host, self._dst_port)
        if self._dst_username and self._dst_password:
            self._dst_mc.admin.authenticate(self._dst_username, self._dst_password)
        if self._dst_mc.is_mongos:
            self._is_dst_mongos = True

    def __del__(self):
        """ Destructor.
        """
        if self._src_mc:
            self._src_mc.close()
        if self._dst_mc:
            self._dst_mc.close()

    def _sync(self, mc):
        """ Sync databases and oplog.
        """
        oplog_start = mongo_helper.get_optime(mc)
        if not oplog_start:
            self._logger.error('[%s] get oplog_start failed, terminated' % self._current_process_name)
            sys.exit(1)
        self._last_optime = oplog_start
        self._sync_databases(mc)
        self._sync_oplog(mc, oplog_start)

    def _sync_databases(self, mc):
        """ Sync databases except admin and local.
        """
        host, port = mc.primary
        self._logger.info('[%s] sync databases from %s:%d...' % (self._current_process_name, host, port))
        dbnames = mc.database_names()
        for dbname in dbnames:
            if dbname not in ['admin', 'local']:
                if self._filter:
                    if not self._filter.valid_database(dbname):
                        continue
                self._sync_database(mc, dbname)
        self._logger.info('[%s] all databases done' % self._current_process_name)

    def _sync_database(self, mc, dbname):
        """ Sync a database.
        """
        # Q: Why create indexes first?
        # A: It may occured that create indexes failed after you have imported the data,
        #    for example, when you create an unique index without 'dropDups' option and get a 'duplicate keys' error.
        #    Because when you export and import data, duplicate key document may be produced.
        #    Another reason is for TokuMX. It does not support 'dropDups' option for an uniuqe index.
        #    The 'duplicate keys' error must cause index creation failed.
        self._sync_indexes(mc, dbname)
        self._sync_collections(mc, dbname)

    def _sync_collections(self, mc, dbname):
        """ Sync all collections in the database except system collections.
        """
        collnames = mc[dbname].collection_names(include_system_collections=False)
        for collname in collnames:
            if self._filter:
                if not self._filter.valid_collection('%s.%s' % (dbname, collname)):
                    continue
            #if self._is_dst_mongos:
            #    self._sync_collection_mp(mc, dbname, collname)
            #else:
            #    self._sync_collection(mc, dbname, collname)
            self._sync_collection_mp(mc, dbname, collname)

    #def _sync_collection(self, mc, dbname, collname):
    #    """ Sync a collection.
    #    """
    #    self._logger.info('[%s] >>>> %s.%s' % (self._current_process_name, dbname, collname))
    #    n = 0 # counter
    #    buf = []
    #    buf_max_size = 1000
    #    dst_coll = self._dst_mc[dbname][collname]
    #    cursor = mc[dbname][collname].find(spec=self._query, snapshot=True, timeout=False)
    #    for doc in cursor:
    #        buf.append(doc)
    #        if len(buf) == buf_max_size:
    #            dst_coll.insert(buf)
    #            buf = []
    #        n += 1
    #        if n % 1000 == 0:
    #            self._logger.info('[%s] >> %d' % (self._current_process_name, n))
    #    if len(buf) > 0:
    #        dst_coll.insert(buf)
    #    self._logger.info('[%s] ==== %s.%s %d' % (self._current_process_name, dbname, collname, n))

    def _sync_collection_mp2(self, mc, dbname, collname):
        dw = DocWriter(self._dst_host, self._dst_port, dbname, collname)
        cursor = mc[dbname][collname].find(spec=self._query, snapshot=True, timeout=False)
        n = 0
        for doc in cursor:
            dw.write(doc)
            n += 1
            if n % 10000 == 0:
                self._logger.info('[%s] >> %d' % (self._current_process_name, n))
        dw.close()
        self._logger.info('[%s] >> %d all done' % (self._current_process_name, n))

    def _sync_collection_mp(self, mc, dbname, collname):
        """ Sync a collection with multi-processes.
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
        cursor = mc[dbname][collname].find(spec=self._query, snapshot=True, timeout=False)
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
                        self._dst_mc[dbname][collname].save(doc)
                        break
                    except pymongo.errors.DuplicateKeyError as e:
                        # TODO
                        # through unique index, delete old, insert new
                        self._logger.error(e)
                        self._logger.info(doc)
                        break
                    except pymongo.errors.AutoReconnect:
                        self._dst_mc = self.reconnect(self._dst_host, self._dst_port)
                    except Exception as e:
                        self._logger.error('%s' % e)
                n += 1
            except Queue.Empty:
                if ev.is_set():
                    self._logger.info('==== %s write %d' % (self._current_process_name, n))
                    sys.exit(0)

    def _sync_indexes(self, mc, dbname):
        """ Create indexes.
        """
        def index_parse(index_map):
            index_list = []
            for fieldname, direction in index_map.items():
                if isinstance(direction, float):
                    direction = int(direction)
                index_list.append((fieldname, direction))
            return index_list

        if self._ignore_indexes:
            return

        for doc in mc[dbname]['system.indexes'].find():
            collname = doc['ns'].replace(dbname, '', 1)[1:]

            if self._filter:
                if not self._filter.valid_index('%s.%s' % (dbname, collname)):
                    continue

            if 'expireAfterSeconds' in doc:
                self._dst_mc[dbname][collname].create_index(index_parse(doc['key']),
                        unique=doc.get('unique', False),
                        dropDups=doc.get('dropDups', False),
                        background=doc.get('background', False),
                        expireAfterSeconds=doc.get('expireAfterSeconds'))
            else:
                self._dst_mc[dbname][collname].create_index(index_parse(doc['key']),
                        unique=doc.get('unique', False),
                        dropDups=doc.get('dropDups', False),
                        background=doc.get('background', False))

    def _sync_oplog(self, mc, oplog_start):
        """ Apply oplog.
        """
        try:
            host, port = mc.primary
            self._logger.info('sync oplog from %s:%d...' % (host, port))

            cursor = mc['local']['oplog.rs'].find({'ts': {'$gte': oplog_start}}, tailable=True)
            if not cursor:
                self._logger.error('oplog not found')
                return
            # verify oplog is healthy
            if cursor[0]['ts'] != oplog_start:
                self._logger.error('oplog is stale, oplog-sync terminate.')
                return
            self._logger.info('oplog is valid')

        except pymongo.errors.AutoReconnect:
            mc = self.reconnect(host, port)
            cursor = mc['local']['oplog.rs'].find({'ts': {'$gte': self._last_optime}}, tailable=True)
        except Exception as e:
            self._logger.error(e)
            raise e

        # no matter actually
        # skip the first oplog-entry
        #cursor.skip(1)

        n = 0 # counter

        while True:
            try:
                if not cursor.alive:
                    self._logger.error('cursor is dead')
                    raise pymongo.errors.AutoReconnect
                oplog = cursor.next()
            
                # validate oplog
                if self._filter:
                    if not self._filter.valid_oplog(oplog):
                        continue

                # use while make sure oplog is applied successfully
                while True:
                    try:
                        # parse oplog
                        ts = oplog['ts']
                        op = oplog['op'] # 'n' or 'i' or 'u' or 'c' or 'd'
                        ns = oplog['ns']
                        dbname = ns.split('.', 1)[0]
                        if op == 'i': # insert
                            collname = ns.split('.', 1)[1]
                            self._dst_mc[dbname][collname].save(oplog['o'])
                        elif op == 'u': # update
                            collname = ns.split('.', 1)[1]
                            self._dst_mc[dbname][collname].update(oplog['o2'], oplog['o'])
                        elif op == 'd': # delete
                            collname = ns.split('.', 1)[1]
                            self._dst_mc[dbname][collname].remove(oplog['o'])
                        elif op == 'c': # command
                            self._dst_mc[dbname].command(oplog['o'])
                        elif op == 'n': # no-op
                            self._logger.info('no-op')
                        else:
                            self._logger.error('unknown command: %s' % oplog)

                        self._last_optime = ts
                        n += 1
                        if n % 10000 == 0:
                            self._logger.info('apply %d, %s' % (n, datetime.datetime.fromtimestamp(ts.time)))
                        break

                    except pymongo.errors.DuplicateKeyError as e:
                        # TODO
                        # through unique index, delete old, insert new
                        self._logger.error(e)
                        self._logger.error(oplog)
                        break
                    except pymongo.errors.AutoReconnect:
                        self._dst_mc = self.reconnect(self._dst_host, self._dst_port)
                    except Exception as e:
                        self._logger.error(e)
                        self._logger.error(oplog)
                        break

            except StopIteration as e:
                # there is no operation to apply, wait a moment
                time.sleep(0.1)
            except pymongo.errors.AutoReconnect:
                mc = self.reconnect(host, port)
                cursor = mc['local']['oplog.rs'].find({'ts': {'$gte': self._last_optime}}, tailable=True)
            except Exception as e:
                self._logger.error(e)
                raise e

    @property
    def _current_process_name(self):
        return multiprocessing.current_process().name

    def run(self):
        """ Start data synchronization.
        """
        self._sync(self._src_mc)

        # never drop database automatically
        # you should clear the databases by self if necessary

        try:
            self._sync(self._src_mc)
        except exceptions.KeyboardInterrupt:
            self._logger.info('terminating...')
        except Exception as e:
            self._logger.error(e)
            raise e

    def reconnect(self, host, port, **kwargs):
        """ Try to reconnect until done.
        """
        while True:
            try:
                self._logger.info('try to reconnect %s:%d' % (host, port))
                mc = mongo_helper.mongo_connect(host, port, **kwargs)
                if mc.alive():
                    self._logger.info('reconnect ok')
                    return mc
                else:
                    mc.close()
                    time.sleep(1)
            except Exception as e:
                self._logger.error(e)
                time.sleep(1)

