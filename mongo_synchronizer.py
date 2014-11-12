import time
import datetime
import logging
import exceptions
import multiprocessing
import Queue
import pymongo
import mongo_helper
import filter

class MongoSynchronizer(object):
    """ MongoDB multi-source synchronizer.
    """
    def __init__(self, src_hostportstr=[], dst_hostportstr='', **kwargs):
        """ Constructor.
        """
        self._src_mc_list = []
        self._dst_mc = None
        self._is_dst_mongos = False
        self._logger = logging.getLogger()
        self._filter = None

        self._dst_username = kwargs.get('dst_username')
        self._dst_password = kwargs.get('dst_password')
        self._collections = kwargs.get('collections')
        self._ignore_indexes = kwargs.get('ignore_indexes')

        if self._collections:
            self._filter = filter.CollectionFilter()
            self._filter.add_target_collections(self._collections)

        # init source mongo clients
        if not isinstance(src_hostportstr, list):
            raise Exception('source hostportstr should be a list')
        if not src_hostportstr:
            raise Exception('source hostportstr is empty')
        for hostportstr in src_hostportstr:
            mc = pymongo.MongoReplicaSetClient(hostportstr,
                    replicaSet=mongo_helper.get_replset_name(hostportstr),
                    read_preference=pymongo.read_preferences.ReadPreference.PRIMARY)
            self._src_mc_list.append(mc)

        # init destination mongo client
        if not dst_hostportstr:
            raise Exception('destination hostportstr is empty')
        dst_replset_name = mongo_helper.get_replset_name(dst_hostportstr)
        if dst_replset_name:
            self._dst_mc = pymongo.MongoReplicaSetClient(dst_hostportstr, replicaSet=dst_replset_name, w=0)
        else:
            host = dst_hostportstr.split(':')[0]
            port = int(dst_hostportstr.split(':')[1])
            self._dst_mc = pymongo.MongoClient(host, port, w=0)
        if self._dst_username and self._dst_password:
            self._dst_mc.admin.authenticate(self._dst_username, self._dst_password)
        if self._dst_mc.is_mongos:
            self._is_dst_mongos = True

    def __del__(self):
        """ Destructor.
        """
        for mc in self._src_mc_list:
            mc.close()
        if self._dst_mc:
            self._dst_mc.close()

    def _sync(self, mc):
        """ Sync databases and oplog.
        """
        oplog_start = mongo_helper.get_optime(mc)
        if not oplog_start:
            self._logger.error('[%s] get oplog_start failed, terminated' % self._current_process_name)
            sys.exit(1)
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
            if self._is_dst_mongos:
                self._sync_collection_mp(mc, dbname, collname)
            else:
                self._sync_collection(mc, dbname, collname)

    def _sync_collection(self, mc, dbname, collname):
        """ Sync a collection.
        """
        self._logger.info('[%s] >>>> %s.%s' % (self._current_process_name, dbname, collname))
        n = 0 # counter
        buf = []
        buf_max_size = 1000
        dst_coll = self._dst_mc[dbname][collname]
        cursor = mc[dbname][collname].find(spec=None, fileds={'_id': False}, snapshot=True, timeout=False)
        for doc in cursor:
            buf.append(doc)
            if len(buf) == buf_max_size:
                dst_coll.insert(buf)
                buf = []
            n += 1
            if n % 10000 == 0:
                self._logger.info('[%s] >> %d' % (self._current_process_name, n))
        if len(buf) > 0:
            dst_coll.insert(buf)
        self._logger.info('[%s] ==== %s.%s %d' % (self._current_process_name, dbname, collname, n))

    def _sync_collection_mp(self, mc, dbname, collname):
        """ Sync a collection with multi-processes.
        """
        self._logger.info('>>>> %s.%s' % (dbname, collname))
        doc_q = multiprocessing.Queue()
        ev = multiprocessing.Event()
        ev.clear()
        processes = []
        for i in range(0, 8):
            p = multiprocessing.Process(target=self._write_document, args=(dbname, collname, doc_q, ev))
            p.start()
            processes.append(p)
        n = 0
        cursor = mc[dbname][collname].find(spec=None, fileds={'_id': False}, snapshot=True, timeout=False)
        for doc in cursor:
            while doc_q.qsize() > 20000:
                time.sleep(0.2) # wait subprocess consume
            doc_q.put(doc)
            n += 1
            if n % 10000 == 0:
                self._logger.info('[%s] push %d, size: %d' % (self._current_process_name, n, doc_q.qsize()))
        ev.set()
        for p in processes:
            p.join()
        self._logger.info('==== %s.%s %d' % (dbname, collname, n))

    def _write_document(self, dbname, collname, q, ev):
        """ Write document to destination in subprocess.
        """
        n = 0
        while True:
            try:
                doc = q.get(block=True, timeout=0.1)
                self._dst_mc[dbname][collname].insert(doc)
                n += 1
            except Queue.Empty:
                if ev.is_set():
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
            self._logger.info('[%s] sync oplog from %s:%d...' % (self._current_process_name, host, port))

            n = 0 # counter
            cursor = mc['local']['oplog.rs'].find({'ts': {'$gte': oplog_start}}, tailable=True)
            if not cursor:
                self._logger.error('oplog not found')
                return

            # verify oplog is healthy
            if cursor[0]['ts'] != oplog_start:
                self._logger.error('[%s] oplog is stale, oplog-sync terminate.' % self._current_process_name)
                return
            self._logger.info('[%s] oplog is valid' % self._current_process_name)

            # no matter actually
            # skip the first oplog-entry
            #cursor.skip(1)

            while True:
                try:
                    if not cursor.alive:
                        self._logger.error('[%s] cursor is dead' % self._current_process_name)
                        return

                    oplog = cursor.next()

                    # validate oplog
                    if self._filter:
                        if not self._filter.valid_oplog(oplog):
                            continue

                    # parse oplog
                    ts = oplog['ts']
                    op = oplog['op'] # 'n' or 'i' or 'u' or 'c' or 'd'
                    ns = oplog['ns']
                    dbname = ns.split('.', 1)[0]
                    if op == 'i': # insert
                        collname = ns.split('.', 1)[1]
                        self._dst_mc[dbname][collname].insert(oplog['o'])
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
                        self._logger.error('[%s] unknown command: %s' % (self._current_process_name, oplog))
                    n += 1
                    if n % 10000 == 0:
                        self._logger.info('[%s] apply %d, %s' % (self._current_process_name, n, datetime.datetime.fromtimestamp(ts.time)))
                except Exception as e:
                    # there is no operation to apply, wait a moment
                    time.sleep(0.1)
        except Exception as e:
            self._logger.error(e)
            raise e

    @property
    def _current_process_name(self):
        return multiprocessing.current_process().name

    def run(self):
        """ Start data synchronization.
        """
        try:
            # never drop database automatically
            # you should clear the databases by self if necessary

            processes = []
            for mc in self._src_mc_list:
                p = multiprocessing.Process(target=self._sync, args=(mc,))
                p.start()
                processes.append(p)
            while True:
                for p in processes:
                    if not p.is_alive():
                        self._logger.error('[%s] %s terminated' % (self._current_process_name, p.name))
                time.sleep(60)
        except exceptions.KeyboardInterrupt:
            self._logger.info('terminating...')
        except Exception as e:
            self._logger.error(e)
            raise e

