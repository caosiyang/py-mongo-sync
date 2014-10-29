import time
import logging
import threading
import exceptions
import pymongo
import mongo_helper

class Source:
    def __init__(self):
        self.mc = None
        self.oplog_start = 0

class MongoMultiSourceSynchronizer(object):
    """ MongoDB multi-source synchronizer.
    """
    def __init__(self, src_hostportstr_list=[], dst_hostportstr='', **kwargs):
        """ Constructor.
        """
        self._dst_username = kwargs.get('dst_username')
        self._dst_password = kwargs.get('dst_password')

        self._src_mc_list = []
        self._dst_mc = None
        self._logger = logging.getLogger()

        if not src_hostportstr_list:
            pass
            #raise Exception('src_hostportstr_list is empty')

        # init source mongo clients
        for hostportstr in src_hostportstr_list:
            mc = pymongo.MongoReplicaSetClient(hostportstr,
                    replicaSet=mongo_helper.get_replset_name(hostportstr),
                    read_preference=pymongo.read_preferences.ReadPreference.PRIMARY)
            self._src_mc_list.append(mc)
        if not self._src_mc_list:
            raise Exception('source mongo client list is empty')

        # init destination mongo client
        dst_replset_name = mongo_helper.get_replset_name(dst_hostportstr)
        if dst_replset_name:
            self._dst_mc = pymongo.MongoReplicaSetClient(dst_hostportstr, replicaSet=dst_replset_name, w=0)
        else:
            host = dst_hostportstr.split(':')[0]
            port = int(dst_hostportstr.split(':')[1])
            self._dst_mc = pymongo.MongoClient(host, port, w=0)
        if self._dst_username and self._dst_password:
            self._dst_mc.admin.authenticate(self._dst_username, self._dst_password)

    def __del__(self):
        """ Destructor.
        """
        for mc in self._src_mc_list:
            mc.close()
        if self._dst_mc:
            self._dst_mc.close()

    def _get_current_optime(self, mc):
        """ Get current optime of source mongod.
        """
        ts = None
        db = mc['admin']
        rs_status = db.command({'replSetGetStatus': 1})
        members = rs_status.get('members')
        if members:
            for member in members:
                role = member.get('stateStr')
                if role == 'PRIMARY':
                    ts = member.get('optime')
                    break
        return ts

    def _sync(self, mc, oplog_start):
        """ Sync databases and oplog.
        """
        self._sync_databases(mc)
        self._sync_oplog(mc, oplog_start)

    def _sync_databases(self, mc):
        """ Sync databases except admin and local.
        """
        self._logger.info('[%s] sync databases...' % self._current_thread_name)
        dbnames = mc.database_names()
        for dbname in dbnames:
            if dbname not in ['admin', 'local']:
                self._sync_database(mc, dbname)
        self._logger.info('[%s] all databases done' % self._current_thread_name)

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
            if collname != 'qiyirc':
                self._sync_collection(mc, dbname, collname)
            #self._sync_collection(mc, dbname, collname)

    def _sync_collection(self, mc, dbname, collname):
        """ Sync a collection.
        """
        self._logger.info('[%s] >>>> %s.%s' % (self._current_thread_name, dbname, collname))
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
                self._logger.info('[%s] >> %d' % (self._current_thread_name, n))
        if len(buf) > 0:
            dst_coll.insert(buf)
        self._logger.info('[%s] ==== %s.%s %d' % (self._current_thread_name, dbname, collname, n))

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

        for doc in mc[dbname]['system.indexes'].find():
            collname = doc['ns'].replace(dbname, '', 1)[1:]
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
            self._logger.info('[%s] sync oplog...' % self._current_thread_name)

            n = 0 # counter
            cursor = mc['local']['oplog.rs'].find({'ts': {'$gte': oplog_start}}, tailable=True)
            if not cursor:
                self._logger.error('oplog not found')
                return
            # verify oplog is healthy
            if cursor[0]['ts'] != oplog_start:
                self._logger.error('oplog is stale, oplog-sync terminate.')
                return

            # no matter actually
            # skip the first oplog-entry
            #cursor.skip(1)

            while True:
                try:
                    if not cursor.alive:
                        self._logger.error('cursor is dead')
                        return
                    oplog = cursor.next()
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
                        self._logger.error('unknown command: %s' % oplog)
                    n += 1
                    if n % 1000 == 0:
                        self._logger.info('[%s] apply %d, ts: %s' % (self._current_thread_name, ts))
                except Exception as e:
                    # there is no operation to apply, wait a moment
                    time.sleep(0.1)
        except Exception as e:
            self._logger.error(e)
            raise e

    @property
    def _current_thread_name(self):
        return threading.current_thread().name

    def run(self):
        """ Start data synchronization.
        """
        try:
            # drop all databases that to sync
            for mc in self._src_mc_list:
                for dbname in mc.database_names():
                    if dbname not in ['admin', 'local']:
                        self._dst_mc.drop_database(dbname)
            # sync data
            id = 1
            for mc in self._src_mc_list:
                oplog_start = self._get_current_optime(mc)
                if not oplog_start:
                    raise Exception('oplog_start is None, sync terminated')
                t = threading.Thread(target=self._sync, args=(mc, oplog_start))
                t.setName('sync-thread-%d' % id)
                t.setDaemon(True)
                t.start()
                id += 1
            while True:
                time.sleep(10)
        except exceptions.KeyboardInterrupt:
            self._logger.info('terminating...')
        except Exception as e:
            self._logger.error(e)
            raise e
