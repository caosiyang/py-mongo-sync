import time
import Queue
import threading
import logging
import exceptions
import pymongo
import mongo_helper


class MongoSynchronizer(object):
    """ MongoDB synchronizer.
    """
    def __init__(self, src_hostportstr='', dst_hostportstr='', **kwargs):
        """ Constructor.
        """
        self._src_username = kwargs.get('src_username')
        self._src_password = kwargs.get('src_password')
        self._dst_username = kwargs.get('dst_username')
        self._dst_password = kwargs.get('dst_password')

        self._src_mc = None
        self._dst_mc = None
        self._optime = None
        self._oplog_queue = Queue.Queue()
        self._logger = logging.getLogger()

        # init source mongo client
        self._src_mc = pymongo.MongoReplicaSetClient(src_hostportstr,
                replicaSet=mongo_helper.get_replset_name(src_hostportstr),
                read_preference=pymongo.read_preferences.ReadPreference.PRIMARY)
        if self._src_username and self._src_password:
            self._src_mc.admin.authenticate(self._src_username, self._src_password)

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
        if self._src_mc:
            self._src_mc.close()
        if self._dst_mc:
            self._dst_mc.close()

    def get_src_optime(self):
        """ Get current optime of source mongod.
        """
        ts = None
        db = self._src_mc['admin']
        rs_status = db.command({'replSetGetStatus': 1})
        members = rs_status.get('members')
        if members:
            for member in members:
                role = member.get('stateStr')
                if role == 'PRIMARY':
                    ts = member.get('optime')
                    break
        return ts

    def sync_databases(self):
        """ Sync databases except admin and local.
        """
        self._logger.info('sync databases...')
        dbnames = self._src_mc.database_names()
        for dbname in dbnames:
            if dbname not in ['admin', 'local']:
                self._sync_database(dbname)
        self._logger.info('all databases done')

    def _sync_database(self, dbname):
        """ Sync a database.
        """
        self._dst_mc.drop_database(dbname)
        # Q: Why create indexes first?
        # A: It may occured that create indexes failed after you have imported the data,
        #    for example, when you create an unique index without 'dropDups' option and get a 'duplicate keys' error.
        #    Because when you export and import data, duplicate key document may be produced.
        #    Another reason is for TokuMX. It does not support 'dropDups' option for an uniuqe index.
        #    The 'duplicate keys' error must cause index creation failed.
        self._sync_indexes(dbname)
        self._sync_collections(dbname)

    def _sync_collections(self, dbname):
        """ Sync all collections in the database except system collections.
        """
        collnames = self._src_mc[dbname].collection_names(include_system_collections=False)
        for collname in collnames:
            self._sync_collection(dbname, collname)

    def _sync_collection(self, dbname, collname):
        """ Sync a collection.
        """
        self._logger.info('>>>> %s.%s' % (dbname, collname))
        n = 0
        buf = []
        buf_max_size = 1000
        dst_coll = self._dst_mc[dbname][collname]
        cursor = self._src_mc[dbname][collname].find(spec=None, fileds={'_id': False}, snapshot=True, timeout=False)
        for doc in cursor:
            buf.append(doc)
            if len(buf) == buf_max_size:
                dst_coll.insert(buf)
                buf = []
            n += 1
            if n % 10000 == 0:
                self._logger.info('>> %d' % n)
        if len(buf) > 0:
            dst_coll.insert(buf)
        self._logger.info('==== %s.%s %d' % (dbname, collname, n))

    def _sync_indexes(self, dbname):
        """ Create indexes.
        """
        def index_parse(index_map):
            index_list = []
            for fieldname, direction in index_map.items():
                if isinstance(direction, float):
                    direction = int(direction)
                index_list.append((fieldname, direction))
            return index_list

        for doc in self._src_mc[dbname]['system.indexes'].find():
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

    def sync_oplog(self):
        """ Apply oplog.
        """
        try:
            self._logger.info('sync oplog...')
            self._logger.info('optime: %s' % self._optime)

            cursor = self._src_mc['local']['oplog.rs'].find({'ts': {'$gte': self._optime}}, tailable=True)
            if not cursor:
                self._logger.error('oplog not found')
                return
            # verify oplog is healthy
            if cursor[0]['ts'] != self._optime:
                self._logger.error('oplog is expired, oplog sync terminate...')
                return
            self._logger.info('oplog is healthy')

            # no matter actually
            # skip the first oplog-entry
            #cursor.skip(1)

            # counter
            n = 0
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
                        self._logger.info('apply %d, ts: %s' % (n, ts))
                except Exception as e:
                    # there is no operation to apply, wait a moment
                    time.sleep(0.1)
        except Exception as e:
            self._logger.error(e)
            raise e

    def buffer_oplog(self):
        """ Buffer oplog in queue.
        """
        self._logger.info('buffer-oplog thread start')
        try:
            self._logger.info('optime: %s' % self._optime)
            cursor = self._src_mc['local']['oplog.rs'].find({'ts': {'$gte': self._optime}}, tailable=True)
            if not cursor:
                self._logger.error('[oplog-backup-thread] oplog not found')
                raise Exception('[oplog-backup-thread] oplog not found')
            while True:
                if self._ev.is_set():
                    self._logger.info('buffer-oplog thread end')
                    return
                if not cursor.alive:
                    self._logger.error('[oplog-backup-thread] cursor is dead')
                    raise Exception('[oplog-backup-thread] cursor is dead')
                try:
                    oplog = cursor.next()
                    try: 
                        self._oplog_queue.put(oplog)
                    except Exception as e:
                        self._logger.error(e)
                        raise e
                except Exception as e:
                    # not found new operation, wait a moment
                    time.sleep(0.1)
        except Exception as e:
            self._logger.error(e)
            raise e

    def apply_oplog(self):
        """ Apply oplog in queue.
        """
        self._logger.info('start applying oplog...')
        n = 0
        while True:
            try:
                oplog = self._oplog_queue.get(block=True, timeout=3)
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
                #self._logger.info('%d\nop: %s\nns: %s\napply oplog: %s' % (n, op, ns, oplog))
                n += 1
                if n % 1000 == 0:
                    self._logger.info('apply %d, ts: %s' % (n, ts))
            except Queue.Empty:
                pass
            except Exception as e:
                self._logger.error(e)
                self._logger.error('apply oplog failed: %s' % oplog)

    def run(self):
        """ Start data synchronization.
        """
        try:
            self._optime = self.get_src_optime()
            self.sync_databases()
            self.sync_oplog()

            ## if the data to sync is too big or oplogsize is small,
            ## the oplog may be expired before data sync done.
            ## you can you a thread to buffer the oplog in memory and apply later
            #self._optime = self.get_src_optime()
            #self._ev = threading.Event()
            #self._ev.clear()
            ## create a thread to buffer oplog
            #t = threading.Thread(target=self.buffer_oplog)
            #t.setDaemon(True)
            #t.start()
            ## sync databases
            #self.sync_databases()
            ## apply oplog in memory
            #self.apply_oplog()
        except exceptions.KeyboardInterrupt:
            #self._ev.set()
            #t.join()
            pass
        except Exception as e:
            self._logger.error(e)
            #self._ev.set()
            #t.join()
