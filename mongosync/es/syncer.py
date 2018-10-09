import time
import pymongo
import bson
import elasticsearch
import elasticsearch.helpers
from mongosync.logger import Logger
from mongosync.common_syncer import CommonSyncer
from mongosync.config import MongoConfig, EsConfig
from mongosync.doc_utils import gen_doc_with_fields, doc_flat_to_nested, merge_doc
from mongosync.mongo_utils import parse_namespace, gen_namespace
from mongosync.mongo.handler import MongoHandler
from mongosync.es.handler import EsHandler

try:
    import gevent
except ImportError:
    pass

log = Logger.get()


class EsSyncer(CommonSyncer):
    """ Elasticsearch synchronizer.
    """
    def __init__(self, conf):
        CommonSyncer.__init__(self, conf)

        if not isinstance(self._conf.src_conf, MongoConfig):
            raise Exception('invalid src config type')
        self._src = MongoHanler(self._conf.src_conf)
        if not self._src.connect():
            raise Exception('connect to mongodb(src) failed: %s' % self._conf.src_hostportstr)

        if not isinstance(self._conf.dst_conf, EsConfig):
            raise Exception('invalid dst config type')
        self._dst = Es(self._conf.dst_conf)
        if not self._dst.connect():
            raise Exception('connect to elasticsearch(dst) failed: %s' % self._conf.dst_hostportstr)

        self._action_buf = []  # used to bulk write oplogs
        self._last_bulk_optime = None

    def _action_buf_full(self):
        return len(self._action_buf) >= 40

    def _sync_database(self, dbname):
        """ Sync a database.
        """
        log.info("sync database '%s'" % dbname)
        # create index
        idxname = self._conf.db_mapping(dbname)
        if self._dst.client().indices.exists(index=idxname):
            log.info('index already existed: %s' % idxname)
        else:
            log.info('create index: %s' % idxname)
            self._dst.client().indices.create(index=idxname)
        self._sync_collections(dbname)

    def _sync_collection(self, dbname, collname):
        """ Sync a collection until success.
        """
        src_dbname, src_collname = dbname, collname
        idxname, typename = self._conf.db_coll_mapping(dbname, collname)
        fields = self._conf.fieldmap.get(gen_namespace(dbname, collname))

        while True:
            try:
                log.info("sync collection '%s.%s' => '%s.%s'" % (src_dbname, src_collname, idxname, typename))
                cursor = self._src.client()[src_dbname][src_collname].find(filter=None,
                                                                           cursor_type=pymongo.cursor.CursorType.EXHAUST,
                                                                           no_cursor_timeout=True,
                                                                           modifiers={'$snapshot': True})
                count = cursor.count()
                if count == 0:
                    log.info('    skip empty collection')
                    return

                n = 0
                actions = []
                actions_max = 20
                groups = []
                groups_max = 10

                for doc in cursor:
                    if self._conf.asyncio:
                        id = str(doc['_id'])
                        del doc['_id']
                        source = gen_doc_with_fields(doc, fields) if fields else doc
                        if source:
                            actions.append({'_op_type': 'index', '_index': idxname, '_type': typename, '_id': id, '_source': source})
                        if len(actions) == actions_max:
                            groups.append(actions)
                            actions = []
                        if len(groups) == groups_max:
                            threads = [gevent.spawn(self._dst.bulk_write, groups[i]) for i in xrange(groups_max)]
                            gevent.joinall(threads)
                            groups = []
                    else:
                        id = str(doc['_id'])
                        del doc['_id']
                        source = gen_doc_with_fields(doc, fields) if fields else doc
                        if source:
                            actions.append({'_op_type': 'index', '_index': idxname, '_type': typename, '_id': id, '_source': source})
                        if len(actions) == actions_max:
                            elasticsearch.helpers.bulk(client=self._dst.client(), actions=actions)
                            actions = []

                    n += 1
                    if n % 1000 == 0:
                        log.info('    %s.%s %d/%d (%.2f%%)' % (src_dbname, src_collname, n, count, float(n)/count*100))

                if self._conf.asyncio:
                    if len(groups) > 0:
                        threads = [gevent.spawn(self._dst.bulk_write, groups[i]) for i in xrange(len(groups))]
                        gevent.joinall(threads)
                    if len(actions) > 0:
                        elasticsearch.helpers.bulk(client=self._dst.client(), actions=actions)
                else:
                    if len(actions) > 0:
                        elasticsearch.helpers.bulk(client=self._dst.client(), actions=actions)

                log.info('    %s.%s %d/%d (%.2f%%)' % (src_dbname, src_collname, n, count, float(n)/count*100))
                return
            except pymongo.errors.AutoReconnect:
                self._src.reconnect()

    def _sync_oplog(self, oplog_start):
        """ Replay oplog.
        """
        self._last_bulk_optime = oplog_start

        n_total = 0
        n_skip = 0

        while True:
            # try to get cursor until success
            try:
                host, port = self._src.client().address
                log.info('try to sync oplog from %s on %s:%d' % (self._last_bulk_optime, host, port))
                # set codec options to guarantee the order of keys in command
                coll = self._src.client()['local'].get_collection('oplog.rs',
                                                                  codec_options=bson.codec_options.CodecOptions(document_class=bson.son.SON))
                cursor = coll.find({'ts': {'$gte': oplog_start}},
                                   cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT,
                                   no_cursor_timeout=True)

                # New in version 3.2
                # src_version = mongo_utils.get_version(self._src.client())
                # if mongo_utils.version_higher_or_equal(src_version, '3.2.0'):
                #     cursor.max_await_time_ms(1000)

                valid_start_optime = False  # need to validate

                while True:
                    try:
                        if not cursor.alive:
                            log.error('cursor is dead')
                            raise pymongo.errors.AutoReconnect

                        oplog = cursor.next()
                        n_total += 1

                        if not valid_start_optime:
                            if oplog['ts'] == oplog_start:
                                log.info('oplog is ok: %s' % oplog_start)
                                valid_start_optime = True
                            else:
                                log.error('oplog %s is stale, terminate' % oplog_start)
                                return

                        # validate oplog
                        if not self._conf.data_filter.valid_oplog(oplog):
                            n_skip += 1
                            self._last_optime = oplog['ts']
                            continue

                        op = oplog['op']
                        ns = oplog['ns']

                        if op == 'i':  # insert
                            dbname, collname = parse_namespace(ns)
                            idxname, typename = self._conf.db_coll_mapping(dbname, collname)
                            fields = self._conf.fieldmap.get(gen_namespace(dbname, collname))

                            doc = oplog['o']
                            id = str(doc['_id'])
                            del doc['_id']
                            if fields:
                                doc = gen_doc_with_fields(doc, fields)
                            if doc:
                                self._action_buf.append({'_op_type': 'index', '_index': idxname, '_type': typename, '_id': id, '_source': doc})

                        elif op == 'u':  # update
                            dbname, collname = parse_namespace(ns)
                            idxname, typename = self._conf.db_coll_mapping(dbname, collname)
                            fields = self._conf.fieldmap.get(gen_namespace(dbname, collname))

                            id = str(oplog['o2']['_id'])

                            if '$set' in oplog['o']:
                                doc = {}
                                for k, v in oplog['o']['$set'].iteritems():
                                    if not fields or k in fields:
                                        sub_doc = doc_flat_to_nested(k.split('.'), v)
                                        merge_doc(doc, sub_doc)
                                if doc:
                                    self._action_buf.append({'_op_type': 'update',
                                                             '_index': idxname,
                                                             '_type': typename,
                                                             '_id': id,
                                                             '_retry_on_conflict': 3,
                                                             'doc': doc,
                                                             'doc_as_upsert': True})

                            if '$unset' in oplog['o']:
                                script_statements = []
                                for keypath in oplog['o']['$unset'].iterkeys():
                                    if not fields or keypath in fields:
                                        pos = keypath.rfind('.')
                                        if pos >= 0:
                                            script_statements.append('ctx._source.%s.remove("%s")' % (keypath[:pos], keypath[pos+1:]))
                                        else:
                                            script_statements.append('ctx._source.remove("%s")' % keypath)
                                if script_statements:
                                    doc = {'script': '; '.join(script_statements)}
                                    self._action_buf.append({'_op_type': 'update',
                                                             '_index': idxname,
                                                             '_type': typename,
                                                             '_id': id,
                                                             '_retry_on_conflict': 3,
                                                             'script': doc['script']})

                            if '$set' not in oplog['o'] and '$unset' not in oplog['o']:
                                log.warn('unexpect oplog: %s', oplog['o'])

                        elif op == 'd':  # delete
                            dbname, collname = parse_namespace(ns)
                            idxname, typename = self._conf.db_coll_mapping(dbname, collname)
                            id = str(oplog['o']['_id'])
                            self._action_buf.append({'_op_type': 'delete', '_index': idxname, '_type': typename, '_id': id})

                        elif op == 'c':  # command
                            dbname, _ = parse_namespace(ns)
                            idxname = self._conf.db_mapping(dbname)
                            if 'drop' in oplog['o']:
                                # TODO
                                # how to delete type?
                                pass
                                log.warn('you should implement document type deletion.')
                            if 'dropDatabase' in oplog['o']:
                                # delete index
                                self._dst.client().indices.delete(index=idxname)

                        elif op == 'n':  # no-op
                            pass
                        else:
                            log.error('invalid optype: %s' % oplog)

                        # flush
                        if self._action_buf_full():
                            self._dst.bulk_write(self._action_buf)
                            self._action_buf = []
                            self._last_bulk_optime = oplog['ts']

                        self._last_optime = oplog['ts']
                        self._log_optime(oplog['ts'])
                        self._log_progress()
                    except StopIteration as e:
                        # flush
                        if len(self._action_buf) > 0:
                            self._dst.bulk_write(self._action_buf)
                            self._action_buf = []
                            self._last_bulk_optime = self._last_optime
                        self._log_optime(self._last_optime)
                        self._log_progress('latest')
                        time.sleep(0.1)
                    except pymongo.errors.AutoReconnect as e:
                        log.error(e)
                        self._src.reconnect()
                        break
                    except elasticsearch.helpers.BulkIndexError as e:
                        log.error(e)
                        self._action_buf = []
            except IndexError as e:
                log.error(e)
                log.error('%s not found, terminate' % oplog_start)
                return
