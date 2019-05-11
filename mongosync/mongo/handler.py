import sys
import time
import pymongo
import bson
from mongosync import mongo_utils
from mongosync.config import MongoConfig
from mongosync.logger import Logger

log = Logger.get()


class MongoHandler(object):
    def __init__(self, conf):
        if not isinstance(conf, MongoConfig):
            raise Exception('expect MongoConfig')
        self._conf = conf
        self._mc = None

    def __del__(self):
        self.close()

    def connect(self):
        """ Connect to server.
        """
        try:
            if isinstance(self._conf.hosts, unicode):
                host, port = mongo_utils.parse_hostportstr(self._conf.hosts)
                self._mc = mongo_utils.connect(host, port,
                                               authdb=self._conf.authdb,
                                               username=self._conf.username,
                                               password=self._conf.password)
                self._mc.admin.command('ismaster')
                return True
            elif isinstance(self._conf.__hosts, list):
                # TODO
                return False
        except Exception as e:
            log.error('connect failed: %s' % e)
            return False

    def reconnect(self):
        """ Try to reconnect until success.
        """
        while True:
            try:
                self.close()
                self.connect()
                self.client().admin.command('ismaster')
                return
            except Exception as e:
                log.error('reconnect failed: %s' % e)
                time.sleep(1)

    def close(self):
        """ Close connection.
        """
        if self._mc:
            self._mc.close()
            self._mc = None

    def client(self):
        return self._mc

    def create_index(self, dbname, collname, keys, **options):
        """ Create index.
        """
        while True:
            try:
                self._mc[dbname][collname].create_index(keys, **options)
                return
            except pymongo.errors.AutoReconnect as e:
                log.error('%s' % e)
                self.reconnect()

    def bulk_write(self, dbname, collname, reqs):
        """ Bulk write documents until success.
        """
        while True:
            try:
                self._mc[dbname][collname].bulk_write(reqs,
                                                      ordered=True,
                                                      bypass_document_validation=False)
                return
            except pymongo.errors.AutoReconnect as e:
                log.error('%s' % e)
                self.reconnect()
            except Exception as e:
                log.error('bulk write failed: %s' % e)
                self.locate_bulk_write_error(dbname, collname, reqs)
                # if bulk write failed,
                # generally it's an odd oplog that program cannot process
                # so abort it and bugfix
                sys.exit(1)

    def locate_bulk_write_error(self, dbname, collname, reqs):
        """ Write documents one by one to locate the error(s).
        """
        for req in reqs:
            while True:
                try:
                    if isinstance(req, pymongo.ReplaceOne):
                        self._mc[dbname][collname].replace_one(req._filter, req._doc, upsert=req._upsert)
                    elif isinstance(req, pymongo.InsertOne):
                        self._mc[dbname][collname].insert_one(req._doc)
                    elif isinstance(req, pymongo.UpdateOne):
                        self._mc[dbname][collname].update_one(req._filter, req._doc, upsert=req._upsert)
                    elif isinstance(req, pymongo.DeleteOne):
                        self._mc[dbname][collname].delete_one(req._filter)
                    else:
                        log.error('unknown operation type: %s' % req)
                        sys.exit(1)
                    break
                except pymongo.errors.AutoReconnect as e:
                    log.error('%s' % e)
                    self.reconnect()
                except Exception as e:
                    log.error('%s when excuting %s on %s.%s' % (e, req, dbname, collname))
                    sys.exit(1)

    def tail_oplog(self, start_optime=None, await_time_ms=None):
        """ Return a tailable curosr of local.oplog.rs from the specified optime.
        """
        # set codec options to guarantee the order of keys in command
        coll = self._mc['local'].get_collection('oplog.rs',
                                                codec_options=bson.codec_options.CodecOptions(document_class=bson.son.SON))
        cursor = coll.find({'ts': {'$gte': start_optime}},
                           cursor_type=pymongo.cursor.CursorType.TAILABLE_AWAIT,
                           no_cursor_timeout=True)
        # New in version 3.2
        # src_version = mongo_utils.get_version(self._mc)
        # if mongo_utils.version_higher_or_equal(src_version, '3.2.0'):
        #     cursor.max_await_time_ms(1000)
        return cursor

    def replay_oplog(self, oplog):
        """ Replay oplog.
        """
        dbname, collname = mongo_utils.parse_namespace(oplog['ns'])
        while True:
            try:
                op = oplog['op']  # 'n' or 'i' or 'u' or 'c' or 'd'

                if op == 'i':  # insert
                    if '_id' in oplog['o']:
                        self._mc[dbname][collname].replace_one({'_id': oplog['o']['_id']}, oplog['o'], upsert=True)
                    else:
                        # create index
                        # insert into db.system.indexes
                        self._mc[dbname][collname].insert(oplog['o'], check_keys=False)
                elif op == 'u':  # update
                    self._mc[dbname][collname].update(oplog['o2'], oplog['o'])
                elif op == 'd':  # delete
                    self._mc[dbname][collname].delete_one(oplog['o'])
                elif op == 'c':  # command
                    # FIX ISSUE #4 and #5
                    # if use option '--colls' to sync target collections,
                    # commands running on other collections in the same database may replay failed
                    try:
                        self._mc[dbname].command(oplog['o'])
                    except pymongo.errors.OperationFailure as e:
                        log.error('%s: %s' % (e, oplog))
                elif op == 'n':  # no-op
                    pass
                else:
                    log.error('invaid op: %s' % oplog)
                return
            except pymongo.errors.DuplicateKeyError as e:
                # TODO
                # through unique index, delete old, insert new
                log.error('%s: %s' % (e, oplog))
                return
            except pymongo.errors.AutoReconnect as e:
                self.reconnect()
            except pymongo.errors.WriteError as e:
                log.error('%s' % e)

                # For case:
                #   Update the values of shard key fields when syncing from replica set to sharded cluster.
                #
                # Once you shard a collection, the shard key and the shard key values are immutable.
                # Reference: https://docs.mongodb.com/manual/core/sharding-shard-key/
                if self._mc.is_mongos and oplog['op'] == 'u' and 'the (immutable) field' in str(e):
                    old_doc = self._mc[dbname][collname].find_one(oplog['o2'])
                    if not old_doc:
                        log.error('replay update failed: document not found:', oplog['o2'])
                        sys.exit(1)
                    if '$set' in oplog['o']:
                        new_doc = old_doc.update(oplog['o']['$set'])
                    else:
                        new_doc = oplog['o']

                    # TODO: here need a transaction to delete old and insert new

                    # delete old document
                    res = self._mc[dbname][collname].delete_one(oplog['o2'])
                    if res.deleted_count != 1:
                        log.error('replay update failed: delete old document failed:', oplog['o2'])
                        sys.exit(1)
                    # insert new document
                    res = self._dst_mc[dbname][collname].insert_one(new_doc)
                    if not res.inserted_id:
                        log.error('replay update failed: insert new document failed:', new_doc)
                        sys.exit(1)
