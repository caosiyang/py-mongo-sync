import sys
import logging
import pymongo
from mongosync.mongo_utils import get_version
from mongosync.data_filter import DataFilter


class CheckConfig(object):
    def __init__(self):
        self.src_uri = ''
        self.dst_uri = ''
        self.dbs = []
        self.src_db = ''
        self.dst_db = ''


class MongoConfig(object):
    def __init__(self, hosts, authdb, username, password):
        self.hosts = hosts
        self.authdb = authdb
        self.username = username
        self.password = password


class EsConfig(object):
    def __init__(self, hosts):
        self.hosts = hosts


class Config(object):
    """ Configuration.
    """
    def __init__(self):
        self.src_conf = None
        self.dst_conf = None

        self.data_filter = DataFilter()

        # rename mapping
        self.dbmap = {}

        # fields {'ns' : frozenset(['field0', 'field1'])}
        self.fieldmap = {}

        self.start_optime = None
        self.optime_logfilepath = ''
        self.logfilepath = ''

    @property
    def src_hostportstr(self):
        return self.hostportstr(self.src_conf.hosts)

    @property
    def dst_hostportstr(self):
        return self.hostportstr(self.dst_conf.hosts)

    @property
    def dbmap_str(self):
        return ', '.join(['%s => %s' % (k, v) for k, v in self.dbmap.iteritems()])

    @property
    def fieldmap_str(self):
        return ', '.join(['%s {%s}' % (k, ', '.join(v)) for k, v in self.fieldmap.iteritems()])

    def db_mapping(self, dbname):
        mapping_dbname = self.dbmap.get(dbname.strip())
        return mapping_dbname if mapping_dbname else dbname

    def db_coll_mapping(self, dbname, collname):
        return self.db_mapping(dbname.strip()), collname.strip()

    def ns_mapping(self, dbname, collname):
        return '%s.%s' % (self.db_mapping(dbname.strip()), collname.strip())

    def hostportstr(self, hosts):
        if isinstance(hosts, str) or isinstance(hosts, unicode):
            return hosts
        elif isinstance(hosts, list):
            return ', '.join(hosts)

    def info(self, logger):
        """ Output to logfile or stdout.
        """
        if isinstance(logger, logging.Logger):
            f = lambda s: logger.info(s)
        elif isinstance(logger, file):
            f = lambda s: logger.write('%s\n' % s)
        else:
            raise Exception('error logger')

        f('================================================')
        f('src hostportstr :  %s' % self.src_hostportstr)
        f('src authdb      :  %s' % self.src_conf.authdb)
        f('src username    :  %s' % self.src_conf.username)
        f('src password    :  %s' % self.src_conf.password)
        if isinstance(self.src_conf.hosts, str) or isinstance(self.src_conf.hosts, unicode):
            f('src db version  :  %s' % get_version(self.src_conf.hosts))

        f('dst hostportstr :  %s' % self.dst_hostportstr)
        if isinstance(self.dst_conf, MongoConfig):
            if isinstance(self.dst_conf.hosts, str) or isinstance(self.dst_conf.hosts, unicode):
                f('dst authdb      :  %s' % self.dst_conf.authdb)
                f('dst username    :  %s' % self.dst_conf.username)
                f('dst password    :  %s' % self.dst_conf.password)
                f('dst db version  :  %s' % get_version(self.dst_conf.hosts))

        f('databases       :  %s' % ', '.join(self.data_filter._related_dbs))
        f('collections     :  %s' % ', '.join(self.data_filter._include_colls))
        f('db mapping      :  %s' % self.dbmap_str)
        f('fileds          :  %s' % self.fieldmap_str)

        f('start optime    :  %s' % self.start_optime)
        f('optime logfile  :  %s' % self.optime_logfilepath)
        f('log filepath    :  %s' % self.logfilepath)
        f('pymongo version :  %s' % pymongo.version)
        f('================================================')
