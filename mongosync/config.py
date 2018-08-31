import pymongo
from mongosync.mongo_utils import get_version
from mongosync.data_filter import DataFilter


class MongoConfig(object):
    def __init__(self, hosts, authdb='', username='', password=''):
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

        try:
            import gevent
            self.asyncio = True
        except ImportError:
            self.asyncio = False

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

    def info(self, logger=None):
        """ Output to logfile or stdout.
        """
        if logger:
            logger.info('================================================')
            logger.info('src hostportstr :  %s' % self.src_hostportstr)
            logger.info('src authdb      :  %s' % self.src_conf.authdb)
            logger.info('src username    :  %s' % self.src_conf.username)
            logger.info('src password    :  %s' % self.src_conf.password)
            if isinstance(self.src_conf.hosts, str) or isinstance(self.src_conf.hosts, unicode):
                logger.info('src db version  :  %s' % get_version(self.src_conf.hosts))

            logger.info('dst hostportstr :  %s' % self.dst_hostportstr)
            if isinstance(self.dst_conf, MongoConfig):
                if isinstance(self.dst_conf.hosts, str) or isinstance(self.dst_conf.hosts, unicode):
                    logger.info('dst authdb      :  %s' % self.dst_conf.authdb)
                    logger.info('dst username    :  %s' % self.dst_conf.username)
                    logger.info('dst password    :  %s' % self.dst_conf.password)
                    logger.info('dst db version  :  %s' % get_version(self.dst_conf.hosts))

            logger.info('databases       :  %s' % ', '.join(self.data_filter._related_dbs))
            logger.info('collections     :  %s' % ', '.join(self.data_filter._include_colls))
            logger.info('db mapping      :  %s' % self.dbmap_str)
            logger.info('fileds          :  %s' % self.fieldmap_str)

            logger.info('start optime    :  %s' % self.start_optime)
            logger.info('optime logfile  :  %s' % self.optime_logfilepath)
            logger.info('log filepath    :  %s' % self.logfilepath)
            logger.info('gevent          :  %s' % self.asyncio)
            logger.info('pymongo version :  %s' % pymongo.version)
            logger.info('================================================')
        else:
            print '================================================'
            print 'src hostportstr :  %s' % self.src_hostportstr
            print 'src authdb      :  %s' % self.src_conf.authdb
            print 'src username    :  %s' % self.src_conf.username
            print 'src password    :  %s' % self.src_conf.password
            if isinstance(self.dst_conf.hosts, unicode):
                print 'src db version  :  %s' % get_version(self.src_conf.hosts)

            print 'dst hostportstr :  %s' % self.dst_hostportstr
            if isinstance(self.dst_conf, MongoConfig):
                if isinstance(self.dst_conf.hosts, str) or isinstance(self.dst_conf.hosts, unicode):
                    print 'dst authdb      :  %s' % self.dst_conf.authdb
                    print 'dst username    :  %s' % self.dst_conf.username
                    print 'dst password    :  %s' % self.dst_conf.password
                    print 'dst db version  :  %s' % get_version(self.dst_conf.hosts)

            print 'databases       :  %s' % ', '.join(self.data_filter._related_dbs)
            print 'collections     :  %s' % ', '.join(self.data_filter._include_colls)
            print 'db mapping      :  %s' % self.dbmap_str
            print 'fileds          :  %s' % self.fieldmap_str

            print 'start optime    :  %s' % self.start_optime
            print 'optime logfile  :  %s' % self.optime_logfilepath
            print 'log filepath    :  %s' % self.logfilepath
            print 'gevent          :  %s' % self.asyncio
            print 'pymongo version :  %s' % pymongo.version
            print '================================================'
