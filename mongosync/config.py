import pymongo
from mongosync.mongo_helper import get_version

class Config(object):
    """ Configuration.
    """
    def __init__(self):
        self.src_hostportstr = ''
        self.src_host = ''
        self.src_port = 0
        self.src_engine = 'mongodb'
        self.src_authdb = 'admin'
        self.src_username = ''
        self.src_password = ''
        self.dst_hostportstr = ''
        self.dst_host = ''
        self.dst_port = 0
        self.dst_authdb = 'admin'
        self.dst_username = ''
        self.dst_password = ''
        self.dbs = []
        self.colls = []
        self.src_db = ''
        self.dst_db = ''
        self.start_optime = ''
        self.logfilepath = ''
        self.gevent_support = False

    def info(self, logger=None):
        """ Output to logfile or stdout.
        """
        if logger:
            logger.info('================================================')
            logger.info('src hostportstr :  %s' % self.src_hostportstr)
            logger.info('src engine      :  %s' % self.src_engine)
            logger.info('src db version  :  %s' % get_version(self.src_host, self.src_port))
            logger.info('src authdb      :  %s' % self.src_authdb)
            logger.info('src username    :  %s' % self.src_username)
            logger.info('src password    :  %s' % self.src_password)
            logger.info('dst hostportstr :  %s' % self.dst_hostportstr)
            logger.info('dst db version  :  %s' % get_version(self.dst_host, self.dst_port))
            logger.info('dst authdb      :  %s' % self.dst_authdb)
            logger.info('dst username    :  %s' % self.dst_username)
            logger.info('dst password    :  %s' % self.dst_password)
            logger.info('databases       :  %s' % self.dbs)
            logger.info('collections     :  %s' % self.colls)
            logger.info('src db          :  %s' % self.src_db)
            logger.info('dst db          :  %s' % self.dst_db)
            logger.info('start optime    :  %s' % self.start_optime)
            logger.info('log filepath    :  %s' % self.logfilepath)
            logger.info('pymongo version :  %s' % pymongo.version)
            logger.info('================================================')
        else:
            print '================================================'
            print 'src hostportstr :  %s' % self.src_hostportstr
            print 'src engine      :  %s' % self.src_engine
            print 'src db version  :  %s' % get_version(self.src_host, self.src_port)
            print 'src authdb      :  %s' % self.src_authdb
            print 'src username    :  %s' % self.src_username
            print 'src password    :  %s' % self.src_password
            print 'dst hostportstr :  %s' % self.dst_hostportstr
            print 'dst db version  :  %s' % get_version(self.dst_host, self.dst_port)
            print 'dst authdb      :  %s' % self.dst_authdb
            print 'dst username    :  %s' % self.dst_username
            print 'dst password    :  %s' % self.dst_password
            print 'databases       :  %s' % self.dbs
            print 'collections     :  %s' % self.colls
            print 'src db          :  %s' % self.src_db
            print 'dst db          :  %s' % self.dst_db
            print 'start optime    :  %s' % self.start_optime
            print 'log filepath    :  %s' % self.logfilepath
            print 'pymongo version :  %s' % pymongo.version
            print '================================================'

