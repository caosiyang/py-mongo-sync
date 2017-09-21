import sys
import argparse
from config import Config
from mongo_helper import parse_hostportstr

class CommandOptions(object):
    """ Command options.
    """
    @staticmethod
    def parse():
        """ Parse command options and generate config.
        """
        conf = Config()

        parser = argparse.ArgumentParser(description='Sync data from a replica-set to another mongod/replica-set/sharded-cluster.')
        parser.add_argument('--from', nargs='?', required=True, help='source must be a member of replica-set')
        parser.add_argument('--src-authdb', nargs='?', required=False, help="authentication database, default is 'admin'")
        parser.add_argument('--src-username', nargs='?', required=False, help='src username')
        parser.add_argument('--src-password', nargs='?', required=False, help='src password')
        parser.add_argument('--src-engine', nargs='?', required=False, help='src engine, the value could be mongodb or tokumx, default is mongodb')
        parser.add_argument('--to', nargs='?', required=True, help='destination should be a mongos or mongod instance')
        parser.add_argument('--dst-authdb', nargs='?', required=False, help="authentication database, default is 'admin'")
        parser.add_argument('--dst-username', nargs='?', required=False, help='dst username')
        parser.add_argument('--dst-password', nargs='?', required=False, help='dst password')
        parser.add_argument('--dbs', nargs='+', required=False, help="databases to sync, conflict with '--colls'")
        parser.add_argument('--colls', nargs='+', required=False, help="collections to sync, conflict with '--dbs'")
        parser.add_argument('--src-db', nargs='?', required=False, help="src database name, work with '--dst-db', conflict with '--dbs' and '--colls'")
        parser.add_argument('--dst-db', nargs='?', required=False, help="dst database name, work with '--src-db', conflict with '--dbs' and '--colls'")
        parser.add_argument('--start-optime', nargs='?', required=False, help="start optime, a timestamp value in second for MongoDB or a 'YYYYmmddHHMMSS' value for TokuMX")
        parser.add_argument('--log', nargs='?', required=False, help='log file path')

        args = vars(parser.parse_args())

        if args['from'] != None:
            conf.src_hostportstr = args['from']
            conf.src_host, conf.src_port = parse_hostportstr(conf.src_hostportstr)
        if args['src_engine'] != None:
            if args['src_engine'] not in ['mongodb', 'tokumx']:
                print 'invalid src_engine, terminate'
                sys.exit(1)
            conf.src_engine = args['src_engine']
        if args['src_authdb'] != None:
            conf.src_authdb = args['src_authdb']
        if args['src_username'] != None:
            conf.src_username = args['src_username']
        if args['src_password'] != None:
            conf.src_password = args['src_password']
        if args['to'] != None:
            conf.dst_hostportstr = args['to']
            conf.dst_host, conf.dst_port = parse_hostportstr(conf.dst_hostportstr)
        if args['dst_authdb'] != None:
            conf.dst_authdb = args['dst_authdb']
        if args['dst_username'] != None:
            conf.dst_username = args['dst_username']
        if args['dst_password'] != None:
            conf.dst_password = args['dst_password']
        if args['dbs'] != None:
            conf.dbs = args['dbs']
        if args['colls'] != None:
            conf.colls = args['colls']
        if args['src_db'] != None:
            conf.src_db = args['src_db']
        if args['dst_db'] != None:
            conf.dst_db = args['dst_db']
        if args['start_optime'] != None:
            conf.start_optime = args['start_optime']
        if args['log'] != None:
            conf.logfilepath = args['log']

        conflict = False
        conflict = conflict or (conf.dbs and (conf.colls or conf.src_db or conf.dst_db))
        conflict = conflict or (conf.colls and (conf.dbs or conf.src_db or conf.dst_db))
        conflict = conflict or (conf.src_db and (conf.dbs or conf.colls))
        conflict = conflict or (conf.dst_db and (conf.dbs or conf.colls))
        if conflict:
            print "Terminated, conflict command options found"
            sys.exit(1)
        if conf.src_db and not conf.dst_db:
            print "Terminated, require command option '--dst-db'"
            sys.exit(1)
        if conf.dst_db and not conf.src_db:
            print "Terminated, require command option '--src-db'"
            sys.exit(1)
        if conf.src_db and conf.dst_db and conf.src_db == conf.dst_db:
            print 'Terminated, src_db is same as dst_db'
            sys.exit(1)

        return conf

class CheckCommandOptions(object):
    """ Check command options.
    """
    @staticmethod
    def parse():
        """ Parse command options and generate config.
        """
        conf = Config()

        parser = argparse.ArgumentParser(description='Check data consistency including data and indexes.')
        parser.add_argument('--from', nargs='?', required=True, help='the source must be a mongod instance of replica-set')
        parser.add_argument('--src-authdb', nargs='?', required=False, help="authentication database, default is 'admin'")
        parser.add_argument('--src-username', nargs='?', required=False, help='src username')
        parser.add_argument('--src-password', nargs='?', required=False, help='src password')
        parser.add_argument('--to', nargs='?', required=True, help='the destionation should be a mongos or mongod instance')
        parser.add_argument('--dst-authdb', nargs='?', required=False, help="authentication database, default is 'admin'")
        parser.add_argument('--dst-username', nargs='?', required=False, help='dst username')
        parser.add_argument('--dst-password', nargs='?', required=False, help='dst password')
        parser.add_argument('--dbs', nargs='+', required=False, help='databases to check')
        parser.add_argument('--src-db', nargs='?', required=False, help="src database to check, work with '--dst-db', conflict with '--dbs'")
        parser.add_argument('--dst-db', nargs='?', required=False, help="dst database to check, work with '--src-db', conflict with '--dbs'")

        args = vars(parser.parse_args())
        if args['from'] != None:
            conf.src_hostportstr = args['from']
            conf.src_host, conf.src_port = parse_hostportstr(conf.src_hostportstr)
        if args['src_authdb'] != None:
            conf.src_authdb = args['src_authdb']
        if args['src_username'] != None:
            conf.src_username = args['src_username']
        if args['src_password'] != None:
            conf.src_password = args['src_password']
        if args['to'] != None:
            conf.dst_hostportstr = args['to']
            conf.dst_host, conf.dst_port = parse_hostportstr(conf.dst_hostportstr)
        if args['dst_authdb'] != None:
            conf.dst_authdb = args['dst_authdb']
        if args['dst_username'] != None:
            conf.dst_username = args['dst_username']
        if args['dst_password'] != None:
            conf.dst_password = args['dst_password']
        if args['dbs'] != None:
            conf.dbs = args['dbs']
        if args['src_db'] != None:
            conf.src_db = args['src_db']
        if args['dst_db'] != None:
            conf.dst_db = args['dst_db']

        if conf.dbs and (conf.src_db or conf.dst_db):
            print "Terminated, conflict command options found"
            sys.exit(1)
        if conf.src_db and not conf.dst_db:
            print "Terminated, require command option '--dst-db'"
            sys.exit(1)
        if conf.dst_db and not conf.src_db:
            print "Terminated, require command option '--src-db'"
            sys.exit(1)
        if conf.src_db and conf.dst_db and conf.src_db == conf.dst_db:
            print 'Terminated, src_db is same as dst_db'
            sys.exit(1)

        return conf

