import sys
import argparse
from bson.timestamp import Timestamp
from mongosync.config import Config, CheckConfig
from mongosync.config_file import ConfigFile
from mongosync.mongo_utils import parse_hostportstr
from mongosync.optime_logger import OptimeLogger


class CommandOptions(object):
    """ Command options.
    """
    @staticmethod
    def parse():
        """ Parse command options and generate config.
        """
        conf = Config()

        parser = argparse.ArgumentParser(description='Sync data from a replica-set to another MongoDB/Elasticsearch.')
        parser.add_argument('-f', '--config', nargs='?', required=False, help='configuration file, note that command options will override items in config file')
        parser.add_argument('--src', nargs='?', required=False, help='source should be hostportstr of a replica-set member')
        parser.add_argument('--src-authdb', nargs='?', required=False, help="src authentication database, default is 'admin'")
        parser.add_argument('--src-username', nargs='?', required=False, help='src username')
        parser.add_argument('--src-password', nargs='?', required=False, help='src password')
        parser.add_argument('--dst', nargs='?', required=False, help='destination should be hostportstr of a mongos or mongod instance')
        parser.add_argument('--dst-authdb', nargs='?', required=False, help="dst authentication database, default is 'admin', for MongoDB")
        parser.add_argument('--dst-username', nargs='?', required=False, help='dst username, for MongoDB')
        parser.add_argument('--dst-password', nargs='?', required=False, help='dst password, for MongoDB')
        parser.add_argument('--start-optime', type=int, nargs='?', required=False, help='timestamp in second, indicates oplog based increment sync')
        parser.add_argument('--optime-logfile', nargs='?', required=False, help="optime log file path, use this as start optime if without '--start-optime'")
        parser.add_argument('--logfile', nargs='?', required=False, help='log file path')

        args = parser.parse_args()

        if args.config is not None:
            conf = ConfigFile.load(args.config)
        if args.src is not None:
            conf.src_conf.hosts = args.src
        if args.src_authdb is not None:
            conf.src_conf.authdb = args.src_authdb
        if args.src_username is not None:
            conf.src_conf.username = args.src_username
        if args.src_password is not None:
            conf.src_conf.password = args.src_password
        if args.dst is not None:
            conf.dst_conf.hosts = args.dst
        if args.dst_authdb is not None:
            conf.dst_conf.authdb = args.dst_authdb
        if args.dst_username is not None:
            conf.dst_conf.username = args.dst_username
        if args.dst_password is not None:
            conf.dst_conf.password = args.dst_password
        if args.start_optime is not None:
            conf.start_optime = Timestamp(args.start_optime, 0)
        if args.optime_logfile is not None:
            conf.optime_logfilepath = args.optime_logfile
            if args.start_optime is None:
                optime_logger = OptimeLogger(args.optime_logfile)
                conf.start_optime = optime_logger.read()
        if args.logfile is not None:
            conf.logfilepath = args.logfile

        return conf


class CheckCommandOptions(object):
    """ Check command options.
    """
    @staticmethod
    def parse():
        """ Parse command options and generate config.
        """
        parser = argparse.ArgumentParser(description='Check data consistency including data and indexes.')
        parser.add_argument('--origin', nargs='?', metavar='URI', required=True, help='origin mongos URI')
        parser.add_argument('--target', nargs='?', metavar='URI', required=True, help='target mongos URI')
        parser.add_argument('--dbs', nargs='+', required=False, help='databases to check')
        parser.add_argument('--src-db', nargs='?', required=False, help="database to check in origin, work with '--dst-db', conflicts with '--dbs'")
        parser.add_argument('--dst-db', nargs='?', required=False, help="database to check in target, work with '--src-db', conflicts with '--dbs'")

        args = vars(parser.parse_args())

        conf = CheckConfig()
        conf.src_uri = args['origin']
        conf.dst_uri = args['target']
        if args['dbs'] is not None:
            conf.dbs = args['dbs']
        if args['src_db'] is not None:
            conf.src_db = args['src_db']
        if args['dst_db'] is not None:
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

        return conf
