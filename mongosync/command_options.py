import sys
import argparse

class CommandOptions(object):
    """ Command options.
    """
    def __init__(self):
        self.src_hostportstr = ''
        self.src_engine = 'mongodb'
        self.src_authdb = 'admin'
        self.src_username = ''
        self.src_password = ''
        self.dst_hostportstr = ''
        self.dst_authdb = 'admin'
        self.dst_username = ''
        self.dst_password = ''
        self.dbs = []
        self.colls = []
        self.src_db = ''
        self.dst_db = ''
        self.start_optime = ''
        self.logfilepath = ''

    def parse(self):
        """ Parse command options.
        """
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
            self.src_hostportstr = args['from']

        if args['src_engine'] != None:
            if args['src_engine'] not in ['mongodb', 'tokumx']:
                print 'invalid src_engine, terminate'
                sys.exit(1)
            self.src_engine = args['src_engine']

        if args['src_authdb'] != None:
            self.src_authdb = args['src_authdb']

        if args['src_username'] != None:
            self.src_username = args['src_username']

        if args['src_password'] != None:
            self.src_password = args['src_password']

        if args['to'] != None:
            self.dst_hostportstr = args['to']

        if args['dst_authdb'] != None:
            self.dst_authdb = args['dst_authdb']

        if args['dst_username'] != None:
            self.dst_username = args['dst_username']

        if args['dst_password'] != None:
            self.dst_password = args['dst_password']

        if args['dbs'] != None:
            self.dbs = args['dbs']

        if args['colls'] != None:
            self.colls = args['colls']

        if args['src_db'] != None:
            self.src_db = args['src_db']

        if args['dst_db'] != None:
            self.dst_db = args['dst_db']

        if args['start_optime'] != None:
            self.start_optime = args['start_optime']

        if args['log'] != None:
            self.logfilepath = args['log']

        conflict = False
        conflict = conflict or (self.dbs and (self.colls or self.src_db or self.dst_db))
        conflict = conflict or (self.colls and (self.dbs or self.src_db or self.dst_db))
        conflict = conflict or (self.src_db and (self.dbs or self.colls))
        conflict = conflict or (self.dst_db and (self.dbs or self.colls))
        if conflict:
            print "Terminated, conflict command options found"
            sys.exit(1)

        if self.src_db and not self.dst_db:
            print "Terminated, require command option '--dst-db'"
            sys.exit(1)

        if self.dst_db and not self.src_db:
            print "Terminated, require command option '--src-db'"
            sys.exit(1)

        if self.src_db and self.dst_db and self.src_db == self.dst_db:
            print 'Terminated, src_db is same as dst_db'
            sys.exit(1)

        return True


class CheckCommandOptions(object):
    """ Check command options.
    """
    def __init__(self):
        self.src_hostportstr = ''
        self.src_authdb = 'admin'
        self.src_username = ''
        self.src_password = ''
        self.dst_hostportstr = ''
        self.dst_authdb = 'admin'
        self.dst_username = ''
        self.dst_password = ''
        self.dbs = []
        self.src_db = ''
        self.dst_db = ''

    def parse(self):
        """ Parse command options.
        """
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
            self.src_hostportstr = args['from']
        if args['src_authdb'] != None:
            self.src_authdb = args['src_authdb']
        if args['src_username'] != None:
            self.src_username = args['src_username']
        if args['src_password'] != None:
            self.src_password = args['src_password']
        if args['to'] != None:
            self.dst_hostportstr = args['to']
        if args['dst_authdb'] != None:
            self.dst_authdb = args['dst_authdb']
        if args['dst_username'] != None:
            self.dst_username = args['dst_username']
        if args['dst_password'] != None:
            self.dst_password = args['dst_password']
        if args['dbs'] != None:
            self.dbs = args['dbs']
        if args['src_db'] != None:
            self.src_db = args['src_db']
        if args['dst_db'] != None:
            self.dst_db = args['dst_db']

        if self.dbs and (self.src_db or self.dst_db):
            print "Terminated, conflict command options found"
            sys.exit(1)
        if self.src_db and not self.dst_db:
            print "Terminated, require command option '--dst-db'"
            sys.exit(1)
        if self.dst_db and not self.src_db:
            print "Terminated, require command option '--src-db'"
            sys.exit(1)
        if self.src_db and self.dst_db and self.src_db == self.dst_db:
            print 'Terminated, src_db is same as dst_db'
            sys.exit(1)

        return True

