import sys
import argparse
import pymongo
import mongo_helper
import filter

# global variables
g_src = ''
g_src_authdb = 'admin'
g_src_username = ''
g_src_password = ''
g_dst = ''
g_dst_authdb = 'admin'
g_dst_username = ''
g_dst_password = ''
g_dbs = []
g_db_filter = None

def parse_args():
    """ Parse arguments.
    """
    global g_src, g_src_authdb, g_src_username, g_src_password, g_dst, g_dst_authdb, g_dst_username, g_dst_password, g_dbs

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

    args = vars(parser.parse_args())
    if args['from'] != None:
        g_src = args['from']
    if args['src_authdb'] != None:
        g_src_authdb = args['src_authdb']
    if args['src_username'] != None:
        g_src_username = args['src_username']
    if args['src_password'] != None:
        g_src_password = args['src_password']
    if args['to'] != None:
        g_dst = args['to']
    if args['dst_authdb'] != None:
        g_dst_authdb = args['dst_authdb']
    if args['dst_username'] != None:
        g_dst_username = args['dst_username']
    if args['dst_password'] != None:
        g_dst_password = args['dst_password']
    if args['dbs'] != None:
        g_dbs = args['dbs']

def get_standard_index_name(index_items):
    """ User can specify any name for a index.
    We should generate a standard name for a index and then compare them.
    """
    index_keys = []
    for key, direction in index_items['key']:
        if isinstance(direction, int) or isinstance(direction, long) or isinstance(direction, float):
            index_keys.append('%s_%d' % (key, int(direction)))
        elif isinstance(direction, str) or isinstance(direction, unicode):
            index_keys.append('%s_%s' % (key, direction))
        else:
            print 'invalid direction for', index_items['key']
            sys.exit(1)
    return '_'.join(index_keys)

if __name__ == '__main__':
    parse_args()

    print '=' * 48
    print 'src             :  %s' % g_src
    print 'src authdb      :  %s' % g_src_authdb
    print 'src username    :  %s' % g_src_username
    print 'src password    :  %s' % g_src_password
    print 'dst             :  %s' % g_dst
    print 'dst authdb      :  %s' % g_dst_authdb
    print 'dst username    :  %s' % g_dst_username
    print 'dst password    :  %s' % g_dst_password
    print '=' * 48

    src_host = g_src.split(':')[0]
    src_port = int(g_src.split(':')[1])
    src_mc = mongo_helper.mongo_connect(
            src_host,
            src_port,
            authdb=g_src_authdb,
            username=g_src_username,
            password=g_src_password)

    dst_host = g_dst.split(':')[0]
    dst_port = int(g_dst.split(':')[1])
    dst_mc = mongo_helper.mongo_connect(
            dst_host,
            dst_port,
            authdb=g_dst_authdb,
            username=g_dst_username,
            password=g_dst_password)
 
    if g_dbs:
        g_db_filter = filter.DatabaseFilter()
        g_db_filter.add_target_databases(g_dbs)

    ignore_dbs = ['admin', 'local']
    ignore_colls = ['system.users', 'system.profile']

    src_version = src_mc['admin'].command('serverStatus')['version']
    dst_version = dst_mc['admin'].command('serverStatus')['version']

    if src_version.startswith('2') and dst_version.startswith('3'):
        ignore_colls.append('system.indexes')

    # validate data
    data_pass = True
    print '-' * 96
    print '%s%s%s%s' % ('RESULT'.ljust(16), 'COLL'.ljust(48), 'SRC'.rjust(16), 'DST'.rjust(16))
    print '-' * 96
    for dbname in sorted(src_mc.database_names()):
        if dbname in ignore_dbs:
            continue
        if g_db_filter and not g_db_filter.valid_database(dbname):
            continue
        for collname in sorted(src_mc[dbname].collection_names(include_system_collections=False)):
            if collname not in ignore_colls:
                src_coll_cnt = src_mc[dbname][collname].count()
                dst_coll_cnt = dst_mc[dbname][collname].count()
                if src_coll_cnt == dst_coll_cnt:
                    res = 'OK'
                else:
                    res = 'ERR'
                    data_pass = False
                print '%s%s%s%s' % (res.ljust(16), (dbname + '.' + collname).ljust(48), str(src_coll_cnt).rjust(16), str(dst_coll_cnt).rjust(16))
    print '-' * 96

    # validate index
    index_pass = True
    print '-' * 128
    print '%s%s%s' % ('RESULT'.ljust(16), 'COLL'.ljust(48), 'INDEX'.ljust(64))
    print '-' * 128
    for dbname in sorted(src_mc.database_names()):
        if dbname in ignore_dbs:
            continue
        if g_db_filter and not g_db_filter.valid_database(dbname):
            continue
        for collname in sorted(src_mc[dbname].collection_names()):
            if collname in ignore_colls:
                continue
            src_index_info = src_mc[dbname][collname].index_information()
            dst_index_info = dst_mc[dbname][collname].index_information()
            src_index_names = set()
            dst_index_names = set()
            for index_items in src_index_info.itervalues():
                index_name = get_standard_index_name(index_items)
                src_index_names.add(index_name)
            for index_items in dst_index_info.itervalues():
                index_name = get_standard_index_name(index_items)
                dst_index_names.add(index_name)
            for index_name in src_index_names:
                if index_name in dst_index_names:
                    res = 'OK'
                else:
                    res = 'ERR'
                    index_pass = False
                print '%s%s%s' % (res.ljust(16), (dbname + '.' + collname).ljust(48), index_name.ljust(64))
    print '-' * 128

    if data_pass:
        print 'data: SUCCESS'
    else:
        print 'data: FAILED'

    if index_pass:
        print 'index: SUCCESS'
    else:
        print 'data: FAILED'
