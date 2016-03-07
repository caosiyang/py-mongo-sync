import sys
import argparse
import pymongo
import mongo_helper

# global variables
g_src = ''
g_src_username = ''
g_src_password = ''
g_dst = ''
g_dst_username = ''
g_dst_password = ''

def parse_args():
    """ Parse arguments.
    """
    global g_src, g_src_username, g_src_password, g_dst, g_dst_username, g_dst_password

    parser = argparse.ArgumentParser(description='Sync data from a replica-set to another mongod/replica-set/sharded-cluster.')
    parser.add_argument('--from', nargs='?', required=True, help='the source must be a mongod instance of replica-set')
    parser.add_argument('--src-username', nargs='?', required=False, help='src username')
    parser.add_argument('--src-password', nargs='?', required=False, help='src password')
    parser.add_argument('--to', nargs='?', required=True, help='the destionation should be a mongos or mongod instance')
    parser.add_argument('--dst-username', nargs='?', required=False, help='dst username')
    parser.add_argument('--dst-password', nargs='?', required=False, help='dst password')

    args = vars(parser.parse_args())
    if args['from'] != None:
        g_src = args['from']
    if args['src_username'] != None:
        g_src_username = args['src_username']
    if args['src_password'] != None:
        g_src_password = args['src_password']
    if args['to'] != None:
        g_dst = args['to']
    if args['dst_username'] != None:
        g_dst_username = args['dst_username']
    if args['dst_password'] != None:
        g_dst_password = args['dst_password']

def get_standard_index_name(index_items):
    """ User can specify any name for a index.
    We should generate a standard name for a index and then compare them.
    """
    index_keys = []
    for key, direction in index_items['key']:
        if isinstance(direction, int) or isinstance(direction, float):
            index_keys.append('%s_%d' % (key, int(direction)))
        elif isinstance(direction, str) or isinstance(direction, unicode):
            index_keys.append('%s_%s' % (key, direction))
        else:
            print 'invalid direction for', index_items['key']
            sys.exit(1)
    return '_'.join(index_keys)

def main():
    global g_src, g_src_username, g_src_password, g_dst, g_dst_username, g_dst_password

    parse_args()

    print '=' * 48
    print 'src             :  %s' % g_src
    print 'src username    :  %s' % g_src_username
    print 'src password    :  %s' % g_src_password
    print 'dst             :  %s' % g_dst
    print 'dst username    :  %s' % g_dst_username
    print 'dst password    :  %s' % g_dst_password
    print '=' * 48

    src_host = g_src.split(':')[0]
    src_port = int(g_src.split(':')[1])
    src_mc = mongo_helper.mongo_connect(
            src_host,
            src_port,
            username=g_src_username,
            password=g_src_password)

    dst_host = g_dst.split(':')[0]
    dst_port = int(g_dst.split(':')[1])
    dst_mc = mongo_helper.mongo_connect(
            dst_host,
            dst_port,
            username=g_dst_username,
            password=g_dst_password)
 
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
        if dbname not in ignore_dbs:
            for collname in sorted(src_mc[dbname].collection_names()):
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
    if data_pass:
        print 'data pass'
    else:
        print 'data not pass'

    # validate index
    index_pass = True
    print '-' * 128
    print '%s%s%s' % ('RESULT'.ljust(16), 'COLL'.ljust(48), 'INDEX'.ljust(64))
    print '-' * 128
    for dbname in sorted(src_mc.database_names()):
        if dbname not in ignore_dbs:
            for collname in sorted(src_mc[dbname].collection_names()):
                if collname not in ignore_colls:
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
    if index_pass:
        print 'index pass'
    else:
        print 'index not pass'

if __name__ == '__main__':
    main()
