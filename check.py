import sys
import argparse
import pymongo
from mongosync.data_filter import DataFilter
from mongosync.mongo_utils import connect
from mongosync.command_options import CheckCommandOptions

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
    conf = CheckCommandOptions.parse()

    print '=' * 48
    print 'src           :  %s' % conf.src_hostportstr
    print 'src authdb    :  %s' % conf.src_authdb
    print 'src username  :  %s' % conf.src_username
    print 'src password  :  %s' % conf.src_password
    print 'dst           :  %s' % conf.dst_hostportstr
    print 'dst authdb    :  %s' % conf.dst_authdb
    print 'dst username  :  %s' % conf.dst_username
    print 'dst password  :  %s' % conf.dst_password
    print '-' * 48
    print 'dbs           :  %s' % conf.dbs
    print 'src db        :  %s' % conf.src_db
    print 'dst db        :  %s' % conf.dst_db
    print '=' * 48

    rename_db_mode = False
    if conf.src_db and conf.dst_db:
        assert conf.dbs == []
        conf.dbs.append(conf.src_db)
        rename_db_mode = True

    data_filter = DataFilter()
    data_filter.add_include_collections(['%s.*' % db for db in conf.dbs])

    src_host = conf.src_hostportstr.split(':')[0]
    src_port = int(conf.src_hostportstr.split(':')[1])
    src_mc = connect(
            src_host,
            src_port,
            authdb=conf.src_authdb,
            username=conf.src_username,
            password=conf.src_password)

    dst_host = conf.dst_hostportstr.split(':')[0]
    dst_port = int(conf.dst_hostportstr.split(':')[1])
    dst_mc = connect(
            dst_host,
            dst_port,
            authdb=conf.dst_authdb,
            username=conf.dst_username,
            password=conf.dst_password)

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
        if not data_filter.valid_db(dbname):
            continue
        for collname in sorted(src_mc[dbname].collection_names(include_system_collections=False)):
            if collname in ignore_colls:
                continue
            if rename_db_mode:
                assert dbname == conf.src_db
                ddb = conf.dst_db
            else:
                ddb = dbname
            src_coll_cnt = src_mc[dbname][collname].count()
            dst_coll_cnt = dst_mc[ddb][collname].count()
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
    print '%s%s%s' % ('RESULT'.ljust(16), 'COLL'.ljust(48), 'INDEX'.rjust(64))
    print '-' * 128
    for dbname in sorted(src_mc.database_names()):
        if dbname in ignore_dbs:
            continue
        if not data_filter.valid_db(dbname):
            continue
        for collname in sorted(src_mc[dbname].collection_names()):
            if collname in ignore_colls:
                continue
            if rename_db_mode:
                assert dbname == conf.src_db
                ddb = conf.dst_db
            else:
                ddb = dbname
            src_index_info = src_mc[dbname][collname].index_information()
            dst_index_info = dst_mc[ddb][collname].index_information()
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
                print '%s%s%s' % (res.ljust(16), (dbname + '.' + collname).ljust(48), index_name.rjust(64))
    print '-' * 128

    if data_pass:
        print 'data: SUCCESS'
    else:
        print 'data: FAILED'

    if index_pass:
        print 'index: SUCCESS'
    else:
        print 'data: FAILED'
