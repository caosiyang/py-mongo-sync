import sys
import pymongo
from mongosync.command_options import CheckCommandOptions


def connect(uri):
    mc = pymongo.MongoClient(uri, connect=True, serverSelectionTimeoutMS=3000)
    return mc


def info(s):
    print s


def warn(s):
    print '\033[01;33;40m%s\033[0m' % s


def error(s):
    print '\033[01;31;40m%s\033[0m' % s


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


ignore_dbs = ['admin', 'config', 'local']
ignore_colls = ['system.users', 'system.profile']


if __name__ == '__main__':
    conf = CheckCommandOptions.parse()

    print '=' * 48
    print 'origin :  %s' % conf.src_uri
    print 'target :  %s' % conf.dst_uri
    print 'dbs    :  %s' % conf.dbs
    print 'src db :  %s' % conf.src_db
    print 'dst db :  %s' % conf.dst_db
    print '=' * 48

    src_mc = connect(conf.src_uri)
    dst_mc = connect(conf.dst_uri)

    rename_db_mode = False
    if conf.src_db and conf.dst_db:
        assert conf.dbs == []
        conf.dbs.append(conf.src_db)
        rename_db_mode = True
    else:
        conf.dbs = src_mc.database_names()

    src_version = src_mc['admin'].command('serverStatus')['version']
    dst_version = dst_mc['admin'].command('serverStatus')['version']

    if src_version.startswith('2') and dst_version.startswith('3'):
        ignore_colls.append('system.indexes')

    # check data
    data_pass = True
    print '-' * 88
    print '%s%s%s%s' % ('RESULT'.ljust(8), 'COLL'.ljust(48), 'ORIGIN'.rjust(16), 'TARGET'.rjust(16))
    print '-' * 88
    for dbname in sorted(src_mc.database_names()):
        if dbname in ignore_dbs:
            continue
        if dbname not in conf.dbs:
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
                info('%s%s%s%s' % (res.ljust(8), (dbname + '.' + collname).ljust(48), str(src_coll_cnt).rjust(16), str(dst_coll_cnt).rjust(16)))
            else:
                res = 'ERR'
                data_pass = False
                warn('%s%s%s%s' % (res.ljust(8), (dbname + '.' + collname).ljust(48), str(src_coll_cnt).rjust(16), str(dst_coll_cnt).rjust(16)))
    print '-' * 96

    # check index
    index_pass = True
    print '-' * 120
    print '%s%s%s' % ('RES'.ljust(8), 'COLL'.ljust(48), 'INDEX'.rjust(64))
    print '-' * 120
    for dbname in sorted(src_mc.database_names()):
        if dbname in ignore_dbs:
            continue
        if dbname not in conf.dbs:
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
                    info('%s%s%s' % (res.ljust(8), (dbname + '.' + collname).ljust(48), index_name.rjust(64)))
                else:
                    res = 'ERR'
                    index_pass = False
                    warn('%s%s%s' % (res.ljust(8), (dbname + '.' + collname).ljust(48), index_name.rjust(64)))
    print '-' * 120

    if data_pass:
        info('data: SUCCESS')
    else:
        error('data: FAILED')

    if index_pass:
        info('index: SUCCESS')
    else:
        error('index: FAILED')
