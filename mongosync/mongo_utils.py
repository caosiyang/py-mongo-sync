import pymongo


def connect(host, port, **kwargs):
    """ Connect and return a available handler.
    Recognize replica set automatically.
    Authenticate automatically if necessary.

    default:
        authdb = admin
        read_preference = PRIMARY
        w = 1
    """
    authdb = kwargs.get('authdb', 'admin')  # default authdb is 'admin'
    username = kwargs.get('username', '')
    password = kwargs.get('password', '')
    w = kwargs.get('w', 1)
    replset_name = get_replica_set_name(host, port, **kwargs)
    if replset_name:
        mc = pymongo.MongoClient(
                host=host,
                port=port,
                connect=True,
                serverSelectionTimeoutMS=3000,
                replicaSet=replset_name,
                read_preference=pymongo.read_preferences.ReadPreference.PRIMARY,
                w=w)
    else:
        mc = pymongo.MongoClient(host, port, connect=True, serverSelectionTimeoutMS=3000, w=w)
    if username and password and authdb:
        # raise exception if auth failed here
        mc[authdb].authenticate(username, password)
    return mc


def get_version(arg):
    """ Get version.
    """
    if isinstance(arg, pymongo.MongoClient):
        return arg.server_info()['version']
    elif isinstance(arg, str) or isinstance(arg, unicode):
        host, port = parse_hostportstr(arg)
        with pymongo.MongoClient(host, port, connect=True, serverSelectionTimeoutMS=3000) as mc:
            return mc.server_info()['version']
    elif isinstance(arg, tuple):
        with pymongo.MongoClient(arg[0], arg[1], connect=True, serverSelectionTimeoutMS=3000) as mc:
            return mc.server_info()['version']
    else:
        raise Exception('invalid argument type @%s' % get_version.__name__)


def get_replica_set_name(host, port, **kwargs):
    """ Get replica set name.
    Return a empty string if it's not a replica set.
    Raise exception if execute failed.
    """
    try:
        username = kwargs.get('username', '')
        password = kwargs.get('password', '')
        authdb = kwargs.get('authdb', 'admin')
        mc = pymongo.MongoClient(host, port, connect=True, serverSelectionTimeoutMS=3000)
        if username and password and authdb:
            mc[authdb].authenticate(username, password)
        status = mc.admin.command({'replSetGetStatus': 1})
        mc.close()
        if status['ok'] == 1:
            return status['set']
        else:
            return ''
    except pymongo.errors.OperationFailure:
        return ''


def get_primary(host, port, **kwargs):
    """ Get host, port, replsetName of the primary node.
    """
    try:
        username = kwargs.get('username', '')
        password = kwargs.get('password', '')
        authdb = kwargs.get('authdb', 'admin')
        mc = pymongo.MongoClient(host, port, connect=True, serverSelectionTimeoutMS=3000)
        if username and password and authdb:
            mc[authdb].authenticate(username, password)
        status = mc.admin.command({'replSetGetStatus': 1})
        mc.close()
        if status['ok'] == 1:
            for member in status['members']:
                if member['stateStr'] == 'PRIMARY':
                    hostportstr = member['name']
                    host = hostportstr.split(':')[0]
                    port = int(hostportstr.split(':')[1])
                    replset_name = status['set']
                    return host, port, replset_name
        else:
            raise Exception('no primary in replica set')
    except Exception as e:
        raise Exception('get_primary %s' % e)


def get_optime(mc):
    """ Get optime of primary in the replica set.

    Changed in version 3.2.
    If using protocolVersion: 1, optime returns a document that contains:
        - ts, the Timestamp of the last operation applied to this member of the replica set from the oplog.
        - t, the term in which the last applied operation was originally generated on the primary.
    If using protocolVersion: 0, optime returns the Timestamp of the last operation applied to this member of the replica set from the oplog.

    Refer to https://docs.mongodb.com/manual/reference/command/replSetGetStatus/
    """
    rs_status = mc['admin'].command({'replSetGetStatus': 1})
    members = rs_status.get('members')
    if members:
        for member in members:
            role = member.get('stateStr')
            if role == 'PRIMARY':
                optime = member.get('optime')
                if isinstance(optime, dict) and 'ts' in optime:  # for MongoDB v3.2
                    return optime['ts']
                else:
                    return optime
    return None


def get_optime_tokumx(mc):
    """ Get optime of primary in the replica set.
    """
    rs_status = mc['admin'].command({'replSetGetStatus': 1})
    members = rs_status.get('members')
    if members:
        for member in members:
            role = member.get('stateStr')
            if role == 'PRIMARY':
                optime = member.get('optimeDate')
                return optime
    return None


def parse_namespace(ns):
    """ Parse namespace.
    """
    res = ns.split('.', 1)
    return res[0], res[1]


def gen_namespace(dbname, collname):
    """ Generate namespace.
    """
    return '%s.%s' % (dbname, collname)


def parse_hostportstr(hostportstr):
    """ Parse hostportstr like 'xxx.xxx.xxx.xxx:xxx'
    """
    host = hostportstr.split(':')[0]
    port = int(hostportstr.split(':')[1])
    return host, port


def collect_server_info(host, port):
    """ Collect general information of server.
    """
    info = {}
    with pymongo.MongoClient(host, port, connect=True, serverSelectionTimeoutMS=3000) as mc:
        info['version'] = mc.server_info()['version']
        return info


def version_higher_or_equal(v1, v2):
    """ Check if v1 is higher than or equal to v2.
    """
    t1 = tuple(int(val) for val in v1.split('.'))
    t2 = tuple(int(val) for val in v2.split('.'))
    return t1 >= t2


def is_command(oplog):
    """ Check if oplog is a command.
    """
    op = oplog['op']
    # createIndex() could insert a document without _id into *.system.indexes
    if op == 'c' or (op == 'i' and '_id' not in oplog['o']):
        return True
    return False
