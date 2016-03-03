import sys
import pymongo

def mongo_connect(host, port, **kwargs):
    """ Connect and return a available handler.
    Recognize replica set automatically.
    Authenticate automatically if necessary.

    default:
        auth_db = admin
        read_preference = PRIMARY
        w = 1
    """
    try:
        username = kwargs.get('username', '')
        password = kwargs.get('password', '')
        auth_db = kwargs.get('auth_db', 'admin') # default auth db is 'admin'
        w = kwargs.get('w', 1) # default w = 1
        connect = kwargs.get('connect', True)
        replset_name = get_replica_set_name(host, port, **kwargs)
        if replset_name:
            mc = pymongo.MongoClient(
                    host=host,
                    port=port,
                    replicaSet=replset_name,
                    read_preference=pymongo.read_preferences.ReadPreference.PRIMARY,
                    w=w,
                    connect=connect)
        else:
            mc = pymongo.MongoClient(host, port, w=w, connect=connect)
        if username and password and auth_db:
            mc[auth_db].authenticate(username, password)
        return mc
    except Exception as e:
        raise Exception('mongo connect %s' % e)

def get_replica_set_name(host, port, **kwargs):
    """ Get replica set name.
    Return a empty string if it's not a replica set. 
    Raise exception if execute failed.
    """
    try:
        username = kwargs.get('username', '')
        password = kwargs.get('password', '')
        auth_db = kwargs.get('auth_db', 'admin')
        mc = pymongo.MongoClient(host, port)
        if username and password and auth_db:
            mc[auth_db].authenticate(username, password)
        status = mc.admin.command({'replSetGetStatus': 1})
        mc.close()
        if status['ok'] == 1:
            return status['set']
        else:
            return ''
    except pymongo.errors.OperationFailure as e:
        return ''
    except Exception as e:
        raise Exception('get_replica_set_name %s' % e)

def get_primary(host, port, **kwargs):
    """ Get host, port, replsetName of the primary node.
    """
    try:
        username = kwargs.get('username', '')
        password = kwargs.get('password', '')
        auth_db = kwargs.get('auth_db', 'admin')
        mc = pymongo.MongoClient(host, port)
        if username and password and auth_db:
            mc[auth_db].authenticate(username, password)
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
    """
    ts = None
    rs_status = mc['admin'].command({'replSetGetStatus': 1})
    members = rs_status.get('members')
    if members:
        for member in members:
            role = member.get('stateStr')
            if role == 'PRIMARY':
                ts = member.get('optime')
                break
    return ts

def get_optime_tokumx(mc):
    """ Get optime of primary in the replica set.
    """
    ts = None
    rs_status = mc['admin'].command({'replSetGetStatus': 1})
    members = rs_status.get('members')
    if members:
        for member in members:
            role = member.get('stateStr')
            if role == 'PRIMARY':
                ts = member.get('optimeDate')
                break
    return ts

def replay_oplog(oplog, mc):
    """ Replay oplog.
    """
    pass

