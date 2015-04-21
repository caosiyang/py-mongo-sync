import pymongo

def mongo_connect(host, port, **kwargs):
    """ Connect and return a available handler.
    default:
        w = 1
        read_preference = PRIMARY
    """
    w = kwargs.get('w', 1) # default w = 1
    replset_name = get_replset_name(host, port)
    if replset_name:
        return pymongo.MongoClient(
                host=host,
                port=port,
                replicaSet=replset_name,
                read_preference=pymongo.read_preferences.ReadPreference.PRIMARY,
                w=w)
    else:
        return pymongo.MongoClient(host, port, w=w)

def is_replica_set(hostportstr):
    # TODO
    return True

def get_replset_name(host, port):
    """ Get replica set name.
    """
    try:
        mc = pymongo.MongoClient(host, port)
        status = mc.admin.command({'replSetGetStatus': 1})
        mc.close()
        if status['ok'] == 1:
            return status['set']
    except pymongo.errors.OperationFailure as e:
        return ''

def get_primary(host, port):
    """ Get host, port, replsetName of the primary node.
    """
    mc = pymongo.MongoClient(host, port)
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
    raise Exception('no primary in replica set')

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

def replay_oplog(oplog, mc):
    """ Replay oplog.
    """
    pass

