import pymongo


def is_replica_set(hostportstr):
    # TODO
    return True


def get_replset_name(hostportstr):
    """ Get replica set name.
    """
    try:
        host = hostportstr.split(':')[0]
        port = int(hostportstr.split(':')[1])
        mc = pymongo.MongoClient(host, port)
        status = mc.admin.command({'replSetGetStatus': 1})
        mc.close()
        if status['ok'] == 1:
            return status['set']
    except pymongo.errors.OperationFailure as e:
        return ''


def get_primary(hostportstr):
    """ Get host, port, replsetName of the primary node.
    """
    host = hostportstr.split(':')[0]
    port = int(hostportstr.split(':')[1])
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
