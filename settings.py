# use python for config now, it may be improved later


# hostportstr format:
#   HOST:PORT
# support:
#   a.single source
#   b.multiple source


class Source:
    """ Source config.
    """
    # single source
    # source should be a mongod hostportstr of the replica set
    # set username and password empty if without authentication
    hostportstr = ''
    username = ''
    password = ''

    # multiple source
    # source should be a list in which the elements are mongod uris of different replica set
    # set 'multiple' True and add mongod hostportstr of different replica sets into hostportstr_list
    # not support authentication
    multiple = False
    hostportstr_list = []


class Destination:
    """ Destination config.
    """
    # set a mongos or a standalone mongod or a mongod instance of a replica set with the format like HOST:PORT
    # set username and password empty if without authentication
    hostportstr = ''
    username = ''
    password = ''
