# use Python for config, it may be improved later


# hostportstr format:
#   HOST:PORT
# support:
#   a.single source
#   b.multiple source


class Source:
    """ Source config.
    """
    # source
    # source should be one or more mongod hostportstr of the different replica sets
    # set username and password empty if without authentication
    hostportstr = []

    # specified collections to sync
    # default is empty, indicates sync all data
    # if not empty, sync the specified collections only
    collections = []

    # without creating indexes, default is False
    # if True, ingore creating indexes
    ignore_indexes = False


class Destination:
    """ Destination config.
    """
    # set a mongos or a standalone mongod or a mongod instance of a replica set with the format like HOST:PORT
    # set username and password empty if without authentication
    hostportstr = ''
    username = ''
    password = ''
