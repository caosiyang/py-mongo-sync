# use python for config now, it may be improved later


# hostportstr format:
#   HOST:PORT
# support:
#   a.from single source to destination: src should be a string of a mongod uri in replica set


class Source:
    """ Source config.
    """
    # single source
    # set username and password empty if without authentication
    hostportstr = ''
    username = ''
    password = ''


class Destination:
    """ Destination config.
    """
    # set a mongos or a standalone mongod or a mongod instance of a replica set with the format like HOST:PORT
    # set username and password empty if without authentication
    hostportstr = ''
    username = ''
    password = ''
