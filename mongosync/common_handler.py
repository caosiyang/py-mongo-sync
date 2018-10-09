class CommonHandler(object):
    """ Common database handler.

    Specific database handler implements the following interfaces.
    """
    def client(self):
        raise Exception('you should implement %s.%s' % (self.__class__.__name_, self.client.__name__))

    def connect(self):
        raise Exception('you should implement %s.%s' % (self.__class__.__name_, self.connect.__name__))

    def reconnect(self):
        raise Exception('you should implement %s.%s' % (self.__class__.__name_, self.reconnect.__name__))

    def close(self):
        raise Exception('you should implement %s.%s' % (self.__class__.__name_, self.close.__name__))

    def bulk_write(self):
        raise Exception('you should implement %s.%s' % (self.__class__.__name_, self.bulk_write.__name__))

    def replay_oplog(self):
        raise Exception('you should implement %s.%s' % (self.__class__.__name_, self.replay_oplog.__name__))
