class Filter(object):
    def valid_database(self, dbname):
        return True

    def valid_collection(self, collname):
        return True

    def valid_index(self, ns):
        return True

    def valid_oplog(self, oplog):
        return True


class DatabaseFilter(Filter):
    """ Database filter.
    """
    def __init__(self):
        self._target_dbs = []

    def add_target_databases(self, dbname_list):
        self._target_dbs.extend([dbname.strip() for dbname in dbname_list])

    def valid_database(self, dbname):
        """ Validate database.
        """
        if self._target_dbs and dbname.strip() in self._target_dbs:
            return True
        return False

    def valid_index(self, ns):
        dbname = ns.strip().split('.', 1)[0]
        return dbname in self._target_dbs

    def valid_oplog(self, oplog):
        """ Validate oplog.
        """
        db = oplog['ns'].split('.', 1)[0]
        return self.valid_database(db)


class CollectionFilter(Filter):
    """ Collection filter.
    """
    def __init__(self):
        self._target_colls = []
        self._db_filter = DatabaseFilter()

    def add_target_collections(self, collname_list):
        collnames = [collname.strip() for collname in collname_list]
        self._target_colls.extend(collnames)
        dbset = set()
        for collname in collnames:
            dbset.add(collname.split('.', 1)[0])
        self._db_filter.add_target_databases(list(dbset))

    def valid_collection(self, ns):
        """ Validate collection.
        """
        if self._target_colls and ns.strip() in self._target_colls:
            return True
        return False

    def valid_index(self, ns):
        ns = ns.strip()
        return ns in self._target_colls

    def valid_oplog(self, oplog):
        """ Validate oplog.
        """
        op = oplog['op'] # 'n' or 'i' or 'u' or 'c' or 'd'
        ns = oplog['ns']
        db = oplog['ns'].split('.', 1)[0]
        if self._db_filter.valid_database(db):
            if op == 'c' or self.valid_collection(ns):
                return True
        return False


if __name__ == '__main__':
    # database filter test
    df = DatabaseFilter()
    df.add_target_databases(['db'])
    assert df.valid_database('db')
    assert df.valid_database('db1') == False
    assert df.valid_index('db.coll')
    assert df.valid_index('db1.coll') == False

    oplog1 = {'op': 'i', 'ns': 'db.coll'}
    oplog2 = {'op': 'u', 'ns': 'db.coll1'}
    oplog3 = {'op': 'd', 'ns': 'db1.coll'}
    oplog4 = {'op': 'd', 'ns': 'db1.coll1'}
    assert df.valid_oplog(oplog1)
    assert df.valid_oplog(oplog2)
    assert df.valid_oplog(oplog3) == False
    assert df.valid_oplog(oplog4) == False

    # collection filter test
    cf = CollectionFilter()
    cf.add_target_collections(['db.coll'])
    assert cf.valid_collection('db.coll')
    assert cf.valid_collection('db.coll1') == False
    assert cf.valid_collection('db1.coll') == False
    assert cf.valid_collection('db1.coll1') == False
    assert cf.valid_index('db.coll')
    assert cf.valid_index('db.coll1') == False
    assert cf.valid_index('db1.coll') == False
    assert cf.valid_index('db1.coll1') == False

    oplog4 = {'op': 'i', 'ns': 'db.coll'}
    oplog5 = {'op': 'u', 'ns': 'db.coll1'}
    oplog6 = {'op': 'd', 'ns': 'db1.coll'}
    oplog7 = {'op': 'c', 'ns': 'db.coll1'}
    assert cf.valid_oplog(oplog4)
    assert cf.valid_oplog(oplog5) == False
    assert cf.valid_oplog(oplog6) == False
    assert cf.valid_oplog(oplog7)

    print 'pass all tests'
