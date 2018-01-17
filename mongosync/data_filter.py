from mongo_utils import parse_namespace, gen_namespace


class DataFilter(object):
    """ Filter for database and collection.
    """
    def __init__(self):
        self._include_colls = set()
        self._related_dbs = set()

    def add_include_coll(self, ns):
        self._include_colls.add(ns)
        self._related_dbs.add(ns.split('.', 1)[0])

    def add_include_colls(self, ns_list):
        for ns in ns_list:
            self.add_include_coll(ns)

    def valid_db(self, dbname):
        if not self._related_dbs:
            return True
        else:
            return dbname in self._related_dbs

    def valid_coll(self, dbname, collname):
        if not self._include_colls:
            return True
        else:
            if '%s.*' % dbname in self._include_colls:
                return True
            return gen_namespace(dbname, collname) in self._include_colls

    def valid_ns(self, ns):
        dbname, collname = parse_namespace(ns)
        return self.valid_coll(dbname, collname)

    def valid_index(self, dbname, collname):
        return self.valid_coll(dbname, collname)

    def valid_oplog(self, oplog):
        if not self._include_colls:
            return True
        op = oplog['op']
        ns = oplog['ns']
        if op == 'n':
            return False
        elif op == 'c':
            dbname, _ = parse_namespace(ns)
            return dbname in self._related_dbs
        else:
            return self.valid_ns(ns)

    @property
    def active(self):
        return True if self._include_colls else False

    @property
    def include_colls(self):
        return self._include_colls


# test case
if __name__ == '__main__':
    f = DataFilter()
    f.add_include_colls(['db0.*'])
    f.add_include_colls(['db1.coll'])
    assert f.valid_db('db0')
    assert f.valid_db('db1')
    assert f.valid_coll('db0', 'coll')
    assert f.valid_coll('dbx', 'coll') is False
    assert f.valid_coll('db1', 'coll')
    assert f.valid_coll('db1', 'collx') is False
    assert f.valid_ns('db0.coll')
    assert f.valid_ns('dbx.coll') is False
    assert f.valid_ns('db1.coll')
    assert f.valid_ns('db1.collx') is False
    assert f.valid_index('db0', 'coll')
    assert f.valid_index('dbx', 'coll') is False
    assert f.valid_index('db1', 'coll')
    assert f.valid_index('db1', 'collx') is False

    oplog4 = {'op': 'i', 'ns': 'db0.coll'}
    oplog5 = {'op': 'u', 'ns': 'db1.coll'}
    oplog6 = {'op': 'd', 'ns': 'db1.collx'}
    oplog7 = {'op': 'c', 'ns': 'db0.$cmd'}
    oplog8 = {'op': 'c', 'ns': 'db1.$cmd'}
    oplog9 = {'op': 'c', 'ns': 'dbx.$cmd'}
    assert f.valid_oplog(oplog4)
    assert f.valid_oplog(oplog5)
    assert f.valid_oplog(oplog6) is False
    assert f.valid_oplog(oplog7)
    assert f.valid_oplog(oplog8)
    assert f.valid_oplog(oplog9) is False

    print 'test cases all pass'
