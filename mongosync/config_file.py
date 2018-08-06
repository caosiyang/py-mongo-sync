import toml
from bson.timestamp import Timestamp
from mongosync.config import Config, MongoConfig, EsConfig
from mongosync.mongo_utils import gen_namespace


class ConfigFile(object):
    @staticmethod
    def load(filepath):
        """ Load config file and generate conf.
        """
        conf = Config()
        tml = toml.load(filepath)
        conf.src_conf = MongoConfig(tml['src']['hosts'],
                                    authdb=tml['src'].get('authdb', 'admin'),  # default authdb is 'admin'
                                    username=tml['src'].get('username', ''),
                                    password=tml['src'].get('password', ''))

        if tml['dst']['type'] == 'mongo':
            conf.dst_conf = MongoConfig(tml['dst']['mongo']['hosts'],
                                        authdb=tml['dst']['mongo'].get('authdb', 'admin'),  # default authdb is 'admin'
                                        username=tml['dst']['mongo'].get('username', ''),
                                        password=tml['dst']['mongo'].get('password', ''))
        elif tml['dst']['type'] == 'es':
            conf.dst_conf = EsConfig(tml['dst']['es']['hosts'])

        if 'sync' in tml and 'dbs' in tml['sync']:
            for dbentry in tml['sync']['dbs']:
                if 'db' not in dbentry:
                    raise Exception("required option 'db' is missing")
                if not dbentry['db']:
                    raise Exception("required option 'db' is empty")
                dbname = dbentry['db'].strip()
                rename_db = dbentry['rename_db'].strip() if 'rename_db' in dbentry else ""

                # update db map
                if dbname and rename_db:
                    if dbname in conf.dbmap:
                        raise Exception("conflict dbname in 'sync.dbs': %s" % dbname)
                    conf.dbmap[dbname] = rename_db

                if 'colls' in dbentry and dbentry['colls']:
                    for collentry in dbentry['colls']:
                        if 'coll' not in collentry:
                            raise Exception("required option 'coll' is missing")
                        if not collentry['coll']:
                            raise Exception("required option 'coll' is empty")

                        collname = collentry['coll'].strip()
                        fields = frozenset([f.strip() for f in collentry['fields']] if 'fields' in collentry else [])

                        if collname == '*' and fields:
                            raise Exception("'fileds' should be empty if 'coll' is '*'")

                        if collname == '*':
                            # update coll filter
                            conf.data_filter.add_include_coll(gen_namespace(dbname, '*'))
                        else:
                            # update coll filter
                            ns = gen_namespace(dbname, collname)
                            conf.data_filter.add_include_coll(ns)

                            # update fields
                            if fields:
                                if ns in conf.fieldmap:
                                    raise Exception("conflict namespace in 'sync.colls': %s" % ns)
                                conf.fieldmap[ns] = fields
                else:
                    # update coll filter
                    conf.data_filter.add_include_coll(gen_namespace(dbname, '*'))

        if 'sync' in tml and 'start_optime' in tml['sync']:
            conf.start_optime = Timestamp(tml['sync']['start_optime'], 0)

        if 'log' in tml and 'filepath' in tml['log']:
            conf.logfilepath = tml['log']['filepath']

        return conf
