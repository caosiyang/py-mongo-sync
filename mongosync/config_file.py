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
                                    tml['src'].get('authdb', 'admin'),
                                    tml['src'].get('username', ''),
                                    tml['src'].get('password', ''))

        if type not in tml['dst'] or tml['dst']['type'] == 'mongo':
            conf.dst_conf = MongoConfig(tml['dst']['hosts'],
                                        tml['dst'].get('authdb', 'admin'),
                                        tml['dst'].get('username', ''),
                                        tml['dst'].get('password', ''))
        elif tml['dst']['type'] == 'es':
            conf.dst_conf = EsConfig(tml['dst']['hosts'])
        else:
            raise Exception('invalid dst.type')

        if 'sync' in tml and 'dbs' in tml['sync']:
            for dbentry in tml['sync']['dbs']:
                if 'db' not in dbentry:
                    raise Exception("'db' is missing in sync.dbs")
                if not dbentry['db']:
                    raise Exception("'db' is empty in sync.dbs")
                dbname = dbentry['db'].strip()
                rename_db = dbentry['rename_db'].strip() if 'rename_db' in dbentry else ""

                # update db map
                if dbname and rename_db:
                    if dbname in conf.dbmap:
                        raise Exception('duplicate dbname in sync.dbs: %s' % dbname)
                    conf.dbmap[dbname] = rename_db

                if 'colls' in dbentry and dbentry['colls']:
                    for collentry in dbentry['colls']:
                        if isinstance(collentry, str) or isinstance(collentry, unicode):
                            collname = collentry.strip()
                            ns = gen_namespace(dbname, collname)
                            conf.data_filter.add_include_coll(ns)
                        elif isinstance(collentry, dict):
                            if 'coll' not in collentry:
                                raise Exception("'coll' is missing in sync.dbs.colls")
                            if not collentry['coll']:
                                raise Exception("'coll' is empty in sync.dbs.colls")

                            collname = collentry['coll'].strip()
                            fields = frozenset([f.strip() for f in collentry['fields']] if 'fields' in collentry else [])

                            # update coll filter
                            ns = gen_namespace(dbname, collname)
                            conf.data_filter.add_include_coll(ns)

                            # update fields
                            if fields:
                                if ns in conf.fieldmap:
                                    raise Exception("duplicate collname in sync.dbs.colls: %s" % ns)
                                conf.fieldmap[ns] = fields
                        else:
                            raise Exception('invalid entry in sync.dbs.colls: %s' % collentry)
                else:
                    # update coll filter
                    conf.data_filter.add_include_coll(gen_namespace(dbname, '*'))

        if 'sync' in tml and 'start_optime' in tml['sync']:
            conf.start_optime = Timestamp(tml['sync']['start_optime'], 0)

        if 'log' in tml and 'filepath' in tml['log']:
            conf.logfilepath = tml['log']['filepath']

        return conf
