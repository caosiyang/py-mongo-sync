import time
import elasticsearch
import elasticsearch.helpers
from mongosync.config import EsConfig
from mongosync.logger import Logger

log = Logger.get()


class EsHandler(object):
    def __init__(self, conf):
        if not isinstance(conf, EsConfig):
            raise Exception('expect EsConfig')
        self._conf = conf
        self._es = None

    def __del__(self):
        self.close()

    def connect(self):
        self._es = elasticsearch.Elasticsearch(self._conf.hosts, timeout=600)
        return self._es.ping()

    def reconnect(self):
        while True:
            res = self.connect()
            if not res:
                time.sleep(1)
                continue
            return

    def close(self):
        self._es = None

    def client(self):
        return self._es

    def bulk_write(self, actions):
        try:
            elasticsearch.helpers.bulk(client=self._es, actions=actions)
        except Exception as e:
            log.error('bulk write failed: %s' % e)
