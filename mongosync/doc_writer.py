import time
import logging
from multiprocessing import Pool
from mongo_helper import mongo_connect

global g_mc
global g_logger

def initializer(host, port, dbname, collname):
    """ Subprocess initial function.
    """
    global g_mc, g_logger
    try:
        g_mc = mongo_connect(host, port, w=1)
        g_logger = logging.getLogger()
    except Exception as e:
        g_logger.error(e)

def write_doc(dbname, collname, doc):
    """ Write document.
    """
    global g_mc, g_logger
    try:
        g_mc[dbname][collname].save(doc)
    except Exception as e:
        g_logger.error(e)

class DocWriter(object):
    """ Document writer.
    """
    def __init__(self, host, port, dbname, collname, nprocs=4):
        """ Create a process pool.
        """
        self.dbname = dbname
        self.collname = collname
        self.__pool = Pool(nprocs, initializer, (host, port, dbname, collname))

    def close(self):
        """ Wait for all documents to be written.
        """
        self.__pool.close()
        self.__pool.join()

    def write(self, doc):
        """ Async write document.
        """
        self.__pool.apply_async(write_doc, (self.dbname, self.collname, doc))

# for test
if __name__ == '__main__':
    start = time.time()
    dw = DocWriter('10.15.188.223', 27017, 'test', 'coll', 10)
    batch = False
    for i in range(0, 20000):
        if batch:
            docs = []
            for i in range(0, 100):
                docs.append({'name': 'hello_world', 'age': 29, 'city': 'Beijing'})
            dw.write(docs)
        else:
            dw.write({'name': 'hello_world', 'age': 29, 'city': 'Beijing'})
    dw.close()
    end = time.time()
    print 'use', end-start

