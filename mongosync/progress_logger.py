import sys
import time
import multiprocessing
import threading
import Queue
from mongosync.logger import Logger

log = Logger.get()


class Message(object):
    """ Progress change message.
    """
    def __init__(self, ns, cnt, done):
        self.ns = ns
        self.cnt = cnt
        self.done = done


class Progress(object):
    """ Progress attibutes.
    """
    def __init__(self, ns, total):
        self.ns = ns
        self.curr = 0
        self.total = total
        self.start_time = time.time()
        self.done = False


class LoggerThread(threading.Thread):
    """ Logger thread.
    """
    def __init__(self, n_colls, **kwargs):
        self._n_colls = n_colls
        self._q = Queue.Queue()
        self._ns_map = {}
        super(LoggerThread, self).__init__(**kwargs)

    def run(self):
        n_colls_done = 0
        while n_colls_done < self._n_colls:
            m = self._q.get()
            if m.ns not in self._ns_map:
                raise Exception('missing namespace: %s' % m.ns)
            self._ns_map[m.ns].curr += m.cnt
            prog = self._ns_map[m.ns]
            s = '\t%s\t%d/%d\t[%.2f%%]' % (
                    prog.ns,
                    prog.curr,
                    prog.total,
                    float(prog.curr)/prog.total*100 if prog.total > 0 else float(prog.curr+1)/(prog.total+1)*100)

            if not m.done:
                log.info(s)
            else:
                log.info('[ OK ] ' + s)
                n_colls_done += 1
                time_used = time.time() - prog.start_time
                sys.stdout.write('\r\33[K')
                sys.stdout.write('\r[\033[32m OK \033[0m]\t[%d/%d]\t%s\t%d/%d\t%.1fs\n' % (n_colls_done, self._n_colls, m.ns, prog.curr, prog.total, time_used))
                sys.stdout.flush()
                del self._ns_map[m.ns]

            # s = ''
            # for ns, prog in self._ns_map.iteritems():
            #     s += '|| %s  %d/%d  %.1f%% ' % (ns, prog.curr, prog.total, float(prog.curr)/prog.total*100)
            # if len(s) > 0:
            #     s += '||'
            #     sys.stdout.write('\r%s' % s)
            #     sys.stdout.flush()

        log.info('ProgressLogger thread %s exit' % threading.currentThread().name)

    def register(self, ns, total):
        """ Register collection.
        """
        if ns in self._ns_map:
            raise Exception('duplicate collection %s' % ns)
        self._ns_map[ns] = Progress(ns, total)

    def add(self, ns, count, done=False):
        """ Update progress.
        """
        self._q.put(Message(ns, count, done))


class LoggerProcess(multiprocessing.Process):
    """ Logger progress.
    """
    def __init__(self, n_colls, **kwargs):
        self._n_colls = n_colls
        self._q = multiprocessing.Queue()
        self._ns_map = multiprocessing.Manager().dict()
        super(LoggerProcess, self).__init__(**kwargs)

    def run(self):
        n_colls_done = 0
        while n_colls_done < self._n_colls:
            m = self._q.get()
            if m.ns not in self._ns_map:
                raise Exception('missing namespace: %s' % m.ns)
            self._ns_map[m.ns].curr += m.cnt
            prog = self._ns_map[m.ns]
            s = '\t%s\t%d/%d\t[%.2f%%]' % (
                    prog.ns,
                    prog.curr,
                    prog.total,
                    float(prog.curr)/prog.total*100 if prog.total > 0 else float(prog.curr+1)/(prog.total+1)*100)

            if not m.done:
                log.info(s)
            else:
                log.info('[ OK ] ' + s)
                n_colls_done += 1
                time_used = time.time() - prog.start_time
                sys.stdout.write('\r\33[K')
                sys.stdout.write('\r[\033[32m OK \033[0m]\t[%d/%d]\t%s\t%d/%d\t%.1fs\n' % (n_colls_done, self._n_colls, m.ns, prog.curr, prog.total, time_used))
                sys.stdout.flush()
                del self._ns_map[m.ns]

            # s = ''
            # for ns, prog in self._ns_map.iteritems():
            #     s += '|| %s  %d/%d  %.1f%% ' % (ns, prog.curr, prog.total, float(prog.curr)/prog.total*100)
            # if len(s) > 0:
            #     s += '||'
            #     sys.stdout.write('\r%s' % s)
            #     sys.stdout.flush()

        log.info('ProgressLogger process %s exit' % multiprocessing.current_process().name)

    def register(self, ns, total):
        """ Register collection.
        """
        if ns in self._ns_map:
            raise Exception('duplicate collection %s' % ns)
        self._ns_map[ns] = Progress(ns, total)

    def add(self, ns, count, done=False):
        """ Update progress.
        """
        self._q.put(Message(ns, count, done))
