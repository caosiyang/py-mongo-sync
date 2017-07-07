import sys
import logging
import logging.handlers

class Logger(object):
    """ Global logger.
    """
    @staticmethod
    def init(filepath):
        """ Init logger.
        """
        logger = logging.getLogger()
        logger.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
        if filepath:
            handler_log = logging.handlers.RotatingFileHandler(filepath, mode='a', maxBytes=1024*1024*100, backupCount=3)
            handler_log.setFormatter(formatter)
            handler_log.setLevel(logging.INFO)
            logger.addHandler(handler_log)
        else:
            handler_stdout = logging.StreamHandler(sys.stdout)
            handler_stdout.setFormatter(formatter)
            handler_stdout.setLevel(logging.INFO)
            logger.addHandler(handler_stdout)
    
    @staticmethod
    def get():
        """ Get logger.
        """
        return logging.getLogger()

