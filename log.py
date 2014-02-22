import sys
import logging
import logging.handlers

class MyFilter(logging.Filter):
    """Filter log records of which the level is less than logging.ERROR.
    """
    def filter(self, record):
        """
        @return
        zero - to be filterd
        nozero -  to be logged
        """
        if isinstance(record, logging.LogRecord) and record.levelno >= logging.ERROR:
            return 1
        return 0

formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')

stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setFormatter(formatter)
stdout_handler.setLevel(logging.WARNING)
rotating_file_handler = logging.handlers.RotatingFileHandler('log/info.log', mode='a', maxBytes=1024*1024*100, backupCount=10)
rotating_file_handler.setFormatter(formatter)
error_file_handler = logging.FileHandler('log/error.log', mode='a')
error_file_handler.setFormatter(formatter)
error_file_handler.setLevel(logging.WARNING)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
logger.addHandler(stdout_handler)
logger.addHandler(rotating_file_handler)
logger.addHandler(error_file_handler)

if __name__ == '__main__':
    logger.info('info')
    logger.warning('warning')
    logger.error('error')
