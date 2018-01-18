import os
import struct
from bson.timestamp import Timestamp


class OptimeLogger(object):
    """ Record optime in file.
    """
    def __init__(self, filepath):
        assert filepath
        assert isinstance(filepath, str) or isinstance(filepath, unicode)
        if not os.path.exists(filepath):
            os.mknod(filepath)
        assert os.path.isfile(filepath)
        self._filepath = filepath
        self._fd = open(filepath, 'rb+')

    def __del__(self):
        self._fd.close()

    def write(self, optime):
        """ Write optime.
        """
        self._fd.seek(0, os.SEEK_SET)
        time_data = struct.pack('I', optime.time)
        inc_data = struct.pack('I', optime.inc)
        self._fd.write(time_data)
        self._fd.write(inc_data)
        self._fd.flush()

    def read(self):
        """ Read optime.
        Return optime if OK else None.
        """
        if self.filesize != 8:
            return None
        self._fd.seek(0, os.SEEK_SET)
        time = struct.unpack('I', self._fd.read(4))[0]
        inc = struct.unpack('I', self._fd.read(4))[0]
        return Timestamp(time, inc)

    @property
    def filesize(self):
        """ Return the length of file.
        """
        self._fd.seek(0, os.SEEK_END)
        return self._fd.tell()

    @property
    def filepath(self):
        """ Return filepath.
        """
        return self._filepath

if __name__ == '__main__':
    optime_logger = OptimeLogger('optimelog.tmp.0')
    optime_logger.write(Timestamp(0, 1))
    optime = optime_logger.read()
    assert optime is not None
    assert optime.time == 0
    assert optime.inc == 1
    assert optime_logger.filesize == 8

    optime_logger = OptimeLogger('optimelog.tmp.1')
    optime_logger.write(Timestamp(4294967295, 2))
    optime_logger = OptimeLogger('optimelog.tmp.1')
    optime = optime_logger.read()
    assert optime is not None
    assert optime.time == 4294967295
    assert optime.inc == 2
    assert optime_logger.filesize == 8

    optime_logger = OptimeLogger('optimelog.tmp.emtpy')
    optime = optime_logger.read()
    assert optime is None
    assert optime_logger.filesize == 0
    print 'test pass'
