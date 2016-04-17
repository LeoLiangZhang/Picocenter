import sys
import logging, datetime

LOGGER_FORMAT = '%(asctime)s - %(levelname)s - %(message)s'
LOGGER_DATEFMT= '%Y-%m-%d %H:%M:%S.%f'

class MicrosecondFormatter(logging.Formatter):
    converter=datetime.datetime.fromtimestamp
    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt)
        else:
            t = ct.strftime("%Y-%m-%d %H:%M:%S")
            s = "%s,%06d(%f)" % (t, ct.microsecond, record.created)
        return s

def get_logger(name=None, stream=sys.stderr):
    """ Return a well configured logger that outputs to stderr.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    formatter = MicrosecondFormatter(LOGGER_FORMAT, datefmt=LOGGER_DATEFMT)
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)
    logger.addHandler(handler)
    return logger

def time_diff(t1, t2):
    """ Return time difference of t2-t1 in milliseconds.
    """
    delta = t2-t1
    return ((delta.seconds * 1000) + (delta.microseconds / 1000))
