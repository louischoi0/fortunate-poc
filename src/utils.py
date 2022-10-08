import logging
import time


def get_logger(appname):
    logger = logging.getLogger(appname)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter("%(asctime)s: [%(levelname)s] [%(name)s] %(message)s")
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def get_timestamp():
    n = time.time()
    ts = str(n).replace(".", "")
    pad = 18 - len(ts)
    return ts + ("0" * pad)


def padding_msg(msg, tlen, pad="0"):
    msg = str(msg)
    cnt = tlen - len(msg)
    return msg + (pad * cnt)


def padding_msg_front(msg, tlen, pad="0"):
    msg = str(msg)
    cnt = tlen - len(msg)
    return (pad * cnt) + msg


def verify_msg_optype(msg, optype):
    msg = msg.decode("utf8")
    optype = msg[:6]

    assert optype == optype


class BufferCursor:
    def __init__(self, buffer):
        self.cursor = 0
        self.buffer = buffer

    def advance(self, length):
        msg = self.buffer[self.cursor : self.cursor + length]
        self.cursor += length
        return msg
