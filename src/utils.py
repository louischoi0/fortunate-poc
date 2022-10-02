import logging
import time

def get_logger(appname):
    logger = logging.getLogger(appname)
    logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s: [%(levelname)s] [%(name)s] %(message)s')
    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)
    return logger


def get_timestamp() :
    n = time.time()
    return str(n).replace(".","") 

def padding_msg(msg, tlen):
    cnt = tlen - len(msg)
    return msg + ( "-" * cnt ) 
