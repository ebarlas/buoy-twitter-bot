import logging
import time


def init_logger(prefix):
    formatter = logging.Formatter('[%(asctime)s] <%(threadName)s> %(levelname)s - %(message)s')

    file_name = f'{prefix}{time.strftime("%Y%m%d-%H%M%S")}.log'
    handler = logging.FileHandler(file_name)
    handler.setFormatter(formatter)

    log = logging.getLogger()
    log.setLevel(logging.INFO)
    log.addHandler(handler)

    for name in ['buoy.lib.batchput', 'buoy.lib.dynamo', 'buoy.lib.noaa']:
        log = logging.getLogger(name)
        log.setLevel(logging.DEBUG)