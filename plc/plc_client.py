from functools import wraps
from time import sleep
import traceback

import snap7
from snap7.exceptions import Snap7Exception

from logs.logger import logger


def singleton(cls):
    instances = {}

    @wraps(cls)
    def wrapper(*args, **kwargs):
        if cls not in instances:
          instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper


def reconnect_on_fail(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):
        while True:
            try:
                if self.connected:
                    return func(self, *args, **kwargs)

                self.connect()

            except Snap7Exception as error:
                logger.warning(f'PLC error: {error}:\n\n {traceback.format_exc()}\n')
                self.disconnect()
                sleep(5)

    return wrapper


@singleton
class PLCClient:
    def __init__(self, ip: str):
        self.ip = ip
        self.client = snap7.client.Client()
        self.connect()

    @property
    def connected(self):
        return self.client.get_connected()

    def connect(self):
        return self.client.connect(self.ip, 0, 1)

    def disconnect(self):
        return self.client.disconnect()

    @reconnect_on_fail
    def read_data(self, db_number: int, offset: int, size: int):
        return self.client.db_read(db_number, offset, size)

    @reconnect_on_fail
    def write_data(self, db_number: int, start: int, data: bytearray):
        result = self.client.db_write(db_number, start, data)
        return result
