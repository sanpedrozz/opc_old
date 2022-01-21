# from time import sleep
#
# import snap7
# import threading
#
# ip = "192.168.29.40"
# client = snap7.client.Client()
# client.connect(ip, 0, 1)
#
# sem = threading.Semaphore()
#
#
# class PLC(threading.Thread):
#     def __init__(self, db):
#         super().__init__()
#         self.db = db
#
#     def run(self) -> None:
#         while True:
#             sem.acquire()
#             data = client.db_read(self.db, 0, 1)
#             sem.release()
#             sleep(0.05)
#             print(f'{self.name}: {data}')
#
#
# if __name__ == '__main__':
#     x = PLC(1000)
#     y = PLC(1001)
#
#     x.start()
#     y.start()
from worker import Worker
from config import Config

w = Worker(Config.PLC_IP)
w.test()
