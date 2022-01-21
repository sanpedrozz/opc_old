from datetime import datetime
from time import sleep
from threading import Thread, Semaphore
from functools import wraps

from plc import PLC
from plc.variables import DB_TAGS
from queue_client.client import RedisClient
from logs.logger import logger
from config import Config

semaphore = Semaphore()


def thread_lock(func):
    @wraps(func)
    def wrapper(*args, **kwargs):

        semaphore.acquire()
        result = func(*args, **kwargs)
        semaphore.release()

        sleep(0.05)

        return result
    return wrapper


class Robot(Thread):
    def __init__(self, name: str, ip: str, db_tags: int):
        super().__init__()

        self.name = name
        self.plc = PLC(ip, db_tags)
        self.redis_client = RedisClient()
        self.current_task = None  # Task object

    @property
    def plc_status(self):
        decorated_func = thread_lock(self.plc.uiTaskStatus.get)
        return decorated_func()

    @plc_status.setter
    def plc_status(self, status):
        decorated_func = thread_lock(self.plc.uiTaskStatus.set)
        decorated_func(status)

    def change_zone(self, zone: int) -> int:
        zone_changed = 0
        if self.name[-1] == '1' and zone == 4 or self.name[-1] == '2' and zone == 6:
            zone_changed = 1
        elif self.name[-1] == '1' and zone == 1 or self.name[-1] == '2' and zone == 3:
            zone_changed = 2
        elif zone == 2:
            zone_changed = 3
        elif zone == 5:
            zone_changed = 4
        return zone_changed

    # def get_current_pid(self) -> bool:
    #     pid = self.redis_client.get_pid(self.name)
    #     if pid:
    #         self.current_pid = pid
    #         logger.info(f'{self.name} recieved PID: {pid}')
    #         return True
    #     else:
    #         self.current_pid = None
    #         sleep(1)
    #         return False

    def get_task(self):
        self.current_task = self.redis_client.get_task(self.name)

        if not self.current_task:
            logger.info(f'{self.name} no task')
            sleep(1)
            return

        logger.info(f'{self.name} received task {self.current_task.task}')

    def configure_task(self):
        self.current_task.zone_in = self.change_zone(self.current_task.zone_in)
        self.current_task.zone_out = self.change_zone(self.current_task.zone_out)
        self.current_task.dt_start = datetime.now()

        logger.info(f'{self.name} task configured')

    @thread_lock
    def set_task(self):
        thickness_list = [self.current_task.depth for _ in range(self.current_task.qty)]
        cover_list = [self.current_task.cover for _ in range(self.current_task.qty)]

        self.plc.uiZoneFrom.set(self.current_task.zone_in)
        self.plc.uiZoneTo.set(self.current_task.zone_out)
        self.plc.uiRequired.set(self.current_task.qty)
        self.plc.usiCover.set(cover_list)
        self.plc.usiThickness.set(thickness_list)

        logger.info(f'{self.name} task writed to PLC')

    @thread_lock
    def check_task(self):
        thickness_list = [self.current_task.depth for _ in range(self.current_task.qty)]
        cover_list = [self.current_task.cover for _ in range(self.current_task.qty)]

        try:
            assert self.current_task.zone_in == self.plc.uiZoneFrom.get()
            assert self.current_task.zone_out == self.plc.uiZoneTo.get()
            assert self.current_task.qty == self.plc.uiRequired.get()
            assert thickness_list == self.plc.usiThickness.get()[:self.current_task.qty]
            assert cover_list == self.plc.usiCover.get()[:self.current_task.qty]

        except AssertionError as error:
            logger.warning(f'{self.name}')
            logger.warning('!!!!!!!!!!!!\n!!!!!!!!!!\n TASK DOESNT SET PROPERLY \n!!!!!!!!!!!!\n!!!!!!!!!!\n')
            logger.warning('value | task | plc_data')
            logger.warning(f'zone_in | {self.current_task.zone_in} | {self.plc.uiZoneFrom.get()}')
            logger.warning(f'zone_out | {self.current_task.zone_out} | {self.plc.uiZoneTo.get()}')
            logger.warning(f'qty | {self.current_task.qty} | {self.plc.uiRequired.get()}')
            logger.warning(f'thickness | {thickness_list} | {self.plc.usiThickness.get()[:self.current_task.qty]}')
            logger.warning(f'cover | {cover_list} | {self.plc.usiCover.get()[:self.current_task.qty]}')
            return False

        return True

    def task_working(self):
        self.plc_status = 1

        logger.info(f'{self.name} status writed to PLC')

    def wait_until_finish(self):
        while self.plc_status not in {0, 3}:
            logger.info(f'{self.name} waiting for finish task')

            sleep(1)

    def finish_task(self):
        self.current_task.dt_finish = datetime.now()
        self.redis_client.add_db_queue_task(self.current_task)
        self.redis_client.remove_task(self.robot_name)
        self.current_task = None

        logger.info(f'{self.name} task finished')

    def run(self):
        while True:
            if self.current_task is None:
                self.get_task()
                if self.current_task is None:
                    continue

            if self.plc_status in {0, 3}:

                if self.current_task:
                    self.configure_task()

                    self.set_task()

                    while not self.check_task():
                        self.set_task()

                    self.task_working()

            if self.plc_status in {1, 2}:

                self.wait_until_finish()
                self.finish_task()


class Worker:
    def __init__(self, ip, name, robot_quantity):
        self.ip = ip
        self.name = name
        self.robot_quantity = robot_quantity
        self.robots = []

    def create_robots(self):
        for i in range(0, self.robot_quantity):

            robot = Robot(f'{self.name}{i + 1}', self.ip, DB_TAGS + i)
            self.robots.append(robot)

            logger.info(f'robot {self.name}{i + 1} created')

    def run(self):
        self.create_robots()

        for robot in self.robots:
            robot.start()

    def test(self):
        logger.info('OPC started')
        self.create_robots()

        while True:
            for robot in self.robots:
                robot.plc_status = 3

            sleep(15)
