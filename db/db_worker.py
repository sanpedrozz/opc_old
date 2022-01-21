from pprint import pprint
from time import sleep

from db.sql import DBConnector
from queue_client.client import RedisClient
from plc.task import Task
from config import Config
from logs.logger import logger


class DBWorker:
    def __init__(self, plcs):
        self.connection = DBConnector(db_name=Config.DB_NAME,
                                      user=Config.DB_USER,
                                      psw=Config.DB_PASSWORD,
                                      host=Config.DB_HOST,
                                      port=Config.DB_PORT)
        self.queue_client = RedisClient()
        self.plcs = plcs

    def get_task_to_insert(self) -> Task:
        return self.queue_client.get_db_queue_task()

    def remove_task(self):
        self.queue_client.remove_db_queue_task()

    def get_tasks_from_db(self):
        """test function"""
        for plc in self.plcs:
            for i in range(plc['robot_quantity']):
                robot = f'{plc["name"]}{i + 1}'
                query = "SELECT task, pid, i, device, zone_out, zone_in, qty, cover, depth, status, dcreate " \
                        f"FROM tasks WHERE device = '{robot}' AND status = 0 ORDER BY i"
                tasks = self.connection.fetchall_query(query)
                tasks_to_queue = [Task(task) for task in tasks]
                pids = {task.pid for task in tasks_to_queue}
                for pid in pids:
                    pid_tasks = [task for task in tasks_to_queue if task.pid == pid]
                    pid_tasks.sort(key=lambda task: task.i)

                    self.queue_client.add_pid_to_queue(robot, pid)
                    self.queue_client.add_tasks(pid, pid_tasks)

                    query = f"UPDATE tasks SET status = 1 WHERE pid = '{pid}'"

                    self.connection.execute_query(query)
                    self.connection.connect.commit()

                pprint(tasks)

    def get_tasks_to_update(self):
        pass

    def update_task(self, task):
        query = f"UPDATE tasks SET status = 3 WHERE task = '{task.task}'"
        self.connection.execute_query(query)

    def insert_task(self, task):
        query = f"INSERT INTO task (task, pid, i, device, cell, zone_in, " \
                f"zone_out, quantity, dcreate, cover, employee, depth, decore, dstart, dfinish) VALUES " \
                f"('{task.task}', '{task.pid}', {task.i}, '{task.device}', {task.cell}, {task.zone_in}, " \
                f"{task.zone_out}, {task.qty}, '{task.dcreate}', {task.cover}, '{task.employee}', {task.depth}, " \
                f"'{task.decore}', '{task.dt_start}', '{task.dt_finish}');"

        self.connection.insert_query(query)

        logger.info(f'{task.task} inserted into DB')

    def run(self):
        while True:
            try:
                # task = self.get_task_to_insert()
                # if task is None:
                #     sleep(5)
                #     continue
                # self.insert_task(task)
                # self.remove_task()
                self.get_tasks_from_db()
                sleep(100500)

            except Exception as error:
                logger.warning(f'DB worker error {error}')
                self.connection.connect.rollback()
                sleep(15)
