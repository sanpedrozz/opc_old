import pickle
from functools import wraps
from typing import List

from redis.client import Redis
from plc.task import Task
from queue_client.redis_variables import SQL_INSERT


def singleton(cls):
    instances = {}

    @wraps(cls)
    def wrapper(*args, **kwargs):
        if cls not in instances:
            instances[cls] = cls(*args, **kwargs)
        return instances[cls]

    return wrapper


@singleton
class RedisClient:
    def __init__(self):
        self.client = Redis()

    def get_task(self, robot_name: str):
        task = self.client.lrange(robot_name, 0, 1)
        return pickle.loads(task[0]) if task else None

    def remove_task(self, robot_name: str):
        return self.client.lpop(robot_name)

    def set_status(self, robot_name: str, status: int):
        return self.client.set(f'{robot_name}_status', status)

    def get_status(self, robot_name: str) -> int:
        status = self.client.get(f'{robot_name}_status')
        return int(status) if status is not None else 0

    def add_tasks(self, pid: str, tasks: List):
        tasks = [pickle.dumps(task) for task in tasks]
        return self.client.rpush(pid, *tasks)

    def add_db_queue_task(self, task: Task):
        self.client.rpush(SQL_INSERT, pickle.dumps(task))

    def get_db_queue_task(self) -> Task:
        task = self.client.lrange(SQL_INSERT, 0, 1)
        if task:
            task = pickle.loads(task[0])
            return task

    def remove_db_queue_task(self) -> None:
        self.client.lpop(SQL_INSERT)
