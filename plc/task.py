from datetime import datetime
from typing import Dict


class Task:
    def __init__(self, task: Dict):
        self.dt_start = None
        self.dt_finish = datetime.now()
        self.__dict__.update(task)