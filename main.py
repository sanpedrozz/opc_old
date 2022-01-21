import multiprocessing as mp

from aiohttp import web

from server.server import app
from logs.logger import logger
from worker import Worker
from db.db_worker import DBWorker
from config import Config

print(1)
if __name__ == '__main__':
    try:
        server = mp.Process(target=web.run_app, args=(app, ), kwargs={'port': 8081})
        server.start()
        logger.info("Web server started")

        for plc in Config.PLCS:
            worker = Worker(**plc)
            worker_process = mp.Process(target=worker.run)
            worker_process.start()

        db_worker = DBWorker(Config.PLCS)
        # db_worker.run()
        db_worker_process = mp.Process(target=db_worker.run)
        db_worker_process.start()

    except Exception as error:
        logger.error(error)
