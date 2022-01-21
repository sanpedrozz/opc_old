import os.path

from loguru import logger
from config import Config

logger.add(
    os.path.join(Config.PATH, 'logs', 'data', 'logs.log'),
    format='{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | {message}',
    level='DEBUG',
    rotation='1 MB',
    compression='zip',
    backtrace=True,
)
