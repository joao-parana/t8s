import logging
from logging import Logger
from typing import Any

from t8s.constants import PROJECT_ROOT


class LogConfigMeta(type):
    """
    The Singleton class can be implemented in different ways in Python. Some possible methods include:
    base class, decorator, metaclass. We will use the metaclass because it is best suited for this purpose.
    """

    _instances = {}

    def __call__(cls, *args, **kwargs):
        """
        Possible changes to the value of the `__init__` argument do not affect
        the returned instance.
        """
        if cls not in cls._instances:
            instance = super().__call__(*args, **kwargs)
            cls._instances[cls] = instance
        return cls._instances[cls]


class LogConfig(metaclass=LogConfigMeta):
    def __init__(self) -> None:
        self.logger = None
        self.initialize_logger(level=logging.INFO)

    def initialize_logger(
        self, level=logging.INFO, log_file: str = f'{PROJECT_ROOT}/logs/timeseries.log'
    ):
        my_global_logger = logging.getLogger(__name__)
        my_global_logger.setLevel(level)
        # logger_handler = RotatingFileHandler('timeseries.log', maxBytes=1_000_000, backupCount=10)
        logger_handler = logging.FileHandler(log_file, mode='w')
        # logger_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(filename)s - %(funcName)s | %(message)s")) #"%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s(%(lineno)d) - %(message)s"
        logger_handler.setFormatter(
            logging.Formatter(
                "%(levelname)s - %(filename)s - %(funcName)s | %(message)s"
            )
        )  # "%(asctime)s - %(name)s - %(levelname)s - %(filename)s:%(funcName)s(%(lineno)d) - %(message)s"
        my_global_logger.handlers.clear()
        my_global_logger.addHandler(logger_handler)
        self.logger = my_global_logger
        self.logger.info('self.logger = ' + str(self.logger))

    def get_logger(self) -> Logger | Any:
        return self.logger
