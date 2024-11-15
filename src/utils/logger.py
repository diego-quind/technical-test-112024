from functools import wraps
import logging
import time

import os
import logging
from datetime import datetime

class Logger:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Logger, cls).__new__(cls)
            cls._instance.logger = logging.getLogger("PipelineLogger")
            cls._instance.logger.setLevel(logging.INFO)
            handler = logging.StreamHandler()
            formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
            handler.setFormatter(formatter)
            cls._instance.logger.addHandler(handler)
            cls._instance.log_messages = []
        return cls._instance

    def log(self, message):
        self._instance.logger.info(message)
        self._instance.log_messages.append(message)

    def log_error(self, message):
        self._instance.logger.error(message)
        self._instance.log_messages.append(f"ERROR: {message}")

    def save_to_file(self, pipeline_id):
        log_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), '../logs'
            )
        os.makedirs(log_dir, exist_ok=True)

        log_file = os.path.join(log_dir, f"pipeline_{pipeline_id}.log")
        with open(log_file, 'w') as f:
            for message in self._instance.log_messages:
                f.write(f"{datetime.now()} - {message}\n")
        self._instance.log_messages.clear()


def execution_time_logger(task_func):
    @wraps(task_func)
    def wrapper(*args, **kwargs):
        logger = Logger()
        start_time = time.time()
        try:
            result = task_func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.log(f"Tarea {task_func.__name__} completada en {elapsed_time:.2f} segundos.")
            return result
        except Exception as e:
            end_time = time.time()
            elapsed_time = end_time - start_time
            logger.log_error(f"Tarea {task_func.__name__} falló tras {elapsed_time:.2f} segundos. Error: {str(e)}")
            raise
    return wrapper