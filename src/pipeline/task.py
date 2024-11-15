from abc import ABC, abstractmethod
from src.utils.logger import execution_time_logger

class PipelineTask(ABC):
    def __init__(self, name):
        self.name = name

    @abstractmethod
    @execution_time_logger
    def run(self):
        pass