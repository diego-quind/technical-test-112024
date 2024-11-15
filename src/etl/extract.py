from src.pipeline.task import PipelineTask
from src.utils.logger import Logger, execution_time_logger
from src.utils.config import ConfigLoader
from pyspark.sql import SparkSession

class ExtractCSVTask(PipelineTask):
    def __init__(self, name, spark: SparkSession):
        super().__init__(name)
        self.spark = spark
        self.data = None

    @execution_time_logger
    def run(self):
        config = ConfigLoader()
        input_path = config.get("input_path")
        logger = Logger()
        logger.log(f"Cargando datos desde {input_path}")
        self.data = self.spark.read.csv(input_path, header=True, inferSchema=True)
        return self.data