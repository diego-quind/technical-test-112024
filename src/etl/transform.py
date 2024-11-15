from src.pipeline.task import PipelineTask
from src.utils.logger import Logger, execution_time_logger
from pyspark.sql.functions import col, when
from pyspark.sql import DataFrame

class DataCleanTask(PipelineTask):
    def __init__(self, name, data: DataFrame):
        super().__init__(name)
        self.data = data

    @execution_time_logger
    def run(self):
        logger = Logger()
        logger.log("Iniciando limpieza de datos")
        self.data = self.data.withColumn("age", when(col("age") < 0, None).otherwise(col("age"))) \
                             .dropna(subset=["name", "age"]) \
                             .filter(col("salary") > 0)
        return self.data
