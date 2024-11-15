from src.pipeline.task import PipelineTask
from src.utils.logger import Logger, execution_time_logger
from src.utils.config import ConfigLoader
from pyspark.sql import DataFrame

class LoadParquetTask(PipelineTask):
    def __init__(self, name, data: DataFrame):
        super().__init__(name)
        self.data = data

    @execution_time_logger
    def run(self):
        config = ConfigLoader()
        output_path = config.get("output_path")
        logger = Logger()
        logger.log(f"Guardando datos en {output_path} en formato Parquet")
        self.data.write.mode("overwrite").parquet(output_path)