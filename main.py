from pyspark.sql import SparkSession
from src.etl.extract import ExtractCSVTask
from src.etl.transform import DataCleanTask
from src.etl.load import LoadParquetTask
from src.pipeline.pipeline import Pipeline
from src.pipeline.manager import PipelineManager

spark = SparkSession.builder.appName("DataPipelineManager").getOrCreate()

load_task = ExtractCSVTask("Carga de CSV", spark)
clean_task = DataCleanTask("Limpieza de Datos", load_task.run())
save_task = LoadParquetTask("Guardar Parquet", clean_task.run())
    
pipeline = Pipeline("Pipeline de ETL", [load_task, clean_task, save_task])
manager = PipelineManager()
manager.add_pipeline(pipeline)

execution = manager.register_execution(pipeline.pipeline_id)
pipeline.execute(execution)

history = manager.get_execution_history(pipeline.pipeline_id)
for exec in history:
    print(f"Historial de Ejecución {exec.execution_id}: Estado {exec.state}")

spark.stop()
