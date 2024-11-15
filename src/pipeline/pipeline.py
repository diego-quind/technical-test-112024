import uuid
from src.utils.logger import Logger

class Pipeline:
    def __init__(self, name, tasks):
        self.pipeline_id = uuid.uuid4()
        self.name = name
        self.tasks = tasks

    def execute(self, execution):
        logger = Logger()
        logger.log(f"Iniciando ejecución de pipeline {self.pipeline_id} - {self.name}")
        try:
            data = None
            for task in self.tasks:
                data = task.run()
                execution.add_log(f"Tarea {task.name} completada.")
            execution.complete_execution()
            logger.log(f"Pipeline {self.pipeline_id} completado exitosamente.")
        except Exception as e:
            execution.fail_execution(str(e))
            logger.log_error(f"Pipeline {self.pipeline_id} falló. Error: {str(e)}")